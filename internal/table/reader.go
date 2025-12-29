// Package table provides SST file reading and writing functionality.
// This implements the RocksDB block-based table format (format_version 0-7).
//
// SST File Layout:
//
//	[data block 1]
//	[data block 2]
//	...
//	[data block N]
//	[meta block 1: filter block]     (optional)
//	[meta block 2: index block]
//	[meta block 3: compression dict] (optional)
//	[meta block 4: range del block]  (optional)
//	[meta block 5: properties block]
//	[metaindex block]
//	[Footer]                         (fixed size, at end of file)
//
// Reference: RocksDB v10.7.5
//   - table/block_based/block_based_table_reader.h
//   - table/format.h
//   - table/format.cc
package table

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/encoding"
	"github.com/aalhour/rockyardkv/internal/filter"
	"github.com/aalhour/rockyardkv/internal/rangedel"
)

var (
	// ErrInvalidSST indicates the file is not a valid SST file.
	ErrInvalidSST = errors.New("table: invalid SST file")

	// ErrUnsupportedVersion indicates the format version is not supported.
	ErrUnsupportedVersion = errors.New("table: unsupported format version")

	// ErrChecksumMismatch indicates a block checksum verification failed.
	ErrChecksumMismatch = errors.New("table: checksum mismatch")

	// ErrBlockNotFound indicates a requested block was not found.
	ErrBlockNotFound = errors.New("table: block not found")

	// ErrUnsupportedPartitionedIndex indicates the SST uses partitioned index which is not supported.
	// Partitioned index splits the index across multiple blocks; our reader treats the index
	// as a single block and would produce incorrect results.
	ErrUnsupportedPartitionedIndex = errors.New("table: partitioned index not supported")
)

// ReadableFile is an interface for reading from an SST file.
type ReadableFile interface {
	io.Closer

	// ReadAt reads len(p) bytes from the file starting at offset.
	ReadAt(p []byte, off int64) (n int, err error)

	// Size returns the total size of the file.
	Size() int64
}

// ReaderOptions controls the behavior of the table reader.
type ReaderOptions struct {
	// VerifyChecksums enables checksum verification for all blocks.
	VerifyChecksums bool

	// CacheBlocks enables caching of data blocks.
	// (Not implemented yet - for future block cache integration)
	CacheBlocks bool
}

// Reader reads an SST file in the block-based table format.
type Reader struct {
	file    ReadableFile
	size    int64
	options ReaderOptions

	// Parsed from footer
	footer *block.Footer

	// Block handles from metaindex
	indexHandle      block.Handle
	propertiesHandle block.Handle
	filterHandle     block.Handle
	rangeDelHandle   block.Handle

	// Cached blocks (loaded on Open)
	indexBlock *block.Block
	properties *TableProperties

	// Bloom filter reader (optional, nil if no filter)
	filterReader *filter.BloomFilterReader

	// Index format detection: true if index uses value_delta_encoding (C++ RocksDB format)
	// false if index uses standard block format (Go-generated SSTs)
	indexUsesValueDeltaEncoding bool
}

// Open opens an SST file for reading.
func Open(file ReadableFile, opts ReaderOptions) (*Reader, error) {
	size := file.Size()
	if size < int64(block.MinEncodedLength) {
		return nil, ErrInvalidSST
	}

	r := &Reader{
		file:    file,
		size:    size,
		options: opts,
	}

	// Read and parse footer
	if err := r.readFooter(); err != nil {
		return nil, err
	}

	// Read metaindex block to find other meta blocks
	if err := r.readMetaindex(); err != nil {
		return nil, err
	}

	// Check for unsupported index types (partitioned/hash) before reading index
	// This prevents reading corruption from misinterpreting the index format.
	if err := r.checkUnsupportedFeatures(); err != nil {
		return nil, err
	}

	// Read index block (for format_version < 6 it's in footer,
	// for format_version >= 6 it's in metaindex)
	if err := r.readIndex(); err != nil {
		return nil, err
	}

	// Read filter block if present
	if err := r.readFilter(); err != nil {
		// Filter reading failure is not fatal - just means we won't use the filter
		r.filterReader = nil
	}

	return r, nil
}

// readFooter reads and parses the footer from the end of the file.
func (r *Reader) readFooter() error {
	// Footer is at the end of the file
	footerSize := block.MaxEncodedFooterLength
	if r.size < int64(footerSize) {
		footerSize = int(r.size)
	}

	buf := make([]byte, footerSize)
	offset := r.size - int64(footerSize)
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return err
	}

	// Decode footer
	footer, err := block.DecodeFooter(buf, uint64(offset), 0)
	if err != nil {
		return err
	}

	// Verify it's a block-based table
	if footer.TableMagicNumber != block.BlockBasedTableMagicNumber &&
		footer.TableMagicNumber != block.LegacyBlockBasedTableMagicNumber {
		return ErrInvalidSST
	}

	r.footer = footer
	return nil
}

// readMetaindex reads and parses the metaindex block.
func (r *Reader) readMetaindex() error {
	if r.footer.MetaindexHandle.IsNull() {
		return nil // No metaindex
	}

	// Read metaindex block
	metaBlock, err := r.readBlock(r.footer.MetaindexHandle)
	if err != nil {
		return err
	}

	// Parse metaindex entries
	// Metaindex maps meta block names to their handles
	iter := metaBlock.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		name := string(iter.Key())
		handleBytes := iter.Value()

		handle, _, err := block.DecodeHandle(handleBytes)
		if err != nil {
			continue // Skip invalid entries
		}

		switch {
		case name == "rocksdb.index":
			r.indexHandle = handle
		case name == "rocksdb.properties":
			r.propertiesHandle = handle
		case name == "rocksdb.filter" || strings.HasPrefix(name, "fullfilter."):
			r.filterHandle = handle
		case name == "rocksdb.range_del":
			r.rangeDelHandle = handle
		}
	}

	return nil
}

// checkUnsupportedFeatures reads properties and returns an error if the SST uses
// unsupported features (partitioned index, hash index).
// This check runs early to prevent misinterpreting corrupted data.
func (r *Reader) checkUnsupportedFeatures() error {
	// Properties are optional - if we can't read them, skip the check
	if r.propertiesHandle.IsNull() {
		return nil
	}

	props, err := r.Properties()
	if err != nil {
		// Can't read properties - skip check rather than fail
		// This is intentional: we prefer to try reading the SST even if
		// properties are malformed, as the data blocks may still be readable.
		return nil //nolint:nilerr // intentional: skip check on properties read failure
	}

	// Check for partitioned index
	// IndexPartitions > 0 means the index is split across multiple blocks.
	// Our reader treats the index as a single block, so partitioned index would
	// cause incorrect behavior (reading partial index as if it were complete).
	if props.IndexPartitions > 0 {
		return ErrUnsupportedPartitionedIndex
	}

	// Note: IndexKeyIsUserKey > 0 is NOT hash index — it means index keys are
	// user keys (without 8-byte seqno suffix) vs internal keys. This is a
	// common optimization and our IndexBlockIterator handles it correctly.

	return nil
}

// IndexBlockIterator is a specialized iterator for index blocks.
// Index blocks use value_delta_encoding (format_version >= 4), where:
// - Entries have format: <shared:byte><non_shared:byte><key_delta><value>
// - No value_size is stored; value extends to next entry or end of data
type IndexBlockIterator struct {
	data        []byte // Block data (without restarts/footer)
	dataEnd     int    // End of entry data
	entryStart  int    // Start of current entry (for Prev tracking)
	current     int    // Current position (after parsing, points to next entry)
	key         []byte // Current key
	valueOffset int    // Start of current value
	valueEnd    int    // End of current value
	valid       bool
	err         error
}

// NewIndexBlockIterator creates an iterator for an index block.
func NewIndexBlockIterator(data []byte, dataEnd int) *IndexBlockIterator {
	return &IndexBlockIterator{
		data:    data,
		dataEnd: dataEnd,
	}
}

func (it *IndexBlockIterator) SeekToFirst() {
	it.key = it.key[:0]
	it.current = 0
	it.parseCurrentEntry()
}

func (it *IndexBlockIterator) Valid() bool {
	return it.valid && it.err == nil
}

func (it *IndexBlockIterator) Next() {
	if it.current >= it.dataEnd {
		it.valid = false
		return
	}
	// current is already positioned at the next entry after parseCurrentEntry()
	it.parseCurrentEntry()
}

// Prev moves to the previous entry.
func (it *IndexBlockIterator) Prev() {
	if it.entryStart == 0 {
		it.valid = false
		return
	}

	// We need to scan from the beginning to find the entry before the current one.
	targetEntryStart := it.entryStart
	it.SeekToFirst()

	var prevKey []byte
	var prevValueOffset, prevValueEnd, prevEntryStart int
	found := false

	for it.Valid() && it.entryStart < targetEntryStart {
		prevKey = append(prevKey[:0], it.key...)
		prevValueOffset = it.valueOffset
		prevValueEnd = it.valueEnd
		prevEntryStart = it.entryStart
		found = true
		it.Next()
	}

	if found {
		it.key = prevKey
		it.valueOffset = prevValueOffset
		it.valueEnd = prevValueEnd
		it.entryStart = prevEntryStart
		it.current = it.valueEnd // Ready for next
		it.valid = true
	} else {
		it.valid = false
	}
}

func (it *IndexBlockIterator) Key() []byte {
	return it.key
}

func (it *IndexBlockIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.data[it.valueOffset:it.valueEnd]
}

func (it *IndexBlockIterator) SeekToLast() {
	// Iterate to find the last entry
	it.SeekToFirst()
	if !it.Valid() {
		return
	}

	var lastKey []byte
	var lastValueOffset, lastValueEnd, lastEntryStart int

	for it.Valid() {
		lastKey = append(lastKey[:0], it.key...)
		lastValueOffset = it.valueOffset
		lastValueEnd = it.valueEnd
		lastEntryStart = it.entryStart
		it.Next()
	}

	// Restore to last entry
	it.key = lastKey
	it.valueOffset = lastValueOffset
	it.valueEnd = lastValueEnd
	it.entryStart = lastEntryStart
	it.current = lastValueEnd
	it.valid = true
	it.err = nil
}

func (it *IndexBlockIterator) Seek(target []byte) {
	// Simple linear scan for now
	it.SeekToFirst()
	for it.Valid() {
		// Compare key with target using internal key comparison
		if block.CompareInternalKeys(it.key, target) >= 0 {
			return
		}
		it.Next()
	}
}

func (it *IndexBlockIterator) parseCurrentEntry() {
	if it.current >= it.dataEnd {
		it.valid = false
		return
	}

	// Remember where this entry starts (for Prev)
	it.entryStart = it.current

	// Format for C++ RocksDB index blocks with value_delta_encoding (format_version >= 4):
	// <shared:varint32><non_shared:varint32><key_delta><value>
	// Note: NO value_length field! The value is a BlockHandle (two varints: offset, size).

	// Read shared key length
	shared, n := decodeVarint32FromBytes(it.data[it.current:it.dataEnd])
	if n == 0 {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}
	it.current += n

	// Read non-shared key length
	nonShared, n := decodeVarint32FromBytes(it.data[it.current:it.dataEnd])
	if n == 0 {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}
	it.current += n

	// Check bounds for key
	if it.current+int(nonShared) > it.dataEnd {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}

	// Build key
	if int(shared) > len(it.key) {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}
	it.key = append(it.key[:shared], it.data[it.current:it.current+int(nonShared)]...)
	it.current += int(nonShared)

	// Value is a BlockHandle (two varints: offset and size)
	it.valueOffset = it.current

	// Decode offset varint
	_, n = decodeVarint32FromBytes(it.data[it.current:it.dataEnd])
	if n == 0 {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}
	it.current += n

	// Decode size varint
	_, n = decodeVarint32FromBytes(it.data[it.current:it.dataEnd])
	if n == 0 {
		it.err = ErrInvalidSST
		it.valid = false
		return
	}
	it.current += n

	it.valueEnd = it.current // Value ends after the two varints

	it.valid = true
}

// decodeVarint32FromBytes decodes a varint32 from the start of data.
// Returns the value and number of bytes consumed (0 if error).
func decodeVarint32FromBytes(data []byte) (uint32, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var result uint32
	var shift uint
	for i := 0; i < len(data) && i < 5; i++ {
		b := data[i]
		result |= uint32(b&0x7F) << shift
		if b < 128 {
			return result, i + 1
		}
		shift += 7
	}
	return 0, 0 // Overflow or incomplete
}

// readIndex reads and caches the index block.
func (r *Reader) readIndex() error {
	// For format_version < 6, index handle is in footer
	// For format_version >= 6, index handle is in metaindex
	var handle block.Handle
	if r.footer.FormatVersion < 6 {
		handle = r.footer.IndexHandle
	} else {
		handle = r.indexHandle
	}

	if handle.IsNull() {
		return ErrBlockNotFound
	}

	// Use readBlock to properly handle checksums
	indexBlock, err := r.readBlock(handle)
	if err != nil {
		return err
	}

	r.indexBlock = indexBlock

	// Detect whether index block uses value_delta_encoding (C++ RocksDB format)
	// or standard block format (Go-generated SSTs).
	// For format_version >= 4, C++ RocksDB uses value_delta_encoding.
	// We detect by trying to parse the first entry with IndexBlockIterator.
	if r.footer.FormatVersion >= 4 {
		r.indexUsesValueDeltaEncoding = r.detectValueDeltaEncoding()
	}

	return nil
}

// detectValueDeltaEncoding tries to determine if the index block uses value_delta_encoding.
// It does this by trying to parse the first entry and checking if the resulting
// BlockHandle makes sense (offset < file_size, reasonable size).
func (r *Reader) detectValueDeltaEncoding() bool {
	data := r.indexBlock.Data()
	dataEnd := r.indexBlock.DataEnd()

	if dataEnd == 0 {
		return false // Empty block
	}

	// Try parsing with IndexBlockIterator (value_delta_encoding format)
	iter := NewIndexBlockIterator(data, dataEnd)
	iter.SeekToFirst()

	if !iter.Valid() || iter.err != nil {
		return false // Failed to parse, use standard format
	}

	// Check if the parsed value is a valid BlockHandle
	value := iter.Value()
	if len(value) < 2 {
		return false // Too short for a BlockHandle
	}

	// Decode the block handle
	offset, n1 := decodeVarint32FromBytes(value)
	if n1 == 0 {
		return false
	}
	size, n2 := decodeVarint32FromBytes(value[n1:])
	if n2 == 0 {
		return false
	}

	// Validate the handle makes sense
	// A valid data block must have size > 0 (empty blocks are not written)
	if size == 0 {
		return false // Invalid: blocks must have non-zero size
	}
	if uint64(offset)+uint64(size) > uint64(r.size) {
		return false // Handle points beyond file
	}
	if uint64(size) > uint64(r.size)/2 {
		return false // Unreasonably large block
	}

	return true // Looks like valid value_delta_encoding
}

// readFilter reads and caches the filter block if present.
func (r *Reader) readFilter() error {
	if r.filterHandle.IsNull() {
		return nil // No filter, not an error
	}

	// Read filter block data (filter has its own structure, but we still need to
	// read past the block trailer if present)
	trailerSize := int(r.footer.BlockTrailerSize)
	totalSize := int(r.filterHandle.Size) + trailerSize

	buf := make([]byte, totalSize)
	if _, err := r.file.ReadAt(buf, int64(r.filterHandle.Offset)); err != nil {
		return err
	}

	// Filter data is just the block without trailer
	filterData := buf[:r.filterHandle.Size]

	// Create filter reader
	r.filterReader = filter.NewBloomFilterReader(filterData)
	if r.filterReader == nil {
		// Invalid or unsupported filter format, not fatal
		return nil
	}

	return nil
}

// KeyMayMatch returns true if the key may be in this SST file.
// Returns true (may match) if:
// - No filter is present
// - The filter indicates the key might be present
// Returns false (definitely not present) if the filter says the key is not present.
func (r *Reader) KeyMayMatch(key []byte) bool {
	if r.filterReader == nil {
		return true // No filter, assume may match
	}
	return r.filterReader.MayContain(key)
}

// HasFilter returns true if this table has a Bloom filter.
func (r *Reader) HasFilter() bool {
	return r.filterReader != nil
}

// maxBlockSize is the maximum size we'll allocate for a single block.
// This prevents memory exhaustion from corrupted block handles.
// 256 MiB is well above typical block sizes (4 KiB to 4 MiB).
const maxBlockSize = 256 * 1024 * 1024

// readBlock reads and optionally verifies a block from the file.
func (r *Reader) readBlock(handle block.Handle) (*block.Block, error) {
	// Block format:
	// [block data] [compression type: 1 byte] [checksum: 4 bytes]
	// Total size = handle.Size + BlockTrailerSize

	trailerSize := int(r.footer.BlockTrailerSize)

	// Reject offsets that cannot be represented as int64.
	// ReadAt takes an int64 offset, and some test files use slices that panic on negative offsets.
	const maxInt64AsUint64 = ^uint64(0) >> 1
	if handle.Offset > maxInt64AsUint64 {
		return nil, fmt.Errorf("block offset %d exceeds maximum %d: %w", handle.Offset, maxInt64AsUint64, ErrInvalidSST)
	}

	// Validate block size to prevent memory exhaustion from corrupted handles
	if handle.Size > maxBlockSize {
		return nil, fmt.Errorf("block size %d exceeds maximum %d: %w", handle.Size, maxBlockSize, ErrInvalidSST)
	}

	totalSize := int(handle.Size) + trailerSize

	// Additional validation: block must fit within the file
	end := handle.Offset + uint64(totalSize)
	if end < handle.Offset || end > uint64(r.size) {
		return nil, fmt.Errorf("block at offset %d size %d exceeds file size %d: %w",
			handle.Offset, totalSize, r.size, ErrInvalidSST)
	}

	buf := make([]byte, totalSize)
	n, err := r.file.ReadAt(buf, int64(handle.Offset))
	if err != nil {
		return nil, err
	}
	if n < totalSize {
		return nil, ErrInvalidSST
	}

	// Verify checksum if requested
	if r.options.VerifyChecksums && trailerSize > 0 {
		blockData := buf[:len(buf)-trailerSize]
		compressionType := buf[len(buf)-trailerSize]
		storedChecksum := encoding.DecodeFixed32(buf[len(buf)-4:])

		// Compute checksum (includes compression type)
		var computed uint32
		switch r.footer.ChecksumType {
		case block.ChecksumTypeCRC32C:
			crc := checksum.Value(blockData)
			crc = checksum.Extend(crc, []byte{compressionType})
			computed = checksum.Mask(crc)
		case block.ChecksumTypeXXHash64:
			computed = checksum.XXHash64ChecksumWithLastByte(blockData, compressionType)
		case block.ChecksumTypeXXH3:
			computed = checksum.XXH3ChecksumWithLastByte(blockData, compressionType)
		default:
			// Skip verification for unsupported types (kNoChecksum, kxxHash)
			computed = storedChecksum
		}

		// For format_version >= 6, add context checksum modifier
		if r.footer.FormatVersion >= 6 && r.footer.BaseContextChecksum != 0 {
			computed += checksumModifierForContext(r.footer.BaseContextChecksum, handle.Offset)
		}

		if computed != storedChecksum {
			return nil, ErrChecksumMismatch
		}
	}

	// Get block data and compression type
	blockData := buf[:handle.Size]
	compressionType := compression.NoCompression
	if trailerSize > 0 {
		compressionType = compression.Type(buf[len(buf)-trailerSize])
	}

	// Decompress if needed
	if compressionType != compression.NoCompression {
		// For format_version >= 2, most compression types have a varint32 prefix
		// containing the decompressed size. Exception: Snappy embeds the size
		// in its format, so no external prefix is used.
		// Reference: RocksDB util/compression.h lines 873-874
		compressedData := blockData
		expectedSize := 0
		if r.footer.FormatVersion >= 2 && !compressionHasEmbeddedSize(compressionType) {
			// Read the varint32 decompressed size prefix
			size, prefixLen, err := encoding.DecodeVarint32(compressedData)
			if err != nil {
				return nil, fmt.Errorf("decode compressed block size prefix: %w", err)
			}
			expectedSize = int(size)
			compressedData = compressedData[prefixLen:]
		}

		decompressed, err := compression.DecompressWithSize(compressionType, compressedData, expectedSize)
		if err != nil {
			return nil, fmt.Errorf("decompress block: %w", err)
		}
		blockData = decompressed
	}

	return block.NewBlock(blockData)
}

// checksumModifierForContext computes the context checksum modifier.
// This matches RocksDB's ChecksumModifierForContext function.
func checksumModifierForContext(baseContextChecksum uint32, offset uint64) uint32 {
	if baseContextChecksum == 0 {
		return 0
	}
	lower32 := uint32(offset)
	upper32 := uint32(offset >> 32)
	return baseContextChecksum ^ (lower32 + upper32)
}

// NewIterator returns an iterator over the table contents.
// The iterator is initially invalid; call SeekToFirst or Seek before use.
func (r *Reader) NewIterator() *TableIterator {
	ti := &TableIterator{
		reader:    r,
		dataBlock: nil,
		dataIter:  nil,
	}

	// Use IndexBlockIterator if the index block uses value_delta_encoding
	// (C++ RocksDB format_version >= 4). Otherwise use standard block iterator.
	if r.indexUsesValueDeltaEncoding {
		ti.indexIter = NewIndexBlockIterator(r.indexBlock.Data(), r.indexBlock.DataEnd())
		ti.useIndexIter = true
	} else {
		// Standard block format (Go-generated SSTs or older format versions)
		ti.indexBlockIter = r.indexBlock.NewIterator()
		ti.useIndexIter = false
	}

	return ti
}

// Close releases resources associated with the reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Footer returns the parsed footer.
func (r *Reader) Footer() *block.Footer {
	return r.footer
}

// Properties returns the table properties, loading them if necessary.
func (r *Reader) Properties() (*TableProperties, error) {
	if r.properties != nil {
		return r.properties, nil
	}

	if r.propertiesHandle.IsNull() {
		return nil, ErrBlockNotFound
	}

	// Read properties block
	propsBlock, err := r.readBlock(r.propertiesHandle)
	if err != nil {
		return nil, err
	}

	// Parse properties
	props, err := ParsePropertiesBlock(propsBlock.Data())
	if err != nil {
		return nil, err
	}

	r.properties = props
	return props, nil
}

// HasRangeTombstones returns true if the SST file contains range tombstones.
func (r *Reader) HasRangeTombstones() bool {
	return !r.rangeDelHandle.IsNull()
}

// GetRangeTombstones reads and parses range tombstones from the SST file.
// Returns a FragmentedRangeTombstoneList for efficient lookup.
// Returns an empty list if there are no range tombstones.
func (r *Reader) GetRangeTombstones() (*rangedel.FragmentedRangeTombstoneList, error) {
	if r.rangeDelHandle.IsNull() {
		return rangedel.NewFragmentedRangeTombstoneList(), nil
	}

	// Read range deletion block
	rangeDelBlock, err := r.readBlock(r.rangeDelHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to read range del block: %w", err)
	}

	// Parse tombstones from block
	tombstones := rangedel.NewTombstoneList()
	iter := rangeDelBlock.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		// Key is internal key: start_key + seq + TypeRangeDeletion
		internalKey := iter.Key()
		if len(internalKey) < dbformat.NumInternalBytes {
			continue // Skip invalid entries
		}

		// Parse internal key
		parsed, err := dbformat.ParseInternalKey(internalKey)
		if err != nil {
			continue // Skip invalid entries
		}

		// Value is end_key
		endKey := iter.Value()

		tombstones.AddRange(parsed.UserKey, endKey, parsed.Sequence)
	}

	if iter.Error() != nil {
		return nil, fmt.Errorf("error iterating range del block: %w", iter.Error())
	}

	// Fragment the tombstones for efficient lookup
	fragmenter := rangedel.NewFragmenter()
	for _, t := range tombstones.All() {
		fragmenter.AddTombstone(t)
	}

	return fragmenter.Finish(), nil
}

// GetRangeTombstoneList returns the raw (non-fragmented) tombstone list.
func (r *Reader) GetRangeTombstoneList() (*rangedel.TombstoneList, error) {
	if r.rangeDelHandle.IsNull() {
		return rangedel.NewTombstoneList(), nil
	}

	// Read range deletion block
	rangeDelBlock, err := r.readBlock(r.rangeDelHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to read range del block: %w", err)
	}

	// Parse tombstones from block
	tombstones := rangedel.NewTombstoneList()
	iter := rangeDelBlock.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		internalKey := iter.Key()
		if len(internalKey) < dbformat.NumInternalBytes {
			continue
		}

		parsed, err := dbformat.ParseInternalKey(internalKey)
		if err != nil {
			continue
		}

		endKey := iter.Value()
		tombstones.AddRange(parsed.UserKey, endKey, parsed.Sequence)
	}

	if iter.Error() != nil {
		return nil, fmt.Errorf("error iterating range del block: %w", iter.Error())
	}

	return tombstones, nil
}

// TableIterator iterates over key-value pairs in an SST file.
type TableIterator struct {
	reader         *Reader
	indexIter      *IndexBlockIterator // For format_version >= 4 (value_delta_encoded)
	indexBlockIter *block.Iterator     // For format_version < 4 (standard block format)
	useIndexIter   bool                // true if using IndexBlockIterator
	dataBlock      *block.Block        // Current data block
	dataIter       *block.Iterator     // Iterator over current data block
	err            error
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *TableIterator) Valid() bool {
	return it.err == nil && it.dataIter != nil && it.dataIter.Valid()
}

// SeekToFirst positions the iterator at the first entry.
func (it *TableIterator) SeekToFirst() {
	if it.useIndexIter {
		it.indexIter.SeekToFirst()
	} else {
		it.indexBlockIter.SeekToFirst()
	}
	it.loadDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
}

// SeekToLast positions the iterator at the last entry.
func (it *TableIterator) SeekToLast() {
	if it.useIndexIter {
		it.indexIter.SeekToLast()
	} else {
		it.indexBlockIter.SeekToLast()
	}
	it.loadDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
}

// Seek positions the iterator at the first entry with key >= target.
func (it *TableIterator) Seek(target []byte) {
	// Use index to find the data block that may contain target
	if it.useIndexIter {
		it.indexIter.Seek(target)
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
	} else {
		it.indexBlockIter.Seek(target)
		if !it.indexBlockIter.Valid() {
			it.dataIter = nil
			return
		}
	}
	it.loadDataBlock()
	if it.dataIter != nil {
		it.dataIter.Seek(target)
	}
}

// Next moves to the next entry.
func (it *TableIterator) Next() {
	if it.dataIter == nil {
		return
	}
	it.dataIter.Next()
	if !it.dataIter.Valid() {
		// Move to next data block
		if it.useIndexIter {
			it.indexIter.Next()
		} else {
			it.indexBlockIter.Next()
		}
		it.loadDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

// Prev moves to the previous entry.
func (it *TableIterator) Prev() {
	if it.dataIter == nil {
		return
	}
	it.dataIter.Prev()
	if !it.dataIter.Valid() {
		// Move to previous data block
		if it.useIndexIter {
			it.indexIter.Prev()
		} else {
			it.indexBlockIter.Prev()
		}
		it.loadDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}

// Key returns the current key.
func (it *TableIterator) Key() []byte {
	if it.dataIter == nil {
		return nil
	}
	return it.dataIter.Key()
}

// Value returns the current value.
func (it *TableIterator) Value() []byte {
	if it.dataIter == nil {
		return nil
	}
	return it.dataIter.Value()
}

// Error returns any error encountered during iteration.
func (it *TableIterator) Error() error {
	return it.err
}

// loadDataBlock loads the data block pointed to by the current index entry.
func (it *TableIterator) loadDataBlock() {
	var valid bool
	var handleBytes []byte

	if it.useIndexIter {
		if !it.indexIter.Valid() {
			it.dataBlock = nil
			it.dataIter = nil
			return
		}
		valid = true
		handleBytes = it.indexIter.Value()
	} else {
		if !it.indexBlockIter.Valid() {
			it.dataBlock = nil
			it.dataIter = nil
			return
		}
		valid = true
		handleBytes = it.indexBlockIter.Value()
	}

	if !valid {
		it.dataBlock = nil
		it.dataIter = nil
		return
	}

	// Index value contains the block handle
	handle, _, err := block.DecodeHandle(handleBytes)
	if err != nil {
		it.err = err
		it.dataBlock = nil
		it.dataIter = nil
		return
	}

	// Read the data block
	dataBlock, err := it.reader.readBlock(handle)
	if err != nil {
		it.err = err
		it.dataBlock = nil
		it.dataIter = nil
		return
	}

	it.dataBlock = dataBlock
	it.dataIter = dataBlock.NewIterator()
}
