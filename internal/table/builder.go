// Package table provides SST file reading and writing.
//
// TableBuilder creates SST files in the block-based table format.
// The format is compatible with RocksDB v10.7.5.
//
// Reference: RocksDB v10.7.5
//   - table/block_based/block_based_table_builder.h
//   - table/block_based/block_based_table_builder.cc
//   - table/table_builder.h
//
// # Whitebox Testing Hooks
//
// This file contains kill points for crash testing (requires -tags crashtest).
// In production builds, these compile to no-ops with zero overhead.
// See docs/testing.md for usage.
package table

import (
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"sort"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/encoding"
	"github.com/aalhour/rockyardkv/internal/filter"
	"github.com/aalhour/rockyardkv/internal/rangedel"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// compressionHasEmbeddedSize returns true if the compression type embeds the
// uncompressed size in its format and doesn't need an external varint32 prefix.
// Reference: RocksDB util/compression.h lines 873-874:
// "Snappy and XPRESS instead extract the decompressed size from the
// compressed block itself, same as version 1."
func compressionHasEmbeddedSize(t compression.Type) bool {
	switch t {
	case compression.SnappyCompression:
		return true
	// Note: XpressCompression also has embedded size but is not supported
	default:
		return false
	}
}

// BuilderOptions configures the TableBuilder.
type BuilderOptions struct {
	// BlockSize is the target size for data blocks (default: 4KB).
	BlockSize int

	// BlockRestartInterval is the number of keys between restart points (default: 16).
	BlockRestartInterval int

	// FormatVersion is the SST format version (default: 6 for v10.7.5 compatibility).
	FormatVersion uint32

	// ChecksumType is the checksum algorithm (default: XXH3).
	ChecksumType checksum.Type

	// ComparatorName is the name of the key comparator.
	ComparatorName string

	// ColumnFamilyID is the column family ID.
	ColumnFamilyID uint32

	// ColumnFamilyName is the column family name.
	ColumnFamilyName string

	// FilterBitsPerKey controls Bloom filter accuracy (default: 10 = ~1% FP rate).
	// Set to 0 to disable filter.
	FilterBitsPerKey int

	// FilterPolicy is the name of the filter policy (e.g., "rocksdb.BuiltinBloomFilter").
	FilterPolicy string

	// Compression is the compression type for data blocks.
	Compression compression.Type
}

// DefaultBuilderOptions returns default options for TableBuilder.
func DefaultBuilderOptions() BuilderOptions {
	return BuilderOptions{
		BlockSize:            4096,
		BlockRestartInterval: 16,
		FormatVersion:        3, // Use version 3 (standard index blocks, no value_delta_encoding)
		ChecksumType:         checksum.TypeCRC32C,
		ComparatorName:       "leveldb.BytewiseComparator",
		ColumnFamilyID:       0,
		ColumnFamilyName:     "default",
		FilterBitsPerKey:     10, // ~1% false positive rate
		FilterPolicy:         "rocksdb.BuiltinBloomFilter",
		Compression:          compression.NoCompression, // Default to no compression for stability
	}
}

// TableBuilder builds SST files in the block-based table format.
type TableBuilder struct {
	writer  io.Writer
	options BuilderOptions

	// Current data block being built
	dataBlock *block.Builder

	// Index block builder (maps last key of each data block to its handle)
	indexBlock *block.Builder

	// Properties block builder
	propertiesBlock *block.Builder

	// Range deletion block builder
	// Range tombstones are stored as key-value pairs where:
	// - key: start_key encoded as internal key with TypeRangeDeletion
	// - value: end_key
	rangeDelBlock *block.Builder

	// Filter builder (optional, nil if disabled)
	filterBuilder *filter.BloomFilterBuilder

	// Pending index entry for the last flushed data block
	pendingIndexEntry bool
	pendingHandle     block.Handle
	lastKey           []byte

	// File offset tracking
	offset uint64

	// Statistics for table properties
	numEntries        uint64
	numDataBlocks     uint64
	rawKeySize        uint64
	rawValueSize      uint64
	dataSize          uint64 // size of all data blocks (excluding trailer)
	indexSize         uint64 // size of index block (excluding trailer)
	filterSize        uint64 // size of filter block
	numRangeDeletions uint64 // number of range tombstones

	// State tracking
	finished bool
	err      error

	// Base context checksum for format version 6+ (random non-zero value)
	baseContextChecksum uint32
}

// NewTableBuilder creates a new TableBuilder that writes to w.
func NewTableBuilder(w io.Writer, opts BuilderOptions) *TableBuilder {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4096
	}
	if opts.BlockRestartInterval <= 0 {
		opts.BlockRestartInterval = 16
	}
	if opts.FormatVersion == 0 {
		opts.FormatVersion = 6
	}
	if opts.ChecksumType == 0 {
		opts.ChecksumType = checksum.TypeXXH3
	}
	if opts.ComparatorName == "" {
		opts.ComparatorName = "leveldb.BytewiseComparator"
	}

	tb := &TableBuilder{
		writer:          w,
		options:         opts,
		dataBlock:       block.NewBuilder(opts.BlockRestartInterval),
		indexBlock:      block.NewBuilder(1), // Index uses restart interval of 1
		propertiesBlock: block.NewBuilder(1),
		rangeDelBlock:   block.NewBuilder(1), // Range deletions use restart interval of 1
	}

	// Generate base context checksum for format version 6+
	// Must be non-zero to enable context-dependent checksums
	if opts.FormatVersion >= 6 {
		for tb.baseContextChecksum == 0 {
			tb.baseContextChecksum = rand.Uint32()
		}
	}

	// Create filter builder if enabled
	if opts.FilterBitsPerKey > 0 {
		tb.filterBuilder = filter.NewBloomFilterBuilder(opts.FilterBitsPerKey)
	}

	return tb
}

// AddRangeTombstone adds a range deletion to the table.
// The range [startKey, endKey) will be marked as deleted at the given sequence number.
// Range tombstones are stored in a separate block from regular data.
func (tb *TableBuilder) AddRangeTombstone(startKey, endKey []byte, seqNum dbformat.SequenceNumber) error {
	if tb.finished {
		return errors.New("table: builder already finished")
	}
	if tb.err != nil {
		return tb.err
	}

	// Create internal key for start key with TypeRangeDeletion
	internalKey := dbformat.NewInternalKey(startKey, seqNum, dbformat.TypeRangeDeletion)

	// Add to range deletion block: key = internal key, value = end key
	tb.rangeDelBlock.Add(internalKey, endKey)
	tb.numRangeDeletions++

	return nil
}

// AddRangeTombstones adds multiple range tombstones from a tombstone list.
func (tb *TableBuilder) AddRangeTombstones(list *rangedel.TombstoneList) error {
	if list == nil || list.IsEmpty() {
		return nil
	}

	for _, t := range list.All() {
		if err := tb.AddRangeTombstone(t.StartKey, t.EndKey, t.SequenceNum); err != nil {
			return err
		}
	}
	return nil
}

// AddFragmentedRangeTombstones adds tombstones from a fragmented list.
func (tb *TableBuilder) AddFragmentedRangeTombstones(list *rangedel.FragmentedRangeTombstoneList) error {
	if list == nil || list.IsEmpty() {
		return nil
	}

	for _, frag := range list.All() {
		if err := tb.AddRangeTombstone(frag.StartKey, frag.EndKey, frag.SequenceNum); err != nil {
			return err
		}
	}
	return nil
}

// HasRangeTombstones returns true if range tombstones have been added.
func (tb *TableBuilder) HasRangeTombstones() bool {
	return tb.numRangeDeletions > 0
}

// Add adds a key-value pair to the table.
// Keys must be added in sorted order.
func (tb *TableBuilder) Add(key, value []byte) error {
	if tb.finished {
		return errors.New("table: builder already finished")
	}
	if tb.err != nil {
		return tb.err
	}

	// If we have a pending index entry, add it now that we have the next key
	if tb.pendingIndexEntry {
		// Use the last key from the previous block as the separator
		tb.indexBlock.Add(tb.lastKey, tb.pendingHandle.EncodeToSlice())
		tb.pendingIndexEntry = false
	}

	// Add to data block
	tb.dataBlock.Add(key, value)
	tb.numEntries++
	tb.rawKeySize += uint64(len(key))
	tb.rawValueSize += uint64(len(value))

	// Add to filter (using user key portion only for internal keys)
	if tb.filterBuilder != nil {
		// For internal keys, the user key is everything except the last 8 bytes
		userKey := key
		if len(key) > 8 {
			userKey = key[:len(key)-8]
		}
		tb.filterBuilder.AddKey(userKey)
	}

	// Save last key for index
	tb.lastKey = append(tb.lastKey[:0], key...)

	// Check if we should flush the data block
	if tb.dataBlock.EstimatedSize() >= tb.options.BlockSize {
		if err := tb.flushDataBlock(); err != nil {
			tb.err = err
			return err
		}
	}

	return nil
}

// flushDataBlock writes the current data block to the file.
func (tb *TableBuilder) flushDataBlock() error {
	if tb.dataBlock.Empty() {
		return nil
	}

	// Finish the block
	blockContents := tb.dataBlock.Finish()

	// Write block with trailer
	handle, err := tb.writeBlockWithTrailer(blockContents, block.TypeData)
	if err != nil {
		return err
	}

	tb.dataSize += handle.Size
	tb.numDataBlocks++

	// Set up pending index entry
	tb.pendingHandle = handle
	tb.pendingIndexEntry = true

	// Reset data block for next batch
	tb.dataBlock.Reset()

	return nil
}

// writeBlockWithTrailer writes a block with its trailer (compression type + checksum).
// Returns the handle (offset, size) of the written block.
func (tb *TableBuilder) writeBlockWithTrailer(blockData []byte, blockType block.Type) (block.Handle, error) {
	// Apply compression
	compressedData := blockData
	compressionType := block.CompressionNone

	if tb.options.Compression != compression.NoCompression && blockType == block.TypeData {
		compressed, err := compression.Compress(tb.options.Compression, blockData)
		if err == nil && compressed != nil && len(compressed) < len(blockData) {
			// Only use compression if it actually reduces size
			// For format_version >= 2, prepend varint32 decompressed size for most algorithms.
			// Exception: Snappy embeds the uncompressed size in its format, so no prefix needed.
			// Reference: util/compression.h lines 873-874:
			// "Snappy and XPRESS instead extract the decompressed size from the
			// compressed block itself, same as version 1."
			if tb.options.FormatVersion >= 2 && !compressionHasEmbeddedSize(tb.options.Compression) {
				prefix := encoding.AppendVarint32(nil, uint32(len(blockData)))
				compressedData = append(prefix, compressed...)
			} else {
				compressedData = compressed
			}
			compressionType = block.CompressionType(tb.options.Compression)
		}
	}

	handle := block.Handle{
		Offset: tb.offset,
		Size:   uint64(len(compressedData)),
	}

	// Write block data (possibly compressed)
	n, err := tb.writer.Write(compressedData)
	if err != nil {
		return block.Handle{}, err
	}
	tb.offset += uint64(n)

	// Write trailer: compression type (1 byte) + checksum (4 bytes)
	trailer := make([]byte, block.BlockTrailerSize)
	trailer[0] = byte(compressionType)

	// Compute checksum based on type (checksum is over compressed data + compression type)
	var cksum uint32
	switch tb.options.ChecksumType {
	case checksum.TypeCRC32C:
		cksum = checksum.ComputeCRC32CChecksumWithLastByte(compressedData, trailer[0])
	case checksum.TypeXXH3:
		cksum = checksum.ComputeXXH3ChecksumWithLastByte(compressedData, trailer[0])
	default:
		cksum = 0
	}

	// For Format V6+, add context-dependent checksum modifier
	// This makes checksums unique based on their file position
	if tb.options.FormatVersion >= 6 && tb.baseContextChecksum != 0 {
		cksum += checksum.ChecksumModifierForContext(tb.baseContextChecksum, handle.Offset)
	}

	binary.LittleEndian.PutUint32(trailer[1:], cksum)

	n, err = tb.writer.Write(trailer)
	if err != nil {
		return block.Handle{}, err
	}
	tb.offset += uint64(n)

	return handle, nil
}

// Finish finalizes the table and writes the footer.
// After calling Finish, the TableBuilder should not be used.
func (tb *TableBuilder) Finish() error {
	// Whitebox [crashtest]: crash before SST finalize — tests incomplete SST handling
	testutil.MaybeKill(testutil.KPSSTClose0)

	if tb.finished {
		return errors.New("table: builder already finished")
	}
	if tb.err != nil {
		return tb.err
	}
	tb.finished = true

	// Flush any remaining data
	if err := tb.flushDataBlock(); err != nil {
		tb.err = err
		return err
	}

	// Add the final pending index entry
	if tb.pendingIndexEntry {
		tb.indexBlock.Add(tb.lastKey, tb.pendingHandle.EncodeToSlice())
		tb.pendingIndexEntry = false
	}

	// Collect metaindex entries - will be sorted before writing
	// C++ uses std::map which maintains sorted order; we must do the same.
	type metaEntry struct {
		key   string
		value []byte
	}
	var metaEntries []metaEntry

	// Write filter block if enabled
	if tb.filterBuilder != nil && tb.filterBuilder.NumKeys() > 0 {
		filterHandle, err := tb.writeFilterBlock()
		if err != nil {
			tb.err = err
			return err
		}
		// Use fullfilter.<policy_name> as the key (RocksDB convention)
		filterKey := "fullfilter." + tb.options.FilterPolicy
		metaEntries = append(metaEntries, metaEntry{filterKey, filterHandle.EncodeToSlice()})
	}

	// Write range deletion block if we have range tombstones
	if tb.numRangeDeletions > 0 {
		rangeDelHandle, err := tb.writeRangeDelBlock()
		if err != nil {
			tb.err = err
			return err
		}
		metaEntries = append(metaEntries, metaEntry{"rocksdb.range_del", rangeDelHandle.EncodeToSlice()})
	}

	// Write properties block
	propertiesHandle, err := tb.writePropertiesBlock()
	if err != nil {
		tb.err = err
		return err
	}
	metaEntries = append(metaEntries, metaEntry{"rocksdb.properties", propertiesHandle.EncodeToSlice()})

	// Write index block
	indexContents := tb.indexBlock.Finish()
	indexHandle, err := tb.writeBlockWithTrailer(indexContents, block.TypeIndex)
	if err != nil {
		tb.err = err
		return err
	}
	tb.indexSize = indexHandle.Size

	// For format_version >= 6, index handle goes in metaindex
	if !block.FormatVersionUsesIndexHandleInFooter(tb.options.FormatVersion) {
		metaEntries = append(metaEntries, metaEntry{"rocksdb.index", indexHandle.EncodeToSlice()})
	}

	// Sort metaindex entries by key (C++ uses std::map which maintains sorted order)
	sort.Slice(metaEntries, func(i, j int) bool {
		return metaEntries[i].key < metaEntries[j].key
	})

	// Build metaindex block with entries in sorted order
	metaindexBuilder := block.NewBuilder(1)
	for _, entry := range metaEntries {
		metaindexBuilder.Add([]byte(entry.key), entry.value)
	}

	// Write metaindex block
	metaindexContents := metaindexBuilder.Finish()
	metaindexHandle, err := tb.writeBlockWithTrailer(metaindexContents, block.TypeMetaIndex)
	if err != nil {
		tb.err = err
		return err
	}

	// Write footer
	if err := tb.writeFooter(metaindexHandle, indexHandle); err != nil {
		tb.err = err
		return err
	}

	// Whitebox [crashtest]: crash after SST complete — SST is valid on disk
	testutil.MaybeKill(testutil.KPSSTClose1)

	return nil
}

// writeFilterBlock writes the Bloom filter block.
func (tb *TableBuilder) writeFilterBlock() (block.Handle, error) {
	filterData := tb.filterBuilder.Finish()
	tb.filterSize = uint64(len(filterData))

	// Write filter block (no compression, includes its own checksum scheme)
	handle := block.Handle{
		Offset: tb.offset,
		Size:   uint64(len(filterData)),
	}

	n, err := tb.writer.Write(filterData)
	if err != nil {
		return block.Handle{}, err
	}
	tb.offset += uint64(n)

	// Write trailer: compression type (1 byte) + checksum (4 bytes)
	trailer := make([]byte, block.BlockTrailerSize)
	trailer[0] = byte(block.CompressionNone)

	var cksum uint32
	switch tb.options.ChecksumType {
	case checksum.TypeCRC32C:
		cksum = checksum.ComputeCRC32CChecksumWithLastByte(filterData, trailer[0])
	case checksum.TypeXXH3:
		cksum = checksum.ComputeXXH3ChecksumWithLastByte(filterData, trailer[0])
	default:
		cksum = 0
	}
	binary.LittleEndian.PutUint32(trailer[1:], cksum)

	n, err = tb.writer.Write(trailer)
	if err != nil {
		return block.Handle{}, err
	}
	tb.offset += uint64(n)

	return handle, nil
}

// writeRangeDelBlock writes the range deletion block.
// Range tombstones are stored as key-value pairs where:
// - key: internal key (start_key + seq + TypeRangeDeletion)
// - value: end_key
func (tb *TableBuilder) writeRangeDelBlock() (block.Handle, error) {
	rangeDelContents := tb.rangeDelBlock.Finish()

	// Whitebox [crashtest]: crash during range-del block write
	testutil.MaybeKill(testutil.KPSSTClose0)

	return tb.writeBlockWithTrailer(rangeDelContents, block.TypeData)
}

// writePropertiesBlock writes the table properties block.
func (tb *TableBuilder) writePropertiesBlock() (block.Handle, error) {
	// Collect all properties first, then sort by key name
	type prop struct {
		name  string
		value []byte
	}
	var properties []prop

	// Add uint64 properties
	addUint64Prop := func(name string, value uint64) {
		buf := make([]byte, encoding.MaxVarintLen64)
		n := encoding.PutVarint64(buf, value)
		properties = append(properties, prop{name: name, value: buf[:n]})
	}

	// Add string properties
	addStringProp := func(name string, value string) {
		properties = append(properties, prop{name: name, value: []byte(value)})
	}

	// Collect all properties
	addUint64Prop("rocksdb.column.family.id", uint64(tb.options.ColumnFamilyID))
	addStringProp("rocksdb.column.family.name", tb.options.ColumnFamilyName)
	addStringProp("rocksdb.comparator", tb.options.ComparatorName)
	addStringProp("rocksdb.compression", tb.options.Compression.String())
	addUint64Prop("rocksdb.data.size", tb.dataSize)
	if tb.options.FilterPolicy != "" && tb.filterSize > 0 {
		addStringProp("rocksdb.filter.policy", tb.options.FilterPolicy)
	}
	addUint64Prop("rocksdb.filter.size", tb.filterSize)
	addUint64Prop("rocksdb.format.version", uint64(tb.options.FormatVersion))
	addUint64Prop("rocksdb.index.size", tb.indexSize)
	addUint64Prop("rocksdb.num.data.blocks", tb.numDataBlocks)
	addUint64Prop("rocksdb.num.entries", tb.numEntries)
	if tb.numRangeDeletions > 0 {
		addUint64Prop("rocksdb.num.range-deletions", tb.numRangeDeletions)
	}
	addUint64Prop("rocksdb.raw.key.size", tb.rawKeySize)
	addUint64Prop("rocksdb.raw.value.size", tb.rawValueSize)

	// Sort properties by name (required by RocksDB)
	sort.Slice(properties, func(i, j int) bool {
		return properties[i].name < properties[j].name
	})

	// Build the properties block with sorted properties
	props := block.NewBuilder(1) // Restart interval of 1 for properties
	for _, p := range properties {
		props.Add([]byte(p.name), p.value)
	}

	// Write properties block
	propsContents := props.Finish()
	return tb.writeBlockWithTrailer(propsContents, block.TypeProperties)
}

// writeFooter writes the SST file footer.
func (tb *TableBuilder) writeFooter(metaindexHandle, indexHandle block.Handle) error {
	footer := &block.Footer{
		TableMagicNumber:    block.BlockBasedTableMagicNumber,
		FormatVersion:       tb.options.FormatVersion,
		ChecksumType:        block.ToChecksumType(uint8(tb.options.ChecksumType)),
		MetaindexHandle:     metaindexHandle,
		IndexHandle:         indexHandle,
		BlockTrailerSize:    block.BlockTrailerSize,
		BaseContextChecksum: tb.baseContextChecksum, // For format version 6+
	}

	// Use EncodeToAt for format version 6+ to compute correct footer checksum
	footerOffset := tb.offset
	footerData := footer.EncodeToAt(footerOffset)
	_, err := tb.writer.Write(footerData)
	if err != nil {
		return err
	}
	tb.offset += uint64(len(footerData))

	return nil
}

// Abandon abandons the table being built.
// After calling Abandon, the TableBuilder should not be used.
func (tb *TableBuilder) Abandon() {
	tb.finished = true
}

// NumEntries returns the number of entries added so far.
func (tb *TableBuilder) NumEntries() uint64 {
	return tb.numEntries
}

// FileSize returns the size of the file generated so far.
func (tb *TableBuilder) FileSize() uint64 {
	return tb.offset
}

// Status returns any error encountered during building.
func (tb *TableBuilder) Status() error {
	return tb.err
}
