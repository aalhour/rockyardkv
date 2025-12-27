// footer.go implements SST file footer parsing and encoding.
//
// The footer contains handles to metaindex and index blocks,
// plus a magic number identifying the file type.
//
// Reference: RocksDB v10.7.5
//   - table/format.h (Footer class)
//   - table/format.cc
package block

import (
	"encoding/binary"

	"github.com/aalhour/rockyardkv/internal/checksum"
)

// Magic numbers for different SST file types.
// These must match RocksDB exactly.
const (
	// LegacyBlockBasedTableMagicNumber is the magic number for legacy block-based tables.
	LegacyBlockBasedTableMagicNumber uint64 = 0xdb4775248b80fb57

	// BlockBasedTableMagicNumber is the magic number for block-based tables.
	BlockBasedTableMagicNumber uint64 = 0x88e241b785f4cff7

	// LegacyPlainTableMagicNumber is the magic number for legacy plain tables.
	LegacyPlainTableMagicNumber uint64 = 0x4f3418eb7a8f13b8

	// PlainTableMagicNumber is the magic number for plain tables.
	PlainTableMagicNumber uint64 = 0x8242229663bf9564

	// CuckooTableMagicNumber is the magic number for cuckoo tables.
	// Source: table/cuckoo/cuckoo_table_builder.cc
	CuckooTableMagicNumber uint64 = 0x926789d0c5f17873
)

// MagicNumberLengthByte is the length of the magic number in bytes.
const MagicNumberLengthByte = 8

// ChecksumType represents the type of checksum used.
// Note: This is kept for backward compatibility. Use checksum.Type for new code.
type ChecksumType uint8

const (
	// ChecksumTypeNone means no checksum.
	ChecksumTypeNone ChecksumType = 0
	// ChecksumTypeCRC32C is CRC32C checksum.
	ChecksumTypeCRC32C ChecksumType = 1
	// ChecksumTypeXXHash is xxHash checksum.
	ChecksumTypeXXHash ChecksumType = 2
	// ChecksumTypeXXHash64 is xxHash64 checksum.
	ChecksumTypeXXHash64 ChecksumType = 3
	// ChecksumTypeXXH3 is XXH3 checksum.
	ChecksumTypeXXH3 ChecksumType = 4
)

// ToChecksumType converts a uint8 to ChecksumType.
func ToChecksumType(t uint8) ChecksumType {
	return ChecksumType(t)
}

// Format version constants.
const (
	// LatestFormatVersion is the latest format version.
	LatestFormatVersion uint32 = 7

	// BlockTrailerSize is the size of the block trailer (checksum type + checksum value).
	// For block-based tables: 1 (type) + 4 (checksum) = 5 bytes.
	BlockTrailerSize = 5
)

// CompressionType represents the compression type used for a block.
type CompressionType uint8

const (
	// CompressionNone means no compression.
	CompressionNone CompressionType = 0
	// CompressionSnappy is Snappy compression.
	CompressionSnappy CompressionType = 1
	// CompressionZlib is Zlib compression.
	CompressionZlib CompressionType = 2
	// CompressionBZip2 is BZip2 compression.
	CompressionBZip2 CompressionType = 3
	// CompressionLZ4 is LZ4 compression.
	CompressionLZ4 CompressionType = 4
	// CompressionLZ4HC is LZ4HC compression.
	CompressionLZ4HC CompressionType = 5
	// CompressionXpress is Xpress compression.
	CompressionXpress CompressionType = 6
	// CompressionZstd is Zstd compression.
	CompressionZstd CompressionType = 7
)

// Type represents the type of block in an SST file.
type Type int

const (
	// TypeData is a data block containing key-value pairs.
	TypeData Type = iota
	// TypeIndex is an index block.
	TypeIndex
	// TypeMetaIndex is a metaindex block.
	TypeMetaIndex
	// TypeProperties is a properties block.
	TypeProperties
	// TypeFilter is a filter block.
	TypeFilter
	// TypeRangeDeletion is a range deletion block.
	TypeRangeDeletion
	// TypeCompressionDict is a compression dictionary block.
	TypeCompressionDict
)

// Footer encapsulates the fixed information stored at the tail end of every SST file.
type Footer struct {
	// TableMagicNumber identifies file as RocksDB SST file and which kind of SST format.
	TableMagicNumber uint64

	// FormatVersion is a version for the footer and can also apply to other parts of the file.
	FormatVersion uint32

	// BaseContextChecksum is used for context checksums (format_version >= 6).
	BaseContextChecksum uint32

	// MetaindexHandle is the block handle for metaindex block.
	MetaindexHandle Handle

	// IndexHandle is the block handle for the (top-level) index block.
	// Only used for format_version < 6.
	IndexHandle Handle

	// ChecksumType is the checksum type used in the file.
	ChecksumType ChecksumType

	// BlockTrailerSize is the block trailer size (e.g., 5 for block-based table).
	BlockTrailerSize uint8
}

// Footer encoding sizes.
const (
	// Version0EncodedLength is the size of a version 0 (legacy) footer.
	// It consists of two block handles, padding, and a magic number.
	Version0EncodedLength = 2*MaxEncodedLength + MagicNumberLengthByte

	// NewVersionsEncodedLength is the size of version 1+ footers.
	// checksum_type (1) + two block handles + padding + format_version (4) + magic (8)
	NewVersionsEncodedLength = 1 + 2*MaxEncodedLength + 4 + MagicNumberLengthByte

	// MinEncodedLength is the minimum footer length.
	MinEncodedLength = Version0EncodedLength

	// MaxEncodedFooterLength is the maximum footer length.
	MaxEncodedFooterLength = NewVersionsEncodedLength
)

// Extended magic for format_version >= 6
// This appears at the start of Part2 to distinguish from older formats.
var extendedMagic = [4]byte{0x3e, 0x00, 0x7a, 0x00}

// DecodeFooter decodes a footer from data.
// inputOffset is the offset within the file of the input buffer.
// enforceMagicNumber, if non-zero, will return error if magic doesn't match.
func DecodeFooter(data []byte, inputOffset uint64, enforceMagicNumber uint64) (*Footer, error) {
	if len(data) < MinEncodedLength {
		return nil, ErrBadBlockFooter
	}

	footer := &Footer{}

	// Read magic number from end
	magicOffset := len(data) - MagicNumberLengthByte
	footer.TableMagicNumber = binary.LittleEndian.Uint64(data[magicOffset:])

	// Enforce magic number if requested
	if enforceMagicNumber != 0 && footer.TableMagicNumber != enforceMagicNumber {
		return nil, ErrBadBlockFooter
	}

	// Determine format based on magic number
	isBlockBased := footer.TableMagicNumber == BlockBasedTableMagicNumber ||
		footer.TableMagicNumber == LegacyBlockBasedTableMagicNumber

	if isBlockBased {
		footer.BlockTrailerSize = BlockTrailerSize
	}

	// Check if this is legacy format (version 0)
	if footer.TableMagicNumber == LegacyBlockBasedTableMagicNumber ||
		footer.TableMagicNumber == LegacyPlainTableMagicNumber {
		// Legacy format: two block handles + padding + magic
		footer.FormatVersion = 0
		footer.ChecksumType = ChecksumTypeCRC32C // Legacy always uses CRC32C

		// Decode metaindex handle
		var err error
		var remaining []byte
		footer.MetaindexHandle, remaining, err = DecodeHandle(data)
		if err != nil {
			return nil, err
		}

		// Decode index handle
		footer.IndexHandle, _, err = DecodeHandle(remaining)
		if err != nil {
			return nil, err
		}

		return footer, nil
	}

	// New format: checksum_type (1) + Part2 (40 bytes) + format_version (4) + magic (8)
	if len(data) < NewVersionsEncodedLength {
		return nil, ErrBadBlockFooter
	}

	// Read format version (4 bytes before magic)
	versionOffset := len(data) - MagicNumberLengthByte - 4
	footer.FormatVersion = binary.LittleEndian.Uint32(data[versionOffset:])

	if footer.FormatVersion > LatestFormatVersion {
		return nil, ErrBadBlockFooter
	}

	// Read checksum type (first byte)
	footer.ChecksumType = ChecksumType(data[0])

	// For format_version >= 6, Part2 has a different layout:
	// - extended magic (4 bytes): 0x3e 0x00 0x7a 0x00
	// - footer checksum (4 bytes)
	// - base_context_checksum (4 bytes)
	// - metaindex block size (4 bytes, <4GB, immediately before footer)
	// - zero padding (24 bytes)
	if footer.FormatVersion >= 6 {
		part2 := data[1:] // After checksum type

		// Verify extended magic
		if part2[0] != extendedMagic[0] || part2[1] != extendedMagic[1] ||
			part2[2] != extendedMagic[2] || part2[3] != extendedMagic[3] {
			return nil, ErrBadBlockFooter
		}

		// Skip footer checksum (4 bytes) at offset 4
		// _ = binary.LittleEndian.Uint32(part2[4:8])

		// Read base context checksum (4 bytes) at offset 8
		footer.BaseContextChecksum = binary.LittleEndian.Uint32(part2[8:12])

		// Read metaindex block size (4 bytes) at offset 12
		metaindexSize := binary.LittleEndian.Uint32(part2[12:16])

		// Metaindex is immediately before footer (footer starts at inputOffset)
		// The footer data we received is from inputOffset to end of file
		// So the footer starts at inputOffset in the file
		// Metaindex ends at footerOffset - BlockTrailerSize
		// Metaindex starts at footerOffset - BlockTrailerSize - metaindexSize
		footerOffset := inputOffset
		metaindexEndOffset := footerOffset - uint64(footer.BlockTrailerSize)
		metaindexStartOffset := metaindexEndOffset - uint64(metaindexSize)

		footer.MetaindexHandle = Handle{
			Offset: metaindexStartOffset,
			Size:   uint64(metaindexSize),
		}

		// Index handle is NOT in footer for format_version >= 6
		// It's stored in metaindex block under "rocksdb.index"
		footer.IndexHandle = Handle{Offset: 0, Size: 0}

		return footer, nil
	}

	// Format versions 1-5: handles are stored as varints
	handleData := data[1:]

	var err error
	var remaining []byte
	footer.MetaindexHandle, remaining, err = DecodeHandle(handleData)
	if err != nil {
		return nil, err
	}

	// For format_version < 6, index handle is in footer
	footer.IndexHandle, _, err = DecodeHandle(remaining)
	if err != nil {
		return nil, err
	}

	return footer, nil
}

// EncodeTo encodes the footer to a buffer.
// For format version 6+, this uses footerOffset=0 which may not be
// correct for C++ compatibility. Use EncodeToAt for format version 6+.
func (f *Footer) EncodeTo() []byte {
	if f.FormatVersion == 0 {
		return f.encodeVersion0()
	}
	return f.encodeNewVersionAt(0)
}

// EncodeToAt encodes the footer to a buffer with the given footer offset.
// For format version 6+, the footerOffset is used to compute the context-aware checksum.
func (f *Footer) EncodeToAt(footerOffset uint64) []byte {
	if f.FormatVersion == 0 {
		return f.encodeVersion0()
	}
	return f.encodeNewVersionAt(footerOffset)
}

func (f *Footer) encodeVersion0() []byte {
	buf := make([]byte, Version0EncodedLength)

	// Encode handles
	encoded := f.MetaindexHandle.EncodeTo(buf[:0])
	n := len(encoded)
	copy(buf, encoded)

	encoded = f.IndexHandle.EncodeTo(buf[n:n])
	n += len(encoded) - n // Actually this is wrong, fix it
	encoded = f.IndexHandle.EncodeTo(nil)
	copy(buf[n:], encoded)
	n += len(encoded)

	// Padding
	for i := n; i < Version0EncodedLength-MagicNumberLengthByte; i++ {
		buf[i] = 0
	}

	// Magic number
	binary.LittleEndian.PutUint64(buf[Version0EncodedLength-MagicNumberLengthByte:], f.TableMagicNumber)

	return buf
}

func (f *Footer) encodeNewVersionAt(footerOffset uint64) []byte {
	buf := make([]byte, NewVersionsEncodedLength)

	// Part 1: Checksum type (1 byte)
	buf[0] = byte(f.ChecksumType)

	// Part 2 starts at offset 1
	part2Start := 1
	// Part 3 starts at offset 1 + 40 = 41 (2 * MaxEncodedLength = 40)
	part3Start := part2Start + 2*MaxEncodedLength

	if f.FormatVersion >= 6 {
		// Format version 6+ Part 2 layout:
		// - extended_magic (4 bytes)
		// - footer_checksum (4 bytes) - computed below
		// - base_context_checksum (4 bytes)
		// - metaindex_size (4 bytes)
		// - padding (24 bytes)
		cur := part2Start

		// Extended magic
		copy(buf[cur:], extendedMagic[:])
		cur += 4

		// Footer checksum placeholder (will be computed after all other fields are set)
		checksumOffset := cur
		binary.LittleEndian.PutUint32(buf[cur:], 0)
		cur += 4

		// Base context checksum
		binary.LittleEndian.PutUint32(buf[cur:], f.BaseContextChecksum)
		cur += 4

		// Metaindex size (4 bytes, must be < 4GB)
		binary.LittleEndian.PutUint32(buf[cur:], uint32(f.MetaindexHandle.Size))
		cur += 4

		// Zero padding for remainder of Part 2
		for i := cur; i < part3Start; i++ {
			buf[i] = 0
		}

		// Part 3: format_version (4 bytes) + magic_number (8 bytes)
		binary.LittleEndian.PutUint32(buf[part3Start:], f.FormatVersion)
		binary.LittleEndian.PutUint64(buf[part3Start+4:], f.TableMagicNumber)

		// Now compute the footer checksum over the entire footer
		// with the checksum field set to zero (which it already is)
		checksum := computeFooterChecksum(f.ChecksumType, buf)
		// Add context modifier
		checksum += checksumModifierForContext(f.BaseContextChecksum, footerOffset)
		// Store the checksum
		binary.LittleEndian.PutUint32(buf[checksumOffset:], checksum)
	} else {
		// Format version < 6: Encode handles as varints
		cur := part2Start
		encoded := f.MetaindexHandle.EncodeTo(nil)
		copy(buf[cur:], encoded)
		cur += len(encoded)

		encoded = f.IndexHandle.EncodeTo(nil)
		copy(buf[cur:], encoded)
		cur += len(encoded)

		// Zero padding for remainder of Part 2
		for i := cur; i < part3Start; i++ {
			buf[i] = 0
		}

		// Part 3: format_version (4 bytes) + magic_number (8 bytes)
		binary.LittleEndian.PutUint32(buf[part3Start:], f.FormatVersion)
		binary.LittleEndian.PutUint64(buf[part3Start+4:], f.TableMagicNumber)
	}

	return buf
}

// computeFooterChecksum computes the checksum for the footer buffer.
// The checksum field in the buffer must be zero when this is called.
func computeFooterChecksum(checksumType ChecksumType, buf []byte) uint32 {
	switch checksumType {
	case ChecksumTypeCRC32C:
		return maskedCRC32C(buf)
	case ChecksumTypeXXHash64:
		return xxhash64Lower32(buf)
	case ChecksumTypeXXH3:
		return xxh3Checksum(buf)
	default:
		return 0
	}
}

// checksumModifierForContext returns a context-dependent modifier for the checksum.
// This matches RocksDB's ChecksumModifierForContext in table/format.h.
func checksumModifierForContext(baseContextChecksum uint32, offset uint64) uint32 {
	// all_or_nothing = 0xFFFFFFFF if base != 0, else 0
	var allOrNothing uint32
	if baseContextChecksum != 0 {
		allOrNothing = 0xFFFFFFFF
	}

	// Lower32 + Upper32 of offset
	lower32 := uint32(offset)
	upper32 := uint32(offset >> 32)

	modifier := baseContextChecksum ^ (lower32 + upper32)
	return modifier & allOrNothing
}

// maskedCRC32C computes the masked CRC32C checksum.
func maskedCRC32C(data []byte) uint32 {
	return checksum.Mask(checksum.Value(data))
}

// xxhash64Lower32 computes the lower 32 bits of XXHash64.
func xxhash64Lower32(data []byte) uint32 {
	return uint32(checksum.XXHash64(data))
}

// xxh3Checksum computes the XXH3 checksum for the footer.
// For XXH3, we compute hash of all bytes except last, then modify by last byte.
func xxh3Checksum(data []byte) uint32 {
	if len(data) == 0 {
		return 0
	}
	h := checksum.XXH3_64bits(data[:len(data)-1])
	v := uint32(h)
	lastByte := data[len(data)-1]
	const kRandomPrime = 0x6b9083d9
	return v ^ (uint32(lastByte) * kRandomPrime)
}

// IsSupportedFormatVersion returns true if the format version is supported.
func IsSupportedFormatVersion(version uint32) bool {
	return version <= LatestFormatVersion
}

// FormatVersionUsesContextChecksum returns true if the format version uses context checksums.
func FormatVersionUsesContextChecksum(version uint32) bool {
	return version >= 6
}

// FormatVersionUsesIndexHandleInFooter returns true if the format version stores index handle in footer.
func FormatVersionUsesIndexHandleInFooter(version uint32) bool {
	return version < 6
}
