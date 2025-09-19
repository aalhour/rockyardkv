// types.go defines checksum type constants matching RocksDB.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/table.h (ChecksumType enum)
package checksum

// Type represents the type of checksum algorithm.
type Type uint8

const (
	// TypeNoChecksum means no checksum is used.
	TypeNoChecksum Type = 0
	// TypeCRC32C is CRC32C (Castagnoli) checksum.
	TypeCRC32C Type = 1
	// TypeXXHash is XXHash32 checksum.
	TypeXXHash Type = 2
	// TypeXXHash64 is XXHash64 checksum.
	TypeXXHash64 Type = 3
	// TypeXXH3 is XXH3 checksum (used in RocksDB format_version 5+).
	TypeXXH3 Type = 4
)

// String returns a human-readable name for the checksum type.
func (t Type) String() string {
	switch t {
	case TypeNoChecksum:
		return "NoChecksum"
	case TypeCRC32C:
		return "CRC32C"
	case TypeXXHash:
		return "XXHash"
	case TypeXXHash64:
		return "XXHash64"
	case TypeXXH3:
		return "XXH3"
	default:
		return "Unknown"
	}
}

// ComputeCRC32CChecksumWithLastByte computes CRC32C checksum with a separate last byte.
// This is used for block checksums where the compression type is not in the data buffer.
func ComputeCRC32CChecksumWithLastByte(data []byte, lastByte byte) uint32 {
	// Extend CRC with the last byte
	crc := Value(data)
	crc = Extend(crc, []byte{lastByte})
	return Mask(crc)
}

// ComputeXXH3ChecksumWithLastByte computes XXH3 checksum with a separate last byte.
// This is used for block checksums where the compression type is not in the data buffer.
func ComputeXXH3ChecksumWithLastByte(data []byte, lastByte byte) uint32 {
	return XXH3ChecksumWithLastByte(data, lastByte)
}

// ComputeXXHash64ChecksumWithLastByte computes XXHash64 checksum with a separate last byte.
// This is used for block checksums where the compression type is not in the data buffer.
func ComputeXXHash64ChecksumWithLastByte(data []byte, lastByte byte) uint32 {
	return XXHash64ChecksumWithLastByte(data, lastByte)
}

// ComputeChecksum computes a checksum of the given type.
// For block checksums, data is the block content and lastByte is the compression type.
func ComputeChecksum(t Type, data []byte, lastByte byte) uint32 {
	switch t {
	case TypeCRC32C:
		return ComputeCRC32CChecksumWithLastByte(data, lastByte)
	case TypeXXHash64:
		return ComputeXXHash64ChecksumWithLastByte(data, lastByte)
	case TypeXXH3:
		return ComputeXXH3ChecksumWithLastByte(data, lastByte)
	case TypeNoChecksum:
		return 0
	default:
		// For unsupported types, return 0
		return 0
	}
}
