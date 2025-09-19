// Package checksum provides checksum algorithms compatible with RocksDB.
//
// This package implements:
// - CRC32C (Castagnoli) with RocksDB-compatible masking
// - XXHash32/64
//
// The implementations are bit-compatible with RocksDB's checksum functions.
//
// Reference: RocksDB v10.7.5
//   - util/crc32c.h
//   - util/crc32c.cc
package checksum

import (
	"hash/crc32"
)

// crc32cTable is the Castagnoli polynomial table used for CRC32C.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// maskDelta is the constant added during masking.
// From RocksDB: static const uint32_t kMaskDelta = 0xa282ead8ul;
const maskDelta = 0xa282ead8

// Value computes the CRC32C checksum of data.
// This is equivalent to RocksDB's crc32c::Value().
func Value(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// Extend computes the CRC32C of concat(A, data) where initCRC is the CRC32C of A.
// This is equivalent to RocksDB's crc32c::Extend().
func Extend(initCRC uint32, data []byte) uint32 {
	return crc32.Update(initCRC, crc32cTable, data)
}

// Mask returns a masked representation of crc.
//
// From RocksDB comments:
// Motivation: it is problematic to compute the CRC of a string that
// contains embedded CRCs. Therefore we recommend that CRCs stored
// somewhere (e.g., in files) should be masked before being stored.
//
// This is equivalent to RocksDB's crc32c::Mask().
func Mask(crc uint32) uint32 {
	// Rotate right by 15 bits and add a constant.
	return ((crc >> 15) | (crc << 17)) + maskDelta
}

// Unmask returns the crc whose masked representation is maskedCRC.
// This is equivalent to RocksDB's crc32c::Unmask().
func Unmask(maskedCRC uint32) uint32 {
	rot := maskedCRC - maskDelta
	return (rot >> 17) | (rot << 15)
}

// MaskedValue computes the CRC32C and masks it in one call.
// This is a convenience function equivalent to Mask(Value(data)).
func MaskedValue(data []byte) uint32 {
	return Mask(Value(data))
}

// MaskedExtend extends an existing CRC and masks the result.
// This is equivalent to Mask(Extend(initCRC, data)).
func MaskedExtend(initCRC uint32, data []byte) uint32 {
	return Mask(Extend(initCRC, data))
}
