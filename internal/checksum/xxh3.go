// Package checksum provides checksum functions compatible with RocksDB.
//
// XXH3 implementation uses the well-tested github.com/zeebo/xxh3 library.
// RocksDB v10.7.5 uses XXH3_64bits() for block checksums.
//
// Reference: RocksDB v10.7.5
//   - table/format.cc (ComputeBuiltinChecksumWithLastByte)

package checksum

import (
	"github.com/zeebo/xxh3"
)

// XXH3_64bits computes the 64-bit XXH3 hash of data.
// This uses the zeebo/xxh3 library which is bit-compatible with the official xxHash.
func XXH3_64bits(data []byte) uint64 {
	return xxh3.Hash(data)
}

// XXH3Checksum computes the RocksDB-style XXH3 checksum for a block.
// This matches ComputeBuiltinChecksum with kXXH3 in RocksDB.
// The checksum is computed over all bytes except the last, then modified
// by the last byte using a special formula.
func XXH3Checksum(data []byte) uint32 {
	if len(data) == 0 {
		return 0
	}

	// Compute XXH3 over all bytes except last
	h := XXH3_64bits(data[:len(data)-1])
	v := uint32(h) // Lower 32 bits

	// Modify checksum for last byte
	lastByte := data[len(data)-1]
	const kRandomPrime = 0x6b9083d9
	return v ^ (uint32(lastByte) * kRandomPrime)
}

// XXH3ChecksumWithLastByte computes XXH3 checksum with a separate last byte.
// This is used when the last byte (compression type) is not in the data buffer.
// This matches ComputeBuiltinChecksumWithLastByte with kXXH3 in RocksDB.
func XXH3ChecksumWithLastByte(data []byte, lastByte byte) uint32 {
	// Compute XXH3 over all data
	h := XXH3_64bits(data)
	v := uint32(h) // Lower 32 bits

	// Modify checksum for last byte
	// This formula is from RocksDB table/format.cc:ModifyChecksumForLastByte
	const kRandomPrime = 0x6b9083d9
	return v ^ (uint32(lastByte) * kRandomPrime)
}
