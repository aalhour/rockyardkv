// Package checksum provides checksum functions compatible with RocksDB.
//
// XXHash64 implementation based on the xxHash specification.
// Reference: https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md

package checksum

import (
	"encoding/binary"
)

// XXHash64 constants
const (
	xxh64Prime1 uint64 = 0x9E3779B185EBCA87
	xxh64Prime2 uint64 = 0xC2B2AE3D27D4EB4F
	xxh64Prime3 uint64 = 0x165667B19E3779F9
	xxh64Prime4 uint64 = 0x85EBCA77C2B2AE63
	xxh64Prime5 uint64 = 0x27D4EB2F165667C5
)

// XXHash64 computes the 64-bit XXHash of data.
func XXHash64(data []byte) uint64 {
	return XXHash64WithSeed(data, 0)
}

// XXHash64WithSeed computes the 64-bit XXHash of data with a seed.
func XXHash64WithSeed(data []byte, seed uint64) uint64 {
	n := len(data)
	var h64 uint64

	if n >= 32 {
		// Initialize accumulators
		v1 := seed + xxh64Prime1 + xxh64Prime2
		v2 := seed + xxh64Prime2
		v3 := seed
		v4 := seed - xxh64Prime1

		// Process 32-byte blocks
		for len(data) >= 32 {
			v1 = xxh64Round(v1, binary.LittleEndian.Uint64(data[0:8]))
			v2 = xxh64Round(v2, binary.LittleEndian.Uint64(data[8:16]))
			v3 = xxh64Round(v3, binary.LittleEndian.Uint64(data[16:24]))
			v4 = xxh64Round(v4, binary.LittleEndian.Uint64(data[24:32]))
			data = data[32:]
		}

		// Merge accumulators
		h64 = xxh64RotateLeft(v1, 1) + xxh64RotateLeft(v2, 7) +
			xxh64RotateLeft(v3, 12) + xxh64RotateLeft(v4, 18)
		h64 = xxh64MergeRound(h64, v1)
		h64 = xxh64MergeRound(h64, v2)
		h64 = xxh64MergeRound(h64, v3)
		h64 = xxh64MergeRound(h64, v4)
	} else {
		h64 = seed + xxh64Prime5
	}

	h64 += uint64(n)

	// Process remaining bytes
	for len(data) >= 8 {
		k1 := xxh64Round(0, binary.LittleEndian.Uint64(data[:8]))
		h64 ^= k1
		h64 = xxh64RotateLeft(h64, 27)*xxh64Prime1 + xxh64Prime4
		data = data[8:]
	}

	for len(data) >= 4 {
		h64 ^= uint64(binary.LittleEndian.Uint32(data[:4])) * xxh64Prime1
		h64 = xxh64RotateLeft(h64, 23)*xxh64Prime2 + xxh64Prime3
		data = data[4:]
	}

	for len(data) > 0 {
		h64 ^= uint64(data[0]) * xxh64Prime5
		h64 = xxh64RotateLeft(h64, 11) * xxh64Prime1
		data = data[1:]
	}

	// Final avalanche
	h64 = xxh64Avalanche(h64)

	return h64
}

// xxh64Round applies one round of the xxh64 algorithm.
func xxh64Round(acc, input uint64) uint64 {
	acc += input * xxh64Prime2
	acc = xxh64RotateLeft(acc, 31)
	acc *= xxh64Prime1
	return acc
}

// xxh64MergeRound merges an accumulator into the hash.
func xxh64MergeRound(acc, val uint64) uint64 {
	val = xxh64Round(0, val)
	acc ^= val
	acc = acc*xxh64Prime1 + xxh64Prime4
	return acc
}

// xxh64RotateLeft rotates a uint64 left by n bits.
func xxh64RotateLeft(x uint64, n uint) uint64 {
	return (x << n) | (x >> (64 - n))
}

// xxh64Avalanche performs the final avalanche step.
func xxh64Avalanche(h uint64) uint64 {
	h ^= h >> 33
	h *= xxh64Prime2
	h ^= h >> 29
	h *= xxh64Prime3
	h ^= h >> 32
	return h
}

// XXHash64ChecksumWithLastByte computes XXHash64 checksum with a separate last byte,
// returning the lower 32 bits as used by RocksDB.
func XXHash64ChecksumWithLastByte(data []byte, lastByte byte) uint32 {
	// Create a buffer with the extra byte
	buf := make([]byte, len(data)+1)
	copy(buf, data)
	buf[len(data)] = lastByte

	// Compute XXHash64 and return lower 32 bits
	h64 := XXHash64(buf)
	return uint32(h64)
}
