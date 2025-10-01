// Package filter implements Bloom filters for SST files.
//
// This package provides a RocksDB-compatible Bloom filter implementation
// based on FastLocalBloom (format_version=5). The filter is cache-local,
// meaning all probes for a key occur within a single 64-byte cache line.
//
// Filter Block Format (at end of filter data):
//
//	data[0:len-5]  = Bloom filter bits (cache-line aligned chunks)
//	data[len-5]    = -1 (0xFF, marker for newer Bloom implementations)
//	data[len-4]    = 0 (sub-implementation marker for FastLocalBloom)
//	data[len-3]    = num_probes (number of hash probes per key)
//	data[len-2]    = 0 (block size indicator: 0 = 64 bytes)
//	data[len-1]    = 0 (reserved)
//
// Reference: RocksDB v10.7.5
//   - util/bloom_impl.h (FastLocalBloomImpl)
//   - table/block_based/filter_policy.cc (FastLocalBloomBitsBuilder)
package filter

import (
	"github.com/aalhour/rockyardkv/internal/checksum"
)

const (
	// CacheLineSize is the size of a CPU cache line in bytes (Intel).
	CacheLineSize = 64

	// CacheLineBits is the number of bits in a cache line.
	CacheLineBits = CacheLineSize * 8 // 512 bits

	// MetadataLen is the number of metadata bytes at the end of the filter.
	MetadataLen = 5

	// NewBloomMarker marks newer Bloom filter implementations (format_version=5+).
	NewBloomMarker = byte(0xFF) // -1 as signed byte

	// FastLocalBloomMarker identifies FastLocalBloom sub-implementation.
	FastLocalBloomMarker = byte(0x00)
)

// BloomFilterBuilder builds a Bloom filter from a set of keys.
type BloomFilterBuilder struct {
	bitsPerKey int      // Target bits per key (e.g., 10)
	hashes     []uint64 // Collected key hashes
}

// NewBloomFilterBuilder creates a new Bloom filter builder.
// bitsPerKey controls filter accuracy (10 = ~1% false positive rate).
func NewBloomFilterBuilder(bitsPerKey int) *BloomFilterBuilder {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	return &BloomFilterBuilder{
		bitsPerKey: bitsPerKey,
		hashes:     make([]uint64, 0, 256),
	}
}

// AddKey adds a key to the filter.
func (b *BloomFilterBuilder) AddKey(key []byte) {
	// Use XXH3 hash (same as RocksDB)
	h := checksum.XXH3_64bits(key)
	b.hashes = append(b.hashes, h)
}

// EstimatedSize returns the estimated filter size in bytes.
func (b *BloomFilterBuilder) EstimatedSize() int {
	numEntries := len(b.hashes)
	if numEntries == 0 {
		return 0
	}
	return calculateSpace(numEntries, b.bitsPerKey)
}

// Finish builds the filter and returns the filter data.
// The returned slice includes the metadata suffix.
func (b *BloomFilterBuilder) Finish() []byte {
	numEntries := len(b.hashes)
	if numEntries == 0 {
		// Empty filter: return metadata only with always-false marker
		return []byte{NewBloomMarker, FastLocalBloomMarker, 0, 0, 0}
	}

	// Calculate filter size
	lenWithMetadata := calculateSpace(numEntries, b.bitsPerKey)
	filterLen := lenWithMetadata - MetadataLen

	// Allocate filter data
	data := make([]byte, lenWithMetadata)

	// Calculate number of probes
	numProbes := chooseNumProbes(b.bitsPerKey * 1000) // millibits

	// Add all keys to filter
	for _, h := range b.hashes {
		addHash(h, uint32(filterLen), numProbes, data)
	}

	// Write metadata at end
	data[filterLen+0] = NewBloomMarker       // -1 marker
	data[filterLen+1] = FastLocalBloomMarker // sub-implementation
	data[filterLen+2] = byte(numProbes)      // num_probes
	data[filterLen+3] = 0                    // block size (0 = 64 bytes)
	data[filterLen+4] = 0                    // reserved

	// Clear hashes for potential reuse
	b.hashes = b.hashes[:0]

	return data
}

// Reset clears the builder for reuse.
func (b *BloomFilterBuilder) Reset() {
	b.hashes = b.hashes[:0]
}

// NumKeys returns the number of keys added.
func (b *BloomFilterBuilder) NumKeys() int {
	return len(b.hashes)
}

// BloomFilterReader reads a Bloom filter.
type BloomFilterReader struct {
	data      []byte
	filterLen uint32
	numProbes int
}

// NewBloomFilterReader creates a reader from filter data.
// Returns nil if the filter is empty or invalid.
func NewBloomFilterReader(data []byte) *BloomFilterReader {
	if len(data) < MetadataLen {
		return nil
	}

	filterLen := len(data) - MetadataLen

	// Check markers
	if data[filterLen] != NewBloomMarker {
		// Legacy filter format not supported
		return nil
	}
	if data[filterLen+1] != FastLocalBloomMarker {
		// Unknown sub-implementation
		return nil
	}

	numProbes := int(data[filterLen+2])
	if numProbes == 0 {
		// Always-false filter
		return &BloomFilterReader{
			data:      data,
			filterLen: 0,
			numProbes: 0,
		}
	}

	return &BloomFilterReader{
		data:      data,
		filterLen: uint32(filterLen),
		numProbes: numProbes,
	}
}

// MayContain returns true if the key may be in the set.
// A false return means the key is definitely not in the set.
// A true return means the key might be in the set (false positive possible).
func (r *BloomFilterReader) MayContain(key []byte) bool {
	if r == nil || r.filterLen == 0 || r.numProbes == 0 {
		return false // Empty or always-false filter
	}

	h := checksum.XXH3_64bits(key)
	return hashMayMatch(h, r.filterLen, r.numProbes, r.data)
}

// calculateSpace calculates the filter size including metadata.
func calculateSpace(numEntries, bitsPerKey int) int {
	// Total bits needed
	totalBits := numEntries * bitsPerKey

	// Round up to cache line size
	numCacheLines := (totalBits + CacheLineBits - 1) / CacheLineBits
	if numCacheLines == 0 {
		numCacheLines = 1
	}

	return numCacheLines*CacheLineSize + MetadataLen
}

// chooseNumProbes determines the optimal number of hash probes.
// millibitsPerKey is bits_per_key * 1000.
// Reference: FastLocalBloomImpl::ChooseNumProbes in bloom_impl.h
func chooseNumProbes(millibitsPerKey int) int {
	switch {
	case millibitsPerKey <= 2080:
		return 1
	case millibitsPerKey <= 3580:
		return 2
	case millibitsPerKey <= 5100:
		return 3
	case millibitsPerKey <= 6640:
		return 4
	case millibitsPerKey <= 8300:
		return 5
	case millibitsPerKey <= 10070:
		return 6
	case millibitsPerKey <= 11720:
		return 7
	case millibitsPerKey <= 14001:
		return 8
	case millibitsPerKey <= 16050:
		return 9
	case millibitsPerKey <= 18300:
		return 10
	case millibitsPerKey <= 22001:
		return 11
	case millibitsPerKey <= 25501:
		return 12
	case millibitsPerKey > 50000:
		return 24
	default:
		return (millibitsPerKey-1)/2000 - 1
	}
}

// fastRange32 computes (h * n) >> 32, which gives a value in [0, n).
// This is faster than h % n for uniformly distributed h.
func fastRange32(h, n uint32) uint32 {
	return uint32((uint64(h) * uint64(n)) >> 32)
}

// addHash adds a hash value to the filter.
// Reference: FastLocalBloomImpl::AddHash
func addHash(hash uint64, lenBytes uint32, numProbes int, data []byte) {
	// Split 64-bit hash into two 32-bit values
	h1 := uint32(hash)
	h2 := uint32(hash >> 32)

	// Select cache line using h1
	numCacheLines := lenBytes >> 6 // divide by 64
	cacheLineOffset := fastRange32(h1, numCacheLines) << 6

	// Probe within cache line using h2
	addHashPrepared(h2, numProbes, data[cacheLineOffset:cacheLineOffset+CacheLineSize])
}

// addHashPrepared adds probes to a specific cache line.
// Reference: FastLocalBloomImpl::AddHashPrepared
func addHashPrepared(h2 uint32, numProbes int, cacheLine []byte) {
	h := h2
	for range numProbes {
		// 9-bit address within 512-bit cache line
		bitpos := h >> (32 - 9)
		cacheLine[bitpos>>3] |= 1 << (bitpos & 7)

		// Golden ratio multiplication for next probe
		h *= 0x9e3779b9
	}
}

// hashMayMatch checks if a hash value may be in the filter.
// Reference: FastLocalBloomImpl::HashMayMatch
func hashMayMatch(hash uint64, lenBytes uint32, numProbes int, data []byte) bool {
	// Split 64-bit hash into two 32-bit values
	h1 := uint32(hash)
	h2 := uint32(hash >> 32)

	// Select cache line using h1
	numCacheLines := lenBytes >> 6 // divide by 64
	cacheLineOffset := fastRange32(h1, numCacheLines) << 6

	// Check probes within cache line using h2
	return hashMayMatchPrepared(h2, numProbes, data[cacheLineOffset:cacheLineOffset+CacheLineSize])
}

// hashMayMatchPrepared checks probes within a specific cache line.
// Reference: FastLocalBloomImpl::HashMayMatchPrepared (non-AVX2 path)
func hashMayMatchPrepared(h2 uint32, numProbes int, cacheLine []byte) bool {
	h := h2
	for range numProbes {
		// 9-bit address within 512-bit cache line
		bitpos := h >> (32 - 9)
		if (cacheLine[bitpos>>3] & (1 << (bitpos & 7))) == 0 {
			return false
		}

		// Golden ratio multiplication for next probe
		h *= 0x9e3779b9
	}
	return true
}
