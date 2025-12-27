// XXH3 adversarial tests
//
// Reference: RocksDB v10.7.5
//   - table/format.cc (ComputeBuiltinChecksum, ComputeBuiltinChecksumWithLastByte)
//
// These tests verify that the XXH3 implementation matches the official spec
// and produces checksums compatible with RocksDB.

package checksum

import (
	"testing"
)

// TestAdversarial_XXH3_OfficialTestVectors verifies XXH3 against official test vectors.
// This is the "breach" test that would have caught the bug before the fix.
func TestAdversarial_XXH3_OfficialTestVectors(t *testing.T) {
	testCases := []struct {
		name     string
		data     []byte
		expected uint64
	}{
		// Official XXH3 test vectors from https://github.com/Cyan4973/xxHash
		{
			name:     "empty",
			data:     nil,
			expected: 0x2d06800538d394c2,
		},
		{
			name:     "single byte",
			data:     []byte{0x00},
			expected: 0xc44bdff4074eecdb,
		},
		{
			name:     "hello",
			data:     []byte("hello"),
			expected: 0x9555e8555c62dcfd,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := XXH3_64bits(tc.data)
			if got != tc.expected {
				t.Errorf("XXH3_64bits(%q) = 0x%016x, want 0x%016x",
					tc.data, got, tc.expected)
			}
		})
	}
}

// TestAdversarial_XXH3ChecksumWithLastByte verifies the RocksDB-specific checksum formula.
func TestAdversarial_XXH3ChecksumWithLastByte(t *testing.T) {
	// The formula is: lower32(XXH3(data)) XOR (lastByte * kRandomPrime)
	// where kRandomPrime = 0x6b9083d9
	const kRandomPrime = 0x6b9083d9

	testCases := []struct {
		name     string
		data     []byte
		lastByte byte
	}{
		{"empty_zero", nil, 0x00},
		{"empty_nonzero", nil, 0x01},
		{"hello_zero", []byte("hello"), 0x00},
		{"hello_compression", []byte("hello"), 0x01}, // Simulate compression type
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := XXH3ChecksumWithLastByte(tc.data, tc.lastByte)

			// Compute expected value manually
			h := XXH3_64bits(tc.data)
			expected := uint32(h) ^ (uint32(tc.lastByte) * kRandomPrime)

			if got != expected {
				t.Errorf("XXH3ChecksumWithLastByte(%q, 0x%02x) = 0x%08x, want 0x%08x",
					tc.data, tc.lastByte, got, expected)
			}
		})
	}
}

// TestAdversarial_XXH3_Deterministic verifies that XXH3 is deterministic.
func TestAdversarial_XXH3_Deterministic(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")

	h1 := XXH3_64bits(data)
	h2 := XXH3_64bits(data)
	h3 := XXH3_64bits(data)

	if h1 != h2 || h2 != h3 {
		t.Errorf("XXH3 not deterministic: got %x, %x, %x", h1, h2, h3)
	}
}

// TestAdversarial_XXH3_DifferentInputs verifies that different inputs produce different hashes.
func TestAdversarial_XXH3_DifferentInputs(t *testing.T) {
	inputs := [][]byte{
		nil,
		{},
		{0x00},
		{0x01},
		[]byte("a"),
		[]byte("b"),
		[]byte("hello"),
		[]byte("Hello"),
	}

	seen := make(map[uint64][]byte)
	for _, input := range inputs {
		h := XXH3_64bits(input)
		if prev, ok := seen[h]; ok {
			// Note: empty slice and nil produce the same hash, which is expected
			if len(prev) == 0 && len(input) == 0 {
				continue
			}
			t.Errorf("hash collision: XXH3(%q) = XXH3(%q) = 0x%x", prev, input, h)
		}
		seen[h] = input
	}
}
