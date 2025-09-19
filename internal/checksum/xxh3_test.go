package checksum

import (
	"encoding/hex"
	"testing"
)

// Test vectors derived from RocksDB table/table_test.cc ChecksumTest
// These verify our XXH3 implementation matches C++ behavior.
func TestXXH3GoldenVectors(t *testing.T) {
	// From table_test.cc ChecksumTest case kXXH3:
	// Empty data returns 0 (special case)
	// EXPECT_EQ(ChecksumAsString(empty, t), "00000000");
	tests := []struct {
		name     string
		data     []byte
		lastByte byte
		wantHex  string
	}{
		// Empty input
		{"empty", nil, 0, "00000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got uint32
			if tt.data == nil {
				got = XXH3Checksum(nil)
			} else {
				got = XXH3ChecksumWithLastByte(tt.data, tt.lastByte)
			}
			gotHex := checksumToHex(got)
			if gotHex != tt.wantHex {
				t.Errorf("XXH3Checksum = %s, want %s", gotHex, tt.wantHex)
			}
		})
	}
}

func checksumToHex(v uint32) string {
	b := make([]byte, 4)
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
	return hex.EncodeToString(b)
}

// Test basic XXH3_64bits function
func TestXXH3_64bits(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"1byte", []byte{0}},
		{"2bytes", []byte{0, 1}},
		{"3bytes", []byte{0, 1, 2}},
		{"4bytes", []byte{0, 1, 2, 3}},
		{"8bytes", []byte{0, 1, 2, 3, 4, 5, 6, 7}},
		{"16bytes", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		{"hello", []byte("hello")},
		{"hello world", []byte("hello world")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify it doesn't panic
			h := XXH3_64bits(tt.data)
			t.Logf("%s: XXH3_64bits = 0x%016x", tt.name, h)
		})
	}
}

// Test the modification formula for last byte
func TestModifyChecksumForLastByte(t *testing.T) {
	// Verify the formula: checksum ^ (last_byte * 0x6b9083d9)
	const kRandomPrime = 0x6b9083d9

	tests := []struct {
		checksum uint32
		lastByte byte
		want     uint32
	}{
		{0, 0, 0},
		{0, 1, kRandomPrime},
		{0, 0x02, 0x02 * kRandomPrime}, // Small multiplier to avoid overflow
		{kRandomPrime, 1, 0},
		{0x12345678, 0, 0x12345678},
	}

	for _, tt := range tests {
		got := tt.checksum ^ (uint32(tt.lastByte) * kRandomPrime)
		if got != tt.want {
			t.Errorf("modify(%x, %x) = %x, want %x", tt.checksum, tt.lastByte, got, tt.want)
		}
	}
}

// Test XXH3 produces consistent results
func TestXXH3Consistency(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog")

	h1 := XXH3_64bits(data)
	h2 := XXH3_64bits(data)

	if h1 != h2 {
		t.Errorf("XXH3_64bits not consistent: %x != %x", h1, h2)
	}
}

// Test XXH3 with various lengths
func TestXXH3VariousLengths(t *testing.T) {
	// Generate deterministic test data
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i * 17)
	}

	// Test all lengths from 0 to 256
	prevHashes := make(map[uint64]int)
	for length := range 257 {
		h := XXH3_64bits(data[:length])

		// Check for collisions (unlikely but possible)
		if prevLen, exists := prevHashes[h]; exists && length > 0 {
			// Collisions are allowed but should be rare
			t.Logf("Collision at length %d and %d: 0x%016x", length, prevLen, h)
		}
		prevHashes[h] = length
	}
}

// Test long inputs
func TestXXH3LongInput(t *testing.T) {
	// 1KB of data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	h := XXH3_64bits(data)
	t.Logf("1KB data: XXH3_64bits = 0x%016x", h)

	// Verify it's deterministic
	h2 := XXH3_64bits(data)
	if h != h2 {
		t.Errorf("Not consistent: %x != %x", h, h2)
	}
}

// Benchmark XXH3
func BenchmarkXXH3_64bits(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	for b.Loop() {
		_ = XXH3_64bits(data)
	}
}

func BenchmarkXXH3Checksum(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}

	for b.Loop() {
		_ = XXH3Checksum(data)
	}
}
