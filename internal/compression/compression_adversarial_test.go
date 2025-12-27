// compression_adversarial_test.go contains adversarial tests for compression
// handling, including edge cases and malformed input.
//
// These tests verify that we handle C++ RocksDB compressed data correctly,
// particularly the raw deflate format used by zlib compression.
package compression

import (
	"bytes"
	"compress/flate"
	"testing"
)

// TestAdversarial_ZlibRawDeflateVariousSizes tests raw deflate with various data sizes.
func TestAdversarial_ZlibRawDeflateVariousSizes(t *testing.T) {
	sizes := []int{0, 1, 10, 100, 1000, 10000, 100000}

	for _, size := range sizes {
		t.Run(sizeTestName(size), func(t *testing.T) {
			// Create test data
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Compress with raw deflate (like RocksDB does)
			var buf bytes.Buffer
			w, err := flate.NewWriter(&buf, flate.DefaultCompression)
			if err != nil {
				t.Fatalf("NewWriter error: %v", err)
			}
			w.Write(data)
			w.Close()

			compressed := buf.Bytes()

			// Our Decompress should handle this
			result, err := Decompress(ZlibCompression, compressed)
			if err != nil {
				t.Fatalf("Decompress error: %v", err)
			}

			if !bytes.Equal(result, data) {
				t.Errorf("Decompressed data mismatch: got %d bytes, want %d", len(result), len(data))
			}
		})
	}
}

// TestAdversarial_ZlibTruncatedData tests behavior with truncated compressed data.
func TestAdversarial_ZlibTruncatedData(t *testing.T) {
	data := bytes.Repeat([]byte("test data for compression "), 100)

	// Compress with raw deflate
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(data)
	w.Close()

	compressed := buf.Bytes()

	// Try various truncation points
	truncPoints := []int{1, 5, 10, len(compressed) / 2, len(compressed) - 1}

	for _, truncAt := range truncPoints {
		if truncAt >= len(compressed) {
			continue
		}

		t.Run(sizeTestName(truncAt)+"_truncated", func(t *testing.T) {
			truncated := compressed[:truncAt]
			_, err := Decompress(ZlibCompression, truncated)
			// Should either fail or return partial data, but not panic
			if err != nil {
				t.Logf("Truncation at %d bytes: error = %v (expected)", truncAt, err)
			}
		})
	}
}

// TestAdversarial_ZlibGarbageData tests behavior with random garbage.
func TestAdversarial_ZlibGarbageData(t *testing.T) {
	garbage := [][]byte{
		{0x00},
		{0xFF, 0xFF, 0xFF, 0xFF},
		{0x78, 0x9C}, // Looks like zlib header but garbage after
		bytes.Repeat([]byte{0xAB}, 100),
	}

	for i, data := range garbage {
		t.Run(sizeTestName(i), func(t *testing.T) {
			_, err := Decompress(ZlibCompression, data)
			// Should fail gracefully, not panic
			if err != nil {
				t.Logf("Garbage test %d: error = %v (expected)", i, err)
			}
		})
	}
}

// TestAdversarial_ZlibBothFormatsRoundTrip tests that we can handle both
// standard zlib (with header) and raw deflate formats.
func TestAdversarial_ZlibBothFormatsRoundTrip(t *testing.T) {
	data := []byte("test data that needs compression for proper testing")

	// Our Compress uses standard zlib with header
	compressed, err := Compress(ZlibCompression, data)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	// Should be able to decompress our own output
	result, err := Decompress(ZlibCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Error("Round trip failed")
	}
}

// TestAdversarial_AllCompressionTypesWithCorruptedInput tests that all
// compression types handle corrupted input gracefully.
func TestAdversarial_AllCompressionTypesWithCorruptedInput(t *testing.T) {
	types := []Type{
		SnappyCompression,
		ZlibCompression,
		LZ4Compression,
		LZ4HCCompression,
		ZstdCompression,
	}

	garbage := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 100)

	for _, ct := range types {
		t.Run(ct.String(), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic with corrupted %s input: %v", ct, r)
				}
			}()

			_, err := Decompress(ct, garbage)
			// Should fail but not panic
			if err != nil {
				t.Logf("%s with garbage: error = %v (expected)", ct, err)
			}
		})
	}
}

func sizeTestName(size int) string {
	return "size_" + itoa(size)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
