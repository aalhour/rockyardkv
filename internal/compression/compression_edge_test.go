package compression

import (
	"testing"
)

// TestCompressionTypeStringAllValues tests String() for all compression types
func TestCompressionTypeStringAllValues(t *testing.T) {
	testCases := []struct {
		ct   Type
		want string
	}{
		{NoCompression, "NoCompression"},
		{SnappyCompression, "Snappy"},
		{ZlibCompression, "Zlib"},
		{BZip2Compression, "BZip2"},
		{LZ4Compression, "LZ4"},
		{LZ4HCCompression, "LZ4HC"},
		{XpressCompression, "Xpress"},
		{ZstdCompression, "ZSTD"},
		{Type(255), "Unknown(255)"}, // Unknown type
	}

	for _, tc := range testCases {
		got := tc.ct.String()
		if got != tc.want {
			t.Errorf("Type(%d).String() = %q, want %q", tc.ct, got, tc.want)
		}
	}
}

// TestCompressUnsupportedTypes tests Compress with unsupported types
func TestCompressUnsupportedTypes(t *testing.T) {
	data := []byte("test data to compress")

	// Test unsupported compression types
	unsupportedTypes := []Type{BZip2Compression, XpressCompression}
	for _, ct := range unsupportedTypes {
		_, err := Compress(ct, data)
		if err == nil {
			t.Errorf("Compress(%v) should return error for unsupported type", ct)
		}
	}
}

// TestDecompressUnsupportedTypes tests Decompress with unsupported types
func TestDecompressUnsupportedTypes(t *testing.T) {
	data := []byte("some compressed data placeholder")

	unsupportedTypes := []Type{BZip2Compression, XpressCompression}
	for _, ct := range unsupportedTypes {
		_, err := Decompress(ct, data)
		if err == nil {
			t.Errorf("Decompress(%v) should return error for unsupported type", ct)
		}
	}
}

// TestCompressEmptyData tests compression with empty data
func TestCompressEmptyData(t *testing.T) {
	supportedTypes := []Type{NoCompression, SnappyCompression, ZlibCompression}
	for _, ct := range supportedTypes {
		if !ct.IsSupported() {
			continue
		}
		compressed, err := Compress(ct, []byte{})
		if err != nil {
			t.Errorf("Compress(%v) empty data failed: %v", ct, err)
			continue
		}

		decompressed, err := Decompress(ct, compressed)
		if err != nil {
			t.Errorf("Decompress(%v) empty data failed: %v", ct, err)
			continue
		}

		if len(decompressed) != 0 {
			t.Errorf("Decompress(%v) empty data returned %d bytes, want 0", ct, len(decompressed))
		}
	}
}

// TestCompressRoundTrip tests compression round-trip for all supported types
func TestCompressRoundTrip(t *testing.T) {
	data := []byte("The quick brown fox jumps over the lazy dog. " +
		"This sentence is repeated to increase compressibility. " +
		"The quick brown fox jumps over the lazy dog.")

	supportedTypes := []Type{NoCompression, SnappyCompression, ZlibCompression}
	for _, ct := range supportedTypes {
		if !ct.IsSupported() {
			t.Logf("Skipping %v (not supported)", ct)
			continue
		}

		compressed, err := Compress(ct, data)
		if err != nil {
			t.Errorf("Compress(%v) failed: %v", ct, err)
			continue
		}

		decompressed, err := Decompress(ct, compressed)
		if err != nil {
			t.Errorf("Decompress(%v) failed: %v", ct, err)
			continue
		}

		if string(decompressed) != string(data) {
			t.Errorf("Decompress(%v) mismatch: got %d bytes, want %d bytes", ct, len(decompressed), len(data))
		}
	}
}

// TestDecompressInvalidData tests decompression with corrupted data
func TestDecompressInvalidData(t *testing.T) {
	invalidData := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB}

	// These should all fail gracefully with invalid data
	compressionTypes := []Type{SnappyCompression, ZlibCompression}
	for _, ct := range compressionTypes {
		if !ct.IsSupported() {
			continue
		}
		_, err := Decompress(ct, invalidData)
		if err == nil {
			t.Errorf("Decompress(%v) with invalid data should fail", ct)
		}
	}
}

// TestIsSupportedAllTypes tests IsSupported for all types
func TestIsSupportedAllTypes(t *testing.T) {
	// NoCompression should always be supported
	if !NoCompression.IsSupported() {
		t.Error("NoCompression should be supported")
	}

	// Unknown types should not be supported
	if Type(254).IsSupported() {
		t.Error("Unknown type should not be supported")
	}
}
