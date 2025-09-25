package compression

import (
	"bytes"
	"testing"
)

func TestNoCompression(t *testing.T) {
	data := []byte("hello world, this is test data for no compression")

	compressed, err := Compress(NoCompression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	if !bytes.Equal(compressed, data) {
		t.Error("NoCompression should return data unchanged")
	}

	decompressed, err := Decompress(NoCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestSnappyCompression(t *testing.T) {
	// Generate test data with repetition (compressible)
	data := bytes.Repeat([]byte("hello world "), 100)

	compressed, err := Compress(SnappyCompression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Snappy should actually compress repeated data
	if len(compressed) >= len(data) {
		t.Logf("Warning: compressed size %d >= original %d (this can happen for small/random data)",
			len(compressed), len(data))
	}

	decompressed, err := Decompress(SnappyCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestZlibCompression(t *testing.T) {
	data := bytes.Repeat([]byte("zlib compression test "), 50)

	compressed, err := Compress(ZlibCompression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Zlib should compress repeated data well
	t.Logf("Zlib: %d -> %d bytes (%.1f%%)", len(data), len(compressed),
		float64(len(compressed))/float64(len(data))*100)

	decompressed, err := Decompress(ZlibCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestCompressionTypeString(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{NoCompression, "NoCompression"},
		{SnappyCompression, "Snappy"},
		{ZlibCompression, "Zlib"},
		{LZ4Compression, "LZ4"},
		{ZstdCompression, "ZSTD"},
		{Type(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.expected)
		}
	}
}

func TestCompressionTypeIsSupported(t *testing.T) {
	supported := []Type{NoCompression, SnappyCompression, ZlibCompression, LZ4Compression, LZ4HCCompression, ZstdCompression}
	unsupported := []Type{BZip2Compression, XpressCompression}

	for _, typ := range supported {
		if !typ.IsSupported() {
			t.Errorf("%s should be supported", typ)
		}
	}

	for _, typ := range unsupported {
		if typ.IsSupported() {
			t.Errorf("%s should not be supported", typ)
		}
	}
}

func TestUnsupportedCompressionType(t *testing.T) {
	data := []byte("test data")

	_, err := Compress(BZip2Compression, data)
	if err == nil {
		t.Error("Expected error for unsupported compression type BZip2")
	}

	_, err = Decompress(BZip2Compression, data)
	if err == nil {
		t.Error("Expected error for unsupported decompression type BZip2")
	}
}

func TestLZ4Compression(t *testing.T) {
	data := bytes.Repeat([]byte("lz4 compression test "), 100)

	compressed, err := Compress(LZ4Compression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	t.Logf("LZ4: %d -> %d bytes (%.1f%%)", len(data), len(compressed),
		float64(len(compressed))/float64(len(data))*100)

	decompressed, err := Decompress(LZ4Compression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestLZ4HCCompression(t *testing.T) {
	data := bytes.Repeat([]byte("lz4hc high compression test "), 100)

	compressed, err := Compress(LZ4HCCompression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	t.Logf("LZ4HC: %d -> %d bytes (%.1f%%)", len(data), len(compressed),
		float64(len(compressed))/float64(len(data))*100)

	decompressed, err := Decompress(LZ4HCCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestZstdCompression(t *testing.T) {
	data := bytes.Repeat([]byte("zstandard compression test "), 100)

	compressed, err := Compress(ZstdCompression, data)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	t.Logf("ZSTD: %d -> %d bytes (%.1f%%)", len(data), len(compressed),
		float64(len(compressed))/float64(len(data))*100)

	decompressed, err := Decompress(ZstdCompression, compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("Decompressed data should match original")
	}
}

func TestEmptyData(t *testing.T) {
	types := []Type{NoCompression, SnappyCompression, ZlibCompression, LZ4Compression, ZstdCompression}

	for _, typ := range types {
		compressed, err := Compress(typ, []byte{})
		if err != nil {
			t.Errorf("%s: Compress empty failed: %v", typ, err)
			continue
		}

		decompressed, err := Decompress(typ, compressed)
		if err != nil {
			t.Errorf("%s: Decompress empty failed: %v", typ, err)
			continue
		}

		if len(decompressed) != 0 {
			t.Errorf("%s: decompressed empty should be empty, got %d bytes", typ, len(decompressed))
		}
	}
}

func TestLargeData(t *testing.T) {
	// 1MB of test data
	data := bytes.Repeat([]byte("large data block for compression testing "), 25000)

	types := []Type{NoCompression, SnappyCompression, ZlibCompression, LZ4Compression, LZ4HCCompression, ZstdCompression}

	for _, typ := range types {
		compressed, err := Compress(typ, data)
		if err != nil {
			t.Errorf("%s: Compress large failed: %v", typ, err)
			continue
		}

		t.Logf("%s: %d -> %d bytes", typ, len(data), len(compressed))

		decompressed, err := Decompress(typ, compressed)
		if err != nil {
			t.Errorf("%s: Decompress large failed: %v", typ, err)
			continue
		}

		if !bytes.Equal(decompressed, data) {
			t.Errorf("%s: decompressed data doesn't match original", typ)
		}
	}
}

func BenchmarkSnappyCompress(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for snappy compression "), 1000)

	for b.Loop() {
		_, _ = Compress(SnappyCompression, data)
	}
}

func BenchmarkSnappyDecompress(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark data for snappy compression "), 1000)
	compressed, _ := Compress(SnappyCompression, data)

	for b.Loop() {
		_, _ = Decompress(SnappyCompression, compressed)
	}
}
