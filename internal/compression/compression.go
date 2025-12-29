// Package compression provides compression and decompression for RocksDB blocks.
//
// RocksDB supports multiple compression algorithms. Each data block in an SST file
// is stored with a 1-byte compression type indicator followed by the compressed
// (or uncompressed) data.
//
// Reference: util/compression.h, util/compression.cc
package compression

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Type represents a compression algorithm.
type Type uint8

const (
	// NoCompression indicates no compression.
	NoCompression Type = 0x0

	// SnappyCompression uses Google Snappy compression.
	SnappyCompression Type = 0x1

	// ZlibCompression uses zlib compression.
	ZlibCompression Type = 0x2

	// BZip2Compression uses bzip2 compression.
	// Note: Not commonly used and not implemented here.
	BZip2Compression Type = 0x3

	// LZ4Compression uses LZ4 compression.
	// Note: Requires external library.
	LZ4Compression Type = 0x4

	// LZ4HCCompression uses LZ4 High Compression mode.
	LZ4HCCompression Type = 0x5

	// XpressCompression is Windows-specific (not implemented).
	XpressCompression Type = 0x6

	// ZstdCompression uses Zstandard compression.
	// Note: Requires external library.
	ZstdCompression Type = 0x7
)

// String returns the human-readable name of the compression type.
func (t Type) String() string {
	switch t {
	case NoCompression:
		return "NoCompression"
	case SnappyCompression:
		return "Snappy"
	case ZlibCompression:
		return "Zlib"
	case BZip2Compression:
		return "BZip2"
	case LZ4Compression:
		return "LZ4"
	case LZ4HCCompression:
		return "LZ4HC"
	case XpressCompression:
		return "Xpress"
	case ZstdCompression:
		return "ZSTD"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// IsSupported returns true if the compression type is supported.
func (t Type) IsSupported() bool {
	switch t {
	case NoCompression, SnappyCompression, ZlibCompression, LZ4Compression, LZ4HCCompression, ZstdCompression:
		return true
	default:
		return false
	}
}

// Compress compresses data using the specified compression type.
func Compress(t Type, data []byte) ([]byte, error) {
	switch t {
	case NoCompression:
		return data, nil

	case SnappyCompression:
		return snappy.Encode(nil, data), nil

	case ZlibCompression:
		// RocksDB uses raw deflate format (no zlib header) with windowBits = -14.
		// Go's compress/flate produces raw deflate (no headers).
		var buf bytes.Buffer
		// Use BestSpeed (level 1) for compatibility with RocksDB's default
		w, err := flate.NewWriter(&buf, flate.BestSpeed)
		if err != nil {
			return nil, fmt.Errorf("raw deflate writer: %w", err)
		}
		if _, err := w.Write(data); err != nil {
			return nil, fmt.Errorf("raw deflate write: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("raw deflate close: %w", err)
		}
		return buf.Bytes(), nil

	case LZ4Compression:
		return compressLZ4(data, false)

	case LZ4HCCompression:
		return compressLZ4(data, true)

	case ZstdCompression:
		return compressZstd(data, zstd.SpeedDefault)

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", t)
	}
}

// compressLZ4 compresses data using LZ4 raw block format.
// RocksDB uses LZ4_compress_fast() which produces raw block format,
// NOT the LZ4 Frame format (which has magic bytes and frame headers).
// The highCompression flag selects LZ4HC (slower but better ratio) vs standard LZ4.
func compressLZ4(data []byte, highCompression bool) ([]byte, error) {
	// Allocate buffer for worst-case compressed size
	dst := make([]byte, lz4.CompressBlockBound(len(data)))

	var n int
	var err error
	if highCompression {
		// LZ4HC - higher compression ratio, slower
		var ht [1 << 16]int
		n, err = lz4.CompressBlockHC(data, dst, lz4.CompressionLevel(9), ht[:], nil)
	} else {
		// Standard LZ4 - fast compression
		var ht [1 << 16]int
		n, err = lz4.CompressBlock(data, dst, ht[:])
	}

	if err != nil {
		return nil, fmt.Errorf("lz4 compress block: %w", err)
	}
	if n == 0 {
		// Data is incompressible, return nil to signal no compression benefit
		return nil, nil
	}

	return dst[:n], nil
}

// compressZstd compresses data using Zstandard.
func compressZstd(data []byte, level zstd.EncoderLevel) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("zstd encoder: %w", err)
	}
	return encoder.EncodeAll(data, nil), nil
}

// Decompress decompresses data using the specified compression type.
// For LZ4/LZ4HC, use DecompressWithSize if the uncompressed size is known.
func Decompress(t Type, data []byte) ([]byte, error) {
	return DecompressWithSize(t, data, 0)
}

// DecompressWithSize decompresses data with a known uncompressed size.
// For LZ4 raw block format, the expectedSize is required for correct decompression.
// If expectedSize is 0, a fallback strategy is used (may be slower or fail).
func DecompressWithSize(t Type, data []byte, expectedSize int) ([]byte, error) {
	switch t {
	case NoCompression:
		return data, nil

	case SnappyCompression:
		return snappy.Decode(nil, data)

	case ZlibCompression:
		// RocksDB uses raw deflate format (no zlib header) with windowBits = -14.
		// Try raw deflate first (RocksDB's default), then fall back to zlib header format.
		result, err := decompressRawDeflate(data)
		if err == nil {
			return result, nil
		}
		// Fall back to standard zlib (with header) for compatibility
		r, zlibErr := zlib.NewReader(bytes.NewReader(data))
		if zlibErr != nil {
			// Return the original raw deflate error as it's more likely
			return nil, fmt.Errorf("zlib decompress: raw deflate failed: %w", err)
		}
		defer func() { _ = r.Close() }()
		return io.ReadAll(r)

	case LZ4Compression, LZ4HCCompression:
		return decompressLZ4(data, expectedSize)

	case ZstdCompression:
		return decompressZstd(data)

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", t)
	}
}

// decompressLZ4 decompresses LZ4 raw block data.
// RocksDB uses LZ4_decompress_safe() which requires the expected uncompressed size.
func decompressLZ4(data []byte, expectedSize int) ([]byte, error) {
	if expectedSize > 0 {
		// Known size - decompress directly
		dst := make([]byte, expectedSize)
		n, err := lz4.UncompressBlock(data, dst)
		if err != nil {
			return nil, fmt.Errorf("lz4 uncompress block: %w", err)
		}
		return dst[:n], nil
	}

	// Unknown size - try progressively larger buffers
	// Start with 4x compressed size, then grow exponentially
	bufSize := max(len(data)*4, 256)

	for range 10 {
		dst := make([]byte, bufSize)
		n, err := lz4.UncompressBlock(data, dst)
		if err == nil {
			return dst[:n], nil
		}
		// Buffer too small - double it and retry
		bufSize *= 2
	}

	return nil, fmt.Errorf("lz4 uncompress block: buffer too small after retries")
}

// decompressZstd decompresses Zstandard data.
func decompressZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decoder: %w", err)
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

// decompressRawDeflate decompresses data using raw DEFLATE (no zlib header).
// This matches RocksDB's zlib compression which uses windowBits = -14.
func decompressRawDeflate(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}
