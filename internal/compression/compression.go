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
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, err := w.Write(data)
		if err != nil {
			return nil, fmt.Errorf("zlib write: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("zlib close: %w", err)
		}
		return buf.Bytes(), nil

	case LZ4Compression:
		return compressLZ4(data, lz4.Fast)

	case LZ4HCCompression:
		return compressLZ4(data, lz4.Level9)

	case ZstdCompression:
		return compressZstd(data, zstd.SpeedDefault)

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", t)
	}
}

// compressLZ4 compresses data using LZ4.
func compressLZ4(data []byte, level lz4.CompressionLevel) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if err := w.Apply(lz4.CompressionLevelOption(level)); err != nil {
		return nil, fmt.Errorf("lz4 apply level: %w", err)
	}
	_, err := w.Write(data)
	if err != nil {
		return nil, fmt.Errorf("lz4 write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 close: %w", err)
	}
	return buf.Bytes(), nil
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
func Decompress(t Type, data []byte) ([]byte, error) {
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
		return decompressLZ4(data)

	case ZstdCompression:
		return decompressZstd(data)

	default:
		return nil, fmt.Errorf("unsupported compression type: %s", t)
	}
}

// decompressLZ4 decompresses LZ4 data.
func decompressLZ4(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	return io.ReadAll(r)
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
