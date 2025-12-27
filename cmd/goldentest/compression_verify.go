// Compression format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - util/compression.h (compression options)
//   - util/compression.cc (zlib raw deflate with windowBits=-14)
package main

import (
	"bytes"
	"compress/flate"
	"fmt"

	"github.com/aalhour/rockyardkv/internal/compression"
)

// verifyRawDeflateCompatible verifies that Go can decompress raw deflate
// data (the format RocksDB uses for zlib with windowBits=-14).
// This is the oracle verification for Issue 8 (zlib compatibility).
func verifyRawDeflateCompatible() error {
	original := []byte("The quick brown fox jumps over the lazy dog. " +
		"This is test data compressed using raw deflate.")

	// Compress using raw deflate (what C++ does with inflateInit2(..., -14))
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.DefaultCompression)
	if err != nil {
		return err
	}
	if _, err := writer.Write(original); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	// Verify Go can decompress it
	decompressed, err := compression.Decompress(compression.ZlibCompression, compressed.Bytes())
	if err != nil {
		return fmt.Errorf("Go Decompress failed on raw deflate: %w", err)
	}

	if !bytes.Equal(decompressed, original) {
		return fmt.Errorf("decompressed data doesn't match original")
	}

	if *verbose {
		fmt.Printf("    Go correctly decompresses raw deflate (RocksDB zlib format)\n")
	}

	return nil
}
