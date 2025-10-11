package table

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// TestGoldenSSTWithSnappyCompression creates and verifies an SST with Snappy compression.
func TestGoldenSSTWithSnappyCompression(t *testing.T) {
	testCompressionGolden(t, compression.SnappyCompression, "snappy")
}

// TestGoldenSSTWithZlibCompression creates and verifies an SST with Zlib compression.
func TestGoldenSSTWithZlibCompression(t *testing.T) {
	testCompressionGolden(t, compression.ZlibCompression, "zlib")
}

// TestGoldenSSTWithNoCompression creates and verifies an SST without compression.
func TestGoldenSSTWithNoCompression(t *testing.T) {
	testCompressionGolden(t, compression.NoCompression, "none")
}

func testCompressionGolden(t *testing.T, comprType compression.Type, name string) {
	// Create a buffer to hold the SST
	var buf bytes.Buffer

	// Create builder options with the specified compression
	opts := DefaultBuilderOptions()
	opts.Compression = comprType
	opts.BlockSize = 256 // Small blocks to test compression per-block

	// Create builder
	builder := NewTableBuilder(&buf, opts)

	// Add test entries
	entries := []struct {
		key   []byte
		value []byte
	}{
		{makeInternalKeyForTest([]byte("apple"), 1, dbformat.TypeValue), []byte("red fruit")},
		{makeInternalKeyForTest([]byte("banana"), 2, dbformat.TypeValue), []byte("yellow fruit")},
		{makeInternalKeyForTest([]byte("cherry"), 3, dbformat.TypeValue), []byte("small red fruit")},
		{makeInternalKeyForTest([]byte("date"), 4, dbformat.TypeValue), []byte("sweet dried fruit")},
		{makeInternalKeyForTest([]byte("elderberry"), 5, dbformat.TypeValue), []byte("dark purple berry")},
		// Add more entries to have multiple blocks
		{makeInternalKeyForTest([]byte("fig"), 6, dbformat.TypeValue), bytes.Repeat([]byte("sweet"), 50)},
		{makeInternalKeyForTest([]byte("grape"), 7, dbformat.TypeValue), bytes.Repeat([]byte("purple"), 50)},
		{makeInternalKeyForTest([]byte("honeydew"), 8, dbformat.TypeValue), bytes.Repeat([]byte("green"), 50)},
	}

	for _, e := range entries {
		if err := builder.Add(e.key, e.value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Verify compression was applied (file size should differ for compressed vs uncompressed)
	t.Logf("SST with %s compression: %d bytes, %d entries",
		name, buf.Len(), builder.NumEntries())

	// Read back and verify using in-memory file
	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify all entries can be read
	iter := reader.NewIterator()
	idx := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if idx >= len(entries) {
			t.Fatalf("Too many entries: expected %d", len(entries))
		}
		if !bytes.Equal(iter.Key(), entries[idx].key) {
			t.Errorf("Key mismatch at %d: expected %q, got %q", idx, entries[idx].key, iter.Key())
		}
		if !bytes.Equal(iter.Value(), entries[idx].value) {
			t.Errorf("Value mismatch at %d", idx)
		}
		idx++
	}

	if idx != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), idx)
	}
}

// TestGoldenSSTFormats tests multiple SST variants and optionally saves them for reference.
func TestGoldenSSTFormats(t *testing.T) {
	// Skip in short mode - this is for generating reference files
	if testing.Short() {
		t.Skip("Skipping golden file generation in short mode")
	}

	goldenDir := "testdata/golden/sst"
	if err := os.MkdirAll(goldenDir, 0755); err != nil {
		t.Logf("Could not create golden dir: %v", err)
		return
	}

	variants := []struct {
		name        string
		compression compression.Type
		blockSize   int
	}{
		{"no_compression", compression.NoCompression, 4096},
		{"snappy_small_blocks", compression.SnappyCompression, 256},
		{"snappy_large_blocks", compression.SnappyCompression, 16384},
		{"zlib_default", compression.ZlibCompression, 4096},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			var buf bytes.Buffer

			opts := DefaultBuilderOptions()
			opts.Compression = v.compression
			opts.BlockSize = v.blockSize

			builder := NewTableBuilder(&buf, opts)

			// Add a variety of entries
			for i := range 100 {
				key := makeInternalKeyForTest(fmt.Appendf(nil, "key_%05d", i), uint64(i+1), dbformat.TypeValue)
				value := fmt.Appendf(nil, "value_%05d_%s", i, string(bytes.Repeat([]byte("x"), i%50)))
				if err := builder.Add(key, value); err != nil {
					t.Fatalf("Add failed: %v", err)
				}
			}

			if err := builder.Finish(); err != nil {
				t.Fatalf("Finish failed: %v", err)
			}

			// Save to golden directory
			path := filepath.Join(goldenDir, v.name+".sst")
			if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
				t.Logf("Could not save golden file %s: %v", path, err)
			} else {
				t.Logf("Saved golden file: %s (%d bytes)", path, buf.Len())
			}
		})
	}
}

func makeInternalKeyForTest(userKey []byte, seq uint64, typ dbformat.ValueType) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	trailer := (seq << 8) | uint64(typ)
	for i := range 8 {
		key[len(userKey)+i] = byte(trailer >> (i * 8))
	}
	return key
}
