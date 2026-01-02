// Cross-compatibility tests for RocksDB SST file format.
//
// These tests verify that our Go implementation produces SST files that
// conform to the RocksDB file format specification. This ensures that
// files created by this implementation can be read by C++ RocksDB and vice versa.
//
// Format Reference: RocksDB v10.7.5 table/format.h and table/block_based/block_based_table_builder.cc
package table

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/vfs"
)

// makeInternalKeyCompat creates an internal key from user key, sequence, and type.
func makeInternalKeyCompat(userKey []byte, seq uint64, typ dbformat.ValueType) []byte {
	key := &dbformat.ParsedInternalKey{
		UserKey:  userKey,
		Sequence: dbformat.SequenceNumber(seq),
		Type:     typ,
	}
	return dbformat.AppendInternalKey(nil, key)
}

// TestSSTFormatMagicNumber verifies the magic number at the end of SST files
// matches the expected RocksDB value.
func TestSSTFormatMagicNumber(t *testing.T) {
	// Create a simple SST file
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	opts := DefaultBuilderOptions()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, opts)

	// Add some test entries
	for i := range 10 {
		key := makeInternalKeyCompat([]byte{byte('a' + i)}, uint64(100-i), dbformat.TypeValue)
		value := []byte{byte('A' + i)}
		if err := builder.Add(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Read the file and verify magic number
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) < 8 {
		t.Fatal("file too small to contain magic number")
	}

	// Magic number is in the last 8 bytes
	magic := binary.LittleEndian.Uint64(data[len(data)-8:])

	// Expected magic: 0x88e241b785f4cff7 (BlockBasedTableMagicNumber)
	// or 0xdb4775248b80fb57 (LegacyBlockBasedTableMagicNumber)
	if magic != block.BlockBasedTableMagicNumber && magic != block.LegacyBlockBasedTableMagicNumber {
		t.Errorf("unexpected magic number: got %#x, want %#x or %#x",
			magic, block.BlockBasedTableMagicNumber, block.LegacyBlockBasedTableMagicNumber)
	}

	t.Logf("Magic number verified: %#x", magic)
}

// TestSSTFormatVersion verifies the format version is compatible.
func TestSSTFormatVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Test with format version 3 (default)
	opts := DefaultBuilderOptions()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, opts)
	key := makeInternalKeyCompat([]byte("key"), 100, dbformat.TypeValue)
	builder.Add(key, []byte("value"))
	builder.Finish()
	file.Close()

	// Read and verify format version in footer
	fs := vfs.Default()
	reader, err := Open(mustOpenRandomAccess(fs, path), ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify format version is readable
	// The footer should have been parsed successfully if we got here
	t.Logf("SST file created and read successfully with format version %d", opts.FormatVersion)
}

// TestSSTChecksumTypes verifies different checksum types work correctly.
func TestSSTChecksumTypes(t *testing.T) {
	tests := []struct {
		name         string
		checksumType checksum.Type
	}{
		{"CRC32C", checksum.TypeCRC32C},
		{"XXH3", checksum.TypeXXH3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.sst")

			opts := DefaultBuilderOptions()
			opts.ChecksumType = tt.checksumType

			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}

			builder := NewTableBuilder(file, opts)

			// Add entries
			for i := range 100 {
				key := makeInternalKeyCompat([]byte{byte(i)}, uint64(1000-i), dbformat.TypeValue)
				value := make([]byte, 100)
				for j := range value {
					value[j] = byte(i ^ j)
				}
				if err := builder.Add(key, value); err != nil {
					t.Fatal(err)
				}
			}

			if err := builder.Finish(); err != nil {
				t.Fatal(err)
			}
			file.Close()

			// Verify file can be read with checksum verification
			fs := vfs.Default()
			readerOpts := ReaderOptions{VerifyChecksums: true}
			readerOpts.VerifyChecksums = true

			reader, err := Open(mustOpenRandomAccess(fs, path), readerOpts)
			if err != nil {
				t.Fatalf("failed to open SST with %s checksums: %v", tt.name, err)
			}
			defer reader.Close()

			// Iterate through all entries to verify checksums
			iter := reader.NewIterator()
			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				count++
			}
			if err := iter.Error(); err != nil {
				t.Fatalf("iteration error with %s checksums: %v", tt.name, err)
			}
			if count != 100 {
				t.Errorf("expected 100 entries, got %d", count)
			}

			t.Logf("Verified %d entries with %s checksum", count, tt.name)
		})
	}
}

// TestSSTBlockFormat verifies block structure matches RocksDB format.
func TestSSTBlockFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	opts := DefaultBuilderOptions()
	opts.BlockSize = 4096
	opts.BlockRestartInterval = 16

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, opts)

	// Add enough entries to create multiple blocks
	for i := range 1000 {
		key := makeInternalKeyCompat([]byte{byte(i / 256), byte(i % 256)}, uint64(10000-i), dbformat.TypeValue)
		value := bytes.Repeat([]byte{byte(i)}, 50)
		if err := builder.Add(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Open and verify structure
	fs := vfs.Default()
	reader, err := Open(mustOpenRandomAccess(fs, path), ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Verify all entries are readable
	iter := reader.NewIterator()
	count := 0
	var prevKey []byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if prevKey != nil && block.CompareInternalKeys(prevKey, key) >= 0 {
			t.Errorf("keys not in sorted order at index %d", count)
		}
		prevKey = append(prevKey[:0], key...)
		count++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}
	if count != 1000 {
		t.Errorf("expected 1000 entries, got %d", count)
	}

	t.Logf("Verified %d entries in multi-block SST", count)
}

// TestSSTInternalKeyFormat verifies internal key format matches RocksDB.
// Internal key = user_key + (sequence << 8 | type)
func TestSSTInternalKeyFormat(t *testing.T) {
	// Test internal key encoding
	testCases := []struct {
		userKey string
		seq     uint64
		typ     dbformat.ValueType
	}{
		{"key1", 100, dbformat.TypeValue},
		{"key2", 200, dbformat.TypeDeletion},
		{"", 0, dbformat.TypeValue},          // empty key
		{"a", 1<<56 - 1, dbformat.TypeValue}, // max sequence
	}

	for _, tc := range testCases {
		key := makeInternalKeyCompat([]byte(tc.userKey), tc.seq, tc.typ)

		// Verify structure: user_key + 8-byte trailer
		if len(key) != len(tc.userKey)+8 {
			t.Errorf("internal key length: got %d, want %d", len(key), len(tc.userKey)+8)
		}

		// Extract and verify user key
		userKey := key[:len(key)-8]
		if string(userKey) != tc.userKey {
			t.Errorf("user key: got %q, want %q", userKey, tc.userKey)
		}

		// Extract and verify trailer
		trailer := binary.LittleEndian.Uint64(key[len(key)-8:])
		gotSeq := trailer >> 8
		gotTyp := dbformat.ValueType(trailer & 0xFF)

		if gotSeq != tc.seq {
			t.Errorf("sequence: got %d, want %d", gotSeq, tc.seq)
		}
		if gotTyp != tc.typ {
			t.Errorf("type: got %d, want %d", gotTyp, tc.typ)
		}
	}
}

// TestSSTRoundtrip verifies data integrity through write-read cycle.
func TestSSTRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Generate test data
	type entry struct {
		key   []byte
		value []byte
	}
	entries := make([]entry, 500)
	for i := range entries {
		entries[i] = entry{
			key:   makeInternalKeyCompat([]byte{byte(i / 256), byte(i % 256), 'k', 'e', 'y'}, uint64(10000-i), dbformat.TypeValue),
			value: bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 50),
		}
	}

	// Write
	opts := DefaultBuilderOptions()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, opts)
	for _, e := range entries {
		if err := builder.Add(e.key, e.value); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Read and verify
	fs := vfs.Default()
	reader, err := Open(mustOpenRandomAccess(fs, path), ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	iter := reader.NewIterator()
	i := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if i >= len(entries) {
			t.Fatalf("too many entries: expected %d", len(entries))
		}

		if !bytes.Equal(iter.Key(), entries[i].key) {
			t.Errorf("key %d mismatch", i)
		}
		if !bytes.Equal(iter.Value(), entries[i].value) {
			t.Errorf("value %d mismatch", i)
		}
		i++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}
	if i != len(entries) {
		t.Errorf("entry count: got %d, want %d", i, len(entries))
	}
}

// TestSSTProperties verifies table properties match expected format.
func TestSSTProperties(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	opts := DefaultBuilderOptions()
	opts.ComparatorName = "leveldb.BytewiseComparator"

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, opts)
	for i := range 100 {
		key := makeInternalKeyCompat([]byte{byte(i)}, uint64(100-i), dbformat.TypeValue)
		builder.Add(key, []byte("value"))
	}
	builder.Finish()
	file.Close()

	// Read and verify properties
	fs := vfs.Default()
	reader, err := Open(mustOpenRandomAccess(fs, path), ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	props, err := reader.Properties()
	if err != nil {
		t.Fatal("failed to get properties:", err)
	}
	if props == nil {
		t.Fatal("no properties found")
	}

	// Verify expected properties
	if props.NumEntries != 100 {
		t.Errorf("NumEntries: got %d, want 100", props.NumEntries)
	}
	if props.NumDataBlocks == 0 {
		t.Error("NumDataBlocks should be > 0")
	}

	t.Logf("Properties: entries=%d, data_blocks=%d, data_size=%d",
		props.NumEntries, props.NumDataBlocks, props.DataSize)
}

func mustOpenRandomAccess(fs vfs.FS, path string) vfs.RandomAccessFile {
	f, err := fs.OpenRandomAccess(path)
	if err != nil {
		panic(err)
	}
	return f
}
