package table

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// -----------------------------------------------------------------------------
// Go-Written Golden Tests
// These tests verify that Go-written SST files have correct format and can be
// read back correctly. They serve as golden tests for the TableBuilder.
// -----------------------------------------------------------------------------

// TestGoWrittenSSTGolden writes an SST file and verifies its structure.
// This serves as a golden test - the output should be deterministic.
func TestGoWrittenSSTGolden(t *testing.T) {
	// Write an SST file with known content
	memFile := &memFileForGolden{}
	opts := DefaultBuilderOptions()
	opts.BlockSize = 4096 // Standard block size
	builder := NewTableBuilder(memFile, opts)

	// Add known entries
	entries := []struct {
		key   string
		seq   uint64
		value string
	}{
		{"apple", 100, "red fruit"},
		{"banana", 99, "yellow fruit"},
		{"cherry", 98, "red small fruit"},
		{"date", 97, "brown sweet fruit"},
		{"elderberry", 96, "purple berry"},
	}

	for _, e := range entries {
		ikey := makeGoldenInternalKey([]byte(e.key), e.seq)
		if err := builder.Add(ikey, []byte(e.value)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Verify the file can be read back
	reader, err := Open(&readableGoldenFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify footer
	footer := reader.Footer()
	if footer == nil {
		t.Fatal("Footer is nil")
	}
	t.Logf("Format version: %d", footer.FormatVersion)
	t.Logf("Checksum type: %d", footer.ChecksumType)
	t.Logf("File size: %d bytes", len(memFile.data))

	// Verify all entries can be read
	iter := reader.NewIterator()
	idx := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if idx >= len(entries) {
			t.Fatalf("Too many entries: expected %d", len(entries))
		}

		e := entries[idx]
		if !bytes.HasPrefix(iter.Key(), []byte(e.key)) {
			t.Errorf("Entry %d: key prefix = %s, want %s", idx, iter.Key(), e.key)
		}
		if !bytes.Equal(iter.Value(), []byte(e.value)) {
			t.Errorf("Entry %d: value = %s, want %s", idx, iter.Value(), e.value)
		}
		idx++
	}

	if idx != len(entries) {
		t.Errorf("Read %d entries, want %d", idx, len(entries))
	}

	// Verify properties
	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Properties failed: %v", err)
	}
	t.Logf("Num entries: %d", props.NumEntries)
	t.Logf("Data size: %d", props.DataSize)
	t.Logf("Index size: %d", props.IndexSize)

	if props.NumEntries != uint64(len(entries)) {
		t.Errorf("NumEntries = %d, want %d", props.NumEntries, len(entries))
	}
}

// TestGoWrittenSSTSeekGolden tests seek operations on Go-written SST.
func TestGoWrittenSSTSeekGolden(t *testing.T) {
	memFile := &memFileForGolden{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Add entries at known positions
	for i := range 100 {
		key := make([]byte, 8)
		key[0] = byte(i / 10)
		key[1] = byte(i % 10)
		ikey := makeGoldenInternalKey(key, uint64(1000-i))
		value := bytes.Repeat([]byte{byte(i)}, 50)
		if err := builder.Add(ikey, value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableGoldenFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Test seek to various positions
	testCases := []struct {
		name      string
		seekKey   []byte
		wantFound bool
		wantKey0  byte
		wantKey1  byte
	}{
		{"first", []byte{0, 0}, true, 0, 0},
		{"middle", []byte{5, 0}, true, 5, 0},
		{"between", []byte{5, 5}, true, 5, 5},
		{"last", []byte{9, 9}, true, 9, 9},
		{"beyond", []byte{10, 0}, false, 0, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			seekIKey := makeGoldenInternalKey(tc.seekKey, 1000)
			iter.Seek(seekIKey)

			if tc.wantFound {
				if !iter.Valid() {
					t.Error("Expected valid iterator")
					return
				}
				if len(iter.Key()) < 2 {
					t.Error("Key too short")
					return
				}
				if iter.Key()[0] != tc.wantKey0 || iter.Key()[1] != tc.wantKey1 {
					t.Errorf("Key = [%d,%d], want [%d,%d]",
						iter.Key()[0], iter.Key()[1], tc.wantKey0, tc.wantKey1)
				}
			} else {
				if iter.Valid() {
					t.Error("Expected invalid iterator")
				}
			}
		})
	}
}

// TestGoWrittenSSTMultiBlock tests SST with multiple data blocks.
func TestGoWrittenSSTMultiBlock(t *testing.T) {
	memFile := &memFileForGolden{}
	opts := DefaultBuilderOptions()
	opts.BlockSize = 128 // Small blocks to force multiple blocks
	builder := NewTableBuilder(memFile, opts)

	// Add many entries to force multiple blocks
	numEntries := 200
	for i := range numEntries {
		key := []byte{byte(i / 100), byte(i / 10 % 10), byte(i % 10)}
		ikey := makeGoldenInternalKey(key, uint64(10000-i))
		value := bytes.Repeat([]byte{'v', byte(i%26 + 'a')}, 20)
		if err := builder.Add(ikey, value); err != nil {
			t.Fatalf("Add %d failed: %v", i, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableGoldenFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify all entries
	iter := reader.NewIterator()
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != numEntries {
		t.Errorf("Read %d entries, want %d", count, numEntries)
	}

	// Verify properties indicate multiple data blocks
	props, err := reader.Properties()
	if err != nil {
		t.Fatalf("Properties failed: %v", err)
	}
	t.Logf("Num data blocks: %d", props.NumDataBlocks)
	if props.NumDataBlocks < 2 {
		t.Errorf("Expected multiple data blocks, got %d", props.NumDataBlocks)
	}
}

// TestGoWrittenSSTSaveAndReload tests writing SST to file and reading it back.
func TestGoWrittenSSTSaveAndReload(t *testing.T) {
	goldenDir := filepath.Join("..", "..", "testdata", "golden", "v10.7.5", "sst")

	// Ensure directory exists
	if err := os.MkdirAll(goldenDir, 0755); err != nil {
		t.Skipf("Cannot create golden directory: %v", err)
	}

	goldenPath := filepath.Join(goldenDir, "go_written.sst")

	// Write SST to file
	memFile := &memFileForGolden{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	entries := []struct {
		key   string
		seq   uint64
		value string
	}{
		{"key1", 100, "value1"},
		{"key2", 99, "value2"},
		{"key3", 98, "value3"},
	}

	for _, e := range entries {
		ikey := makeGoldenInternalKey([]byte(e.key), e.seq)
		if err := builder.Add(ikey, []byte(e.value)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Save to file
	if err := os.WriteFile(goldenPath, memFile.data, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	t.Logf("Wrote golden SST: %s (%d bytes)", goldenPath, len(memFile.data))

	// Read back from file
	data, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	file := &BytesFile{data: data}
	reader, err := Open(file, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Verify entries
	iter := reader.NewIterator()
	idx := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if idx >= len(entries) {
			t.Fatalf("Too many entries")
		}
		e := entries[idx]
		if !bytes.HasPrefix(iter.Key(), []byte(e.key)) {
			t.Errorf("Entry %d: key = %s, want %s", idx, iter.Key(), e.key)
		}
		if !bytes.Equal(iter.Value(), []byte(e.value)) {
			t.Errorf("Entry %d: value = %s, want %s", idx, iter.Value(), e.value)
		}
		idx++
	}

	if idx != len(entries) {
		t.Errorf("Read %d entries, want %d", idx, len(entries))
	}
}

// memFileForGolden implements WritableFile for golden tests.
type memFileForGolden struct {
	data []byte
}

func (f *memFileForGolden) Write(p []byte) (int, error) {
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *memFileForGolden) Append(p []byte) error {
	f.data = append(f.data, p...)
	return nil
}

func (f *memFileForGolden) Close() error { return nil }
func (f *memFileForGolden) Sync() error  { return nil }

func (f *memFileForGolden) Truncate(size int64) error {
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	return nil
}

func (f *memFileForGolden) Size() (int64, error) {
	return int64(len(f.data)), nil
}

// readableGoldenFile wraps memFileForGolden for reading.
type readableGoldenFile struct {
	*memFileForGolden
}

func (f *readableGoldenFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, nil
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *readableGoldenFile) Size() int64 {
	return int64(len(f.data))
}

// makeGoldenInternalKey creates internal keys for golden tests.
func makeGoldenInternalKey(userKey []byte, seq uint64) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	// Type 1 = TypeValue, packed as (seq << 8) | type
	trailer := (seq << 8) | 1
	key[len(userKey)] = byte(trailer)
	key[len(userKey)+1] = byte(trailer >> 8)
	key[len(userKey)+2] = byte(trailer >> 16)
	key[len(userKey)+3] = byte(trailer >> 24)
	key[len(userKey)+4] = byte(trailer >> 32)
	key[len(userKey)+5] = byte(trailer >> 40)
	key[len(userKey)+6] = byte(trailer >> 48)
	key[len(userKey)+7] = byte(trailer >> 56)
	return key
}
