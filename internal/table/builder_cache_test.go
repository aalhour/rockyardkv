package table

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/vfs"
)

// -----------------------------------------------------------------------------
// TableBuilder.Status() test
// -----------------------------------------------------------------------------

func TestTableBuilderStatus(t *testing.T) {
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	// Initially no error
	if err := builder.Status(); err != nil {
		t.Errorf("Expected no error initially, got %v", err)
	}

	// Add some entries
	builder.Add([]byte("key1"), []byte("value1"))
	builder.Add([]byte("key2"), []byte("value2"))

	// Still no error
	if err := builder.Status(); err != nil {
		t.Errorf("Expected no error after adding entries, got %v", err)
	}

	// Finish the table
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// After finish, should still be ok
	if err := builder.Status(); err != nil {
		t.Errorf("Expected no error after Finish, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// TableCache.NewIterator() test
// -----------------------------------------------------------------------------

func TestTableCacheNewIterator(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Create a table cache
	cacheOpts := DefaultTableCacheOptions()
	cache := NewTableCache(vfs.Default(), cacheOpts)
	defer cache.Close()

	// Create an SST file
	sstPath := filepath.Join(tmpDir, "test.sst")
	file, err := os.Create(sstPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(file, opts)
	builder.Add([]byte("key1\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("value1"))
	builder.Add([]byte("key2\x00\x00\x00\x00\x00\x00\x00\x02"), []byte("value2"))
	builder.Add([]byte("key3\x00\x00\x00\x00\x00\x00\x00\x03"), []byte("value3"))
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	file.Close()

	// Create iterator from cache
	iter, err := cache.NewIterator(1, sstPath)
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}

	// Verify iterator works
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator should be valid after SeekToFirst")
	}

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	if count != 3 {
		t.Errorf("Expected 3 entries, got %d", count)
	}

	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}
}

// -----------------------------------------------------------------------------
// IndexBlockIterator tests (Prev, Key, Seek)
// -----------------------------------------------------------------------------

func TestIndexBlockIteratorSeek(t *testing.T) {
	// Create an SST with multiple data blocks
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	opts.BlockSize = 64 // Small block size to force multiple blocks
	builder := NewTableBuilder(memFile, opts)

	// Add entries - use internal key format (user key + 8 byte trailer)
	// The trailer format is: (sequence << 8) | type
	entries := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("aaa\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("v1")},
		{[]byte("bbb\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("v2")},
		{[]byte("ccc\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("v3")},
		{[]byte("ddd\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("v4")},
		{[]byte("eee\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("v5")},
	}

	for _, e := range entries {
		builder.Add(e.key, e.value)
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Open reader
	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	// Test Seek on TableIterator (which uses IndexBlockIterator internally)
	iter := reader.NewIterator()

	// Seek to "ccc"
	iter.Seek([]byte("ccc\x00\x00\x00\x00\x00\x00\x00\x01"))
	if !iter.Valid() {
		t.Fatal("Should be valid after Seek")
	}

	// Key should be >= "ccc"
	key := iter.Key()
	if len(key) < 3 || string(key[:3]) < "ccc" {
		t.Errorf("Key after Seek should be >= 'ccc', got %q", key)
	}
}

func TestIndexBlockIteratorPrev(t *testing.T) {
	// Create an SST with multiple entries
	memFile := &memFileForTest{}
	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(memFile, opts)

	entries := [][]byte{
		[]byte("key1\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("key2\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("key3\x00\x00\x00\x00\x00\x00\x00\x01"),
	}

	for i, key := range entries {
		builder.Add(key, []byte("value"+string(rune('1'+i))))
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	reader, err := Open(&readableMemFile{memFile}, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Go to last
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("Should be valid at last")
	}

	// Get key at last position
	lastKey := make([]byte, len(iter.Key()))
	copy(lastKey, iter.Key())

	// Prev should work
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("Should be valid after Prev from last")
	}

	// Key should be different from last
	prevKey := iter.Key()
	if string(prevKey) == string(lastKey) {
		t.Error("Key after Prev should be different from last key")
	}

	// One more Prev
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("Should be valid after second Prev")
	}

	// Prev from first should become invalid
	iter.SeekToFirst()
	iter.Prev()
	if iter.Valid() {
		t.Error("Should be invalid after Prev from first")
	}
}

// -----------------------------------------------------------------------------
// TableCache moveToFront test
// -----------------------------------------------------------------------------

func TestTableCacheMoveToFront(t *testing.T) {
	tmpDir := t.TempDir()

	// Create cache with small capacity
	cacheOpts := DefaultTableCacheOptions()
	cacheOpts.MaxOpenFiles = 3
	cache := NewTableCache(vfs.Default(), cacheOpts)
	defer cache.Close()

	// Create multiple SST files
	for i := 1; i <= 3; i++ {
		sstPath := filepath.Join(tmpDir, "test"+string(rune('0'+i))+".sst")
		file, err := os.Create(sstPath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		opts := DefaultBuilderOptions()
		builder := NewTableBuilder(file, opts)
		builder.Add([]byte("key\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("value"))
		if err := builder.Finish(); err != nil {
			t.Fatalf("Finish failed: %v", err)
		}
		file.Close()

		// Get to load into cache
		_, err = cache.Get(uint64(i), sstPath)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		cache.Release(uint64(i))
	}

	// Access file 1 again to move it to front
	sstPath1 := filepath.Join(tmpDir, "test1.sst")
	_, err := cache.Get(1, sstPath1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	cache.Release(1)

	// Cache size should still be 3
	if cache.Size() != 3 {
		t.Errorf("Cache size = %d, want 3", cache.Size())
	}
}

// -----------------------------------------------------------------------------
// Builder with errors test
// -----------------------------------------------------------------------------

type errorWriteFile struct {
	writeCount int
	failAfter  int
}

func (f *errorWriteFile) Write(p []byte) (int, error) {
	f.writeCount++
	if f.writeCount > f.failAfter {
		return 0, os.ErrPermission
	}
	return len(p), nil
}

func (f *errorWriteFile) Append(p []byte) error {
	_, err := f.Write(p)
	return err
}

func (f *errorWriteFile) Close() error { return nil }
func (f *errorWriteFile) Sync() error  { return nil }

func (f *errorWriteFile) Truncate(size int64) error {
	return nil
}

func (f *errorWriteFile) Size() (int64, error) {
	return 0, nil
}

// Note: This test is for documentation - the builder doesn't propagate write errors immediately
// because it buffers data. The error would show up in Finish().
