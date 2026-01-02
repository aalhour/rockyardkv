package table

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/vfs"
)

func TestTableCacheBasic(t *testing.T) {
	fs := vfs.Default()
	tmpDir := t.TempDir()

	// Create a test SST file
	sstPath := filepath.Join(tmpDir, "000001.sst")
	if err := createTestSST(fs, sstPath); err != nil {
		t.Fatalf("failed to create test SST: %v", err)
	}

	// Create table cache
	cache := NewTableCache(fs, DefaultTableCacheOptions())
	defer cache.Close()

	// Get the reader
	reader, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify we can read from it
	iter := reader.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator should be valid")
	}

	// Release the reader
	cache.Release(1)

	// Get again - should return cached reader
	reader2, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Get (cached) failed: %v", err)
	}
	if reader != reader2 {
		t.Error("Expected to get cached reader")
	}
	cache.Release(1)

	// Verify cache size
	if cache.Size() != 1 {
		t.Errorf("cache size = %d, want 1", cache.Size())
	}
}

func TestTableCacheEviction(t *testing.T) {
	fs := vfs.Default()
	tmpDir := t.TempDir()

	// Create multiple test SST files
	numFiles := 5
	for i := 1; i <= numFiles; i++ {
		sstPath := filepath.Join(tmpDir, sstFileName(uint64(i)))
		if err := createTestSST(fs, sstPath); err != nil {
			t.Fatalf("failed to create test SST %d: %v", i, err)
		}
	}

	// Create cache with small max size
	opts := TableCacheOptions{
		MaxOpenFiles:    3,
		VerifyChecksums: false,
	}
	cache := NewTableCache(fs, opts)
	defer cache.Close()

	// Open all files
	for i := 1; i <= numFiles; i++ {
		sstPath := filepath.Join(tmpDir, sstFileName(uint64(i)))
		_, err := cache.Get(uint64(i), sstPath)
		if err != nil {
			t.Fatalf("Get failed for file %d: %v", i, err)
		}
		cache.Release(uint64(i))
	}

	// Cache should have evicted some files
	if cache.Size() > opts.MaxOpenFiles {
		t.Errorf("cache size = %d, want <= %d", cache.Size(), opts.MaxOpenFiles)
	}
}

func TestTableCacheEvict(t *testing.T) {
	fs := vfs.Default()
	tmpDir := t.TempDir()

	sstPath := filepath.Join(tmpDir, "000001.sst")
	if err := createTestSST(fs, sstPath); err != nil {
		t.Fatalf("failed to create test SST: %v", err)
	}

	cache := NewTableCache(fs, DefaultTableCacheOptions())
	defer cache.Close()

	// Get reader
	_, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	cache.Release(1)

	if cache.Size() != 1 {
		t.Errorf("cache size = %d, want 1", cache.Size())
	}

	// Evict
	cache.Evict(1)

	if cache.Size() != 0 {
		t.Errorf("cache size after evict = %d, want 0", cache.Size())
	}
}

func TestTableCacheClose(t *testing.T) {
	fs := vfs.Default()
	tmpDir := t.TempDir()

	sstPath := filepath.Join(tmpDir, "000001.sst")
	if err := createTestSST(fs, sstPath); err != nil {
		t.Fatalf("failed to create test SST: %v", err)
	}

	cache := NewTableCache(fs, DefaultTableCacheOptions())

	_, err := cache.Get(1, sstPath)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	cache.Release(1)

	// Close should close all readers
	if err := cache.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if cache.Size() != 0 {
		t.Errorf("cache size after close = %d, want 0", cache.Size())
	}
}

// createTestSST creates a simple SST file for testing.
func createTestSST(fs vfs.FS, path string) error {
	file, err := fs.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	builder := NewTableBuilder(file, DefaultBuilderOptions())

	// Add some test entries
	for i := range 10 {
		key := makeTestInternalKey([]byte{byte('a' + i)}, 100-uint64(i))
		value := []byte{byte('v'), byte('a' + i), byte('l')}
		if err := builder.Add(key, value); err != nil {
			return err
		}
	}

	if err := builder.Finish(); err != nil {
		return err
	}

	return file.Sync()
}

// makeTestInternalKey creates an internal key for testing.
func makeTestInternalKey(userKey []byte, seq uint64) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	// Type 1 = TypeValue
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

// sstFileName returns the filename for an SST file (same as db package).
func sstFileName(number uint64) string {
	return bytes.NewBufferString("").String() + // just to use bytes package
		formatSSTName(number)
}

func formatSSTName(number uint64) string {
	b := make([]byte, 0, 10)
	n := number
	for {
		b = append(b, byte('0'+n%10))
		n /= 10
		if n == 0 {
			break
		}
	}
	// Reverse
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	// Pad to 6 digits
	for len(b) < 6 {
		b = append([]byte{'0'}, b...)
	}
	return string(b) + ".sst"
}
