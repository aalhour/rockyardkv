package rockyardkv

// sst_file_writer_fuzz_test.go implements tests for sst file writer fuzz.


import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/table"
)

// FuzzSstFileWriter tests that SstFileWriter handles arbitrary key/value inputs.
func FuzzSstFileWriter(f *testing.F) {
	// Seed with interesting cases
	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte(""), []byte("empty_key"))
	f.Add([]byte("a"), []byte(""))
	f.Add([]byte{0x00}, []byte{0xFF})
	f.Add([]byte{0xFF, 0xFF, 0xFF}, []byte{0x00, 0x00, 0x00})
	f.Add(make([]byte, 1000), make([]byte, 10000)) // Large key/value

	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 {
			return // Skip empty keys
		}

		tmpDir := t.TempDir()
		sstPath := filepath.Join(tmpDir, "test.sst")

		writer := NewSstFileWriter(DefaultSstFileWriterOptions())
		if err := writer.Open(sstPath); err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		// Put the fuzzed key/value
		if err := writer.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		info, err := writer.Finish()
		if err != nil {
			t.Fatalf("Finish failed: %v", err)
		}

		if info.NumEntries != 1 {
			t.Errorf("Expected 1 entry, got %d", info.NumEntries)
		}

		// Verify the file is readable
		file, err := os.Open(sstPath)
		if err != nil {
			t.Fatalf("Failed to open SST: %v", err)
		}
		defer file.Close()

		stat, _ := file.Stat()
		wrapper := &fuzzFileWrapper{f: file, size: stat.Size()}
		reader, err := table.Open(wrapper, table.ReaderOptions{})
		if err != nil {
			t.Fatalf("Failed to open reader: %v", err)
		}

		// Verify the key exists
		iter := reader.NewIterator()
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Fatal("Iterator not valid after SeekToFirst")
		}

		foundKey := extractUserKeyForFuzz(iter.Key())
		if !bytes.Equal(foundKey, key) {
			t.Errorf("Key mismatch: expected %q, got %q", key, foundKey)
		}

		foundValue := iter.Value()
		if !bytes.Equal(foundValue, value) {
			t.Errorf("Value mismatch: expected %q, got %q", value, foundValue)
		}
	})
}

// FuzzSstFileWriterMultipleKeys tests writing multiple sorted keys.
func FuzzSstFileWriterMultipleKeys(f *testing.F) {
	// Seed with interesting key sequences
	f.Add([]byte("a"), []byte("b"), []byte("c"), []byte("value"))

	f.Fuzz(func(t *testing.T, k1, k2, k3, value []byte) {
		// Sort keys to ensure valid order
		keys := [][]byte{k1, k2, k3}

		// Filter empty and deduplicate
		var filtered [][]byte
		seen := make(map[string]bool)
		for _, k := range keys {
			if len(k) > 0 && !seen[string(k)] {
				filtered = append(filtered, k)
				seen[string(k)] = true
			}
		}

		if len(filtered) < 2 {
			return // Need at least 2 unique keys
		}

		// Sort
		sortBytesSlice(filtered)

		tmpDir := t.TempDir()
		sstPath := filepath.Join(tmpDir, "test.sst")

		writer := NewSstFileWriter(DefaultSstFileWriterOptions())
		if err := writer.Open(sstPath); err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		for _, k := range filtered {
			if err := writer.Put(k, value); err != nil {
				t.Fatalf("Put failed for key %q: %v", k, err)
			}
		}

		info, err := writer.Finish()
		if err != nil {
			t.Fatalf("Finish failed: %v", err)
		}

		if info.NumEntries != uint64(len(filtered)) {
			t.Errorf("Expected %d entries, got %d", len(filtered), info.NumEntries)
		}
	})
}

func sortBytesSlice(s [][]byte) {
	for i := range len(s) - 1 {
		for j := i + 1; j < len(s); j++ {
			if bytes.Compare(s[i], s[j]) > 0 {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

type fuzzFileWrapper struct {
	f    *os.File
	size int64
}

func (w *fuzzFileWrapper) ReadAt(p []byte, off int64) (int, error) {
	return w.f.ReadAt(p, off)
}

func (w *fuzzFileWrapper) Size() int64 {
	return w.size
}

func (w *fuzzFileWrapper) Close() error {
	return w.f.Close()
}

func extractUserKeyForFuzz(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}
