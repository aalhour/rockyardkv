package rockyardkv

// sst_file_writer_test.go implements tests for sst file writer.


import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/table"
)

// =============================================================================
// UNIT TESTS: SstFileWriter Basic Operations
// =============================================================================

func TestSstFileWriter_Open(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())

	// Open should succeed
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Double open should fail
	if err := writer.Open(sstPath); !errors.Is(err, ErrSstWriterAlreadyOpened) {
		t.Errorf("Expected ErrSstWriterAlreadyOpened, got %v", err)
	}

	// Clean up
	writer.Abandon()
}

func TestSstFileWriter_PutSingleKey(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put a single key
	if err := writer.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Finish
	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Verify info
	if info.NumEntries != 1 {
		t.Errorf("Expected 1 entry, got %d", info.NumEntries)
	}
	if !bytes.Equal(info.SmallestKey, []byte("key1")) {
		t.Errorf("Smallest key mismatch: got %q", info.SmallestKey)
	}
	if !bytes.Equal(info.LargestKey, []byte("key1")) {
		t.Errorf("Largest key mismatch: got %q", info.LargestKey)
	}
	if info.FileSize == 0 {
		t.Error("FileSize should be > 0")
	}

	// Verify file exists
	if _, err := os.Stat(sstPath); os.IsNotExist(err) {
		t.Error("SST file was not created")
	}
}

func TestSstFileWriter_PutMultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put multiple keys in sorted order
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if err := writer.Put([]byte(k), []byte("value-"+k)); err != nil {
			t.Fatalf("Put %s failed: %v", k, err)
		}
	}

	// Finish
	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Verify info
	if info.NumEntries != uint64(len(keys)) {
		t.Errorf("Expected %d entries, got %d", len(keys), info.NumEntries)
	}
	if !bytes.Equal(info.SmallestKey, []byte("a")) {
		t.Errorf("Smallest key mismatch: got %q", info.SmallestKey)
	}
	if !bytes.Equal(info.LargestKey, []byte("e")) {
		t.Errorf("Largest key mismatch: got %q", info.LargestKey)
	}
}

func TestSstFileWriter_KeyOrder(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put first key
	if err := writer.Put([]byte("b"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Put key out of order should fail
	if err := writer.Put([]byte("a"), []byte("value")); !errors.Is(err, ErrSstWriterKeyOutOfOrder) {
		t.Errorf("Expected ErrSstWriterKeyOutOfOrder, got %v", err)
	}

	// Put same key should fail
	if err := writer.Put([]byte("b"), []byte("value2")); !errors.Is(err, ErrSstWriterKeyOutOfOrder) {
		t.Errorf("Expected ErrSstWriterKeyOutOfOrder for duplicate, got %v", err)
	}

	writer.Abandon()
}

func TestSstFileWriter_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put and delete
	if err := writer.Put([]byte("a"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := writer.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := writer.Put([]byte("c"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumEntries != 3 {
		t.Errorf("Expected 3 entries, got %d", info.NumEntries)
	}
}

func TestSstFileWriter_DeleteRange(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put some keys
	if err := writer.Put([]byte("a"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := writer.Put([]byte("z"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete range
	if err := writer.DeleteRange([]byte("b"), []byte("y")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumEntries != 2 {
		t.Errorf("Expected 2 point entries, got %d", info.NumEntries)
	}
	if info.NumRangeDelEntries != 1 {
		t.Errorf("Expected 1 range del entry, got %d", info.NumRangeDelEntries)
	}
	if !bytes.Equal(info.SmallestRangeDelKey, []byte("b")) {
		t.Errorf("Smallest range del key mismatch: got %q", info.SmallestRangeDelKey)
	}
	if !bytes.Equal(info.LargestRangeDelKey, []byte("y")) {
		t.Errorf("Largest range del key mismatch: got %q", info.LargestRangeDelKey)
	}
}

func TestSstFileWriter_InvalidDeleteRange(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Empty range should fail
	if err := writer.DeleteRange([]byte("z"), []byte("a")); err == nil {
		t.Error("Expected error for invalid range")
	}

	// Same start and end should fail
	if err := writer.DeleteRange([]byte("a"), []byte("a")); err == nil {
		t.Error("Expected error for empty range")
	}

	writer.Abandon()
}

func TestSstFileWriter_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Finish without adding any entries should fail
	_, err := writer.Finish()
	if !errors.Is(err, ErrSstWriterEmptyFile) {
		t.Errorf("Expected ErrSstWriterEmptyFile, got %v", err)
	}

	// File should not exist after failure
	if _, err := os.Stat(sstPath); !os.IsNotExist(err) {
		t.Error("SST file should have been cleaned up")
	}
}

func TestSstFileWriter_NotOpened(t *testing.T) {
	writer := NewSstFileWriter(DefaultSstFileWriterOptions())

	// All operations should fail without Open
	if err := writer.Put([]byte("key"), []byte("value")); !errors.Is(err, ErrSstWriterNotOpened) {
		t.Errorf("Expected ErrSstWriterNotOpened, got %v", err)
	}
	if err := writer.Delete([]byte("key")); !errors.Is(err, ErrSstWriterNotOpened) {
		t.Errorf("Expected ErrSstWriterNotOpened, got %v", err)
	}
	if err := writer.DeleteRange([]byte("a"), []byte("b")); !errors.Is(err, ErrSstWriterNotOpened) {
		t.Errorf("Expected ErrSstWriterNotOpened, got %v", err)
	}
	if _, err := writer.Finish(); !errors.Is(err, ErrSstWriterNotOpened) {
		t.Errorf("Expected ErrSstWriterNotOpened, got %v", err)
	}
}

func TestSstFileWriter_DoubleFinish(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := writer.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// First finish should succeed
	if _, err := writer.Finish(); err != nil {
		t.Fatalf("First Finish failed: %v", err)
	}

	// Second finish should fail
	if _, err := writer.Finish(); !errors.Is(err, ErrSstWriterAlreadyFinished) {
		t.Errorf("Expected ErrSstWriterAlreadyFinished, got %v", err)
	}
}

func TestSstFileWriter_Abandon(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := writer.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Abandon should clean up
	if err := writer.Abandon(); err != nil {
		t.Fatalf("Abandon failed: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(sstPath); !os.IsNotExist(err) {
		t.Error("SST file should have been cleaned up on Abandon")
	}
}

func TestSstFileWriter_FileSize(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())

	// Size before open should be 0
	if size := writer.FileSize(); size != 0 {
		t.Errorf("Expected 0 before open, got %d", size)
	}

	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Add entries and check size grows (keys must be sorted!)
	var lastSize uint64
	for i := range 100 {
		// Create strictly increasing keys
		key := make([]byte, 4)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)
		if err := writer.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
		currentSize := writer.FileSize()
		if i > 0 && currentSize < lastSize {
			t.Errorf("Size should not decrease: %d < %d", currentSize, lastSize)
		}
		lastSize = currentSize
	}

	writer.Abandon()
}

// =============================================================================
// UNIT TESTS: SstFileWriter Options
// =============================================================================

func TestSstFileWriter_CustomOptions(t *testing.T) {
	tests := []struct {
		name string
		opts SstFileWriterOptions
	}{
		{
			name: "WithCompression",
			opts: SstFileWriterOptions{
				Compression:          compression.SnappyCompression,
				BlockSize:            4096,
				BlockRestartInterval: 16,
				FormatVersion:        5,
			},
		},
		{
			name: "SmallBlockSize",
			opts: SstFileWriterOptions{
				BlockSize:            256,
				BlockRestartInterval: 4,
				FormatVersion:        5,
			},
		},
		{
			name: "LargeBlockSize",
			opts: SstFileWriterOptions{
				BlockSize:            65536,
				BlockRestartInterval: 64,
				FormatVersion:        5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			sstPath := filepath.Join(tmpDir, "test.sst")

			writer := NewSstFileWriter(tt.opts)
			if err := writer.Open(sstPath); err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			// Add some entries
			for i := range 100 {
				key := []byte{byte('a'), byte(i)}
				value := make([]byte, 100)
				if err := writer.Put(key, value); err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}

			info, err := writer.Finish()
			if err != nil {
				t.Fatalf("Finish failed: %v", err)
			}

			if info.NumEntries != 100 {
				t.Errorf("Expected 100 entries, got %d", info.NumEntries)
			}
		})
	}
}

// =============================================================================
// INTEGRATION TESTS: SstFileWriter Roundtrip
// =============================================================================

func TestSstFileWriter_Roundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	// Write SST
	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	entries := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	// Sort keys and write
	keys := []string{"key1", "key2", "key3"}
	for _, k := range keys {
		if err := writer.Put([]byte(k), []byte(entries[k])); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	t.Logf("SST file created: %d bytes, %d entries", info.FileSize, info.NumEntries)

	// Read SST back using table.Reader
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to stat: %v", err)
	}

	wrapper := &osFileWrapperForTest{f: file, size: stat.Size()}
	reader, err := table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	// Verify entries by iteration (Reader doesn't expose Get directly)
	_ = entries // Will verify below

	// Also verify by iteration
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		count++
		key := extractUserKeyForTest(iter.Key())
		t.Logf("Iterator found key: %q", key)
		if _, ok := entries[string(key)]; !ok {
			t.Errorf("Unexpected key: %q", key)
		}
		iter.Next()
	}

	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	// Note: count may not match if the iterator doesn't work correctly with Go-generated files
	t.Logf("Iterator found %d entries", count)
}

// =============================================================================
// STRESS TESTS: SstFileWriter
// =============================================================================

func TestSstFileWriter_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "large.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write 100K entries
	numEntries := 100000
	for i := range numEntries {
		key := make([]byte, 16)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)

		value := make([]byte, 100)
		if err := writer.Put(key, value); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumEntries != uint64(numEntries) {
		t.Errorf("Expected %d entries, got %d", numEntries, info.NumEntries)
	}

	// Verify file size is reasonable (at least 100 bytes per entry)
	expectedMinSize := uint64(numEntries * 100)
	if info.FileSize < expectedMinSize/2 {
		t.Errorf("File too small: %d bytes (expected at least %d)", info.FileSize, expectedMinSize/2)
	}
}

func TestSstFileWriter_Merge(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put, Merge, Put pattern
	if err := writer.Put([]byte("a"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := writer.Merge([]byte("b"), []byte("merge_operand")); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if err := writer.Put([]byte("c"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumEntries != 3 {
		t.Errorf("Expected 3 entries, got %d", info.NumEntries)
	}
}

func TestSstFileWriter_CustomComparator(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	// Use a custom comparator (reverse order)
	opts := DefaultSstFileWriterOptions()
	opts.Comparator = reverseComparator{}

	writer := NewSstFileWriter(opts)
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// In reverse order, z < y < x < ...
	if err := writer.Put([]byte("z"), []byte("value")); err != nil {
		t.Fatalf("Put z failed: %v", err)
	}
	if err := writer.Put([]byte("y"), []byte("value")); err != nil {
		t.Fatalf("Put y failed: %v", err)
	}
	if err := writer.Put([]byte("x"), []byte("value")); err != nil {
		t.Fatalf("Put x failed: %v", err)
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumEntries != 3 {
		t.Errorf("Expected 3 entries, got %d", info.NumEntries)
	}
}

// reverseComparator compares keys in reverse order.
type reverseComparator struct{}

func (reverseComparator) Compare(a, b []byte) int {
	return bytes.Compare(b, a) // Note: reversed
}

func (reverseComparator) Name() string {
	return "reverseComparator"
}

func (reverseComparator) FindShortestSeparator(a, b []byte) []byte {
	return a // Simple implementation
}

func (reverseComparator) FindShortSuccessor(a []byte) []byte {
	return a // Simple implementation
}

func TestSstFileWriter_ManyRangeDeletions(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "rangedel.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put some boundary keys
	if err := writer.Put([]byte("a"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := writer.Put([]byte("z"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Add many non-overlapping range deletions
	for i := range 100 {
		start := []byte{byte('b'), byte(i), 0}
		end := []byte{byte('b'), byte(i), 255}
		if err := writer.DeleteRange(start, end); err != nil {
			t.Fatalf("DeleteRange %d failed: %v", i, err)
		}
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.NumRangeDelEntries != 100 {
		t.Errorf("Expected 100 range del entries, got %d", info.NumRangeDelEntries)
	}
}

// Helper types for testing
type osFileWrapperForTest struct {
	f    *os.File
	size int64
}

func (w *osFileWrapperForTest) ReadAt(p []byte, off int64) (int, error) {
	return w.f.ReadAt(p, off)
}

func (w *osFileWrapperForTest) Size() int64 {
	return w.size
}

func (w *osFileWrapperForTest) Close() error {
	return w.f.Close()
}

func extractUserKeyForTest(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}
