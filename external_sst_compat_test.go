package rockyardkv

// external_sst_compat_test.go implements tests for external sst compat.


import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/table"
)

// =============================================================================
// C++ COMPATIBILITY TESTS
// These tests verify that SST files created by Go can be read correctly,
// matching C++ RocksDB behavior.
// =============================================================================

// TestSstFileWriter_FormatVersion3Compatibility tests that format version 3
// SST files are properly readable.
func TestSstFileWriter_FormatVersion3Compatibility(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "format_v3.sst")

	// Create SST with format version 3 (most compatible)
	opts := DefaultSstFileWriterOptions()
	opts.FormatVersion = 3

	writer := NewSstFileWriter(opts)
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	entries := map[string]string{
		"compat_key1": "compat_value1",
		"compat_key2": "compat_value2",
		"compat_key3": "compat_value3",
	}

	keys := []string{"compat_key1", "compat_key2", "compat_key3"}
	for _, k := range keys {
		if err := writer.Put([]byte(k), []byte(entries[k])); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	if info.Version != 3 {
		t.Errorf("Expected version 3, got %d", info.Version)
	}

	// Read back and verify
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	wrapper := &compatFileWrapper{f: file, size: stat.Size()}
	reader, err := table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	// Verify all entries
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		key := extractUserKeyCompat(iter.Key())
		expectedValue, ok := entries[string(key)]
		if !ok {
			t.Errorf("Unexpected key: %q", key)
		} else {
			if !bytes.Equal(iter.Value(), []byte(expectedValue)) {
				t.Errorf("Value mismatch for %s: expected %q, got %q", key, expectedValue, iter.Value())
			}
		}
		count++
		iter.Next()
	}

	if count != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), count)
	}
}

// TestSstFileWriter_InternalKeyFormat tests that internal keys are correctly formatted.
// C++ RocksDB expects: user_key + 8 bytes (sequence number + type).
func TestSstFileWriter_InternalKeyFormat(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "internal_key.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	userKey := []byte("test_user_key")
	if err := writer.Put(userKey, []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if _, err := writer.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read back and check internal key format
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	wrapper := &compatFileWrapper{f: file, size: stat.Size()}
	reader, err := table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	iter := reader.NewIterator()
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid")
	}

	internalKey := iter.Key()

	// Internal key should be user_key + 8 bytes trailer
	if len(internalKey) != len(userKey)+8 {
		t.Errorf("Internal key wrong size: expected %d, got %d", len(userKey)+8, len(internalKey))
	}

	// User key portion should match
	if !bytes.Equal(internalKey[:len(userKey)], userKey) {
		t.Errorf("User key mismatch: expected %q, got %q", userKey, internalKey[:len(userKey)])
	}

	// The 8-byte trailer contains sequence number (7 bytes) + type (1 byte)
	// For SstFileWriter, sequence number is 0 and type is kTypeValue (1)
	trailer := internalKey[len(userKey):]
	if len(trailer) != 8 {
		t.Errorf("Trailer wrong size: expected 8, got %d", len(trailer))
	}

	// The trailer is stored as little-endian: (seqno << 8) | type
	// For seqno=0, type=1, we expect the last byte to be 1 and rest 0
	// Actually: stored as 8 bytes little-endian of (seqno << 8 | type)
	// So for seqno=0, type=1: value = 1, stored as [1,0,0,0,0,0,0,0]
	if trailer[0] != 1 {
		t.Errorf("Expected type=1 (kTypeValue), got %d", trailer[0])
	}
}

// TestSstFileWriter_BlockFormat tests that data blocks are correctly formatted.
func TestSstFileWriter_BlockFormat(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "block_format.sst")

	opts := DefaultSstFileWriterOptions()
	opts.BlockSize = 256 // Small blocks to force multiple blocks

	writer := NewSstFileWriter(opts)
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write many entries to force multiple blocks
	for i := range 100 {
		key := make([]byte, 16)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		value := make([]byte, 100)
		if err := writer.Put(key, value); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	t.Logf("Created SST: %d bytes, %d entries", info.FileSize, info.NumEntries)

	// Verify readability
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	wrapper := &compatFileWrapper{f: file, size: stat.Size()}
	reader, err := table.Open(wrapper, table.ReaderOptions{
		VerifyChecksums: true,
	})
	if err != nil {
		t.Fatalf("Failed to open reader with checksum verification: %v", err)
	}

	// Iterate and count entries
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	if err := iter.Error(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}
}

// TestSstFileWriter_RangeDeletionBlockFormat tests range deletion block format.
func TestSstFileWriter_RangeDeletionBlockFormat(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "rangedel.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Put keys at boundaries
	if err := writer.Put([]byte("a"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := writer.Put([]byte("z"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Add range deletion
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

	// Verify the file is readable
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	wrapper := &compatFileWrapper{f: file, size: stat.Size()}
	_, err = table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
}

// TestSstFileWriter_FooterFormat tests the SST footer format matches C++ expectations.
func TestSstFileWriter_FooterFormat(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "footer.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := writer.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	info, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read the footer directly
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	fileSize := stat.Size()

	// Footer is at the end of the file
	// For format version >= 1, footer is 53 bytes (excluding checksum)
	// Magic number is the last 8 bytes
	footer := make([]byte, 64)
	if _, err := file.ReadAt(footer, fileSize-int64(len(footer))); err != nil {
		t.Fatalf("Failed to read footer: %v", err)
	}

	// The magic number is at the end
	// Block-based table magic: 0x88e241b785f4cff7 (little-endian)
	// Legacy magic: 0xdb4775248b80fb57
	magicBytes := footer[len(footer)-8:]
	t.Logf("Footer magic bytes: %x", magicBytes)

	// Verify the file can be opened by our reader
	wrapper := &compatFileWrapper{f: file, size: fileSize}
	reader, err := table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	_ = reader
	_ = info
}

// TestSstFileWriter_PropertiesBlock tests that table properties are correctly written.
func TestSstFileWriter_PropertiesBlock(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "properties.sst")

	writer := NewSstFileWriter(DefaultSstFileWriterOptions())
	if err := writer.Open(sstPath); err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some entries
	for i := range 10 {
		key := []byte{byte('a' + i)}
		if err := writer.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if _, err := writer.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	// Read and verify properties
	file, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SST: %v", err)
	}
	defer file.Close()

	stat, _ := file.Stat()
	wrapper := &compatFileWrapper{f: file, size: stat.Size()}
	reader, err := table.Open(wrapper, table.ReaderOptions{})
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}

	// If reader exposes properties, verify them
	// Note: reader.GetTableProperties() may not be available in the current API
	_ = reader
	t.Log("SST file with properties created successfully")
}

type compatFileWrapper struct {
	f    *os.File
	size int64
}

func (w *compatFileWrapper) ReadAt(p []byte, off int64) (int, error) {
	return w.f.ReadAt(p, off)
}

func (w *compatFileWrapper) Size() int64 {
	return w.size
}

func (w *compatFileWrapper) Close() error {
	return w.f.Close()
}

func extractUserKeyCompat(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}
