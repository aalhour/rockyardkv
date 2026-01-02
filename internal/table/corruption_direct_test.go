package table

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/vfs"
)

// TestCorruptionDetectionDirect verifies that corrupting a data block
// causes checksum verification failure during iteration.
func TestCorruptionDetectionDirect(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "test.sst")

	// Create a valid SST file
	file, err := os.Create(sstPath)
	if err != nil {
		t.Fatal(err)
	}

	builder := NewTableBuilder(file, DefaultBuilderOptions())
	for i := range 100 {
		key := dbformat.NewInternalKey(
			fmt.Appendf(nil, "key%05d", i),
			dbformat.SequenceNumber(100-i),
			dbformat.TypeValue,
		)
		value := fmt.Appendf(nil, "value%05d", i)
		if err := builder.Add(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Read the file and corrupt it
	data, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SST file size: %d bytes", len(data))

	// Corrupt data in the first data block (but not footer/index).
	// The file structure is: [data blocks] [meta blocks] [metaindex] [index] [footer]
	// Footer is ~48-64 bytes at the end.
	// We corrupt the first data block.
	if len(data) > 200 {
		// Flip bits in byte 50-100 (should be in first data block)
		for i := 50; i < 100; i++ {
			data[i] ^= 0xFF
		}
	}

	if err := os.WriteFile(sstPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Now try to read with checksum verification
	fs := vfs.Default()
	rfile, err := fs.OpenRandomAccess(sstPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	reader, err := Open(rfile, ReaderOptions{VerifyChecksums: true})
	if err != nil {
		t.Logf("table.Open correctly failed: %v", err)
		return // Success - corruption detected during open
	}

	// Try to iterate
	iter := reader.NewIterator()
	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
		if err := iter.Error(); err != nil {
			t.Logf("Iterator correctly detected corruption after %d entries: %v", count, err)
			return // Success - corruption detected during iteration
		}
	}
	if err := iter.Error(); err != nil {
		t.Logf("Iterator correctly detected corruption: %v", err)
		return // Success
	}

	t.Errorf("FAILURE: Iterated %d entries with NO corruption detected! Checksum verification is broken.", count)
}
