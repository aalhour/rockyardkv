package rockyardkv

// flush_test.go implements tests for flush.


import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFlushBasic(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := range 100 {
		key := []byte{byte('k'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := db.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify SST file was created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	hasSST := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			hasSST = true
			break
		}
	}

	if !hasSST {
		t.Error("Expected SST file to be created after flush")
	}

	// Data should still be readable
	for i := range 100 {
		key := []byte{byte('k'), byte(i)}
		expectedValue := []byte{byte('v'), byte(i)}

		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get(%v) failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Get(%v) = %v, want %v", key, value, expectedValue)
		}
	}
}

func TestFlushEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Flush empty memtable should not fail
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush empty failed: %v", err)
	}

	// No SST file should be created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			t.Error("SST file should not be created for empty flush")
		}
	}
}

func TestFlushThenWrite(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := range 50 {
		key := []byte{byte('a'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := db.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write more data after flush
	for i := range 50 {
		key := []byte{byte('b'), byte(i)}
		value := []byte{byte('w'), byte(i)}
		if err := db.Put(nil, key, value); err != nil {
			t.Fatalf("Put after flush failed: %v", err)
		}
	}

	// All data should be readable
	for i := range 50 {
		key := []byte{byte('a'), byte(i)}
		expectedValue := []byte{byte('v'), byte(i)}
		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get(%v) failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Get(%v) = %v, want %v", key, value, expectedValue)
		}
	}

	for i := range 50 {
		key := []byte{byte('b'), byte(i)}
		expectedValue := []byte{byte('w'), byte(i)}
		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get(%v) failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Get(%v) = %v, want %v", key, value, expectedValue)
		}
	}
}

func TestFlushMultiple(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Multiple flush cycles
	for cycle := range 3 {
		for i := range 20 {
			key := []byte{byte('k'), byte(cycle), byte(i)}
			value := []byte{byte('v'), byte(cycle), byte(i)}
			if err := db.Put(nil, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		if err := db.Flush(nil); err != nil {
			t.Fatalf("Flush %d failed: %v", cycle, err)
		}
	}

	// Count SST files
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	sstCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstCount++
		}
	}

	if sstCount != 3 {
		t.Errorf("SST count = %d, want 3", sstCount)
	}

	// All data should be readable
	for cycle := range 3 {
		for i := range 20 {
			key := []byte{byte('k'), byte(cycle), byte(i)}
			expectedValue := []byte{byte('v'), byte(cycle), byte(i)}
			value, err := db.Get(nil, key)
			if err != nil {
				t.Errorf("Get(%v) failed: %v", key, err)
				continue
			}
			if string(value) != string(expectedValue) {
				t.Errorf("Get(%v) = %v, want %v", key, value, expectedValue)
			}
		}
	}
}

func TestFlushAndDelete(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write data
	for i := range 10 {
		key := []byte{byte('k'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := db.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Delete some keys
	for i := range 5 {
		key := []byte{byte('k'), byte(i)}
		if err := db.Delete(nil, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Deleted keys should not be found
	for i := range 5 {
		key := []byte{byte('k'), byte(i)}
		_, err := db.Get(nil, key)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%v) = %v, want ErrNotFound", key, err)
		}
	}

	// Non-deleted keys should still be readable
	for i := 5; i < 10; i++ {
		key := []byte{byte('k'), byte(i)}
		expectedValue := []byte{byte('v'), byte(i)}
		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get(%v) failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Get(%v) = %v, want %v", key, value, expectedValue)
		}
	}
}
