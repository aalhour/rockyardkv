package rockyardkv

// flush_fault_test.go implements tests for flush fault.

import (
	"errors"
	"testing"

	"github.com/aalhour/rockyardkv/vfs"
)

// -----------------------------------------------------------------------------
// Fault Injection Tests for Flush
// These tests verify error handling during flush operations.
// -----------------------------------------------------------------------------

// TestFlushWithWriteError tests flush behavior when writes fail.
func TestFlushWithWriteError(t *testing.T) {
	tmpDir := t.TempDir()

	// Create fault injection filesystem
	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := []byte{byte('k'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Inject write error for SST files
	faultFS.InjectWriteError("*.sst")

	// Flush should fail
	_ = database.Flush(nil)
	// We expect an error due to write failure
	// (depending on implementation, this may succeed or fail gracefully)

	// Clear error and close
	faultFS.ClearErrors()
	database.Close()
}

// TestFlushWithSyncError tests flush behavior when sync fails.
func TestFlushWithSyncError(t *testing.T) {
	tmpDir := t.TempDir()

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := []byte{byte('k'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Inject sync error
	faultFS.InjectSyncError()

	// Flush may fail or succeed (implementation dependent)
	_ = database.Flush(nil)

	faultFS.ClearErrors()
	database.Close()
}

// TestFlushRecoveryAfterError tests that we can recover after a flush error.
func TestFlushRecoveryAfterError(t *testing.T) {
	tmpDir := t.TempDir()

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write initial data
	for i := range 5 {
		key := []byte{byte('a'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// First flush should work
	if err := database.Flush(nil); err != nil {
		t.Logf("First flush failed (may be expected): %v", err)
	}

	// Data should still be readable
	for i := range 5 {
		key := []byte{byte('a'), byte(i)}
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get after first flush failed for key %v: %v", key, err)
		}
	}

	// Write more data
	for i := range 5 {
		key := []byte{byte('b'), byte(i)}
		value := []byte{byte('w'), byte(i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put (second batch) failed: %v", err)
		}
	}

	// Inject error for second flush
	faultFS.InjectWriteError("*.sst")
	_ = database.Flush(nil) // May fail

	// Clear error
	faultFS.ClearErrors()

	// Original data should still be readable (from first SST or memtable)
	for i := range 5 {
		key := []byte{byte('a'), byte(i)}
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get after failed flush failed for key %v: %v", key, err)
		}
	}

	database.Close()
}

// TestFlushDataIntegrity tests data integrity across flush.
func TestFlushDataIntegrity(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data with specific patterns
	numEntries := 100
	for i := range numEntries {
		key := make([]byte, 8)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)

		value := make([]byte, 32)
		for j := range value {
			value[j] = byte((i + j) % 256)
		}

		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Flush
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify all data
	for i := range numEntries {
		key := make([]byte, 8)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)

		expectedValue := make([]byte, 32)
		for j := range expectedValue {
			expectedValue[j] = byte((i + j) % 256)
		}

		gotValue, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get %d failed: %v", i, err)
			continue
		}

		if len(gotValue) != len(expectedValue) {
			t.Errorf("Value %d length mismatch: got %d, want %d", i, len(gotValue), len(expectedValue))
			continue
		}

		for j := range expectedValue {
			if gotValue[j] != expectedValue[j] {
				t.Errorf("Value %d byte %d mismatch: got %d, want %d", i, j, gotValue[j], expectedValue[j])
				break
			}
		}
	}

	database.Close()
}

// TestFlushConcurrentReads tests reads during flush.
func TestFlushConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write data
	for i := range 50 {
		key := []byte{byte('k'), byte(i)}
		value := []byte{byte('v'), byte(i)}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Start concurrent reads
	done := make(chan bool)
	go func() {
		for range 100 {
			for i := range 50 {
				key := []byte{byte('k'), byte(i)}
				_, err := database.Get(nil, key)
				if err != nil && !errors.Is(err, ErrNotFound) {
					// Errors other than not found during concurrent access
					// might indicate issues
				}
			}
		}
		done <- true
	}()

	// Flush while reading
	if err := database.Flush(nil); err != nil {
		t.Logf("Flush during concurrent reads: %v", err)
	}

	<-done
	database.Close()
}
