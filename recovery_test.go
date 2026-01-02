package rockyardkv

// recovery_test.go implements tests for recovery.

import (
	"errors"
	"fmt"
	"testing"
)

// TestWALRecoveryBasic tests that unflushed writes are recovered from WAL.
func TestWALRecoveryBasic(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Session 1: Write data but don't flush
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// Write some data (will go to WAL and memtable)
		for i := range 10 {
			key := fmt.Sprintf("key%04d", i)
			value := fmt.Sprintf("value%04d", i)
			if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Verify data is readable before close
		val, err := database.Get(nil, []byte("key0005"))
		if err != nil {
			t.Fatalf("Get before close failed: %v", err)
		}
		if string(val) != "value0005" {
			t.Errorf("Value mismatch: got %s, want value0005", val)
		}

		// DO NOT FLUSH - data is only in WAL and memtable
	}()

	// Session 2: Reopen and verify data is recovered from WAL
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		// Verify all data is recovered
		for i := range 10 {
			key := fmt.Sprintf("key%04d", i)
			expected := fmt.Sprintf("value%04d", i)
			val, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Get %s after reopen failed: %v", key, err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get %s: got %s, want %s", key, val, expected)
			}
		}
	}()
}

// TestWALRecoveryWithFlush tests recovery with both flushed and unflushed data.
func TestWALRecoveryWithFlush(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Session 1: Write some data, flush, then write more
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// Write first batch
		for i := range 10 {
			key := fmt.Sprintf("key%04d", i)
			value := fmt.Sprintf("value%04d", i)
			if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Flush to SST
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Write second batch (goes to new WAL, not flushed)
		for i := 10; i < 20; i++ {
			key := fmt.Sprintf("key%04d", i)
			value := fmt.Sprintf("value%04d", i)
			if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// DO NOT FLUSH the second batch
	}()

	// Session 2: Verify both flushed and unflushed data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		// Verify flushed data (from SST)
		for i := range 10 {
			key := fmt.Sprintf("key%04d", i)
			expected := fmt.Sprintf("value%04d", i)
			val, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Get %s (flushed) failed: %v", key, err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get %s (flushed): got %s, want %s", key, val, expected)
			}
		}

		// Verify unflushed data (recovered from WAL)
		for i := 10; i < 20; i++ {
			key := fmt.Sprintf("key%04d", i)
			expected := fmt.Sprintf("value%04d", i)
			val, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Get %s (unflushed) failed: %v", key, err)
				continue
			}
			if string(val) != expected {
				t.Errorf("Get %s (unflushed): got %s, want %s", key, val, expected)
			}
		}
	}()
}

// TestWALRecoveryOverwrite tests that overwritten values are recovered correctly.
func TestWALRecoveryOverwrite(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Session 1: Write and overwrite
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// Write initial value
		if err := database.Put(nil, []byte("key"), []byte("value1")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Overwrite
		if err := database.Put(nil, []byte("key"), []byte("value2")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Overwrite again
		if err := database.Put(nil, []byte("key"), []byte("value3")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}()

	// Session 2: Verify latest value
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		val, err := database.Get(nil, []byte("key"))
		if err != nil {
			t.Fatalf("Get after reopen failed: %v", err)
		}
		if string(val) != "value3" {
			t.Errorf("Expected value3, got %s", val)
		}
	}()
}

// TestWALRecoveryDelete tests that deletes are recovered correctly.
func TestWALRecoveryDelete(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Session 1: Write and delete
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		// Write
		if err := database.Put(nil, []byte("key1"), []byte("value1")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := database.Put(nil, []byte("key2"), []byte("value2")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Delete key1
		if err := database.Delete(nil, []byte("key1")); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}()

	// Session 2: Verify delete is preserved
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		// key1 should be deleted
		_, err = database.Get(nil, []byte("key1"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for deleted key, got: %v", err)
		}

		// key2 should exist
		val, err := database.Get(nil, []byte("key2"))
		if err != nil {
			t.Fatalf("Get key2 failed: %v", err)
		}
		if string(val) != "value2" {
			t.Errorf("Expected value2, got %s", val)
		}
	}()
}

// TestWALRecoveryLargeData tests recovery of large amounts of data.
func TestWALRecoveryLargeData(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	numEntries := 1000

	// Session 1: Write lots of data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		for i := range numEntries {
			key := fmt.Sprintf("key%08d", i)
			value := make([]byte, 1000) // 1KB values
			for j := range value {
				value[j] = byte((i + j) % 256)
			}
			if err := database.Put(nil, []byte(key), value); err != nil {
				t.Fatalf("Put %d failed: %v", i, err)
			}
		}
	}()

	// Session 2: Verify all data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		for i := range numEntries {
			key := fmt.Sprintf("key%08d", i)
			val, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Get %s failed: %v", key, err)
				continue
			}
			if len(val) != 1000 {
				t.Errorf("Value length: got %d, want 1000", len(val))
				continue
			}
			// Verify content
			for j := range val {
				if val[j] != byte((i+j)%256) {
					t.Errorf("Value mismatch at key %d, offset %d", i, j)
					break
				}
			}
		}
	}()
}

// TestWALRecoveryEmpty tests recovery with no data.
func TestWALRecoveryEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Session 1: Open and close without writing
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		database.Close()
	}()

	// Session 2: Reopen empty database
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen failed: %v", err)
		}
		defer database.Close()

		// Should not find any keys
		_, err = database.Get(nil, []byte("key"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound, got: %v", err)
		}
	}()
}

// TestWALRecoveryMultipleReopens tests multiple open/close cycles.
func TestWALRecoveryMultipleReopens(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Cycle 1: Write some data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		defer database.Close()

		for i := range 10 {
			if err := database.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("v1")); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}()

	// Cycle 2: Add more data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen 1 failed: %v", err)
		}
		defer database.Close()

		for i := 10; i < 20; i++ {
			if err := database.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("v2")); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}()

	// Cycle 3: Flush and add more
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen 2 failed: %v", err)
		}
		defer database.Close()

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		for i := 20; i < 30; i++ {
			if err := database.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("v3")); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}()

	// Cycle 4: Verify all data
	func() {
		database, err := Open(tmpDir, opts)
		if err != nil {
			t.Fatalf("Reopen 3 failed: %v", err)
		}
		defer database.Close()

		for i := range 30 {
			key := fmt.Sprintf("key%d", i)
			_, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Get %s failed: %v", key, err)
			}
		}
	}()
}
