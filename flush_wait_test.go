package rockyardkv

// flush_wait_test.go implements tests for flush wait.


import (
	"sync"
	"testing"
	"time"
)

// TestFlushWaitsForImm verifies that Flush waits for existing immutable
// memtable to be flushed instead of returning an error.
func TestFlushWaitsForImm(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := range 100 {
		if err := db.Put(nil, []byte{byte(i)}, []byte{byte(i)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Concurrent flushes should not error
	var wg sync.WaitGroup
	var errors []error
	var mu sync.Mutex

	for range 5 {
		wg.Go(func() {
			if err := db.Flush(nil); err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			}
		})
	}

	// Wait with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatalf("Flush timed out - possible deadlock")
	}

	if len(errors) > 0 {
		t.Errorf("Flush errors (should be none): %v", errors)
	}
}

// TestFlushConcurrentWrites verifies that writes and flushes can happen
// concurrently without "immutable memtable already exists" errors.
func TestFlushConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Go(func() {
		for i := range 1000 {
			if err := db.Put(nil, []byte{byte(i % 256)}, []byte{byte(i % 256)}); err != nil {
				t.Errorf("Put failed: %v", err)
				return
			}
		}
	})

	// Flusher goroutine
	wg.Go(func() {
		for range 10 {
			if err := db.Flush(nil); err != nil {
				// Should not get "immutable memtable already exists"
				t.Errorf("Flush failed: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	// Wait with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatalf("Test timed out - possible deadlock")
	}
}
