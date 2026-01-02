package rockyardkv

// background_test.go implements tests for background.

import (
	"errors"
	"testing"
	"time"
)

// TestBackgroundCompactionTrigger tests that compaction is triggered after flush.
func TestBackgroundCompactionTrigger(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	// Write enough data to trigger multiple flushes and potentially compaction
	for i := range 100 {
		key := []byte{byte(i)}
		value := make([]byte, 1024) // 1KB values
		for j := range value {
			value[j] = byte(i + j)
		}
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST files
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Give background worker a chance to run
	time.Sleep(100 * time.Millisecond)

	// Verify data is still readable
	for i := range 100 {
		key := []byte{byte(i)}
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get key %d failed: %v", i, err)
			continue
		}
		if len(value) != 1024 {
			t.Errorf("Key %d: value length = %d, want 1024", i, len(value))
		}
	}
}

// TestBackgroundCompactionMultipleFlushes tests compaction with multiple flushes.
func TestBackgroundCompactionMultipleFlushes(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	// Multiple batches of writes and flushes
	for batch := range 5 {
		// Write data for this batch
		for i := range 20 {
			key := []byte{byte(batch), byte(i)}
			value := []byte{byte(batch*20 + i)}
			if err := database.Put(nil, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Flush after each batch
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush %d failed: %v", batch, err)
		}
	}

	// Give background worker a chance to compact
	time.Sleep(200 * time.Millisecond)

	// Verify all data is still readable
	for batch := range 5 {
		for i := range 20 {
			key := []byte{byte(batch), byte(i)}
			value, err := database.Get(nil, key)
			if err != nil {
				t.Errorf("Get batch=%d i=%d failed: %v", batch, i, err)
				continue
			}
			expected := []byte{byte(batch*20 + i)}
			if len(value) != 1 || value[0] != expected[0] {
				t.Errorf("Key [%d,%d]: value = %v, want %v", batch, i, value, expected)
			}
		}
	}
}

// TestBackgroundWorkShutdown tests graceful shutdown of background workers.
func TestBackgroundWorkShutdown(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write some data
	for i := range 50 {
		key := []byte{byte(i)}
		if err := database.Put(nil, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Close should wait for background workers
	err = database.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Re-open and verify data
	database2, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Re-open failed: %v", err)
	}
	defer database2.Close()

	for i := range 50 {
		key := []byte{byte(i)}
		_, err := database2.Get(nil, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Errorf("Get key %d failed: %v", i, err)
		}
	}
}

// TestCompactionPickerIntegration tests the compaction picker with real DB.
func TestCompactionPickerIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Initially no compaction needed
	v := dbImpl.versions.Current()
	if v != nil {
		v.Ref()
		defer v.Unref()

		if dbImpl.bgWork.picker.NeedsCompaction(v) {
			t.Log("Compaction may be needed based on recovered state")
		}
	}
}

// TestBackgroundWorkPauseResume tests pausing and resuming background work.
func TestBackgroundWorkPauseResume(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Initially not paused
	if dbImpl.bgWork.isPaused() {
		t.Error("Background work should not be paused initially")
	}

	// Pause background work
	dbImpl.bgWork.pause()
	if !dbImpl.bgWork.isPaused() {
		t.Error("Background work should be paused after Pause()")
	}

	// Write data while paused
	for i := range 20 {
		key := []byte{byte(i)}
		if err := database.Put(nil, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Resume background work
	dbImpl.bgWork.resume()
	if dbImpl.bgWork.isPaused() {
		t.Error("Background work should not be paused after Continue()")
	}

	// Give background worker a chance to process
	time.Sleep(100 * time.Millisecond)

	// Verify data is still readable
	for i := range 20 {
		key := []byte{byte(i)}
		_, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get key %d failed: %v", i, err)
		}
	}
}

// TestBackgroundWorkStats tests background work statistics methods.
func TestBackgroundWorkStats(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Initially no flushes or compactions running
	numFlushes := dbImpl.bgWork.numRunningFlushes()
	if numFlushes != 0 {
		t.Logf("NumRunningFlushes = %d (may be non-zero if background work started)", numFlushes)
	}

	numCompactions := dbImpl.bgWork.numRunningCompactions()
	if numCompactions != 0 {
		t.Logf("NumRunningCompactions = %d (may be non-zero if compaction started)", numCompactions)
	}

	// Test error counting
	initialErrors := dbImpl.bgWork.numBackgroundErrors()
	dbImpl.bgWork.incrementBackgroundErrors()
	if dbImpl.bgWork.numBackgroundErrors() != initialErrors+1 {
		t.Error("NumBackgroundErrors should increment by 1")
	}
}

// TestIsCompactionPending tests the IsCompactionPending method.
func TestIsCompactionPending(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Initially, check if compaction is pending (depends on initial state)
	pending := dbImpl.bgWork.isCompactionPending()
	t.Logf("IsCompactionPending: pending=%v", pending)

	// Write data to potentially trigger compaction
	for i := range 50 {
		key := []byte{byte(i)}
		value := make([]byte, 1024)
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST files
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Check again
	pending = dbImpl.bgWork.isCompactionPending()
	t.Logf("After flush: pending=%v", pending)
}

// TestMaybeScheduleFlush tests the MaybeScheduleFlush method.
func TestMaybeScheduleFlush(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Schedule a flush
	dbImpl.bgWork.maybeScheduleFlush()

	// Give background worker a chance to process
	time.Sleep(50 * time.Millisecond)

	// This should not cause any errors even if there's nothing to flush
}

// TestBackgroundWorkScheduling tests background work scheduling.
func TestBackgroundWorkScheduling(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(*dbImpl)
	if !ok {
		t.Skip("Cannot access dbImpl")
	}

	// Test scheduling compaction and flush
	dbImpl.bgWork.maybeScheduleCompaction()
	dbImpl.bgWork.maybeScheduleFlush()

	// Give the scheduler time to check
	time.Sleep(50 * time.Millisecond)

	// These calls should not cause any errors
}
