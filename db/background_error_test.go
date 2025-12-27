package db

import (
	"errors"
	"testing"
)

// TestBackgroundErrorBlocksWrites verifies that setting a background error
// prevents new writes.
func TestBackgroundErrorBlocksWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Write should succeed initially
	err = db.Put(nil, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Simulate a background error (e.g., I/O failure)
	testError := errors.New("simulated I/O error: permission denied")
	db.(*DBImpl).SetBackgroundError(testError)

	// Write should now fail with background error
	err = db.Put(nil, []byte("key2"), []byte("value2"))
	if err == nil {
		t.Errorf("Expected Put to fail after background error, but it succeeded")
	}
	if !errors.Is(err, ErrBackgroundError) {
		t.Errorf("Expected ErrBackgroundError, got: %v", err)
	}

	// Verify the original error is wrapped
	if err != nil && !errors.Is(err, ErrBackgroundError) {
		t.Errorf("Error doesn't wrap ErrBackgroundError: %v", err)
	}

	// Reads should still work
	val, err := db.Get(nil, []byte("key1"))
	if err != nil {
		t.Errorf("Get should still work after background error, got: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}
}

// TestBackgroundErrorFirstErrorWins verifies that only the first background
// error is recorded.
func TestBackgroundErrorFirstErrorWins(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	impl := db.(*DBImpl)

	// Set first error
	err1 := errors.New("first error")
	impl.SetBackgroundError(err1)

	// Try to set second error
	err2 := errors.New("second error")
	impl.SetBackgroundError(err2)

	// First error should be retained
	bgErr := impl.GetBackgroundError()
	if !errors.Is(bgErr, err1) {
		t.Errorf("Expected first error, got: %v", bgErr)
	}
}

// TestBackgroundErrorFlushBlocked verifies that Flush is blocked after
// background error.
func TestBackgroundErrorFlushBlocked(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	impl := db.(*DBImpl)

	// Set background error
	testError := errors.New("simulated I/O error")
	impl.SetBackgroundError(testError)

	// Flush should fail with background error
	err = db.Flush(nil)
	if err == nil {
		t.Errorf("Expected Flush to fail after background error")
	}
	if !errors.Is(err, ErrBackgroundError) {
		t.Errorf("Expected ErrBackgroundError, got: %v", err)
	}
}
