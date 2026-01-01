package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/logging"
)

// Contract: After Fatalf, writes are rejected with ErrFatal.
func TestFatalf_RejectsWritesAfterFatal(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// Write should succeed before fatal
	if err := database.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put before fatal: %v", err)
	}

	// Trigger fatal condition via logger
	dbImpl.logger.Fatalf("[test] simulated corruption: %s", "checksum mismatch")

	// Write should be rejected after fatal
	err = database.Put(nil, []byte("key2"), []byte("value2"))
	if err == nil {
		t.Error("Put should fail after Fatalf")
	}
	if !errors.Is(err, ErrBackgroundError) {
		t.Errorf("Expected ErrBackgroundError, got: %v", err)
	}
	if !errors.Is(err, logging.ErrFatal) {
		t.Errorf("Expected logging.ErrFatal, got: %v", err)
	}
}

// Contract: After Fatalf, reads continue to work.
func TestFatalf_ReadsStillWork(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// Write data before fatal
	if err := database.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put before fatal: %v", err)
	}

	// Trigger fatal condition
	dbImpl.logger.Fatalf("[test] simulated invariant violation")

	// Read should still work
	value, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Errorf("Get should work after Fatalf, got: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Got %q, want %q", value, "value1")
	}
}

// Contract: GetBackgroundError returns the fatal error after Fatalf.
func TestFatalf_BackgroundErrorContainsFatalMessage(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// Trigger fatal with specific message
	dbImpl.logger.Fatalf("file already compacting: %d", 42)

	// Check background error
	bgErr := dbImpl.GetBackgroundError()
	if bgErr == nil {
		t.Fatal("GetBackgroundError should return error after Fatalf")
	}
	if !errors.Is(bgErr, logging.ErrFatal) {
		t.Errorf("Background error should wrap ErrFatal, got: %v", bgErr)
	}
	if !strings.Contains(bgErr.Error(), "file already compacting: 42") {
		t.Errorf("Background error should contain fatal message, got: %v", bgErr)
	}
}

// Contract: First fatal wins (subsequent fatals do not overwrite).
func TestFatalf_FirstFatalWins(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// First fatal
	dbImpl.logger.Fatalf("first fatal: corruption")

	// Second fatal
	dbImpl.logger.Fatalf("second fatal: invariant")

	// Check that first error is preserved
	bgErr := dbImpl.GetBackgroundError()
	if !strings.Contains(bgErr.Error(), "first fatal: corruption") {
		t.Errorf("First fatal should win, got: %v", bgErr)
	}
	if strings.Contains(bgErr.Error(), "second fatal") {
		t.Error("Second fatal should not overwrite first")
	}
}

// Contract: DiscardLogger's Fatalf is a no-op (does not set background error).
func TestFatalf_DiscardLogger_NoEffect(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.Discard // Use discard logger

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// Fatalf on Discard logger is a no-op
	dbImpl.logger.Fatalf("this should not set background error")

	// Writes should still work
	if err := database.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Errorf("Put should work when using DiscardLogger, got: %v", err)
	}

	// No background error
	if bgErr := dbImpl.GetBackgroundError(); bgErr != nil {
		t.Errorf("DiscardLogger should not set background error, got: %v", bgErr)
	}
}

// Contract: Fatalf is safe to call from multiple goroutines.
func TestFatalf_ConcurrentSafe(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl := database.(*DBImpl)

	// Trigger many fatals concurrently
	done := make(chan struct{})
	for i := range 10 {
		go func(n int) {
			dbImpl.logger.Fatalf("fatal from goroutine %d", n)
			done <- struct{}{}
		}(i)
	}

	// Wait for all goroutines
	for range 10 {
		<-done
	}

	// Verify writes are rejected
	err = database.Put(nil, []byte("key"), []byte("value"))
	if err == nil {
		t.Error("Put should fail after concurrent Fatalf calls")
	}
	if !errors.Is(err, logging.ErrFatal) {
		t.Errorf("Expected ErrFatal, got: %v", err)
	}
}
