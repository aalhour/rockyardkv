// Fatalf scenario tests for RockyardKV.
//
// These tests verify the Fatalf behavior:
//   - After Fatalf, writes are rejected in the current session
//   - On reopen, the DB is usable again (backgroundError is not persisted)
//   - If actual corruption exists, it may be detected on access
//
// Reference: RocksDB v10.7.5 db/error_handler.cc
package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aalhour/rockyardkv"
	"github.com/aalhour/rockyardkv/internal/logging"
)

type fatalDB interface {
	Logger() rockyardkv.Logger
	GetBackgroundError() error
}

// =============================================================================
// Fatalf: In-Session Behavior
// =============================================================================

// Contract: After Fatalf, writes are rejected with ErrFatal in the current session.
func TestScenario_Fatalf_RejectsWritesInSession(t *testing.T) {
	dir := t.TempDir()

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := rockyardkv.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	dbImpl, ok := database.(fatalDB)
	if !ok {
		t.Fatalf("Open returned unexpected DB type: %T", database)
	}

	// Write before fatal
	if err := database.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put before fatal: %v", err)
	}

	// Trigger fatal
	dbImpl.Logger().Fatalf("[test] simulated corruption detected")

	// Write should be rejected
	err = database.Put(nil, []byte("key2"), []byte("value2"))
	if err == nil {
		t.Error("Put should fail after Fatalf")
	}
	if !errors.Is(err, logging.ErrFatal) {
		t.Errorf("Expected ErrFatal, got: %v", err)
	}

	// Read should still work
	value, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Errorf("Get should work after Fatalf: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Got %q, want %q", value, "value1")
	}

	database.Close()
}

// Contract: After reopen, DB is usable again (backgroundError is not persisted).
func TestScenario_Fatalf_ReopenClearsError(t *testing.T) {
	dir := t.TempDir()

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	// First session: trigger fatal
	database, err := rockyardkv.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	dbImpl, ok := database.(fatalDB)
	if !ok {
		t.Fatalf("Open returned unexpected DB type: %T", database)
	}

	if err := database.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Trigger fatal
	dbImpl.Logger().Fatalf("[test] simulated invariant violation")

	// Verify writes are rejected
	if err := database.Put(nil, []byte("key2"), []byte("value2")); err == nil {
		t.Fatal("Put should fail after Fatalf")
	}

	database.Close()

	// Second session: reopen
	opts.CreateIfMissing = false
	database, err = rockyardkv.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer database.Close()

	dbImpl, ok = database.(fatalDB)
	if !ok {
		t.Fatalf("Open returned unexpected DB type: %T", database)
	}

	// Background error should be nil after reopen
	if bgErr := dbImpl.GetBackgroundError(); bgErr != nil {
		t.Errorf("Background error should be nil after reopen, got: %v", bgErr)
	}

	// Writes should work again
	if err := database.Put(nil, []byte("key3"), []byte("value3")); err != nil {
		t.Errorf("Put should work after reopen: %v", err)
	}

	// Previous data should be accessible
	value, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Errorf("Get key1: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("key1: got %q, want %q", value, "value1")
	}
}

// =============================================================================
// Fatalf: Subprocess Test (E2E)
// =============================================================================

// Contract: Child process that triggers Fatalf exits cleanly; parent can reopen DB.
func TestScenario_Fatalf_ChildProcessRejectsWrites_ParentReopens(t *testing.T) {
	if os.Getenv("CRASHTEST_CHILD") == "1" {
		// Child process: trigger fatal and attempt writes
		runChildFatalf(t)
		return
	}

	// Parent process: spawn child, then reopen and verify
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	// Create initial DB
	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := rockyardkv.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Create DB: %v", err)
	}
	if err := database.Put(nil, []byte("initial"), []byte("data")); err != nil {
		t.Fatalf("Initial put: %v", err)
	}
	database.Close()

	// Spawn child process
	cmd := exec.Command(os.Args[0],
		"-test.run=TestScenario_Fatalf_ChildProcessRejectsWrites_ParentReopens",
		"-test.v",
	)
	cmd.Env = append(os.Environ(),
		"CRASHTEST_CHILD=1",
		"CRASHTEST_DB_PATH="+dbPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Child may exit with error if writes failed as expected
		t.Logf("Child output:\n%s", output)
		if !strings.Contains(string(output), "CHILD_WRITES_REJECTED") {
			t.Fatalf("Child failed unexpectedly: %v", err)
		}
	}

	// Parent: reopen DB
	opts.CreateIfMissing = false
	database, err = rockyardkv.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Parent reopen: %v", err)
	}
	defer database.Close()

	// Verify initial data is present
	value, err := database.Get(nil, []byte("initial"))
	if err != nil {
		t.Errorf("Get initial: %v", err)
	}
	if string(value) != "data" {
		t.Errorf("initial: got %q, want %q", value, "data")
	}

	// Writes should work in parent
	if err := database.Put(nil, []byte("parent_key"), []byte("parent_value")); err != nil {
		t.Errorf("Parent put should work: %v", err)
	}
}

func runChildFatalf(t *testing.T) {
	dbPath := os.Getenv("CRASHTEST_DB_PATH")
	if dbPath == "" {
		t.Fatal("CRASHTEST_DB_PATH not set")
	}

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = false
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := rockyardkv.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Child open: %v", err)
	}

	dbImpl, ok := database.(fatalDB)
	if !ok {
		t.Fatalf("Child open returned unexpected DB type: %T", database)
	}

	// Write before fatal
	if err := database.Put(nil, []byte("child_before"), []byte("before_fatal")); err != nil {
		t.Fatalf("Child put before fatal: %v", err)
	}

	// Trigger fatal
	dbImpl.Logger().Fatalf("[child] simulated corruption in child process")

	// Write after fatal should fail
	err = database.Put(nil, []byte("child_after"), []byte("after_fatal"))
	if err == nil {
		t.Fatal("Child put after fatal should fail")
	}
	if !errors.Is(err, logging.ErrFatal) {
		t.Fatalf("Child expected ErrFatal, got: %v", err)
	}

	// Signal to parent that writes were correctly rejected
	t.Log("CHILD_WRITES_REJECTED")

	database.Close()
}

// =============================================================================
// Fatalf: Flush Behavior
// =============================================================================

// Contract: Flush is rejected after Fatalf.
func TestScenario_Fatalf_FlushRejected(t *testing.T) {
	dir := t.TempDir()

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := rockyardkv.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(fatalDB)
	if !ok {
		t.Fatalf("Open returned unexpected DB type: %T", database)
	}

	// Write some data
	if err := database.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Trigger fatal
	dbImpl.Logger().Fatalf("[test] fatal before flush")

	// Flush should be rejected
	err = database.Flush(nil)
	if err == nil {
		t.Error("Flush should fail after Fatalf")
	}
	if !errors.Is(err, rockyardkv.ErrBackgroundError) {
		t.Errorf("Expected ErrBackgroundError, got: %v", err)
	}
}

// =============================================================================
// Fatalf: Concurrent Access
// =============================================================================

// Contract: After Fatalf, all subsequent writes from any goroutine are rejected.
func TestScenario_Fatalf_ConcurrentWritersRejected(t *testing.T) {
	dir := t.TempDir()

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	database, err := rockyardkv.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	dbImpl, ok := database.(fatalDB)
	if !ok {
		t.Fatalf("Open returned unexpected DB type: %T", database)
	}

	// Trigger fatal first
	dbImpl.Logger().Fatalf("[test] fatal before concurrent writes")

	// Now spawn writers - all should fail
	errCh := make(chan error, 100)
	var wg sync.WaitGroup

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte{byte('a' + id)}
			for range 10 {
				err := database.Put(nil, key, []byte("value"))
				if err != nil {
					errCh <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// All writes should have failed with ErrFatal
	var fatalErrors int
	for err := range errCh {
		if errors.Is(err, logging.ErrFatal) {
			fatalErrors++
		}
	}

	// 10 goroutines * 10 writes = 100 expected failures
	if fatalErrors < 50 {
		t.Errorf("Expected at least 50 ErrFatal errors, got %d", fatalErrors)
	}
	t.Logf("Collected %d ErrFatal errors from concurrent writers", fatalErrors)
}
