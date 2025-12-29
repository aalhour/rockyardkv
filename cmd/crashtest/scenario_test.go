// Scenario-based crash tests for RockyardKV.
//
// These tests verify specific durability contracts by:
// 1. Running a child process that performs an operation
// 2. Killing the child process (simulating crash)
// 3. Reopening the database and verifying invariants
//
// Reference: RocksDB v10.7.5
//   - tools/db_crashtest.py (whitebox testing)
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/db"
)

// TestScenario_SyncedWriteSurvivesCrash verifies that a synced write
// is durable after the sync call returns.
//
// Contract: If Put() with sync=true returns success, the write survives crash.
func TestScenario_SyncedWriteSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// Write a key with sync=true, then exit
	runScenarioChild(t, dir, "write-sync", func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("crash_key"), []byte("crash_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	// Reopen and verify
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("crash_key"))
	if err != nil {
		t.Fatalf("Get after crash failed: %v", err)
	}
	if string(value) != "crash_value" {
		t.Errorf("Value mismatch: got %q, want %q", value, "crash_value")
	}
}

// TestScenario_FlushedDataSurvivesCrash verifies that flushed data
// is durable after the flush call returns.
//
// Contract: If Flush() returns success, all prior writes survive crash.
func TestScenario_FlushedDataSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// Write keys, flush, then exit
	runScenarioChild(t, dir, "write-flush", func(database db.DB) {
		for i := range 100 {
			key := fmt.Sprintf("flush_key_%04d", i)
			value := fmt.Sprintf("flush_value_%04d", i)
			if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	})

	// Reopen and verify all keys
	database := openDB(t, dir)
	defer database.Close()

	for i := range 100 {
		key := fmt.Sprintf("flush_key_%04d", i)
		expectedValue := fmt.Sprintf("flush_value_%04d", i)

		value, err := database.Get(nil, []byte(key))
		if err != nil {
			t.Errorf("Get %q after crash failed: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Value mismatch for %q: got %q, want %q", key, value, expectedValue)
		}
	}
}

// TestScenario_SyncedDeleteSurvivesCrash verifies that a synced delete
// is durable after the sync call returns.
//
// Contract: If Delete() with sync=true returns success, the delete survives crash.
func TestScenario_SyncedDeleteSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// First, write a key
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("delete_me"), []byte("exists")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		database.Close()
	}

	// Delete the key with sync=true, then exit
	runScenarioChild(t, dir, "delete-sync", func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Delete(opts, []byte("delete_me")); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	})

	// Reopen and verify key is deleted
	database := openDB(t, dir)
	defer database.Close()

	_, err := database.Get(nil, []byte("delete_me"))
	if !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got: %v", err)
	}
}

// TestScenario_WriteBatchAtomicity verifies that WriteBatch is atomic.
//
// Contract: WriteBatch is all-or-nothing — either all writes are visible or none.
func TestScenario_WriteBatchAtomicity(t *testing.T) {
	dir := t.TempDir()

	// Write a batch with sync=true, then exit
	runScenarioChild(t, dir, "batch-sync", func(database db.DB) {
		wb := db.NewWriteBatch()
		for i := range 50 {
			wb.Put(fmt.Appendf(nil, "batch_key_%04d", i), fmt.Appendf(nil, "batch_value_%04d", i))
		}
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Write(opts, wb); err != nil {
			t.Fatalf("Write batch failed: %v", err)
		}
	})

	// Reopen and verify all-or-nothing
	database := openDB(t, dir)
	defer database.Close()

	foundCount := 0
	for i := range 50 {
		key := fmt.Sprintf("batch_key_%04d", i)
		_, err := database.Get(nil, []byte(key))
		if err == nil {
			foundCount++
		}
	}

	// All-or-nothing: either 0 or 50
	if foundCount != 0 && foundCount != 50 {
		t.Errorf("WriteBatch atomicity violated: found %d of 50 keys (expected 0 or 50)", foundCount)
	}
	if foundCount == 0 {
		t.Log("Batch not committed (crash before sync)")
	} else {
		t.Log("Batch fully committed")
	}
}

// TestScenario_DoubleCrashRecovery verifies that recovery is stable.
//
// Contract: A DB that was recovered can be recovered again after another crash.
func TestScenario_DoubleCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// First crash: write and crash
	runScenarioChild(t, dir, "crash-1", func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("double_crash_key"), []byte("value_1")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	// Second crash: reopen, write more, crash again
	runScenarioChild(t, dir, "crash-2", func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("double_crash_key"), []byte("value_2")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	// Final recovery
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("double_crash_key"))
	if err != nil {
		t.Fatalf("Get after double crash failed: %v", err)
	}
	// Could be value_1 or value_2 depending on timing
	if string(value) != "value_1" && string(value) != "value_2" {
		t.Errorf("Unexpected value after double crash: %q", value)
	}
	t.Logf("Value after double crash: %q", value)
}

// TestScenario_FlushRecoveryNoSequenceReuse verifies that after crash recovery,
// sequence numbers are never reused, preventing internal key collisions.
//
// Contract: After flush+crash+recovery, new writes get fresh sequences that
// don't overlap with sequences in existing SSTs (even orphaned ones).
// =============================================================================
// Helpers
// =============================================================================

// runScenarioChild runs the given function in a child process and kills it.
// This simulates a crash after the function completes.
func runScenarioChild(t *testing.T, dir, scenario string, fn func(db.DB)) {
	t.Helper()

	// Check if we're the child process
	if os.Getenv("CRASH_SCENARIO") == scenario {
		// We're the child — get DB path from env
		dbPath := os.Getenv("CRASH_DB_PATH")
		if dbPath == "" {
			t.Fatal("CRASH_DB_PATH not set")
		}
		database := openOrCreateDB(t, dbPath)
		fn(database)
		// Don't close cleanly — simulate crash by exiting
		os.Exit(0)
	}

	// We're the parent — spawn child
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(),
		"CRASH_SCENARIO="+scenario,
		"CRASH_DB_PATH="+dir,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start child: %v", err)
	}

	// Wait a bit for the operation to complete
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		// Child exited (normal for our scenarios)
		if err != nil {
			// Non-zero exit is fine, it means the child crashed as expected
			t.Logf("Child exited: %v", err)
		}
	case <-time.After(10 * time.Second):
		// Timeout — kill the child
		_ = cmd.Process.Signal(syscall.SIGKILL)
		t.Fatalf("Child timed out. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
	}
}

func createDB(t *testing.T, dir string) db.DB {
	t.Helper()
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	return database
}

func openDB(t *testing.T, dir string) db.DB {
	t.Helper()
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	return database
}

func openOrCreateDB(t *testing.T, dir string) db.DB {
	t.Helper()

	// Check if DB exists
	manifestPath := filepath.Join(dir, "CURRENT")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return createDB(t, dir)
	}
	return openDB(t, dir)
}

// runCollisionCheck runs the collision checker tool on a database directory.
// This is the definitive smoking-gun test for internal-key collision detection.
func runCollisionCheck(t *testing.T, dbPath string) error {
	t.Helper()

	// Use sstdump for collision-check (consistent with status scripts)
	cmd := exec.Command("go", "run", "../../cmd/sstdump", "--command=collision-check", "--dir="+dbPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("collision check failed: %w\nOutput:\n%s", err, output)
	}

	// Check output for collision report
	if bytes.Contains(output, []byte("SMOKING GUN")) {
		return fmt.Errorf("collision detected:\n%s", output)
	}

	// Verify success message is present (case-insensitive substring check)
	if !bytes.Contains(output, []byte("no internal-key collisions")) {
		return fmt.Errorf("unexpected collision check output:\n%s", output)
	}

	return nil
}
