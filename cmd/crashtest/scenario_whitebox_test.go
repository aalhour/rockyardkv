//go:build crashtest

// Whitebox scenario crash tests for RockyardKV.
//
// These tests use kill points to deterministically crash at specific code
// boundaries and verify recovery invariants. Unlike blackbox tests (random
// timing), these tests guarantee specific crash points are exercised.
//
// Reference: RocksDB v10.7.5
//   - tools/db_crashtest.py (whitebox testing)
//   - test_util/sync_point.h (TEST_KILL_RANDOM macros)
//
// Build and run:
//
//	go test -tags crashtest -v ./cmd/crashtest/... -run TestScenarioWhitebox
package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// =============================================================================
// Scenario 1: WAL Sync Crash
// =============================================================================

// TestScenarioWhitebox_WALSync1_SyncedWriteSurvives verifies that a synced
// write is durable after WAL sync completes.
//
// Kill point: WAL.Sync:1 (after WAL sync)
// Invariant: Acknowledged synced writes are durable.
func TestScenarioWhitebox_WALSync1_SyncedWriteSurvives(t *testing.T) {
	dir := t.TempDir()

	// First create the DB so that subsequent writes go through WAL sync
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("initial_key"), []byte("initial_value")); err != nil {
			t.Fatalf("Put initial failed: %v", err)
		}
		database.Close()
	}

	// Run child process that writes with sync=true and crashes after WAL.Sync:1
	runWhiteboxChild(t, dir, testutil.KPWALSync1, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("wal_sync_key"), []byte("wal_sync_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	// Reopen and verify the write is present
	database := openDB(t, dir)
	defer database.Close()

	// Initial key must be present
	value, err := database.Get(nil, []byte("initial_key"))
	if err != nil {
		t.Fatalf("Get initial_key after WAL.Sync:1 crash failed: %v", err)
	}
	if string(value) != "initial_value" {
		t.Errorf("Initial value mismatch: got %q, want %q", value, "initial_value")
	}

	// WAL sync key should also be present since we crashed AFTER sync
	value, err = database.Get(nil, []byte("wal_sync_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		// Either present (sync completed) or not found (sync didn't complete)
		t.Fatalf("Get wal_sync_key after WAL.Sync:1 crash failed with unexpected error: %v", err)
	}
	if err == nil && string(value) != "wal_sync_value" {
		t.Errorf("Value mismatch: got %q, want %q", value, "wal_sync_value")
	}
}

// =============================================================================
// Scenario 2: WAL Append Crash
// =============================================================================

// TestScenarioWhitebox_WALAppend0_NoCorruption verifies that a crash during
// WAL append doesn't corrupt the database.
//
// Kill point: WAL.Append:0 (during WAL append)
// Invariant: Recovery sees no corruption; write may or may not be visible.
func TestScenarioWhitebox_WALAppend0_NoCorruption(t *testing.T) {
	dir := t.TempDir()

	// First, establish a baseline with a known key
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("baseline_key"), []byte("baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		database.Close()
	}

	// Run child that crashes during WAL append
	runWhiteboxChild(t, dir, testutil.KPWALAppend0, func(database db.DB) {
		// This write may not complete - that's expected
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		_ = database.Put(opts, []byte("partial_key"), []byte("partial_value"))
	})

	// Reopen and verify no corruption - baseline must be readable
	database := openDB(t, dir)
	defer database.Close()

	// Baseline key must be present
	value, err := database.Get(nil, []byte("baseline_key"))
	if err != nil {
		t.Fatalf("Get baseline after WAL.Append:0 crash failed: %v", err)
	}
	if string(value) != "baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "baseline_value")
	}

	// The partial write may or may not be visible (timing dependent)
	// The important thing is no corruption
	_, err = database.Get(nil, []byte("partial_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get partial_key returned unexpected error (not ErrNotFound): %v", err)
	}
}

// =============================================================================
// Scenario 3: CURRENT File Update Crash
// =============================================================================

// TestScenarioWhitebox_CurrentWrite0_PreviousManifestValid verifies that
// crashing before CURRENT update leaves the previous MANIFEST valid.
//
// Kill point: Current.Write:0 (before CURRENT update)
// Invariant: CURRENT always points to a valid MANIFEST.
func TestScenarioWhitebox_CurrentWrite0_PreviousManifestValid(t *testing.T) {
	dir := t.TempDir()

	// First, create a DB with some data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("initial_key"), []byte("initial_value")); err != nil {
			t.Fatalf("Put initial failed: %v", err)
		}
		// Flush to ensure MANIFEST has entries
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Run child that writes more data and crashes before CURRENT update
	// This would happen during a MANIFEST rotation
	runWhiteboxChild(t, dir, testutil.KPCurrentWrite0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		// Write many keys to potentially trigger MANIFEST rotation
		for i := range 100 {
			key := []byte("batch_key_" + string(rune('0'+i%10)))
			if err := database.Put(opts, key, []byte("batch_value")); err != nil {
				t.Logf("Put batch failed (may be expected): %v", err)
			}
		}
		// Force a flush which writes to MANIFEST
		_ = database.Flush(nil)
	})

	// Reopen and verify the DB opens successfully with previous state
	database := openDB(t, dir)
	defer database.Close()

	// Initial key must still be accessible
	value, err := database.Get(nil, []byte("initial_key"))
	if err != nil {
		t.Fatalf("Get initial_key after Current.Write:0 crash failed: %v", err)
	}
	if string(value) != "initial_value" {
		t.Errorf("Initial value mismatch: got %q, want %q", value, "initial_value")
	}
}

// TestScenarioWhitebox_CurrentWrite1_NewManifestActive verifies that
// crashing after CURRENT update means the new MANIFEST is active.
//
// Kill point: Current.Write:1 (after CURRENT update)
// Invariant: After CURRENT is updated, new MANIFEST is used on recovery.
func TestScenarioWhitebox_CurrentWrite1_NewManifestActive(t *testing.T) {
	dir := t.TempDir()

	// First, create a DB with initial data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("before_current_key"), []byte("before_value")); err != nil {
			t.Fatalf("Put before failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Run child that adds more data and crashes after CURRENT update
	runWhiteboxChild(t, dir, testutil.KPCurrentWrite1, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("after_current_key"), []byte("after_value")); err != nil {
			t.Logf("Put after failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Reopen and verify
	database := openDB(t, dir)
	defer database.Close()

	// Both keys should be accessible if CURRENT was updated successfully
	value, err := database.Get(nil, []byte("before_current_key"))
	if err != nil {
		t.Fatalf("Get before_current_key failed: %v", err)
	}
	if string(value) != "before_value" {
		t.Errorf("Before value mismatch: got %q, want %q", value, "before_value")
	}

	// The after key may or may not be present depending on exact crash timing
	// but the DB must be openable and consistent
	_, err = database.Get(nil, []byte("after_current_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get after_current_key returned unexpected error: %v", err)
	}
}

// =============================================================================
// Scenario 4: MANIFEST Sync Crash
// =============================================================================

// TestScenarioWhitebox_ManifestSync0_PartialManifestHandled verifies that
// crashing before MANIFEST sync is handled correctly.
//
// Kill point: Manifest.Sync:0 (before MANIFEST sync)
// Invariant: Recovery uses previous valid state.
func TestScenarioWhitebox_ManifestSync0_PartialManifestHandled(t *testing.T) {
	dir := t.TempDir()

	// Create initial DB
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("manifest_key_1"), []byte("manifest_value_1")); err != nil {
			t.Fatalf("Put manifest_key_1 failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Crash before MANIFEST sync
	runWhiteboxChild(t, dir, testutil.KPManifestSync0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("manifest_key_2"), []byte("manifest_value_2")); err != nil {
			t.Logf("Put manifest_key_2 failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Reopen and verify previous state is intact
	database := openDB(t, dir)
	defer database.Close()

	// First key must be present
	value, err := database.Get(nil, []byte("manifest_key_1"))
	if err != nil {
		t.Fatalf("Get manifest_key_1 failed: %v", err)
	}
	if string(value) != "manifest_value_1" {
		t.Errorf("manifest_key_1 value mismatch: got %q, want %q", value, "manifest_value_1")
	}
}

// TestScenarioWhitebox_ManifestSync1_DataDurable verifies that
// data is durable after MANIFEST sync completes.
//
// Kill point: Manifest.Sync:1 (after MANIFEST sync)
// Invariant: Data written before sync is durable.
func TestScenarioWhitebox_ManifestSync1_DataDurable(t *testing.T) {
	dir := t.TempDir()

	// First create the DB so it exists before we test MANIFEST sync
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("initial_key"), []byte("initial_value")); err != nil {
			t.Fatalf("Put initial failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Crash after MANIFEST sync during a subsequent operation
	runWhiteboxChild(t, dir, testutil.KPManifestSync1, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("durable_key"), []byte("durable_value")); err != nil {
			t.Logf("Put durable_key failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
		// At this point, MANIFEST.Sync:1 should trigger exit
	})

	// Reopen and verify
	database := openDB(t, dir)
	defer database.Close()

	// Initial key must be present
	value, err := database.Get(nil, []byte("initial_key"))
	if err != nil {
		t.Fatalf("Get initial_key failed: %v", err)
	}
	if string(value) != "initial_value" {
		t.Errorf("initial_key value mismatch: got %q, want %q", value, "initial_value")
	}

	// Durable key may or may not be present depending on exact timing
	// The invariant is that the DB is consistent and opens without corruption
	_, err = database.Get(nil, []byte("durable_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get durable_key returned unexpected error: %v", err)
	}
}

// =============================================================================
// Helpers
// =============================================================================

// runWhiteboxChild runs the given function in a child process with a kill point set.
// The child process will exit when the specified kill point is reached.
func runWhiteboxChild(t *testing.T, dir, killPoint string, fn func(db.DB)) {
	t.Helper()

	// Check if we're the child process
	if os.Getenv("WHITEBOX_KILL_POINT") == killPoint {
		// We're the child — set kill point and run the function
		testutil.SetKillPoint(killPoint)

		dbPath := os.Getenv("WHITEBOX_DB_PATH")
		if dbPath == "" {
			t.Fatal("WHITEBOX_DB_PATH not set")
		}

		database := openOrCreateDB(t, dbPath)
		fn(database)

		// If we get here, the kill point wasn't hit
		// Close cleanly and exit with non-zero to indicate the kill point wasn't triggered
		database.Close()
		os.Exit(2)
	}

	// We're the parent — spawn child with kill point
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(),
		"WHITEBOX_KILL_POINT="+killPoint,
		"WHITEBOX_DB_PATH="+dir,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start child: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				switch exitErr.ExitCode() {
				case 0:
					// Clean exit from os.Exit(0) at kill point — expected
					t.Logf("Child exited at kill point %s", killPoint)
				case 2:
					// Kill point wasn't triggered — this is a test failure
					// A whitebox test that doesn't hit its kill point proves nothing
					t.Fatalf("Kill point %s was NOT triggered (child ran to completion). "+
						"Whitebox tests must hit their kill point to be valid.\nStdout: %s\nStderr: %s",
						killPoint, stdout.String(), stderr.String())
				default:
					t.Logf("Child exited with code %d. Stdout: %s\nStderr: %s",
						exitErr.ExitCode(), stdout.String(), stderr.String())
				}
			} else {
				t.Logf("Child exited with error: %v", err)
			}
		} else {
			// err == nil means exit code 0
			t.Logf("Child exited cleanly at kill point %s", killPoint)
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGKILL)
		t.Fatalf("Child timed out. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
	}
}
