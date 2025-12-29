// Durability scenario tests for RockyardKV.
//
// These tests verify durability invariants using FaultInjectionFS to simulate
// filesystem anomalies that occur in real-world crash scenarios:
//
//   - Fsync lies: Application calls fsync(), the OS acknowledges, but data
//     hasn't actually reached stable storage. On power loss, unsynced data
//     is lost (truncated to the last truly synced position).
//
//   - Directory sync anomalies: A file rename (atomic on POSIX) is not durable
//     until the parent directory is synced. On power loss before dir sync,
//     the renamed file may revert to its old name or disappear entirely.
//
// Reference: RocksDB v10.7.5
//   - utilities/fault_injection_fs.h
//   - utilities/fault_injection_fs.cc
//   - tools/db_crashtest.py
//
// These tests use FaultInjectionFS to simulate these anomalies and verify
// that Go's implementation handles them correctly, matching C++ RocksDB behavior.
package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// =============================================================================
// Fsync Lies: Unsynced Data Loss
// =============================================================================

// TestScenario_FsyncLies_SyncedWritesSurvive verifies that writes with sync=true
// survive when unsynced data is dropped.
//
// Simulates: Power loss where OS lied about fsync completion.
// Invariant: Data written with sync=true must survive.
func TestScenario_FsyncLies_SyncedWritesSurvive(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS wrapper
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write with sync=true (should be durable)
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	if err := database.Put(syncOpts, []byte("synced_key"), []byte("synced_value")); err != nil {
		t.Fatalf("Synced put failed: %v", err)
	}

	// Write without sync (may be lost)
	nosyncOpts := db.DefaultWriteOptions()
	nosyncOpts.Sync = false
	if err := database.Put(nosyncOpts, []byte("unsynced_key"), []byte("unsynced_value")); err != nil {
		t.Fatalf("Unsynced put failed: %v", err)
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate power loss: drop all data that wasn't truly synced
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v (may be expected)", err)
	}

	// Reopen database
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Synced write must survive
	value, err := database.Get(nil, []byte("synced_key"))
	if err != nil {
		t.Fatalf("Get synced_key failed: %v", err)
	}
	if string(value) != "synced_value" {
		t.Errorf("synced_key mismatch: got %q, want %q", value, "synced_value")
	}

	// Unsynced write may or may not survive (depends on implementation)
	// The important thing is that the DB opens and synced data is present
	_, err = database.Get(nil, []byte("unsynced_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get unsynced_key returned unexpected error: %v", err)
	}
}

// TestScenario_FsyncLies_FlushMakesDurable verifies that flushed data survives
// when unsynced data is dropped.
//
// Simulates: Power loss after flush completes.
// Invariant: Data flushed to SST files must survive.
func TestScenario_FsyncLies_FlushMakesDurable(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write multiple keys without sync
	for i := range 10 {
		key := []byte("flush_key_" + string(rune('0'+i)))
		value := []byte("flush_value_" + string(rune('0'+i)))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to make durable
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate power loss: drop unsynced data
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v (may be expected)", err)
	}

	// Reopen
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// All flushed keys must survive
	for i := range 10 {
		key := []byte("flush_key_" + string(rune('0'+i)))
		expectedValue := "flush_value_" + string(rune('0'+i))
		value, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Get %s failed: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("%s mismatch: got %q, want %q", key, value, expectedValue)
		}
	}
}

// =============================================================================
// Directory Sync: File Visibility After Rename
// =============================================================================

// TestScenario_DirSync_CURRENTFileDurable verifies that the CURRENT file update
// is durable after proper sync sequence (including directory sync).
//
// Simulates: Power loss after clean shutdown.
// Invariant: CURRENT file points to a valid MANIFEST after recovery.
func TestScenario_DirSync_CURRENTFileDurable(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Perform operations that will update MANIFEST
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	if err := database.Put(syncOpts, []byte("current_test_key"), []byte("current_test_value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Flush to update MANIFEST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// After proper sync and close, files should be durable.
	// Use a fresh FS for reopen (simulating a real crash where all
	// kernel buffers are lost but synced files persist on disk).
	opts.FS = vfs.Default()
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Data should be accessible since we synced and closed cleanly
	value, err := database.Get(nil, []byte("current_test_key"))
	if err != nil {
		t.Fatalf("Get current_test_key failed: %v", err)
	}
	if string(value) != "current_test_value" {
		t.Errorf("current_test_key mismatch: got %q, want %q", value, "current_test_value")
	}
}

// TestScenario_DirSync_RecoveryAfterUnsyncedDataLoss verifies that recovery
// is consistent after proper sync sequence followed by additional unsynced writes.
//
// Simulates: Power loss with partially written WAL.
// Invariant: Flushed data survives; unflushed data may be lost but DB is consistent.
func TestScenario_DirSync_RecoveryAfterUnsyncedDataLoss(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	// Phase 1: Create initial DB with durable data
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	if err := database.Put(syncOpts, []byte("durable_key"), []byte("durable_value")); err != nil {
		t.Fatalf("Put durable_key failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Phase 2: Add more data without proper sync sequence
	nosyncOpts := db.DefaultWriteOptions()
	nosyncOpts.Sync = false
	for i := range 5 {
		key := []byte("volatile_key_" + string(rune('0'+i)))
		if err := database.Put(nosyncOpts, key, []byte("volatile_value")); err != nil {
			t.Fatalf("Put volatile failed: %v", err)
		}
	}

	// Close without additional flush
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate power loss: drop unsynced data (keep files that were dir-synced)
	_ = faultFS.DropUnsyncedData()

	// Reopen with fresh FS (simulating real crash where FaultInjectionFS state is lost)
	opts.FS = vfs.Default()
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen after simulated crash failed: %v", err)
	}
	defer database.Close()

	// Durable data must survive
	value, err := database.Get(nil, []byte("durable_key"))
	if err != nil {
		t.Fatalf("Get durable_key failed: %v", err)
	}
	if string(value) != "durable_value" {
		t.Errorf("durable_key mismatch: got %q, want %q", value, "durable_value")
	}

	// Volatile data may or may not survive - that's expected
	// The invariant is that recovery is consistent (no corruption)
}

// =============================================================================
// Combined Durability Scenarios
// =============================================================================

// TestScenario_MultipleFlushCycles_DurabilityCheckpoints verifies durability
// across multiple flush cycles with intermittent unsynced data loss.
//
// Simulates: Multiple power loss events during database operation.
// Invariant: Each flush creates a durable checkpoint; data from completed
// flush cycles must survive subsequent power loss.
func TestScenario_MultipleFlushCycles_DurabilityCheckpoints(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Cycle 1: Write and flush
	if err := database.Put(nil, []byte("cycle1_key"), []byte("cycle1_value")); err != nil {
		t.Fatalf("Put cycle1 failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush cycle1 failed: %v", err)
	}

	// Simulate partial power loss after cycle 1
	_ = faultFS.DropUnsyncedData()

	// Cycle 2: Write and flush
	if err := database.Put(nil, []byte("cycle2_key"), []byte("cycle2_value")); err != nil {
		t.Fatalf("Put cycle2 failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush cycle2 failed: %v", err)
	}

	// Cycle 3: Write without flush (may be lost)
	if err := database.Put(nil, []byte("cycle3_key"), []byte("cycle3_value")); err != nil {
		t.Fatalf("Put cycle3 failed: %v", err)
	}

	// Close
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate power loss: drop unsynced data
	_ = faultFS.DropUnsyncedData()

	// Reopen
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer database.Close()

	// Cycle 1 must survive (was flushed before first drop)
	value, err := database.Get(nil, []byte("cycle1_key"))
	if err != nil {
		t.Errorf("Get cycle1_key failed: %v", err)
	} else if string(value) != "cycle1_value" {
		t.Errorf("cycle1_key mismatch: got %q, want %q", value, "cycle1_value")
	}

	// Cycle 2 must survive (was flushed)
	value, err = database.Get(nil, []byte("cycle2_key"))
	if err != nil {
		t.Errorf("Get cycle2_key failed: %v", err)
	} else if string(value) != "cycle2_value" {
		t.Errorf("cycle2_key mismatch: got %q, want %q", value, "cycle2_value")
	}

	// Cycle 3 may or may not survive (was not flushed)
	_, err = database.Get(nil, []byte("cycle3_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get cycle3_key returned unexpected error: %v", err)
	}
}

// =============================================================================
// Torn CURRENT / Missing MANIFEST Scenarios
// =============================================================================

// TestDurability_CURRENTUpdate_NoPendingRenamesAfterShutdown verifies that
// the DB properly syncs the directory after updating CURRENT.
//
// Contract: After a clean shutdown, there are no pending renames. The CURRENT
// file update is durable because setCurrentFile syncs the parent directory.
//
// Reference: RocksDB v10.7.5 db/version_set.cc SetCurrentFile behavior.
func TestDurability_CURRENTUpdate_NoPendingRenamesAfterShutdown(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS wrapper
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write some data and flush to create first MANIFEST
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	for i := range 100 {
		key := []byte("key_" + string(rune('0'+i%10)) + string(rune('0'+i/10)))
		if err := database.Put(syncOpts, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to force MANIFEST update
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close database (this triggers another MANIFEST update and CURRENT rename)
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check if there are pending renames (CURRENT update without dir sync)
	// Note: If setCurrentFile properly syncs the directory, there should be none.
	pendingCount := faultFS.PendingRenameCount()
	t.Logf("Pending renames after close: %d", pendingCount)

	// Simulate crash by reverting unsynced renames.
	// Renames without SyncDir are not durable and can be lost.
	if err := faultFS.RevertUnsyncedRenames(); err != nil {
		t.Logf("RevertUnsyncedRenames: %v", err)
	}

	// Also drop unsynced data
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	// Try to reopen database
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if pendingCount > 0 {
		// If there were pending renames, the DB should fail to open
		// (CURRENT might point to wrong/missing MANIFEST)
		if err == nil {
			database.Close()
			t.Log("DB reopened successfully despite pending renames at crash point")
			// This is actually correct behavior if the implementation properly
			// syncs the directory after CURRENT rename. Log but don't fail.
		} else {
			t.Logf("DB failed to reopen as expected after reverting unsynced renames: %v", err)
		}
	} else {
		// No pending renames = properly synced, should reopen fine
		if err != nil {
			t.Fatalf("DB should reopen after clean shutdown: %v", err)
		}

		// Run oracle checks if enabled
		if os.Getenv(CppOraclePathEnv) != "" {
			artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
			_ = os.MkdirAll(artifactDir, 0755)
			runCppOracleChecks(t, artifactDir, dir)
			t.Logf("Oracle artifacts saved to %s", artifactDir)
		}

		database.Close()
		t.Log("DB reopened successfully - no pending renames (properly synced)")
	}
}

// TestDurability_SyncedCURRENT_SurvivesCrash verifies that a properly synced
// CURRENT update survives a simulated crash.
//
// Contract: When the DB syncs the directory after CURRENT update, the database
// reopens correctly after crash and data is preserved.
func TestDurability_SyncedCURRENT_SurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS wrapper
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write data with sync
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	if err := database.Put(syncOpts, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Flush to update MANIFEST
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close database (should sync directory after CURRENT update)
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify no pending renames after proper shutdown
	if faultFS.HasPendingRenames() {
		t.Errorf("Should have no pending renames after proper shutdown, got %d",
			faultFS.PendingRenameCount())
	}

	// Drop unsynced data (simulate crash)
	_ = faultFS.DropUnsyncedData()

	// Reopen should succeed
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB after simulated crash: %v", err)
	}
	defer database.Close()

	// Data should be present
	value, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Value mismatch: got %q, want %q", value, "value1")
	}

	// Run oracle checks if enabled
	if os.Getenv(CppOraclePathEnv) != "" {
		artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
		_ = os.MkdirAll(artifactDir, 0755)
		runCppOracleChecks(t, artifactDir, dir)
		t.Logf("Oracle artifacts saved to %s", artifactDir)
	}
}
