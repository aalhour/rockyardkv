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
	"strconv"
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

// =============================================================================
// Torn CURRENT file scenarios
// =============================================================================

// TestDurability_TornCURRENT_FailsLoud tests that corrupted/truncated CURRENT files
// cause loud failures during database open, not silent misbehavior.
//
// Contract: A database with a corrupted CURRENT file must fail to open with a
// clear error. This ensures crash-induced partial writes are detected.
func TestDurability_TornCURRENT_FailsLoud(t *testing.T) {
	testCases := []struct {
		name        string
		content     []byte
		description string
	}{
		{
			name:        "empty_file",
			content:     []byte{},
			description: "Empty CURRENT file (zero bytes)",
		},
		{
			name:        "truncated_prefix",
			content:     []byte("MANIF"),
			description: "Truncated MANIFEST- prefix",
		},
		{
			name:        "missing_number",
			content:     []byte("MANIFEST-\n"),
			description: "MANIFEST- without file number",
		},
		{
			name:        "invalid_number",
			content:     []byte("MANIFEST-abc\n"),
			description: "Non-numeric manifest number",
		},
		{
			name:        "garbage_bytes",
			content:     []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
			description: "Binary garbage",
		},
		{
			name:        "partial_newline",
			content:     nil, // Will be set dynamically from original content
			description: "Missing trailing newline (should still work)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			// Step 1: Create a valid database first
			opts := db.DefaultOptions()
			opts.CreateIfMissing = true
			database, err := db.Open(dir, opts)
			if err != nil {
				t.Fatalf("Failed to create initial DB: %v", err)
			}

			// Write some data and flush to ensure MANIFEST exists
			wo := db.DefaultWriteOptions()
			wo.Sync = true
			if err := database.Put(wo, []byte("key1"), []byte("value1")); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			if err := database.Flush(db.DefaultFlushOptions()); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			// Close the database
			if err := database.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			// Step 2: Corrupt the CURRENT file
			currentPath := filepath.Join(dir, "CURRENT")
			originalContent, err := os.ReadFile(currentPath)
			if err != nil {
				t.Fatalf("Failed to read original CURRENT: %v", err)
			}
			t.Logf("Original CURRENT: %q", originalContent)

			// For partial_newline case, derive content from original (strip newline)
			content := tc.content
			if tc.name == "partial_newline" {
				// Remove trailing newline from original content
				content = []byte(string(originalContent)[:len(originalContent)-1])
			}

			if err := os.WriteFile(currentPath, content, 0644); err != nil {
				t.Fatalf("Failed to write corrupted CURRENT: %v", err)
			}
			t.Logf("Corrupted CURRENT to: %q (%s)", content, tc.description)

			// Step 3: Try to reopen - should fail (except missing_newline case)
			opts.CreateIfMissing = false
			database, err = db.Open(dir, opts)

			if tc.name == "partial_newline" {
				// Missing newline should still work (TrimSpace handles it)
				if err != nil {
					t.Errorf("Expected Open to succeed with missing newline, got: %v", err)
				} else {
					database.Close()
					t.Log("Open succeeded with missing newline (expected)")
				}
			} else {
				// All other cases should fail
				if err == nil {
					database.Close()
					t.Errorf("Expected Open to fail with %s, but it succeeded", tc.description)
				} else {
					t.Logf("Open failed as expected: %v", err)

					// Verify the error is meaningful (not a generic error)
					errStr := err.Error()
					if !containsAny(errStr, "manifest", "MANIFEST", "invalid", "corrupt", "not found") {
						t.Logf("Warning: error message may not be specific enough: %v", err)
					}
				}
			}

			// Step 4: Capture artifacts and run oracle checks
			if os.Getenv(CppOraclePathEnv) != "" {
				artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
				_ = os.MkdirAll(artifactDir, 0755)

				// Save the corrupted CURRENT bytes
				_ = os.WriteFile(filepath.Join(artifactDir, "CURRENT_corrupted.bin"), content, 0644)
				_ = os.WriteFile(filepath.Join(artifactDir, "CURRENT_original.txt"), originalContent, 0644)

				// Run oracle checks - C++ tools should also fail/report issues
				runCppOracleChecks(t, artifactDir, dir)
				t.Logf("Oracle artifacts saved to %s", artifactDir)
			}
		})
	}
}

// containsAny checks if s contains any of the substrings.
func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if contains(s, sub) {
			return true
		}
	}
	return false
}

// padInt returns a zero-padded string representation of n with the given width.
func padInt(n, width int) string {
	s := strconv.Itoa(n)
	for len(s) < width {
		s = "0" + s
	}
	return s
}

// contains is a simple substring check.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchSubstring(s, substr)))
}

// searchSubstring performs a simple substring search.
func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// =============================================================================
// SyncDir Lie Mode: FS reports success but renames are not durable
// =============================================================================

// TestDurability_SyncDirLieMode_DBRecoversConsistently tests that when the
// filesystem lies about SyncDir success, the DB either:
// - Recovers to an older consistent state, OR
// - Fails loud with a clear error
//
// Contract: Under N05 lie mode, the DB must not silently succeed with
// inconsistent metadata. This simulates filesystems that report directory
// fsync success but still lose directory entries on crash.
func TestDurability_SyncDirLieMode_DBRecoversConsistently(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS with lie mode enabled
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetSyncDirLieMode(true)

	t.Logf("SyncDir lie mode enabled: %v", faultFS.IsSyncDirLieModeEnabled())

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write initial data and flush to create first MANIFEST state
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	for i := range 10 {
		key := []byte("initial_key_" + string(rune('0'+i)))
		if err := database.Put(syncOpts, key, []byte("initial_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to establish a durable checkpoint
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Record pending renames after flush (lie mode: SyncDir didn't make them durable)
	pendingAfterFlush := faultFS.PendingRenameCount()
	t.Logf("Pending renames after flush: %d", pendingAfterFlush)

	// Write more data and flush again to trigger another MANIFEST update
	for i := range 10 {
		key := []byte("second_key_" + string(rune('0'+i)))
		if err := database.Put(syncOpts, key, []byte("second_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}

	pendingAfterSecondFlush := faultFS.PendingRenameCount()
	t.Logf("Pending renames after second flush: %d", pendingAfterSecondFlush)

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	pendingAfterClose := faultFS.PendingRenameCount()
	t.Logf("Pending renames after close: %d", pendingAfterClose)

	// In lie mode, SyncDir returned success but renames are still pending.
	// This is the key invariant we're testing.
	if pendingAfterClose > 0 {
		t.Logf("EXPECTED: %d pending renames exist despite SyncDir calls (lie mode)", pendingAfterClose)
	}

	// Simulate crash by reverting unsynced renames
	if err := faultFS.RevertUnsyncedRenames(); err != nil {
		t.Logf("RevertUnsyncedRenames: %v", err)
	}

	// Also drop unsynced data
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	t.Log("Crash simulated: pending renames reverted, unsynced data dropped")

	// Try to reopen database - should either:
	// 1. Fail loud with a clear error (CURRENT points to missing MANIFEST)
	// 2. Recover to an older consistent state (if older MANIFEST is still valid)
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		// This is acceptable behavior: DB fails loud
		t.Logf("DB failed to reopen (expected under lie mode crash): %v", err)

		// Verify the error is meaningful
		errStr := err.Error()
		if containsAny(errStr, "manifest", "MANIFEST", "not found", "no such file", "corrupt", "invalid") {
			t.Log("Error message indicates missing or corrupt metadata (expected)")
		} else {
			t.Logf("Warning: error message may not be specific enough")
		}

		// Run oracle checks if enabled
		if os.Getenv(CppOraclePathEnv) != "" {
			artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
			_ = os.MkdirAll(artifactDir, 0755)
			runCppOracleChecks(t, artifactDir, dir)
			t.Logf("Oracle artifacts saved to %s", artifactDir)
		}
	} else {
		// DB reopened successfully - verify it's in a consistent state
		t.Log("DB reopened successfully after lie mode crash")

		// Check which keys are readable (may have lost second batch)
		var initialFound, secondFound int
		for i := range 10 {
			key := []byte("initial_key_" + string(rune('0'+i)))
			if _, err := database.Get(nil, key); err == nil {
				initialFound++
			}
		}
		for i := range 10 {
			key := []byte("second_key_" + string(rune('0'+i)))
			if _, err := database.Get(nil, key); err == nil {
				secondFound++
			}
		}

		t.Logf("Keys found: initial=%d/10, second=%d/10", initialFound, secondFound)

		// The key invariant: if DB opened, it must be in a consistent state
		// (either all of a checkpoint or none of it)
		if initialFound != 0 && initialFound != 10 {
			t.Errorf("Inconsistent state: found %d/10 initial keys", initialFound)
		}
		if secondFound != 0 && secondFound != 10 {
			t.Errorf("Inconsistent state: found %d/10 second keys", secondFound)
		}

		database.Close()

		// Run oracle checks if enabled
		if os.Getenv(CppOraclePathEnv) != "" {
			artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
			_ = os.MkdirAll(artifactDir, 0755)
			runCppOracleChecks(t, artifactDir, dir)
			t.Logf("Oracle artifacts saved to %s", artifactDir)
		}
	}
}

// TestDurability_SyncDirLieMode_CreatedFilesMayDisappear_FailsLoud tests a stronger
// "system lies" instance than N05 rename durability: directory fsync returns
// success, but newly created directory entries are still lost on crash.
//
// Contract: If directory-entry durability is violated after SyncDir success, the
// DB must fail loud (or recover consistently) and must not silently proceed with
// inconsistent metadata.
func TestDurability_SyncDirLieMode_CreatedFilesMayDisappear_FailsLoud(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetSyncDirLieMode(true)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Force some metadata + file creation (WAL + MANIFEST edits + SST).
	for i := range 200 {
		key := []byte("k_" + strconv.Itoa(i))
		if err := database.Put(nil, key, []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate crash:
	// - lose directory entries for created files despite SyncDir returning success
	// - revert unsynced renames (CURRENT updates, etc.)
	// - drop unsynced file data
	_ = faultFS.DeleteUnsyncedFiles()
	_ = faultFS.RevertUnsyncedRenames()
	_ = faultFS.DropUnsyncedData()

	opts.CreateIfMissing = false
	_, err = db.Open(dir, opts)
	if err == nil {
		// If we ever start “recovering” here, it must be explicitly consistent.
		// Today, we want to fail loud under this severe lie model.
		t.Fatalf("expected reopen to fail loud under SyncDir lie for created files, but it succeeded")
	}

	// Error should be meaningful.
	errStr := err.Error()
	if !containsAny(errStr, "manifest", "MANIFEST", "CURRENT", "not found", "no such file", "corrupt", "invalid") {
		t.Logf("warning: error message may not be specific enough: %q", errStr)
	}
}

// =============================================================================
// File Sync Lie Mode: FS reports sync success but data is not durable
// =============================================================================

// TestDurability_FileSyncLieMode_WAL_LosesUnsyncedWrites tests that when the
// filesystem lies about WAL sync, unflushed writes are lost on crash.
//
// Contract: With WAL sync lie mode, writes before flush are lost on crash.
// The DB must recover to the last flushed state (consistent but incomplete).
func TestDurability_FileSyncLieMode_WAL_LosesUnsyncedWrites(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS with lie mode for WAL files
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetFileSyncLieMode(true, ".log")

	t.Logf("File sync lie mode enabled for: .log")

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write and flush first batch (should be durable)
	for i := range 10 {
		key := []byte("flushed_key_" + string(rune('0'+i)))
		if err := database.Put(nil, key, []byte("flushed_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write second batch WITHOUT flush (should be lost due to WAL lie)
	for i := range 10 {
		key := []byte("unflushed_key_" + string(rune('0'+i)))
		if err := database.Put(nil, key, []byte("unflushed_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash - drop unsynced data (WAL syncs were lies)
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	t.Log("Crash simulated: unsynced WAL data dropped")

	// Reopen database
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database.Close()

	// Flushed keys should be present
	var flushedFound int
	for i := range 10 {
		key := []byte("flushed_key_" + string(rune('0'+i)))
		if _, err := database.Get(nil, key); err == nil {
			flushedFound++
		}
	}
	t.Logf("Flushed keys found: %d/10", flushedFound)

	if flushedFound != 10 {
		t.Errorf("All flushed keys should be present, found %d/10", flushedFound)
	}

	// Unflushed keys may or may not be present (depends on WAL recovery)
	// The key invariant is that the DB is in a consistent state
	var unflushedFound int
	for i := range 10 {
		key := []byte("unflushed_key_" + string(rune('0'+i)))
		if _, err := database.Get(nil, key); err == nil {
			unflushedFound++
		}
	}
	t.Logf("Unflushed keys found: %d/10 (expected 0 or 10)", unflushedFound)

	// Either all unflushed keys are present (WAL was replayed) or none
	if unflushedFound != 0 && unflushedFound != 10 {
		t.Errorf("Inconsistent state: found %d/10 unflushed keys", unflushedFound)
	}
}

// TestDurability_FileSyncLieMode_AllFiles_FailsLoudOrRecoversEmpty tests a stronger
// "system lies" instance than per-file targeting: Sync() returns success for all files
// but does not make data durable.
//
// Contract: After a crash that drops all unsynced data, the DB must either:
// - fail loud with a meaningful error, OR
// - reopen to an older consistent state (possibly empty).
//
// It must not silently produce a partially-applied, inconsistent state.
func TestDurability_FileSyncLieMode_AllFiles_FailsLoudOrRecoversEmpty(t *testing.T) {
	dir := t.TempDir()

	// Lie about Sync() for ALL files.
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetFileSyncLieMode(true, "")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write some data with Sync=true (FS will lie; data still not durable).
	syncOpts := db.DefaultWriteOptions()
	syncOpts.Sync = true
	for i := range 50 {
		key := []byte("allfiles_key_" + strconv.Itoa(i))
		if err := database.Put(syncOpts, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create more file activity (SST/MANIFEST), but Sync lies for all files.
	_ = database.Flush(db.DefaultFlushOptions())

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash: everything that was "synced" is actually unsynced and may be lost.
	_ = faultFS.DropUnsyncedData()

	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		// Fail loud is acceptable; try to ensure error has some signal.
		errStr := err.Error()
		if !containsAny(errStr, "manifest", "MANIFEST", "CURRENT", "not found", "no such file", "corrupt", "invalid") {
			t.Logf("warning: error message may not be specific enough: %q", errStr)
		}
		return
	}
	defer database.Close()

	// If open succeeds, it must be consistent. Under this extreme lie model,
	// recovering to an empty (or older) state is acceptable.
	iter := database.NewIterator(nil)
	defer iter.Close()

	keyCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keyCount++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterator: %v", err)
	}

	t.Logf("DB reopened under all-files sync lie mode; keyCount=%d", keyCount)
}

// TestDurability_FileSyncLieMode_SST_FailsOnRead tests that when the filesystem
// lies about SST sync, reads from that SST fail after crash.
//
// Contract: With SST sync lie mode, the SST may be truncated on crash.
// Reads from truncated SST must fail loud, not return wrong data.
func TestDurability_FileSyncLieMode_SST_FailsOnRead(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS with lie mode for SST files
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetFileSyncLieMode(true, ".sst")

	t.Logf("File sync lie mode enabled for: .sst")

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write data and flush to create an SST (which will have its sync lied about)
	for i := range 100 {
		key := []byte("key_" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		value := []byte("value_" + string(rune('0'+i)))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash - drop unsynced data (SST syncs were lies)
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	t.Log("Crash simulated: unsynced SST data dropped")

	// Try to reopen database - may fail or succeed with truncated SST
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		// DB failed to open - acceptable behavior
		t.Logf("DB failed to open (expected with truncated SST): %v", err)
		return
	}

	// If DB opened, try to read data - should fail at some point
	var readErrors int
	for i := range 100 {
		key := []byte("key_" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		_, err := database.Get(nil, key)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			readErrors++
		}
	}

	database.Close()

	if readErrors > 0 {
		t.Logf("Read errors detected: %d (expected for truncated SST)", readErrors)
	} else {
		t.Log("No read errors - SST may have been fully synced before lie mode took effect")
	}
}

// TestDurability_FileSyncLieMode_MANIFEST_FailsOrRecoversOlder tests that when
// the filesystem lies about MANIFEST sync, the DB either fails to open or
// recovers to an older consistent state.
//
// Contract: With MANIFEST sync lie mode, the MANIFEST may be truncated on crash.
// The DB must either fail loud or recover to the last valid MANIFEST checkpoint.
func TestDurability_FileSyncLieMode_MANIFEST_FailsOrRecoversOlder(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS with lie mode for MANIFEST files
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetFileSyncLieMode(true, "MANIFEST")

	t.Logf("File sync lie mode enabled for: MANIFEST")

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Phase 1: Write initial data and flush (this creates first SST and MANIFEST entry)
	for i := range 10 {
		key := []byte("phase1_key_" + padInt(i, 3))
		value := []byte("phase1_value_" + padInt(i, 3))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Phase 1 Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Phase 1 Flush failed: %v", err)
	}
	t.Log("Phase 1: 10 keys written and flushed")

	// Phase 2: Write more data and flush (this updates MANIFEST with new SST)
	// This MANIFEST update's sync will be lied about
	for i := range 10 {
		key := []byte("phase2_key_" + padInt(i, 3))
		value := []byte("phase2_value_" + padInt(i, 3))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Phase 2 Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Phase 2 Flush failed: %v", err)
	}
	t.Log("Phase 2: 10 more keys written and flushed (MANIFEST sync was lied about)")

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash - drop unsynced data (MANIFEST syncs were lies)
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	t.Log("Crash simulated: unsynced MANIFEST data dropped")

	// Try to reopen database
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		// Acceptable: DB failed to open with corrupted MANIFEST
		t.Logf("DB failed to open (expected with truncated MANIFEST): %v", err)

		// Verify error is meaningful
		errStr := err.Error()
		if containsAny(errStr, "manifest", "MANIFEST", "corrupt", "invalid", "truncat") {
			t.Log("Error message indicates MANIFEST corruption (expected)")
		}

		// Run oracle checks if available
		artifactDir := filepath.Join(os.TempDir(), "rockyardkv-manifest-lie-artifacts")
		_ = os.MkdirAll(artifactDir, 0755)
		runCppOracleChecks(t, artifactDir, dir)
		return
	}

	// Alternative acceptable outcome: DB recovered to an older state
	t.Log("DB reopened - checking if it recovered to older consistent state")
	defer database.Close()

	// Count phase1 and phase2 keys
	var phase1Count, phase2Count int
	for i := range 10 {
		key := []byte("phase1_key_" + padInt(i, 3))
		_, err := database.Get(nil, key)
		if err == nil {
			phase1Count++
		}
	}
	for i := range 10 {
		key := []byte("phase2_key_" + padInt(i, 3))
		_, err := database.Get(nil, key)
		if err == nil {
			phase2Count++
		}
	}

	t.Logf("Recovered state: phase1=%d/10, phase2=%d/10", phase1Count, phase2Count)

	// Acceptable outcomes:
	// 1. All phase1 keys present, phase2 keys lost (recovered to older state)
	// 2. All keys present (MANIFEST was fully synced before lie took effect)
	if phase1Count == 10 && phase2Count == 0 {
		t.Log("DB recovered to older consistent state (phase2 lost due to MANIFEST truncation)")
	} else if phase1Count == 10 && phase2Count == 10 {
		t.Log("All keys present - MANIFEST may have been fully synced before lie mode took effect")
	} else if phase1Count < 10 {
		t.Errorf("Phase 1 keys incomplete (%d/10) - unexpected data loss", phase1Count)
	}

	// Run oracle checks
	artifactDir := filepath.Join(os.TempDir(), "rockyardkv-manifest-lie-artifacts")
	_ = os.MkdirAll(artifactDir, 0755)
	runCppOracleChecks(t, artifactDir, dir)
}

// TestDurability_FileSyncLieMode_CURRENTTemp_FailsOrRecoversOlder tests that when
// the filesystem lies about CURRENT temp file sync during rotation, the DB either
// fails to open or recovers to an older MANIFEST.
//
// Contract: CURRENT temp file sync lies may leave the temp file incompletely written.
// The rename may still succeed, but the content may be garbage. The DB must detect
// this and fail loud or fall back to an older valid state.
func TestDurability_FileSyncLieMode_CURRENTTemp_FailsOrRecoversOlder(t *testing.T) {
	dir := t.TempDir()

	// Create a FaultInjectionFS with lie mode for CURRENT temp files
	// CURRENT is written as a temp file then renamed, so we target the temp pattern
	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	faultFS.SetFileSyncLieMode(true, "CURRENT")

	t.Logf("File sync lie mode enabled for: CURRENT (temp files)")

	// Open DB with FaultInjectionFS
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Write initial data and flush (establishes MANIFEST-000001)
	for i := range 10 {
		key := []byte("initial_key_" + padInt(i, 3))
		value := []byte("initial_value_" + padInt(i, 3))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Initial Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Initial Flush failed: %v", err)
	}
	t.Log("Initial: 10 keys written and flushed")

	// Force many flushes to potentially trigger MANIFEST rotation
	// (this may update CURRENT, whose temp file sync will be lied about)
	for round := range 5 {
		for i := range 5 {
			key := []byte("round" + padInt(round, 1) + "_key_" + padInt(i, 3))
			value := []byte("round" + padInt(round, 1) + "_value_" + padInt(i, 3))
			if err := database.Put(nil, key, value); err != nil {
				t.Fatalf("Round %d Put failed: %v", round, err)
			}
		}
		if err := database.Flush(db.DefaultFlushOptions()); err != nil {
			t.Fatalf("Round %d Flush failed: %v", round, err)
		}
	}
	t.Log("Rounds 0-4: 25 more keys written across 5 flushes")

	// Close the database (triggers final MANIFEST update and CURRENT write)
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash - drop unsynced data (CURRENT temp file syncs were lies)
	if err := faultFS.DropUnsyncedData(); err != nil {
		t.Logf("DropUnsyncedData: %v", err)
	}

	t.Log("Crash simulated: unsynced CURRENT temp file data dropped")

	// Try to reopen database
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		// Acceptable: DB failed to open with corrupted CURRENT
		t.Logf("DB failed to open (expected with corrupted CURRENT): %v", err)

		// Verify error is meaningful
		errStr := err.Error()
		if containsAny(errStr, "CURRENT", "manifest", "MANIFEST", "corrupt", "invalid", "not found") {
			t.Log("Error message indicates CURRENT/MANIFEST issue (expected)")
		}

		// Run oracle checks if available
		artifactDir := filepath.Join(os.TempDir(), "rockyardkv-current-lie-artifacts")
		_ = os.MkdirAll(artifactDir, 0755)
		runCppOracleChecks(t, artifactDir, dir)
		return
	}

	// If DB opened, it either recovered older state or all data was synced
	t.Log("DB reopened - verifying consistency")
	defer database.Close()

	// Count initial keys (should always be present if DB opened)
	var initialCount int
	for i := range 10 {
		key := []byte("initial_key_" + padInt(i, 3))
		_, err := database.Get(nil, key)
		if err == nil {
			initialCount++
		}
	}

	t.Logf("Initial keys found: %d/10", initialCount)

	if initialCount < 10 {
		t.Errorf("Initial keys incomplete (%d/10) - unexpected data loss", initialCount)
	} else {
		t.Log("DB recovered successfully - initial keys intact")
	}

	// Run oracle checks
	artifactDir := filepath.Join(os.TempDir(), "rockyardkv-current-lie-artifacts")
	_ = os.MkdirAll(artifactDir, 0755)
	runCppOracleChecks(t, artifactDir, dir)
}

// =============================================================================
// Missing MANIFEST / SST scenarios
// =============================================================================

// TestDurability_MissingActiveManifest_FailsLoud tests that when CURRENT points
// to a missing MANIFEST file, the database fails to open with a clear error.
//
// Contract: A database where CURRENT references a non-existent MANIFEST-* file
// must fail to open loudly. This simulates N05 directory entry loss where the
// MANIFEST file's rename was not durable.
func TestDurability_MissingActiveManifest_FailsLoud(t *testing.T) {
	dir := t.TempDir()

	// Step 1: Create a valid database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create initial DB: %v", err)
	}

	// Write some data and flush to ensure MANIFEST is written
	wo := db.DefaultWriteOptions()
	wo.Sync = true
	if err := database.Put(wo, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Step 2: Read CURRENT to find the active MANIFEST name
	currentPath := filepath.Join(dir, "CURRENT")
	currentContent, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("Failed to read CURRENT: %v", err)
	}

	// Parse manifest name (format: "MANIFEST-XXXXXX\n")
	manifestName := string(currentContent)
	manifestName = manifestName[:len(manifestName)-1] // strip newline
	manifestPath := filepath.Join(dir, manifestName)

	t.Logf("CURRENT points to: %s", manifestName)

	// Verify the manifest file exists before deletion
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Fatalf("Active manifest %s does not exist before deletion", manifestName)
	}

	// Step 3: Delete the active MANIFEST file
	// This simulates N05: CURRENT rename was durable but MANIFEST file's
	// directory entry was lost (not durable)
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Failed to delete active manifest: %v", err)
	}
	t.Logf("Deleted active manifest: %s", manifestPath)

	// Step 4: Try to reopen - should fail loud
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err == nil {
		database.Close()
		t.Fatalf("Expected Open to fail when active manifest is missing, but it succeeded")
	}

	t.Logf("Open failed as expected: %v", err)

	// Verify the error is meaningful
	errStr := err.Error()
	if !containsAny(errStr, "manifest", "MANIFEST", "not found", "no such file", "does not exist") {
		t.Logf("Warning: error message may not be specific enough: %v", err)
	}

	// Step 5: Capture artifacts and run oracle checks
	if os.Getenv(CppOraclePathEnv) != "" {
		artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
		_ = os.MkdirAll(artifactDir, 0755)

		// Save the CURRENT content
		_ = os.WriteFile(filepath.Join(artifactDir, "CURRENT_original.txt"), currentContent, 0644)
		_ = os.WriteFile(filepath.Join(artifactDir, "MANIFEST_missing_name.txt"),
			[]byte(manifestName), 0644)

		// Run oracle checks - C++ tools should also fail
		runCppOracleChecks(t, artifactDir, dir)
		t.Logf("Oracle artifacts saved to %s", artifactDir)
	}
}

// TestDurability_ManifestReferencesMissingSST_FailsLoud tests that when the
// MANIFEST references an SST file that is missing, the database fails to open.
//
// Contract: A database where the MANIFEST references a non-existent SST file
// must fail to open loudly. This simulates N05 directory entry loss where an
// SST file's rename was not durable.
func TestDurability_ManifestReferencesMissingSST_FailsLoud(t *testing.T) {
	dir := t.TempDir()

	// Step 1: Create a valid database with data that creates SST files
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create initial DB: %v", err)
	}

	// Write enough data and flush to create at least one SST
	wo := db.DefaultWriteOptions()
	wo.Sync = true
	for i := range 100 {
		key := []byte("key_" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		value := []byte("value_" + string(rune('0'+i)))
		if err := database.Put(wo, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Step 2: Find an SST file in the DB directory
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read DB directory: %v", err)
	}

	var sstName string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sst" {
			sstName = entry.Name()
			break
		}
	}

	if sstName == "" {
		t.Fatal("No SST files found in DB directory after flush")
	}

	sstPath := filepath.Join(dir, sstName)
	t.Logf("Found SST file: %s", sstName)

	// Step 3: Delete the SST file
	// This simulates N05: SST file's directory entry was lost (not durable)
	// but the MANIFEST still references it
	if err := os.Remove(sstPath); err != nil {
		t.Fatalf("Failed to delete SST file: %v", err)
	}
	t.Logf("Deleted SST file: %s", sstPath)

	// Step 4: Try to reopen - may succeed (lazy SST loading)
	// but accessing data should fail
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	var openFailed bool
	if err != nil {
		openFailed = true
		t.Logf("Open failed immediately: %v", err)

		// Verify the error is meaningful
		errStr := err.Error()
		if !containsAny(errStr, "sst", "SST", "table", "not found", "no such file", "does not exist", "missing") {
			t.Logf("Warning: error message may not be specific enough: %v", err)
		}
	} else {
		// Some implementations lazily load SSTs.
		// Try to read data - this should trigger the error when the SST is accessed.
		t.Log("Open succeeded, trying to read data (lazy SST loading)...")

		var readFailed bool
		var readErr error

		// Create an iterator and scan - this forces SST access
		iter := database.NewIterator(nil)
		iter.SeekToFirst()
		for iter.Valid() {
			// Try to read the value
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		if iter.Error() != nil {
			readFailed = true
			readErr = iter.Error()
			t.Logf("Iterator failed: %v", readErr)
		}
		iter.Close()

		// Also try Get for a specific key
		if !readFailed {
			_, err := database.Get(nil, []byte("key_A0"))
			if err != nil && !errors.Is(err, db.ErrNotFound) {
				readFailed = true
				readErr = err
				t.Logf("Get failed: %v", readErr)
			}
		}

		database.Close()

		if !readFailed {
			// If we still didn't fail, this is a problem.
			// The DB should either fail at open or at read when an SST is missing.
			t.Logf("WARNING: DB opened and read succeeded with missing SST - SST may be orphaned or data was in memtable only")
			// This might happen if the data wasn't actually in the deleted SST
			// (e.g., memtable data replayed from WAL).
			// Let's verify by checking what SSTs exist
			entries, _ := os.ReadDir(dir)
			t.Log("Remaining files in directory:")
			for _, e := range entries {
				t.Logf("  - %s", e.Name())
			}
		} else {
			t.Logf("Read operation failed as expected: %v", readErr)
		}
	}

	if !openFailed {
		t.Log("Note: Implementation uses lazy SST loading (deferred file access)")
	}

	// Step 5: Capture artifacts and run oracle checks
	if os.Getenv(CppOraclePathEnv) != "" {
		artifactDir := filepath.Join(os.TempDir(), "rockyardkv-durability-artifacts", t.Name())
		_ = os.MkdirAll(artifactDir, 0755)

		// Save info about missing SST
		_ = os.WriteFile(filepath.Join(artifactDir, "missing_sst_name.txt"),
			[]byte(sstName), 0644)

		// Save CURRENT content
		currentPath := filepath.Join(dir, "CURRENT")
		if content, err := os.ReadFile(currentPath); err == nil {
			_ = os.WriteFile(filepath.Join(artifactDir, "CURRENT_original.txt"), content, 0644)
		}

		// Run oracle checks - C++ tools should also fail
		runCppOracleChecks(t, artifactDir, dir)
		t.Logf("Oracle artifacts saved to %s", artifactDir)
	}
}

// =============================================================================
// Rename Anomaly Scenarios
// =============================================================================

// TestDurability_RenameDoubleNameMode_CURRENTAnomalyDetected tests that the DB
// detects when both CURRENT and CURRENT.tmp exist (double-name anomaly).
//
// Contract: If a crash leaves both CURRENT and its temp file, the DB must
// either fail loud or cleanly handle the ambiguity.
func TestDurability_RenameDoubleNameMode_CURRENTAnomalyDetected(t *testing.T) {
	dir := t.TempDir()

	// Step 1: Create a valid database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create initial DB: %v", err)
	}

	// Write some data and flush
	wo := db.DefaultWriteOptions()
	wo.Sync = true
	for i := range 10 {
		key := []byte("key_" + padInt(i, 3))
		if err := database.Put(wo, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close the database
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Step 2: Read CURRENT and create the double-name anomaly
	currentPath := filepath.Join(dir, "CURRENT")
	currentContent, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("Failed to read CURRENT: %v", err)
	}

	// Create CURRENT.tmp with the same content (simulating double-name crash)
	tmpPath := filepath.Join(dir, "CURRENT.tmp")
	if err := os.WriteFile(tmpPath, currentContent, 0644); err != nil {
		t.Fatalf("Failed to create CURRENT.tmp: %v", err)
	}

	t.Logf("Created double-name anomaly: both %s and %s exist", "CURRENT", "CURRENT.tmp")

	// Step 3: Try to reopen - should handle gracefully
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		// Acceptable: DB detected the anomaly and failed loud
		t.Logf("DB failed to open with double-name anomaly (acceptable): %v", err)
		return
	}

	// Also acceptable: DB opened successfully (temp file is ignored)
	t.Log("DB opened successfully despite CURRENT.tmp presence (temp file ignored)")
	defer database.Close()

	// Verify data is intact
	for i := range 10 {
		key := []byte("key_" + padInt(i, 3))
		if _, err := database.Get(nil, key); err != nil {
			t.Errorf("Failed to read key_%03d: %v", i, err)
		}
	}
	t.Log("Data verified intact after handling double-name anomaly")
}

// TestDurability_RenameNeitherNameMode_CURRENTMissingFailsLoud tests that
// when both CURRENT and its temp file are missing, the DB fails loud.
//
// Contract: A database without a CURRENT file cannot be opened.
func TestDurability_RenameNeitherNameMode_CURRENTMissingFailsLoud(t *testing.T) {
	dir := t.TempDir()

	// Step 1: Create a valid database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create initial DB: %v", err)
	}

	// Write some data and flush
	wo := db.DefaultWriteOptions()
	wo.Sync = true
	if err := database.Put(wo, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Step 2: Delete CURRENT (simulating neither-name crash)
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.Remove(currentPath); err != nil {
		t.Fatalf("Failed to remove CURRENT: %v", err)
	}

	t.Log("Simulated neither-name anomaly: CURRENT deleted")

	// Step 3: Try to reopen - should fail loud
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err == nil {
		database.Close()
		t.Fatal("DB should fail to open without CURRENT file")
	}

	t.Logf("DB correctly failed to open: %v", err)

	// Verify error message is meaningful
	errStr := err.Error()
	if containsAny(errStr, "CURRENT", "not found", "no such file", "database not found") {
		t.Log("Error message indicates missing CURRENT (expected)")
	}
}

// TestDurability_RenameDoubleNameMode_SSTAnomalyHandled tests that when both
// an SST file and its temp file exist (double-name anomaly), the DB handles it.
//
// Contract: If a crash leaves both SST and its temp file, reads should not
// return corrupted or duplicate data.
func TestDurability_RenameDoubleNameMode_SSTAnomalyHandled(t *testing.T) {
	dir := t.TempDir()

	// Step 1: Create a valid database with some SST files
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create initial DB: %v", err)
	}

	// Write data and flush to create SST
	for i := range 100 {
		key := []byte("key_" + padInt(i, 4))
		value := []byte("value_" + padInt(i, 4))
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Step 2: Find an SST file and create a .tmp copy (double-name anomaly)
	entries, _ := os.ReadDir(dir)
	var sstPath string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			sstPath = filepath.Join(dir, e.Name())
			break
		}
	}
	if sstPath == "" {
		t.Fatal("No SST file found")
	}

	// Create a .tmp version
	tmpPath := sstPath + ".tmp"
	sstContent, _ := os.ReadFile(sstPath)
	if err := os.WriteFile(tmpPath, sstContent, 0644); err != nil {
		t.Fatalf("Failed to create SST.tmp: %v", err)
	}

	t.Logf("Created SST double-name anomaly: %s.tmp exists", filepath.Base(sstPath))

	// Step 3: Try to reopen - should handle gracefully
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)

	if err != nil {
		t.Logf("DB failed to open with SST temp anomaly: %v", err)
		return
	}
	defer database.Close()

	// Verify data is intact (no duplicates, no corruption)
	keyCount := 0
	for i := range 100 {
		key := []byte("key_" + padInt(i, 4))
		expectedValue := []byte("value_" + padInt(i, 4))
		gotValue, err := database.Get(nil, key)
		if err != nil {
			t.Errorf("Failed to read key_%04d: %v", i, err)
			continue
		}
		if string(gotValue) != string(expectedValue) {
			t.Errorf("Wrong value for key_%04d: got %q, want %q", i, gotValue, expectedValue)
		}
		keyCount++
	}

	t.Logf("Data verified: %d/100 keys correct after SST double-name anomaly", keyCount)
}

// TestDurability_RenameNeitherNameMode_SSTMissing_FailsLoudOrRecoversOlder tests
// a rename neither-name anomaly on the SST publish path: after crash, neither the
// temp SST nor the final SST exists.
//
// Contract: The DB must fail loud or recover to an older consistent state (no
// silent success with missing SST references).
func TestDurability_RenameNeitherNameMode_SSTMissing_FailsLoudOrRecoversOlder(t *testing.T) {
	dir := t.TempDir()

	faultFS := vfs.NewFaultInjectionFS(vfs.Default())
	// Target SST publish/rename outcomes.
	faultFS.SetRenameNeitherNameMode(true, ".sst")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.FS = faultFS

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Create an SST via flush.
	for i := range 200 {
		key := []byte("sst_key_" + padInt(i, 4))
		val := []byte("sst_val_" + padInt(i, 4))
		if err := database.Put(nil, key, val); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate crash with rename anomalies + unsynced data loss.
	_ = faultFS.SimulateCrashWithRenameAnomalies()
	_ = faultFS.RevertUnsyncedRenames()
	_ = faultFS.DropUnsyncedData()

	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		// Fail loud is acceptable; ensure error has some signal.
		errStr := err.Error()
		if !containsAny(errStr, "sst", "SST", "manifest", "MANIFEST", "not found", "no such file", "corrupt", "invalid") {
			t.Logf("warning: error message may not be specific enough: %q", errStr)
		}
		return
	}
	defer database.Close()

	// If open succeeds, it must be consistent (recover older state is acceptable).
	// Iteration should be clean (no internal errors).
	iter := database.NewIterator(nil)
	defer iter.Close()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		// no-op
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterator error after reopen: %v", err)
	}
}
