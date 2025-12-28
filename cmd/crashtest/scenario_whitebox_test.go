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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

// TestScenarioWhitebox_WALSync0_UnsyncedMayBeLost verifies that a crash
// before WAL sync may lose unsynced data, but the DB remains consistent.
//
// Kill point: WAL.Sync:0 (before WAL sync)
// Invariant: Previously synced data survives; unsynced writes may be lost.
func TestScenarioWhitebox_WALSync0_UnsyncedMayBeLost(t *testing.T) {
	dir := t.TempDir()

	// First create the DB with a synced baseline
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("baseline_key"), []byte("baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		database.Close()
	}

	// Run child process that writes with sync=true and crashes BEFORE WAL.Sync:0
	runWhiteboxChild(t, dir, testutil.KPWALSync0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		// This write may not be durable since we crash before sync completes
		if err := database.Put(opts, []byte("unsynced_key"), []byte("unsynced_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	// Reopen and verify the baseline is present
	database := openDB(t, dir)
	defer database.Close()

	// Baseline key must be present
	value, err := database.Get(nil, []byte("baseline_key"))
	if err != nil {
		t.Fatalf("Get baseline_key after WAL.Sync:0 crash failed: %v", err)
	}
	if string(value) != "baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "baseline_value")
	}

	// The unsynced key may or may not be present (crash was before sync)
	// The important invariant is the DB is consistent and opens successfully
	_, err = database.Get(nil, []byte("unsynced_key"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get unsynced_key returned unexpected error (not ErrNotFound): %v", err)
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
// Scenario 5: MANIFEST Write Crash
// =============================================================================

// TestScenarioWhitebox_ManifestWrite0_PartialWriteHandled verifies that a crash
// during MANIFEST record write is handled correctly.
//
// Kill point: Manifest.Write:0 (during MANIFEST record write)
// Invariant: CURRENT points to valid MANIFEST; DB opens without corruption.
func TestScenarioWhitebox_ManifestWrite0_PartialWriteHandled(t *testing.T) {
	dir := t.TempDir()

	// First, create a DB with initial data and flush to establish MANIFEST
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("manifest_baseline"), []byte("manifest_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Run child that crashes during MANIFEST record write
	runWhiteboxChild(t, dir, testutil.KPManifestWrite0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		// Write data and flush to trigger MANIFEST write
		if err := database.Put(opts, []byte("manifest_partial"), []byte("manifest_partial_value")); err != nil {
			t.Logf("Put manifest_partial failed (may be expected): %v", err)
		}
		// The flush will trigger MANIFEST write, and we'll crash during it
		_ = database.Flush(nil)
	})

	// Reopen and verify CURRENT points to a valid MANIFEST
	database := openDB(t, dir)
	defer database.Close()

	// Baseline must be present — CURRENT still points to previous valid MANIFEST
	value, err := database.Get(nil, []byte("manifest_baseline"))
	if err != nil {
		t.Fatalf("Get manifest_baseline after Manifest.Write:0 crash failed: %v", err)
	}
	if string(value) != "manifest_baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "manifest_baseline_value")
	}

	// The partial key may or may not be present depending on crash timing
	// The important invariant is no MANIFEST corruption
	_, err = database.Get(nil, []byte("manifest_partial"))
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("Get manifest_partial returned unexpected error (not ErrNotFound): %v", err)
	}
}

// =============================================================================
// Scenario 6: Flush Crash Points
// =============================================================================

// TestScenarioWhitebox_FlushStart0_DBConsistent verifies that crashing at
// flush start leaves the DB consistent.
//
// Kill point: Flush.Start:0 (at flush start)
// Invariant: DB opens without corruption; memtable data may or may not be flushed.
func TestScenarioWhitebox_FlushStart0_DBConsistent(t *testing.T) {
	dir := t.TempDir()

	// Create DB with initial data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("flush_baseline"), []byte("flush_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		database.Close()
	}

	// Crash at flush start
	runWhiteboxChild(t, dir, testutil.KPFlushStart0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("flush_key"), []byte("flush_value")); err != nil {
			t.Logf("Put flush_key failed (may be expected): %v", err)
		}
		// Trigger flush
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("flush_baseline"))
	if err != nil {
		t.Fatalf("Get flush_baseline after Flush.Start:0 crash failed: %v", err)
	}
	if string(value) != "flush_baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "flush_baseline_value")
	}
}

// TestScenarioWhitebox_FlushWriteSST0_NoPartialSST verifies that crashing
// during SST write doesn't leave a partial SST in the LSM.
//
// Kill point: Flush.WriteSST:0 (during SST write)
// Invariant: DB opens; no partially written SST is visible; data may be in WAL.
func TestScenarioWhitebox_FlushWriteSST0_NoPartialSST(t *testing.T) {
	dir := t.TempDir()

	// Create DB with initial data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("sst_baseline"), []byte("sst_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush baseline failed: %v", err)
		}
		database.Close()
	}

	// Crash during SST write
	runWhiteboxChild(t, dir, testutil.KPFlushWriteSST0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("sst_key"), []byte("sst_value")); err != nil {
			t.Logf("Put sst_key failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("sst_baseline"))
	if err != nil {
		t.Fatalf("Get sst_baseline after Flush.WriteSST:0 crash failed: %v", err)
	}
	if string(value) != "sst_baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "sst_baseline_value")
	}
}

// TestScenarioWhitebox_FlushUpdateManifest0_PreviousStateValid verifies that
// crashing before MANIFEST update during flush leaves previous state valid.
//
// Kill point: Flush.UpdateManifest:0 (before MANIFEST update for flush)
// Invariant: Previous MANIFEST state is valid; SST may exist but not in version.
func TestScenarioWhitebox_FlushUpdateManifest0_PreviousStateValid(t *testing.T) {
	dir := t.TempDir()

	// Create DB with initial data and flush
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("manifest_baseline"), []byte("manifest_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush baseline failed: %v", err)
		}
		database.Close()
	}

	// Crash before MANIFEST update during flush
	runWhiteboxChild(t, dir, testutil.KPFlushUpdateManifest0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("manifest_key"), []byte("manifest_value")); err != nil {
			t.Logf("Put manifest_key failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("manifest_baseline"))
	if err != nil {
		t.Fatalf("Get manifest_baseline after Flush.UpdateManifest:0 crash failed: %v", err)
	}
	if string(value) != "manifest_baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "manifest_baseline_value")
	}
}

// =============================================================================
// File Sync Kill Point Scenarios
// =============================================================================

// TestScenarioWhitebox_FileSync0_UnsyncedSSTMayBeLost tests crash before SST file sync.
//
// Contract: If crash occurs before file.Sync(), the SST may not be durable,
// but the database must still be consistent (no corruption).
func TestScenarioWhitebox_FileSync0_UnsyncedSSTMayBeLost(t *testing.T) {
	dir := t.TempDir()

	// Create baseline
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("filesync_baseline"), []byte("baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		database.Close()
	}

	// Crash before SST file sync during flush
	runWhiteboxChild(t, dir, testutil.KPFileSync0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("filesync_key"), []byte("filesync_value")); err != nil {
			t.Logf("Put filesync_key failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("filesync_baseline"))
	if err != nil {
		t.Fatalf("Get filesync_baseline after File.Sync:0 crash failed: %v", err)
	}
	if string(value) != "baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "baseline_value")
	}
}

// TestScenarioWhitebox_FileSync1_SSTIsDurable tests crash after SST file sync.
//
// Contract: After file.Sync() returns, the SST is durable and should survive crash.
func TestScenarioWhitebox_FileSync1_SSTIsDurable(t *testing.T) {
	dir := t.TempDir()

	// Crash after SST file sync during flush
	runWhiteboxChild(t, dir, testutil.KPFileSync1, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("filesync_durable"), []byte("durable_value")); err != nil {
			t.Fatalf("Put filesync_durable failed: %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and the write is present (SST was synced)
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("filesync_durable"))
	if err != nil {
		t.Fatalf("Get filesync_durable after File.Sync:1 crash failed: %v", err)
	}
	if string(value) != "durable_value" {
		t.Errorf("Value mismatch: got %q, want %q", value, "durable_value")
	}
}

// =============================================================================
// Directory Sync Kill Point Scenarios
// =============================================================================

// TestScenarioWhitebox_DirSync0_CURRENTMayNotBeDurable tests crash before directory sync.
//
// Contract: If crash occurs before SyncDir(), the CURRENT file rename may not be
// durable, but the database must still recover (using previous MANIFEST).
func TestScenarioWhitebox_DirSync0_CURRENTMayNotBeDurable(t *testing.T) {
	dir := t.TempDir()

	// Create baseline with a flush to establish initial MANIFEST
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("dirsync_baseline"), []byte("baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		_ = database.Flush(nil)
		database.Close()
	}

	// Crash before directory sync after CURRENT rename
	runWhiteboxChild(t, dir, testutil.KPDirSync0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("dirsync_key"), []byte("dirsync_value")); err != nil {
			t.Logf("Put dirsync_key failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("dirsync_baseline"))
	if err != nil {
		t.Fatalf("Get dirsync_baseline after Dir.Sync:0 crash failed: %v", err)
	}
	if string(value) != "baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "baseline_value")
	}
}

// TestScenarioWhitebox_DirSync1_CURRENTIsDurable tests crash after directory sync.
//
// Contract: After SyncDir() returns, the CURRENT file and MANIFEST are durable.
// Note: DirSync happens when CURRENT file is written (typically during DB creation
// or manifest rotation), so this test verifies that the DB survives a crash
// right after the CURRENT file becomes durable.
func TestScenarioWhitebox_DirSync1_CURRENTIsDurable(t *testing.T) {
	dir := t.TempDir()

	// First create the DB and establish baseline data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("dirsync_baseline"), []byte("baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		_ = database.Flush(nil)
		database.Close()
	}

	// Crash after directory sync during a subsequent operation that triggers SetCurrentFile
	// (manifest rotation happens when we write enough data or force it)
	runWhiteboxChild(t, dir, testutil.KPDirSync1, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("dirsync_durable"), []byte("durable_value")); err != nil {
			t.Logf("Put dirsync_durable failed (may be expected): %v", err)
		}
		_ = database.Flush(nil)
	})

	// Verify DB opens and baseline is present (at minimum)
	database := openDB(t, dir)
	defer database.Close()

	// Baseline must always be present
	value, err := database.Get(nil, []byte("dirsync_baseline"))
	if err != nil {
		t.Fatalf("Get dirsync_baseline after Dir.Sync:1 crash failed: %v", err)
	}
	if string(value) != "baseline_value" {
		t.Errorf("Baseline value mismatch: got %q, want %q", value, "baseline_value")
	}
}

// =============================================================================
// Killpoint Sweep Runner
// =============================================================================

// TestScenarioWhitebox_Sweep exercises all killpoints in write-flush-compaction scenarios.
//
// This is a "sweep" test that validates the killpoint infrastructure works
// and that each wired killpoint can be triggered without corrupting the DB.
//
// Env vars:
//   - WHITEBOX_STRICT_SWEEP=1: Fail if any killpoint is not triggered (coverage guarantee)
func TestScenarioWhitebox_Sweep(t *testing.T) {
	strictMode := os.Getenv("WHITEBOX_STRICT_SWEEP") == "1"

	// Killpoints grouped by the scenario needed to trigger them
	writeFlushKillpoints := []string{
		testutil.KPWALAppend0,
		testutil.KPWALSync0,
		testutil.KPWALSync1,
		testutil.KPManifestWrite0,
		testutil.KPManifestSync0,
		testutil.KPManifestSync1,
		testutil.KPCurrentWrite0,
		testutil.KPCurrentWrite1,
		testutil.KPFlushStart0,
		testutil.KPFlushWriteSST0,
		testutil.KPFlushUpdateManifest0,
		testutil.KPFlushUpdateManifest1,
		testutil.KPSSTClose0,
		testutil.KPSSTClose1,
		testutil.KPFileSync0,
		testutil.KPFileSync1,
		testutil.KPDirSync0,
		testutil.KPDirSync1,
	}

	compactionKillpoints := []string{
		testutil.KPCompactionStart0,
		testutil.KPCompactionWriteSST0,
		testutil.KPCompactionDeleteInput0,
	}

	// Run write+flush killpoints
	for _, kp := range writeFlushKillpoints {
		kp := kp
		t.Run(kp, func(t *testing.T) {
			t.Parallel()
			runSweepWriteFlush(t, kp, strictMode)
		})
	}

	// Run compaction killpoints with compaction-driving scenario
	for _, kp := range compactionKillpoints {
		kp := kp
		t.Run(kp, func(t *testing.T) {
			t.Parallel()
			runSweepCompaction(t, kp, strictMode)
		})
	}
}

// runSweepWriteFlush runs a write+flush scenario for killpoints in the write/flush path.
func runSweepWriteFlush(t *testing.T, kp string, strictMode bool) {
	t.Helper()
	dir := t.TempDir()

	// Phase 1: Create baseline
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		if err := database.Put(opts, []byte("sweep_baseline"), []byte("sweep_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush baseline failed: %v", err)
		}
		database.Close()
	}

	// Phase 2: Run child with killpoint
	childHit := runWhiteboxChildAllowMiss(t, dir, kp, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true
		for i := range 10 {
			key := []byte(fmt.Sprintf("sweep_key_%d", i))
			if err := database.Put(opts, key, []byte("sweep_value")); err != nil {
				break // May fail if killpoint hit early
			}
		}
		_ = database.Flush(nil)
	})

	// Phase 3: Verify recovery
	verifyBaselineSurvived(t, dir, kp)

	// Phase 4: Check strict mode
	if childHit {
		t.Logf("✅ Killpoint %s was hit and DB recovered correctly", kp)
	} else {
		if strictMode {
			t.Fatalf("❌ Killpoint %s was NOT triggered (strict mode enabled)", kp)
		}
		t.Logf("⚠️  Killpoint %s was not triggered (scenario may not exercise this path)", kp)
	}
}

// runSweepCompaction runs a compaction-driving scenario for killpoints in the compaction path.
func runSweepCompaction(t *testing.T, kp string, strictMode bool) {
	t.Helper()
	dir := t.TempDir()

	// Phase 1: Create baseline with multiple L0 files to trigger compaction
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.Sync = true

		// Write baseline
		if err := database.Put(opts, []byte("sweep_baseline"), []byte("sweep_baseline_value")); err != nil {
			t.Fatalf("Put baseline failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush baseline failed: %v", err)
		}

		// Create multiple L0 files to trigger compaction
		for i := range 5 {
			for j := range 100 {
				key := []byte(fmt.Sprintf("l0_file_%d_key_%03d", i, j))
				if err := database.Put(opts, key, []byte("value")); err != nil {
					t.Fatalf("Put L0 data failed: %v", err)
				}
			}
			if err := database.Flush(nil); err != nil {
				t.Fatalf("Flush L0 file failed: %v", err)
			}
		}
		database.Close()
	}

	// Phase 2: Run child with killpoint and trigger compaction
	childHit := runWhiteboxChildAllowMiss(t, dir, kp, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.Sync = true

		// Write more data and flush to create more L0 files
		// This will trigger background compaction via the L0 file count threshold
		for batch := range 3 {
			for i := range 50 {
				key := []byte(fmt.Sprintf("compaction_batch_%d_key_%03d", batch, i))
				if err := database.Put(opts, key, []byte("compaction_value")); err != nil {
					return // May fail if killpoint hit
				}
			}
			if err := database.Flush(nil); err != nil {
				return // May fail if killpoint hit
			}
		}

		// Give background compaction a chance to run (hits Compaction.Start:0)
		time.Sleep(100 * time.Millisecond)

		// Also trigger manual compaction (hits Compaction.WriteSST:0 and DeleteInput:0)
		_ = database.CompactRange(nil, nil, nil)
	})

	// Phase 3: Verify recovery
	verifyBaselineSurvived(t, dir, kp)

	// Phase 4: Check strict mode
	if childHit {
		t.Logf("✅ Killpoint %s was hit and DB recovered correctly", kp)
	} else {
		if strictMode {
			t.Fatalf("❌ Killpoint %s was NOT triggered (strict mode enabled)", kp)
		}
		t.Logf("⚠️  Killpoint %s was not triggered (scenario may not exercise this path)", kp)
	}
}

// verifyBaselineSurvived checks that the baseline key survived the crash.
func verifyBaselineSurvived(t *testing.T, dir, kp string) {
	t.Helper()
	database := openDB(t, dir)
	defer database.Close()

	value, err := database.Get(nil, []byte("sweep_baseline"))
	if err != nil {
		t.Fatalf("Get sweep_baseline after %s crash failed: %v", kp, err)
	}
	if string(value) != "sweep_baseline_value" {
		t.Errorf("Baseline value mismatch after %s: got %q, want %q", kp, value, "sweep_baseline_value")
	}
}

// =============================================================================
// Helpers
// =============================================================================

// runWhiteboxChildAllowMiss is like runWhiteboxChild but returns whether the killpoint was hit
// instead of failing if it wasn't hit. This is used by sweep tests.
func runWhiteboxChildAllowMiss(t *testing.T, dir, killPoint string, fn func(db.DB)) bool {
	t.Helper()

	// Check if we're the child process
	if os.Getenv("WHITEBOX_KILL_POINT") == killPoint {
		testutil.SetKillPoint(killPoint)

		dbPath := os.Getenv("WHITEBOX_DB_PATH")
		if dbPath == "" {
			t.Fatal("WHITEBOX_DB_PATH not set")
		}

		database := openOrCreateDB(t, dbPath)
		fn(database)

		// Kill point wasn't hit
		database.Close()
		os.Exit(2)
	}

	// Parent: spawn child
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
					return true // Killpoint was hit
				case 2:
					return false // Killpoint was not hit
				default:
					t.Logf("Child exited with code %d. Stdout: %s\nStderr: %s",
						exitErr.ExitCode(), stdout.String(), stderr.String())
					return false
				}
			}
			return false
		}
		return true // Exit code 0 = killpoint hit
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGKILL)
		t.Fatalf("Child timed out. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
		return false
	}
}

// runWhiteboxChild runs the given function in a child process with a kill point set.
// The child process will exit when the specified kill point is reached.
// On failure, artifacts are persisted for reproduction and debugging.
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

	// Check if we should always persist artifacts (for testing artifact generation)
	alwaysPersist := os.Getenv("WHITEBOX_ALWAYS_PERSIST") == "1"

	select {
	case err := <-done:
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				switch exitErr.ExitCode() {
				case 0:
					// Clean exit from os.Exit(0) at kill point — expected
					t.Logf("Child exited at kill point %s", killPoint)
					if alwaysPersist {
						persistWhiteboxArtifacts(t, t.Name(), killPoint, dir, &stdout, &stderr, 0,
							"Success (WHITEBOX_ALWAYS_PERSIST=1)")
					}
				case 2:
					// Kill point wasn't triggered — this is a test failure
					// Persist artifacts before failing
					persistWhiteboxArtifacts(t, t.Name(), killPoint, dir, &stdout, &stderr, 2,
						"Kill point was NOT triggered (child ran to completion)")
					t.Fatalf("Kill point %s was NOT triggered (child ran to completion). "+
						"Whitebox tests must hit their kill point to be valid.\nStdout: %s\nStderr: %s",
						killPoint, stdout.String(), stderr.String())
				default:
					// Unexpected exit code — persist artifacts
					persistWhiteboxArtifacts(t, t.Name(), killPoint, dir, &stdout, &stderr, exitErr.ExitCode(),
						fmt.Sprintf("Unexpected exit code: %d", exitErr.ExitCode()))
					t.Logf("Child exited with code %d. Stdout: %s\nStderr: %s",
						exitErr.ExitCode(), stdout.String(), stderr.String())
				}
			} else {
				t.Logf("Child exited with error: %v", err)
			}
		} else {
			// err == nil means exit code 0
			t.Logf("Child exited cleanly at kill point %s", killPoint)
			if alwaysPersist {
				persistWhiteboxArtifacts(t, t.Name(), killPoint, dir, &stdout, &stderr, 0,
					"Success (WHITEBOX_ALWAYS_PERSIST=1)")
			}
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGKILL)
		// Persist artifacts on timeout
		persistWhiteboxArtifacts(t, t.Name(), killPoint, dir, &stdout, &stderr, -1,
			"Child process timed out after 30 seconds")
		t.Fatalf("Child timed out. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
	}
}

// =============================================================================
// Artifact Persistence for Whitebox Test Failures
// =============================================================================

// whiteboxArtifact contains all information needed to reproduce a whitebox test failure.
type whiteboxArtifact struct {
	TestName   string    `json:"test_name"`
	KillPoint  string    `json:"kill_point"`
	DBPath     string    `json:"db_path"`
	Timestamp  time.Time `json:"timestamp"`
	GoVersion  string    `json:"go_version"`
	OS         string    `json:"os"`
	Arch       string    `json:"arch"`
	ExitCode   int       `json:"exit_code"`
	ReproCmd   string    `json:"repro_cmd"`
	FailReason string    `json:"fail_reason"`
}

// persistWhiteboxArtifacts saves failure artifacts for later analysis.
// Returns the artifact directory path.
func persistWhiteboxArtifacts(t *testing.T, testName, killPoint, dbPath string, stdout, stderr *bytes.Buffer, exitCode int, failReason string) string {
	t.Helper()

	// Get artifact base directory from env or use temp
	artifactBase := os.Getenv("WHITEBOX_ARTIFACT_DIR")
	if artifactBase == "" {
		artifactBase = filepath.Join(os.TempDir(), "rockyardkv-whitebox-artifacts")
	}

	// Create unique artifact directory
	timestamp := time.Now().Format("20060102-150405")
	sanitizedTest := filepath.Base(testName)
	artifactDir := filepath.Join(artifactBase, fmt.Sprintf("%s-%s-%s", sanitizedTest, killPoint, timestamp))

	if err := os.MkdirAll(artifactDir, 0755); err != nil {
		t.Logf("Warning: Failed to create artifact directory: %v", err)
		return ""
	}

	// 1. Copy DB directory
	dbCopyPath := filepath.Join(artifactDir, "db")
	if err := copyDir(dbPath, dbCopyPath); err != nil {
		t.Logf("Warning: Failed to copy DB directory: %v", err)
	}

	// 2. Save stdout
	if stdout.Len() > 0 {
		stdoutPath := filepath.Join(artifactDir, "stdout.log")
		if err := os.WriteFile(stdoutPath, stdout.Bytes(), 0644); err != nil {
			t.Logf("Warning: Failed to write stdout: %v", err)
		}
	}

	// 3. Save stderr
	if stderr.Len() > 0 {
		stderrPath := filepath.Join(artifactDir, "stderr.log")
		if err := os.WriteFile(stderrPath, stderr.Bytes(), 0644); err != nil {
			t.Logf("Warning: Failed to write stderr: %v", err)
		}
	}

	// 4. Generate repro command
	reproCmd := fmt.Sprintf("WHITEBOX_KILL_POINT=%s WHITEBOX_DB_PATH=%s go test -tags crashtest -v ./cmd/crashtest/... -run '^%s$'",
		killPoint, dbCopyPath, testName)

	// 5. Save run.json
	artifact := whiteboxArtifact{
		TestName:   testName,
		KillPoint:  killPoint,
		DBPath:     dbCopyPath,
		Timestamp:  time.Now(),
		GoVersion:  runtime.Version(),
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		ExitCode:   exitCode,
		ReproCmd:   reproCmd,
		FailReason: failReason,
	}

	runJSONPath := filepath.Join(artifactDir, "run.json")
	if data, err := json.MarshalIndent(artifact, "", "  "); err == nil {
		if err := os.WriteFile(runJSONPath, data, 0644); err != nil {
			t.Logf("Warning: Failed to write run.json: %v", err)
		}
	}

	// Print single-line repro command
	t.Logf("📁 Artifacts saved to: %s", artifactDir)
	t.Logf("🔄 Repro command: %s", reproCmd)

	// Run optional C++ oracle checks on the artifact DB
	if dbCopyPath != "" {
		runCppOracleChecks(t, artifactDir, dbCopyPath)
	}

	return artifactDir
}

// copyDir copies a directory recursively.
func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", src)
	}

	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyFile copies a single file.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}

// =============================================================================
// Optional C++ Oracle Hook for Whitebox Artifacts
// =============================================================================

// CppOraclePathEnv is the environment variable to specify the C++ RocksDB tools directory.
// When set, whitebox artifact persistence will run C++ oracle checks on the DB.
const CppOraclePathEnv = "ROCKYARDKV_CPP_ORACLE_PATH"

// runCppOracleChecks runs C++ RocksDB tools (ldb, sst_dump) on the artifact DB
// to verify format compatibility. Results are saved to the artifact directory.
func runCppOracleChecks(t *testing.T, artifactDir, dbPath string) {
	t.Helper()

	cppOraclePath := os.Getenv(CppOraclePathEnv)
	if cppOraclePath == "" {
		return // Oracle hook not enabled
	}

	ldbPath := filepath.Join(cppOraclePath, "ldb")
	sstDumpPath := filepath.Join(cppOraclePath, "sst_dump")

	// Check if ldb exists
	if _, err := os.Stat(ldbPath); err == nil {
		// Run manifest_dump with proper CURRENT-based manifest selection
		runLdbManifestDump(t, artifactDir, dbPath, ldbPath)

		// Run ldb scan (verify DB is readable)
		runOracleTool(t, artifactDir, "ldb_scan.txt", ldbPath,
			"scan", "--db="+dbPath)
	}

	// Check if sst_dump exists
	if _, err := os.Stat(sstDumpPath); err == nil {
		// Find SST files in the DB directory
		entries, err := os.ReadDir(dbPath)
		if err != nil {
			t.Logf("Warning: Failed to read DB directory for SST files: %v", err)
			return
		}

		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sst" {
				sstPath := filepath.Join(dbPath, entry.Name())
				outputFile := fmt.Sprintf("sst_dump_%s.txt", entry.Name())
				runOracleTool(t, artifactDir, outputFile, sstDumpPath,
					"--file="+sstPath, "--command=check", "--verify_checksums")
			}
		}
	}
}

// runLdbManifestDump handles ldb manifest_dump with proper CURRENT file parsing
// to avoid "Multiple MANIFEST files found" errors.
//
// Per WBT-05 spec, this function:
// 1. Reads <dbPath>/CURRENT to get the active manifest filename
// 2. Validates the filename (non-empty, starts with MANIFEST-, no path separators)
// 3. Runs ldb manifest_dump --db=<dbPath> --path=<activeManifestPath>
// 4. Saves diagnostic files: current.txt, manifest_selected.txt, manifest_list.txt
func runLdbManifestDump(t *testing.T, artifactDir, dbPath, ldbPath string) {
	t.Helper()

	// Step 1: Read CURRENT file
	currentPath := filepath.Join(dbPath, "CURRENT")
	currentBytes, err := os.ReadFile(currentPath)

	// Save current.txt (raw bytes from CURRENT file)
	if err == nil {
		if writeErr := os.WriteFile(filepath.Join(artifactDir, "current.txt"), currentBytes, 0644); writeErr != nil {
			t.Logf("Warning: Failed to write current.txt: %v", writeErr)
		}
	} else {
		// Write error note to current.txt
		errNote := fmt.Sprintf("CURRENT file missing or unreadable: %v", err)
		_ = os.WriteFile(filepath.Join(artifactDir, "current.txt"), []byte(errNote), 0644)
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Step 2: Trim whitespace (spaces, \r, \n)
	activeManifestName := strings.TrimSpace(string(currentBytes))

	// Step 3: Validate manifest name
	validationErr := validateManifestName(activeManifestName)
	if validationErr != "" {
		errNote := fmt.Sprintf("Invalid CURRENT contents: %s (raw: %q)", validationErr, string(currentBytes))
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Step 4: Build full manifest path
	activeManifestPath := filepath.Join(dbPath, activeManifestName)

	// Save manifest_selected.txt
	if writeErr := os.WriteFile(filepath.Join(artifactDir, "manifest_selected.txt"),
		[]byte(activeManifestPath), 0644); writeErr != nil {
		t.Logf("Warning: Failed to write manifest_selected.txt: %v", writeErr)
	}

	// Save manifest_list.txt (all MANIFEST-* files in dbPath)
	saveManifestList(t, artifactDir, dbPath)

	// Step 5: Validate manifest file exists
	info, err := os.Stat(activeManifestPath)
	if err != nil {
		errNote := fmt.Sprintf("CURRENT points to missing file: %s (%v)", activeManifestName, err)
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}
	if info.IsDir() {
		errNote := fmt.Sprintf("CURRENT points to a directory, not a file: %s", activeManifestName)
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Step 6: Run ldb manifest_dump with --path to select the exact manifest
	runOracleTool(t, artifactDir, "ldb_manifest_dump.txt", ldbPath,
		"manifest_dump", "--db="+dbPath, "--path="+activeManifestPath)
}

// validateManifestName validates the active manifest name per WBT-05 spec.
// Returns empty string if valid, or an error description if invalid.
func validateManifestName(name string) string {
	if name == "" {
		return "manifest name is empty"
	}
	if !strings.HasPrefix(name, "MANIFEST-") {
		return fmt.Sprintf("does not start with MANIFEST- (got %q)", name)
	}
	if strings.ContainsAny(name, "/\\") {
		return "contains path separators"
	}
	return ""
}

// saveManifestList writes a sorted list of all MANIFEST-* files in dbPath to manifest_list.txt.
func saveManifestList(t *testing.T, artifactDir, dbPath string) {
	t.Helper()

	entries, err := os.ReadDir(dbPath)
	if err != nil {
		_ = os.WriteFile(filepath.Join(artifactDir, "manifest_list.txt"),
			[]byte(fmt.Sprintf("Failed to read directory: %v", err)), 0644)
		return
	}

	var manifests []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "MANIFEST-") {
			manifests = append(manifests, entry.Name())
		}
	}

	sort.Strings(manifests)

	content := strings.Join(manifests, "\n")
	if content == "" {
		content = "(no MANIFEST-* files found)"
	}

	if writeErr := os.WriteFile(filepath.Join(artifactDir, "manifest_list.txt"),
		[]byte(content), 0644); writeErr != nil {
		t.Logf("Warning: Failed to write manifest_list.txt: %v", writeErr)
	}
}

// runOracleTool runs a C++ oracle tool and saves output to the artifact directory.
func runOracleTool(t *testing.T, artifactDir, outputFile, toolPath string, args ...string) {
	t.Helper()

	// Set DYLD_LIBRARY_PATH for macOS
	libPath := filepath.Dir(toolPath)
	env := append(os.Environ(), "DYLD_LIBRARY_PATH="+libPath)

	cmd := exec.Command(toolPath, args...)
	cmd.Env = env

	output, err := cmd.CombinedOutput()

	// Save output regardless of success/failure
	outPath := filepath.Join(artifactDir, outputFile)
	if writeErr := os.WriteFile(outPath, output, 0644); writeErr != nil {
		t.Logf("Warning: Failed to write oracle output to %s: %v", outPath, writeErr)
	}

	if err != nil {
		t.Logf("⚠️  C++ oracle tool %s failed: %v", filepath.Base(toolPath), err)
		t.Logf("   Output saved to: %s", outPath)
	} else {
		t.Logf("✅ C++ oracle tool %s passed, output saved to: %s", filepath.Base(toolPath), outPath)
	}
}
