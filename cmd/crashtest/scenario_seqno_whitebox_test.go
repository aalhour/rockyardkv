//go:build crashtest

// Whitebox sequence number collision prevention tests (C02).
//
// These tests use deterministic kill points to verify that orphaned SST cleanup
// and sequence number monotonicity prevent internal key collisions.
//
// Unlike blackbox tests (which rely on random crash timing), these tests use
// kill points to crash at specific code boundaries:
//   - Flush.UpdateManifest:0 - After SST sync, before MANIFEST update
//   - Flush.UpdateManifest:1 - After MANIFEST update
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h (TEST_KILL_RANDOM macros)
//   - tools/db_crashtest.py (whitebox mode)
//
// Build and run:
//
//	go test -tags crashtest -v ./cmd/crashtest/... -run TestScenarioWhitebox.*Seqno
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// TestScenarioWhitebox_FlushSST_CrashBeforeManifestSync verifies that orphaned
// SST files are cleaned up after a crash between SST sync and MANIFEST sync,
// preventing internal key collisions from sequence reuse.
//
// Kill point: Flush.UpdateManifest:0 (after SST written, before MANIFEST update)
// Invariant: Orphaned SST is deleted on recovery; no sequence reuse.
//
// Crash window:
//
//	SST file synced → [CRASH HERE] → MANIFEST updated
//
// Without the fix:
//  1. SST file with sequences 51-80 is written to disk
//  2. Crash before MANIFEST update (SST becomes "orphaned")
//  3. Recovery doesn't know about SST, reuses sequences 51-80
//  4. Internal key collision: (key1, seq=51, TypeValue) has two different values
//
// With the fix (db/recovery.go):
//  1. Orphaned SST is detected (exists on disk but not in MANIFEST)
//  2. Orphaned SST is deleted during recovery
//  3. Sequences 51-80 are never reused
func TestScenarioWhitebox_FlushSST_CrashBeforeManifestSync(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create baseline with flushed data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 50 {
			key := []byte(fmt.Sprintf("baseline_%04d", i))
			value := []byte(fmt.Sprintf("value_%04d", i))
			if err := database.Put(opts, key, value); err != nil {
				t.Fatalf("Put baseline failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush baseline failed: %v", err)
		}
		database.Close()
	}

	// Phase 2: Write data, flush, crash at Flush.UpdateManifest:0
	// This creates an orphaned SST (synced to disk but not in MANIFEST)
	runWhiteboxChild(t, dir, testutil.KPFlushUpdateManifest0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 30 {
			key := []byte(fmt.Sprintf("orphan_%04d", i))
			value := []byte(fmt.Sprintf("orphan_value_%04d", i))
			if err := database.Put(opts, key, value); err != nil {
				t.Fatalf("Put orphan failed: %v", err)
			}
		}

		// This flush will create an SST but crash before updating MANIFEST
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush orphan failed: %v", err)
		}
	})

	// Phase 3: Reopen and verify orphaned SST was cleaned up
	{
		database := openDB(t, dir)
		defer database.Close()

		// Verify baseline keys are still present
		for i := range 50 {
			key := []byte(fmt.Sprintf("baseline_%04d", i))
			expectedValue := []byte(fmt.Sprintf("value_%04d", i))
			value, err := database.Get(nil, key)
			if err != nil {
				t.Errorf("Baseline key %s missing: %v", key, err)
				continue
			}
			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Baseline key %s: got %q, want %q", key, value, expectedValue)
			}
		}

		// Orphan keys should NOT be present (SST was orphaned and cleaned up)
		for i := range 30 {
			key := []byte(fmt.Sprintf("orphan_%04d", i))
			_, err := database.Get(nil, key)
			if err == nil {
				t.Errorf("Orphan key %s should not exist (SST was orphaned)", key)
			} else if !errors.Is(err, db.ErrNotFound) {
				t.Errorf("Unexpected error for orphan key %s: %v", key, err)
			}
		}

		// Phase 4: Write new data and verify no sequence reuse
		recoveryOpts := db.DefaultWriteOptions()
		recoveryOpts.DisableWAL = true
		for i := range 50 {
			key := []byte(fmt.Sprintf("recovery_%04d", i))
			value := []byte(fmt.Sprintf("recovery_value_%04d", i))
			if err := database.Put(recoveryOpts, key, value); err != nil {
				t.Fatalf("Put recovery failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush recovery failed: %v", err)
		}
	}

	// Phase 5: Run collision check to verify no internal key collisions
	if err := runCollisionCheck(t, dir); err != nil {
		t.Fatalf("Collision check failed: %v", err)
	}

	t.Log("✅ No sequence reuse detected after orphaned SST cleanup")
}

// TestScenarioWhitebox_ConcurrentFlush_CrashDuringSecondFlush verifies that
// LastSequence is correctly managed when multiple flushes occur and a crash
// happens during the second flush.
//
// Kill point: Flush.UpdateManifest:0 (during second flush)
// Invariant: First flush's LastSequence is preserved; no sequence reuse.
//
// Scenario:
//
//	Flush1 completes (sequences 1-30)
//	→ LastSequence = 30 in MANIFEST
//	Flush2 starts (sequences 31-60)
//	→ Writes SST with sequences 31-60
//	→ [CRASH before MANIFEST update]
//	Recovery:
//	→ LastSequence = 30 (from MANIFEST)
//	→ Orphaned SST (31-60) is deleted
//	→ New writes get sequences 31+
//
// Without the fix:
//   - LastSequence might be 60 (from db.seq, not actual flushed data)
//   - After recovery, sequences 31-60 are "claimed" but no data exists
//   - New writes reuse 31-60 → collision
func TestScenarioWhitebox_ConcurrentFlush_CrashDuringSecondFlush(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: First flush completes successfully
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 30 {
			key := []byte(fmt.Sprintf("flush1_%04d", i))
			value := []byte(fmt.Sprintf("value1_%04d", i))
			if err := database.Put(opts, key, value); err != nil {
				t.Fatalf("Put flush1 failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush1 failed: %v", err)
		}

		firstSeq := database.GetLatestSequenceNumber()
		t.Logf("First flush completed, sequence: %d", firstSeq)

		database.Close()
	}

	// Phase 2: Second flush crashes at UpdateManifest
	runWhiteboxChild(t, dir, testutil.KPFlushUpdateManifest0, func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 30 {
			key := []byte(fmt.Sprintf("flush2_%04d", i))
			value := []byte(fmt.Sprintf("value2_%04d", i))
			if err := database.Put(opts, key, value); err != nil {
				t.Fatalf("Put flush2 failed: %v", err)
			}
		}

		// This will crash before MANIFEST update
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush2 failed: %v", err)
		}
	})

	// Phase 3: Recover and verify
	{
		database := openDB(t, dir)
		defer database.Close()

		// First flush keys must be present
		for i := range 30 {
			key := []byte(fmt.Sprintf("flush1_%04d", i))
			expectedValue := []byte(fmt.Sprintf("value1_%04d", i))
			value, err := database.Get(nil, key)
			if err != nil {
				t.Errorf("Flush1 key %s missing: %v", key, err)
				continue
			}
			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Flush1 key %s: got %q, want %q", key, value, expectedValue)
			}
		}

		// Second flush keys should NOT be present (flush didn't complete)
		for i := range 30 {
			key := []byte(fmt.Sprintf("flush2_%04d", i))
			_, err := database.Get(nil, key)
			if err == nil {
				t.Errorf("Flush2 key %s should not exist (flush crashed)", key)
			} else if !errors.Is(err, db.ErrNotFound) {
				t.Errorf("Unexpected error for flush2 key %s: %v", key, err)
			}
		}

		// Write new data and verify no collision
		recoveryOpts := db.DefaultWriteOptions()
		recoveryOpts.DisableWAL = true
		for i := range 30 {
			key := []byte(fmt.Sprintf("recovery_%04d", i))
			value := []byte(fmt.Sprintf("recovery_value_%04d", i))
			if err := database.Put(recoveryOpts, key, value); err != nil {
				t.Fatalf("Put recovery failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush recovery failed: %v", err)
		}
	}

	// Verify no collisions
	if err := runCollisionCheck(t, dir); err != nil {
		t.Fatalf("Collision check failed: %v", err)
	}

	t.Log("✅ LastSequence correctly managed across concurrent flushes")
}

// =============================================================================
// Helpers
// =============================================================================

// runCollisionCheck runs the collision checker tool on a database directory.
func runCollisionCheck(t *testing.T, dbPath string) error {
	t.Helper()

	// Use sstdump for collision-check (consistent with status scripts)
	cmd := exec.Command("go", "run", "../../cmd/sstdump", "--command=collision-check", "--dir="+dbPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("collision check failed: %v\nOutput:\n%s", err, output)
	}

	// Check output for collision report
	if bytes.Contains(output, []byte("SMOKING GUN")) {
		return fmt.Errorf("collision detected:\n%s", output)
	}

	// Verify success message is present
	if !bytes.Contains(output, []byte("No internal key collisions")) {
		return fmt.Errorf("unexpected collision check output:\n%s", output)
	}

	return nil
}
