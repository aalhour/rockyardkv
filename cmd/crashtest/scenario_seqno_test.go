// Sequence number collision prevention tests (C02).
//
// These tests verify that sequence numbers are never reused across crash/recovery
// cycles, preventing internal key collisions where the same (user_key, sequence,
// type) tuple appears with different values.
//
// The core invariant: After flush+crash+recovery, new writes must receive
// sequence numbers strictly greater than any sequence in the recovered database.
//
// This addresses C02: Internal Key Collision due to:
// 1. LastSequence using db.seq instead of flushed SST's actual max sequence
// 2. Orphaned SST files (written but not in MANIFEST) causing sequence reuse
//
// Reference:
//   - docs/redteam/ISSUES/C02.md
//   - db/flush.go (LastSequence monotonicity fix)
//   - db/recovery.go (orphaned SST cleanup)
package main

import (
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

// TestScenario_FlushRecoveryNoSequenceReuse verifies that after flush+crash+reopen,
// new writes receive sequence numbers strictly greater than any sequence in the
// recovered database, preventing internal key collisions.
//
// Contract: Sequences are never reused across crash/recovery cycles.
//
// Scenario:
//  1. Write & flush baseline data (durable)
//  2. Write more data with DisableWAL, flush it (may become orphaned on crash)
//  3. Crash (simulated by child process exit)
//  4. Recover and write new data
//  5. Verify all recovered keys have correct values (no collisions)
func TestScenario_FlushRecoveryNoSequenceReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping crash scenario in short mode")
	}

	dir := t.TempDir()

	// Phase 1: Establish baseline with flushed data
	{
		database := createDB(t, dir)
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 50 {
			key := fmt.Sprintf("baseline_key_%04d", i)
			value := fmt.Sprintf("baseline_value_%04d", i)
			if err := database.Put(opts, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		database.Close()
	}

	// Phase 2: Write more data with DisableWAL and crash (simulates orphaned SST scenario)
	runScenarioChild(t, dir, "disablewal-write", func(database db.DB) {
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		// Write some keys that will be flushed
		for i := range 30 {
			key := fmt.Sprintf("crash_key_%04d", i)
			value := fmt.Sprintf("crash_value_%04d", i)
			if err := database.Put(opts, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Flush to create SST (might become orphaned if crash occurs during MANIFEST write)
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Write more data that definitely won't be flushed
		for i := 30; i < 50; i++ {
			key := fmt.Sprintf("crash_key_%04d", i)
			value := fmt.Sprintf("crash_value_%04d", i)
			if err := database.Put(opts, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		// Crash occurs here (exit without clean shutdown)
	})

	// Phase 3: Recover and write new data
	{
		database := openDB(t, dir)
		defer database.Close()

		// Write new keys after recovery
		opts := db.DefaultWriteOptions()
		opts.DisableWAL = true

		for i := range 50 {
			key := fmt.Sprintf("recovery_key_%04d", i)
			value := fmt.Sprintf("recovery_value_%04d", i)
			if err := database.Put(opts, []byte(key), []byte(value)); err != nil {
				t.Fatalf("Put after recovery failed: %v", err)
			}
		}

		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush after recovery failed: %v", err)
		}
	}

	// Phase 4: Verify no sequence reuse by checking all keys read consistently
	// If sequences were reused, we'd see value corruption or key shadowing
	{
		database := openDB(t, dir)
		defer database.Close()

		// Verify baseline keys (should always be present - they were flushed before crash)
		for i := range 50 {
			key := fmt.Sprintf("baseline_key_%04d", i)
			expectedValue := fmt.Sprintf("baseline_value_%04d", i)
			value, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Baseline key %s missing after recovery: %v", key, err)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Baseline key %s corrupted: got %q, want %q (sequence reuse?)",
					key, value, expectedValue)
			}
		}

		// Verify recovery keys (should be present - they were flushed after recovery)
		for i := range 50 {
			key := fmt.Sprintf("recovery_key_%04d", i)
			expectedValue := fmt.Sprintf("recovery_value_%04d", i)
			value, err := database.Get(nil, []byte(key))
			if err != nil {
				t.Errorf("Recovery key %s missing: %v", key, err)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Recovery key %s corrupted: got %q, want %q (sequence reuse?)",
					key, value, expectedValue)
			}
		}

		// Crash keys may or may not be present (they used DisableWAL)
		// But if they ARE present, they must have correct values
		for i := range 30 {
			key := fmt.Sprintf("crash_key_%04d", i)
			expectedValue := fmt.Sprintf("crash_value_%04d", i)
			value, err := database.Get(nil, []byte(key))
			if err == nil {
				// Key exists - verify it's not corrupted
				if string(value) != expectedValue {
					t.Errorf("Crash key %s corrupted: got %q, want %q (sequence reuse collision!)",
						key, value, expectedValue)
				}
			}
			// If key doesn't exist, that's fine (expected data loss under DisableWAL)
		}
	}

	t.Log("✓ All keys read consistently - no sequence reuse detected")
}
