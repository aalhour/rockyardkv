// Cross-cycle durability tests verify that durable state is correctly managed
// across multiple crash/recovery cycles.
//
// These tests verify that:
// - Stale expected state from previous cycles is not carried over
// - Durable state only contains values that were actually flushed
package main

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// encodeValueBase encodes a value_base as an 8-byte value.
func encodeValueBase(vb uint32) []byte {
	val := make([]byte, 8)
	binary.LittleEndian.PutUint32(val, vb)
	return val
}

// decodeValueBase decodes a value_base from an 8-byte value.
func decodeValueBase(val []byte) uint32 {
	if len(val) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(val)
}

// TestCrossCycle_DurableStateNotCarriedOver verifies that data that was never
// synced (only in memtable, not flushed) is lost after a simulated crash.
func TestCrossCycle_DurableStateNotCarriedOver(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-cycle test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := db.DefaultOptions()
	opts.FS = faultFS
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := []byte("testkey")

	woSync := &db.WriteOptions{Sync: true}
	if err := database.Put(woSync, key, encodeValueBase(1)); err != nil {
		t.Fatalf("Put V1 failed: %v", err)
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Log("Wrote and flushed V1 (durable)")

	woNoWAL := &db.WriteOptions{DisableWAL: true}
	if err := database.Put(woNoWAL, key, encodeValueBase(2)); err != nil {
		t.Fatalf("Put V2 failed: %v", err)
	}
	t.Log("Wrote V2 with DisableWAL (NOT durable)")

	val, err := database.Get(nil, key)
	if err != nil {
		t.Fatalf("Get before crash failed: %v", err)
	}
	if decodeValueBase(val) != 2 {
		t.Errorf("Before crash: expected V2, got V%d", decodeValueBase(val))
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	t.Log("Simulating crash: dropping unsynced data...")
	faultFS.DropUnsyncedData()

	t.Log("Recovering and verifying...")

	opts2 := db.DefaultOptions()
	opts2.FS = baseFS
	opts2.CreateIfMissing = false

	database2, err := db.Open(dbPath, opts2)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database2.Close()

	val2, err := database2.Get(nil, key)
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}
	recoveredVB := decodeValueBase(val2)
	if recoveredVB != 1 {
		t.Errorf("After recovery: expected V1, got V%d", recoveredVB)
	} else {
		t.Log("SUCCESS: Recovered V1 correctly, V2 was lost as expected")
	}
}

// TestCrossCycle_StaleExpectedStateDetected verifies stale expected state detection.
func TestCrossCycle_StaleExpectedStateDetected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-cycle test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := db.DefaultOptions()
	opts.FS = faultFS
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := []byte("testkey")

	if err := database.Put(&db.WriteOptions{Sync: true}, key, encodeValueBase(1)); err != nil {
		t.Fatalf("Put V1 failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Log("Wrote and flushed V1")

	if err := database.Put(&db.WriteOptions{DisableWAL: true}, key, encodeValueBase(2)); err != nil {
		t.Fatalf("Put V2 failed: %v", err)
	}
	t.Log("Wrote V2 (not flushed)")

	expectedBeforeCrash := uint32(2)

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	faultFS.DropUnsyncedData()

	opts2 := db.DefaultOptions()
	opts2.FS = baseFS
	opts2.CreateIfMissing = false

	database2, err := db.Open(dbPath, opts2)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database2.Close()

	val, err := database2.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	actualVB := decodeValueBase(val)

	if actualVB == expectedBeforeCrash {
		t.Error("BUG: DB has stale expected value - V2 should have been lost")
	} else if actualVB == 1 {
		t.Log("SUCCESS: DB correctly has V1, V2 was lost")
	} else {
		t.Errorf("Unexpected value_base=%d", actualVB)
	}
}

// TestCrossCycle_OnlyCurrentFlushInDurableState verifies only flushed keys are durable.
func TestCrossCycle_OnlyCurrentFlushInDurableState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-cycle test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := db.DefaultOptions()
	opts.FS = faultFS
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	k1, k2 := []byte("key1"), []byte("key2")

	if err := database.Put(&db.WriteOptions{Sync: true}, k1, encodeValueBase(1)); err != nil {
		t.Fatalf("Put K1 failed: %v", err)
	}
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Log("Wrote and flushed K1")

	if err := database.Put(&db.WriteOptions{DisableWAL: true}, k2, encodeValueBase(2)); err != nil {
		t.Fatalf("Put K2 failed: %v", err)
	}
	t.Log("Wrote K2 (not flushed)")

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	faultFS.DropUnsyncedData()

	opts2 := db.DefaultOptions()
	opts2.FS = baseFS
	opts2.CreateIfMissing = false

	database2, err := db.Open(dbPath, opts2)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database2.Close()

	_, err = database2.Get(nil, k1)
	if err != nil {
		t.Errorf("K1 should exist (was flushed): %v", err)
	} else {
		t.Log("K1 exists as expected")
	}

	_, err = database2.Get(nil, k2)
	if err == nil {
		t.Error("K2 should NOT exist (was not flushed)")
	} else if errors.Is(err, db.ErrNotFound) {
		t.Log("K2 correctly not found (was not flushed)")
	} else {
		t.Errorf("Unexpected error for K2: %v", err)
	}
}

// TestDurableState_InvariantDBGEDurableState verifies flush durability.
func TestDurableState_InvariantDBGEDurableState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-cycle test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	baseFS := vfs.Default()

	opts := db.DefaultOptions()
	opts.FS = baseFS
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	type durableEntry struct {
		key       []byte
		valueBase uint32
	}
	var durableState []durableEntry

	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		key := []byte(k)
		vb := uint32(i + 1)
		if err := database.Put(&db.WriteOptions{Sync: true}, key, encodeValueBase(vb)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		durableState = append(durableState, durableEntry{key: key, valueBase: vb})
	}

	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	t.Logf("Flushed %d keys", len(durableState))

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	database, err = db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database.Close()

	violations := 0
	for _, entry := range durableState {
		val, err := database.Get(nil, entry.key)
		if errors.Is(err, db.ErrNotFound) {
			t.Errorf("VIOLATION: Key %q in durable_state but not in DB", entry.key)
			violations++
			continue
		}
		if err != nil {
			t.Errorf("ERROR: Get(%q) failed: %v", entry.key, err)
			violations++
			continue
		}
		actualVB := decodeValueBase(val)
		if actualVB < entry.valueBase {
			t.Errorf("VIOLATION: Key %q: DB has value_base=%d, durable_state claims %d",
				entry.key, actualVB, entry.valueBase)
			violations++
		}
	}

	if violations > 0 {
		t.Errorf("Found %d violations of DB >= durable_state invariant", violations)
	} else {
		t.Log("SUCCESS: All keys satisfy DB >= durable_state invariant")
	}
}

// TestDisableWAL_FaultFS_ValueBaseMonotonicity verifies value_base monotonicity.
func TestDisableWAL_FaultFS_ValueBaseMonotonicity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-cycle test in short mode")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db")

	baseFS := vfs.Default()
	faultFS := vfs.NewFaultInjectionFS(baseFS)

	opts := db.DefaultOptions()
	opts.FS = faultFS
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := []byte("monotonic_key")
	var maxFlushedVB uint32

	for vb := uint32(1); vb <= 3; vb++ {
		if err := database.Put(&db.WriteOptions{Sync: true}, key, encodeValueBase(vb)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := database.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		maxFlushedVB = vb
		t.Logf("Wrote and flushed value_base=%d", vb)
	}

	if err := database.Put(&db.WriteOptions{DisableWAL: true}, key, encodeValueBase(4)); err != nil {
		t.Fatalf("Put vb=4 failed: %v", err)
	}
	t.Log("Wrote value_base=4 (NOT flushed, will be lost)")

	if err := database.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	faultFS.DropUnsyncedData()

	opts2 := db.DefaultOptions()
	opts2.FS = baseFS
	opts2.CreateIfMissing = false

	database2, err := db.Open(dbPath, opts2)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer database2.Close()

	val, err := database2.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	actualVB := decodeValueBase(val)
	t.Logf("Actual DB value_base: %d", actualVB)
	t.Logf("Max flushed value_base: %d", maxFlushedVB)

	if actualVB > maxFlushedVB {
		t.Errorf("VIOLATION: DB value_base (%d) > max flushed (%d)", actualVB, maxFlushedVB)
	} else if actualVB < maxFlushedVB {
		t.Errorf("VIOLATION: DB value_base (%d) < max flushed (%d) - data loss!", actualVB, maxFlushedVB)
	} else {
		t.Logf("SUCCESS: value_base monotonicity verified (actual=%d, max_flushed=%d)",
			actualVB, maxFlushedVB)
	}
}

// Helper to remove a file if it exists
func removeIfExists(path string) {
	_ = os.Remove(path)
}
