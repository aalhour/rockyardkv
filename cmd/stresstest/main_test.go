package main

import (
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// Contract: In crash verification, a key missing due to WAL-replayed DELETE is not treated as data loss
// when the base (no WAL replay) state still contains the key (expected-state persistence can lag).
func TestVerifyAll_AllowsDBAheadDeleteWhenBaseHasKey(t *testing.T) {
	dir := t.TempDir()
	dbRoot := filepath.Join(dir, "run")
	dbDir := filepath.Join(dbRoot, "db")

	// Keep the verification loop small and deterministic for the test.
	prevNumKeys := *numKeys
	prevAllowDBAhead := *allowDBAhead
	prevAllowDataLoss := *allowDataLoss
	prevVerbose := *verbose
	t.Cleanup(func() {
		*numKeys = prevNumKeys
		*allowDBAhead = prevAllowDBAhead
		*allowDataLoss = prevAllowDataLoss
		*verbose = prevVerbose
	})

	*numKeys = 64
	*allowDBAhead = true
	*allowDataLoss = false
	*verbose = true

	keyNum := int64(5)
	key := makeKey(keyNum)
	val := makeValue(keyNum, 1)

	// Step 1: Put+Flush so the base (SST/MANIFEST) state contains the key.
	{
		opts := rockyardkv.DefaultOptions()
		opts.CreateIfMissing = true

		db, err := rockyardkv.Open(dbDir, opts)
		if err != nil {
			t.Fatalf("open (init): %v", err)
		}

		if err := db.Put(&rockyardkv.WriteOptions{Sync: true}, key, val); err != nil {
			_ = db.Close()
			t.Fatalf("put: %v", err)
		}
		if err := db.Flush(nil); err != nil {
			_ = db.Close()
			t.Fatalf("flush: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close (init): %v", err)
		}
	}

	// Step 2: Expected state is saved BEFORE an acknowledged delete.
	// This simulates a crash window where expected-state persistence is behind.
	expected := testutil.NewExpectedStateV2(int64(*numKeys), 1, 2)
	mu := expected.GetMutexForKey(0, keyNum)
	mu.Lock()
	pev := expected.PreparePut(0, keyNum)
	pev.Commit()
	mu.Unlock()

	// Step 3: Delete the key, but do NOT update expected state.
	{
		opts := rockyardkv.DefaultOptions()
		opts.CreateIfMissing = false

		db, err := rockyardkv.Open(dbDir, opts)
		if err != nil {
			t.Fatalf("open (delete): %v", err)
		}
		if err := db.Delete(&rockyardkv.WriteOptions{Sync: true}, key); err != nil {
			_ = db.Close()
			t.Fatalf("delete: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close (delete): %v", err)
		}
	}

	// Step 4: Verify against a write-open DB (replays WAL) but use a base read-only view
	// (no WAL replay) to confirm this is "DB ahead delete", not data loss.
	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = false

	writeDB, err := rockyardkv.Open(dbDir, opts)
	if err != nil {
		t.Fatalf("open (verify write): %v", err)
	}
	t.Cleanup(func() { _ = writeDB.Close() })

	baseDB, err := rockyardkv.OpenForReadOnly(dbDir, opts, false)
	if err != nil {
		t.Fatalf("open (base readonly): %v", err)
	}
	t.Cleanup(func() { _ = baseDB.Close() })

	if err := verifyAll(writeDB, expected, &Stats{}, baseDB); err != nil {
		t.Fatalf("verifyAll: %v", err)
	}
}
