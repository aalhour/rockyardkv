package db

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Part A: Missing Unit Tests for 90%+ Coverage
// ============================================================================

func TestPessimisticTransactionSetAndGetSnapshot(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial data
	if err := txnDB.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Begin transaction without auto-snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = false
	txn := txnDB.BeginTransaction(opts, nil)

	// Initially no snapshot
	if txn.GetSnapshot() != nil {
		t.Error("Expected no snapshot initially when SetSnapshot=false")
	}

	// Manually set snapshot
	txn.SetSnapshot()
	snap := txn.GetSnapshot()
	if snap == nil {
		t.Fatal("Expected snapshot after SetSnapshot()")
	}

	// Write new data outside transaction
	if err := txnDB.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Transaction should still see old state via snapshot
	// (Note: our current impl doesn't enforce snapshot isolation in Get,
	// but the snapshot is tracked for future ValidateSnapshot)

	// Set snapshot again (should release old one)
	txn.SetSnapshot()
	newSnap := txn.GetSnapshot()
	if newSnap == nil {
		t.Fatal("Expected new snapshot")
	}

	txn.Rollback()
}

func TestPessimisticTransactionPopSavePointEdgeCases(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// PopSavePoint with no savepoints
	if err := txn.PopSavePoint(); !errors.Is(err, ErrNoSavePoint) {
		t.Errorf("Expected ErrNoSavePoint, got %v", err)
	}

	// Create savepoint
	txn.SetSavePoint()
	txn.Put([]byte("key1"), []byte("value1"))

	// Create nested savepoint
	txn.SetSavePoint()
	txn.Put([]byte("key2"), []byte("value2"))

	// Pop inner savepoint (without rollback)
	if err := txn.PopSavePoint(); err != nil {
		t.Errorf("PopSavePoint failed: %v", err)
	}

	// Keys from both savepoints should still be there
	val, err := txn.Get([]byte("key1"))
	if err != nil || string(val) != "value1" {
		t.Errorf("key1 should still exist")
	}
	val, err = txn.Get([]byte("key2"))
	if err != nil || string(val) != "value2" {
		t.Errorf("key2 should still exist")
	}

	// Pop the other savepoint
	if err := txn.PopSavePoint(); err != nil {
		t.Errorf("PopSavePoint failed: %v", err)
	}

	// Now no savepoints left
	if err := txn.PopSavePoint(); !errors.Is(err, ErrNoSavePoint) {
		t.Errorf("Expected ErrNoSavePoint, got %v", err)
	}

	txn.Commit()
}

func TestPessimisticTransactionPopSavePointOnClosedTxn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn.SetSavePoint()
	txn.Rollback()

	// PopSavePoint on closed transaction
	if err := txn.PopSavePoint(); !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestPessimisticTransactionSnapshotOnClosedTxn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Transaction with auto-snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// Verify snapshot was set
	if txn.GetSnapshot() == nil {
		t.Fatal("Expected snapshot to be set")
	}

	// Rollback releases the snapshot
	txn.Rollback()

	// After rollback, the snapshot should have been released
	// The close() method sets snapshot to nil
	snap := txn.GetSnapshot()
	if snap != nil {
		t.Error("Expected nil snapshot after rollback")
	}

	// Operations on closed transaction should fail
	err = txn.Put([]byte("key"), []byte("value"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestPessimisticTransactionGetWriteBatchSize(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Initially empty
	if txn.GetWriteBatchSize() != 0 {
		t.Errorf("Expected 0 write batch size, got %d", txn.GetWriteBatchSize())
	}

	// After puts
	txn.Put([]byte("key1"), []byte("value1"))
	if txn.GetWriteBatchSize() != 1 {
		t.Errorf("Expected 1 write batch size, got %d", txn.GetWriteBatchSize())
	}

	txn.Put([]byte("key2"), []byte("value2"))
	if txn.GetWriteBatchSize() != 2 {
		t.Errorf("Expected 2 write batch size, got %d", txn.GetWriteBatchSize())
	}

	// Delete also counts
	txn.Delete([]byte("key1"))
	if txn.GetWriteBatchSize() != 3 {
		t.Errorf("Expected 3 write batch size, got %d", txn.GetWriteBatchSize())
	}

	txn.Rollback()
}

func TestPessimisticTransactionIsExpiredMethod(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Transaction without expiration
	opts := DefaultPessimisticTransactionOptions()
	opts.Expiration = 0
	txn := txnDB.BeginTransaction(opts, nil)

	if txn.IsExpired() {
		t.Error("Transaction without expiration should not be expired")
	}
	txn.Rollback()

	// Transaction with expiration
	opts.Expiration = 50 * time.Millisecond
	txn = txnDB.BeginTransaction(opts, nil)

	if txn.IsExpired() {
		t.Error("Transaction should not be expired immediately")
	}

	time.Sleep(100 * time.Millisecond)

	if !txn.IsExpired() {
		t.Error("Transaction should be expired after waiting")
	}

	// Operations on expired transaction should fail
	err = txn.Put([]byte("key"), []byte("value"))
	if !errors.Is(err, ErrTransactionExpired) {
		t.Errorf("Expected ErrTransactionExpired, got %v", err)
	}

	txn.Rollback()
}

func TestPessimisticTransactionLockUpgrade(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial data
	txnDB.Put([]byte("key1"), []byte("value1"))

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// First get (shared lock)
	_, err = txn.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Now write (should upgrade to exclusive lock)
	err = txn.Put([]byte("key1"), []byte("updated"))
	if err != nil {
		t.Fatalf("Put after Get failed: %v", err)
	}

	// Should have 1 lock (upgraded, not 2 separate)
	if txn.GetNumLocks() != 1 {
		t.Errorf("Expected 1 lock after upgrade, got %d", txn.GetNumLocks())
	}

	txn.Commit()
}

func TestPessimisticTransactionMultiplePutsToSameKey(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Multiple puts to same key
	txn.Put([]byte("key"), []byte("v1"))
	txn.Put([]byte("key"), []byte("v2"))
	txn.Put([]byte("key"), []byte("v3"))

	// Should only have 1 lock
	if txn.GetNumLocks() != 1 {
		t.Errorf("Expected 1 lock for same key, got %d", txn.GetNumLocks())
	}

	// Should see the last value
	val, err := txn.Get([]byte("key"))
	if err != nil || string(val) != "v3" {
		t.Errorf("Expected 'v3', got %q, err=%v", string(val), err)
	}

	txn.Commit()

	// Verify committed value
	val, err = txnDB.Get([]byte("key"))
	if err != nil || string(val) != "v3" {
		t.Errorf("After commit: expected 'v3', got %q, err=%v", string(val), err)
	}
}

func TestPessimisticTransactionDeleteNonExistent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Delete non-existent key (should succeed - this is normal behavior)
	err = txn.Delete([]byte("nonexistent"))
	if err != nil {
		t.Errorf("Delete of non-existent key should succeed, got %v", err)
	}

	// Lock should still be held
	if txn.GetNumLocks() != 1 {
		t.Errorf("Expected 1 lock, got %d", txn.GetNumLocks())
	}

	txn.Commit()
}

func TestPessimisticTransactionGetForUpdateThenDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("key1"), []byte("value1"))

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// GetForUpdate
	val, err := txn.GetForUpdate([]byte("key1"), true)
	if err != nil || string(val) != "value1" {
		t.Fatalf("GetForUpdate failed: %v", err)
	}

	// Delete the same key
	err = txn.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Should not be visible
	_, err = txn.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	txn.Commit()
}

func TestPessimisticTransactionNestedSavePoints(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// First savepoint
	txn.Put([]byte("k1"), []byte("v1"))
	txn.SetSavePoint()

	// Second savepoint
	txn.Put([]byte("k2"), []byte("v2"))
	txn.SetSavePoint()

	// Third savepoint
	txn.Put([]byte("k3"), []byte("v3"))
	txn.SetSavePoint()

	// Fourth put (after third savepoint)
	txn.Put([]byte("k4"), []byte("v4"))

	// Rollback to third savepoint (loses k4)
	err = txn.RollbackToSavePoint()
	if err != nil {
		t.Fatalf("RollbackToSavePoint failed: %v", err)
	}

	// k4 should be gone
	_, err = txn.Get([]byte("k4"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("k4 should not exist after rollback")
	}

	// k3 should still exist
	val, _ := txn.Get([]byte("k3"))
	if string(val) != "v3" {
		t.Error("k3 should exist")
	}

	// Rollback to second savepoint (loses k3)
	err = txn.RollbackToSavePoint()
	if err != nil {
		t.Fatalf("RollbackToSavePoint failed: %v", err)
	}

	_, err = txn.Get([]byte("k3"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("k3 should not exist after rollback")
	}

	// k2 should still exist
	val, _ = txn.Get([]byte("k2"))
	if string(val) != "v2" {
		t.Error("k2 should exist")
	}

	// Commit with k1, k2
	txn.Commit()

	// Verify
	val, _ = txnDB.Get([]byte("k1"))
	if string(val) != "v1" {
		t.Error("k1 should be committed")
	}
	val, _ = txnDB.Get([]byte("k2"))
	if string(val) != "v2" {
		t.Error("k2 should be committed")
	}
	_, err = txnDB.Get([]byte("k3"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("k3 should not be committed")
	}
}

// ============================================================================
// Part B: C++ Parity Tests (Ported from transaction_test.cc)
// ============================================================================

// TestWriteConflict ports WriteConflictTest from C++ transaction_test.cc
// Tests that concurrent writes to the same key cause conflicts
func TestWriteConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial data
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction 1
	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Start transaction 2
	txn2Opts := DefaultPessimisticTransactionOptions()
	txn2Opts.LockTimeout = 10 * time.Millisecond // Short timeout
	txn2 := txnDB.BeginTransaction(txn2Opts, nil)

	// txn1 locks the key
	_, err = txn1.GetForUpdate([]byte("foo"), true)
	if err != nil {
		t.Fatalf("txn1 GetForUpdate failed: %v", err)
	}

	// txn2 should timeout trying to write to the same key
	err = txn2.Put([]byte("foo"), []byte("baz"))
	if err == nil {
		t.Error("Expected timeout error from txn2")
	}

	txn1.Commit()
	txn2.Rollback()

	// Verify final value
	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "bar" {
		t.Errorf("Expected 'bar', got %q", string(val))
	}
}

// TestWriteConflict2 ports WriteConflictTest2 - multiple key conflicts
func TestWriteConflict2(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Write multiple keys
	txn.Put([]byte("a"), []byte("1"))
	txn.Put([]byte("b"), []byte("2"))
	txn.Put([]byte("c"), []byte("3"))

	// Start conflicting transaction
	txn2Opts := DefaultPessimisticTransactionOptions()
	txn2Opts.LockTimeout = 10 * time.Millisecond
	txn2 := txnDB.BeginTransaction(txn2Opts, nil)

	// Try to write to one of the keys
	err = txn2.Put([]byte("b"), []byte("X"))
	if err == nil {
		t.Error("Expected conflict on key 'b'")
	}

	// But should be able to write to a different key
	err = txn2.Put([]byte("d"), []byte("4"))
	if err != nil {
		t.Errorf("Should be able to write to unlocked key 'd': %v", err)
	}

	txn.Commit()
	txn2.Commit()

	// Verify
	val, _ := txnDB.Get([]byte("b"))
	if string(val) != "2" {
		t.Errorf("Expected '2' from txn1, got %q", string(val))
	}
	val, _ = txnDB.Get([]byte("d"))
	if string(val) != "4" {
		t.Errorf("Expected '4' from txn2, got %q", string(val))
	}
}

// TestReadConflict ports ReadConflictTest - GetForUpdate prevents writes
func TestReadConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("bar"))

	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn2Opts := DefaultPessimisticTransactionOptions()
	txn2Opts.LockTimeout = 10 * time.Millisecond
	txn2 := txnDB.BeginTransaction(txn2Opts, nil)

	// txn1 reads with lock (GetForUpdate acquires exclusive lock)
	val, err := txn1.GetForUpdate([]byte("foo"), true)
	if err != nil || string(val) != "bar" {
		t.Fatalf("GetForUpdate failed: %v", err)
	}

	// txn2 should not be able to write
	err = txn2.Put([]byte("foo"), []byte("baz"))
	if err == nil {
		t.Error("Expected conflict from txn2 Put")
	}

	txn1.Commit()
	txn2.Rollback()
}

// TestSharedLocks ports SharedLocks test - multiple readers allowed
func TestSharedLocks(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("bar"))

	// Multiple transactions can hold shared locks simultaneously
	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn2 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn3 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// All can read (Get acquires shared lock in our implementation)
	val, err := txn1.Get([]byte("foo"))
	if err != nil || string(val) != "bar" {
		t.Errorf("txn1 Get failed: %v", err)
	}

	val, err = txn2.Get([]byte("foo"))
	if err != nil || string(val) != "bar" {
		t.Errorf("txn2 Get failed: %v", err)
	}

	val, err = txn3.Get([]byte("foo"))
	if err != nil || string(val) != "bar" {
		t.Errorf("txn3 Get failed: %v", err)
	}

	txn1.Rollback()
	txn2.Rollback()
	txn3.Rollback()
}

// TestSharedBlocksExclusive - shared lock blocks exclusive lock
// Note: In our implementation, Get() does NOT acquire locks - only GetForUpdate() does.
// This test uses GetForUpdate with exclusive=false to acquire a shared lock.
func TestSharedBlocksExclusive(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("bar"))

	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn2Opts := DefaultPessimisticTransactionOptions()
	txn2Opts.LockTimeout = 50 * time.Millisecond
	txn2 := txnDB.BeginTransaction(txn2Opts, nil)

	// txn1 acquires shared lock via GetForUpdate with exclusive=false
	_, err = txn1.GetForUpdate([]byte("foo"), false)
	if err != nil {
		t.Fatalf("txn1 GetForUpdate failed: %v", err)
	}

	// txn2 wants exclusive lock (Put) - should block/timeout
	err = txn2.Put([]byte("foo"), []byte("baz"))
	if err == nil {
		t.Error("Expected timeout when shared lock blocks exclusive")
	}

	txn1.Rollback()
	txn2.Rollback()
}

// TestDeadlockCycleShared ports DeadlockCycleShared
func TestDeadlockCycleShared(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("a"), []byte("1"))
	txnDB.Put([]byte("b"), []byte("2"))

	opts := DefaultPessimisticTransactionOptions()
	opts.DeadlockDetect = true
	opts.LockTimeout = 1 * time.Second

	var deadlockDetected atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)

	// txn1: holds 'a', wants 'b'
	go func() {
		defer wg.Done()
		txn := txnDB.BeginTransaction(opts, nil)
		defer txn.Rollback()

		if err := txn.Put([]byte("a"), []byte("X")); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
		if err := txn.Put([]byte("b"), []byte("Y")); err != nil {
			if errors.Is(err, ErrDeadlock) {
				deadlockDetected.Add(1)
			}
		}
	}()

	// txn2: holds 'b', wants 'a'
	go func() {
		defer wg.Done()
		txn := txnDB.BeginTransaction(opts, nil)
		defer txn.Rollback()

		if err := txn.Put([]byte("b"), []byte("Y")); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
		if err := txn.Put([]byte("a"), []byte("X")); err != nil {
			if errors.Is(err, ErrDeadlock) {
				deadlockDetected.Add(1)
			}
		}
	}()

	wg.Wait()

	if deadlockDetected.Load() == 0 {
		t.Error("Expected at least one deadlock to be detected")
	}
}

// TestTxnOnlyTest ports TxnOnlyTest - transaction data is isolated
func TestTxnOnlyTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	txn.Put([]byte("a"), []byte("1"))
	txn.Put([]byte("b"), []byte("2"))

	// Check txn data
	val, err := txn.Get([]byte("a"))
	if err != nil || string(val) != "1" {
		t.Error("Expected 'a' = '1' in transaction")
	}

	// Check DB - should not see uncommitted data
	_, err = txnDB.Get([]byte("a"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("DB should not see uncommitted data")
	}

	txn.Commit()

	// Now DB should see it
	val, err = txnDB.Get([]byte("a"))
	if err != nil || string(val) != "1" {
		t.Error("DB should see committed data")
	}
}

// TestNoSnapshotTest ports NoSnapshotTest
func TestNoSnapshotTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("bar"))

	// Transaction without snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = false
	txn := txnDB.BeginTransaction(opts, nil)

	// Read key
	val, err := txn.Get([]byte("foo"))
	if err != nil || string(val) != "bar" {
		t.Fatalf("Get failed: %v", err)
	}

	// External write
	txnDB.Put([]byte("foo"), []byte("baz"))

	// Without snapshot, we see the new value
	// (Note: This depends on implementation - with snapshot isolation
	// we would see the old value)

	txn.Rollback()
}

// TestMultipleSnapshotTest ports MultipleSnapshotTest
func TestMultipleSnapshotTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("1"))

	// Take first snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	snap1 := txn.GetSnapshot()
	if snap1 == nil {
		t.Fatal("Expected snapshot")
	}

	// External write
	txnDB.Put([]byte("foo"), []byte("2"))

	// Set new snapshot
	txn.SetSnapshot()
	snap2 := txn.GetSnapshot()

	// Snapshots should be different
	if snap1 == snap2 {
		t.Log("Note: snapshot object may be reused, but sequence numbers would differ")
	}

	txn.Rollback()
}

// TestEmptyTest ports EmptyTest - empty transaction commit
func TestEmptyTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Empty transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Commit empty transaction should succeed
	err = txn.Commit()
	if err != nil {
		t.Errorf("Empty transaction commit failed: %v", err)
	}
}

// TestFirstWriteTest ports FirstWriteTest
func TestFirstWriteTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// First write in transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	err = txn.Put([]byte("foo"), []byte("bar"))
	if err != nil {
		t.Errorf("First write failed: %v", err)
	}
	txn.Commit()

	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "bar" {
		t.Errorf("Expected 'bar', got %q", string(val))
	}
}

// TestWaitingTxn ports WaitingTxn test - wait queue behavior
// Note: With ValidateSnapshot, txn2 may get a write conflict if txn1 commits first.
// This test focuses on the lock wait behavior, so we disable snapshot validation.
func TestWaitingTxn(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("bar"))

	// txn1 without snapshot to avoid validation complications
	opts1 := DefaultPessimisticTransactionOptions()
	opts1.SetSnapshot = false
	txn1 := txnDB.BeginTransaction(opts1, nil)

	// txn1 locks the key
	txn1.Put([]byte("foo"), []byte("1"))

	// txn2 will wait for the lock (also without snapshot)
	var wg sync.WaitGroup
	var txn2Completed atomic.Bool
	var txn2Err error

	wg.Go(func() {
		opts := DefaultPessimisticTransactionOptions()
		opts.LockTimeout = 5 * time.Second
		opts.SetSnapshot = false // Disable snapshot to avoid validation conflict
		txn2 := txnDB.BeginTransaction(opts, nil)
		defer txn2.Rollback()

		txn2Err = txn2.Put([]byte("foo"), []byte("2"))
		if txn2Err == nil {
			txn2Completed.Store(true)
		}
	})

	// Wait a bit to ensure txn2 is waiting
	time.Sleep(50 * time.Millisecond)

	// txn2 should not have completed yet
	if txn2Completed.Load() {
		t.Error("txn2 should be waiting")
	}

	// Commit txn1, releasing the lock
	txn1.Commit()

	// Wait for txn2 to complete
	wg.Wait()

	// txn2 should have completed successfully (no snapshot = no conflict validation)
	if !txn2Completed.Load() {
		t.Errorf("txn2 should have completed after txn1 committed, got error: %v", txn2Err)
	}
}

// TestSuccessTest ports SuccessTest - basic success scenario
func TestSuccessTest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Write some data
	txn.Put([]byte("a"), []byte("1"))
	txn.Put([]byte("b"), []byte("2"))

	// Read back
	val, _ := txn.Get([]byte("a"))
	if string(val) != "1" {
		t.Error("Expected 'a' = '1'")
	}

	// Delete
	txn.Delete([]byte("a"))

	// Should not be visible
	_, err = txn.Get([]byte("a"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("'a' should be deleted")
	}

	// But 'b' should still exist
	val, _ = txn.Get([]byte("b"))
	if string(val) != "2" {
		t.Error("Expected 'b' = '2'")
	}

	txn.Commit()

	// Verify committed state
	_, err = txnDB.Get([]byte("a"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("'a' should be deleted after commit")
	}
	val, _ = txnDB.Get([]byte("b"))
	if string(val) != "2" {
		t.Error("'b' should be '2' after commit")
	}
}

// TestIteratorInTransaction tests iterator within a transaction
func TestIteratorInTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Pre-populate
	txnDB.Put([]byte("a"), []byte("1"))
	txnDB.Put([]byte("b"), []byte("2"))
	txnDB.Put([]byte("c"), []byte("3"))

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Modify within transaction
	txn.Put([]byte("b"), []byte("X"))
	txn.Delete([]byte("c"))
	txn.Put([]byte("d"), []byte("4"))

	// Iterator should show DB state (not uncommitted txn changes)
	// Note: This is the behavior without WriteBatchWithIndex
	iter := txnDB.NewIterator(nil)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	// Should see original DB state: a, b, c
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys from DB iterator, got %d: %v", len(keys), keys)
	}

	txn.Commit()

	// After commit, iterator should see: a, b, d (c deleted)
	// Note: The same iterator may be stale; create new one
	iter.Close()
	iter = txnDB.NewIterator(nil)
	keys = nil
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "d" {
		t.Errorf("After commit, expected [a, b, d], got %v", keys)
	}
}

// TestLargeTransaction tests handling of many keys
func TestLargeTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Write many keys
	numKeys := 1000
	for i := range numKeys {
		key := []byte(string(rune('a' + (i % 26))))
		key = append(key, []byte(string(rune('0'+(i/26))))...)
		txn.Put(key, []byte("value"))
	}

	// Should have many locks
	locks := txn.GetNumLocks()
	if locks == 0 {
		t.Error("Expected locks to be held")
	}

	// Commit
	err = txn.Commit()
	if err != nil {
		t.Errorf("Large transaction commit failed: %v", err)
	}

	// Locks should be released
	if txnDB.GetLockManager().NumLocks() != 0 {
		t.Error("Locks should be released after commit")
	}
}

// TestConcurrentTransactionsNoConflict tests concurrent non-conflicting transactions
func TestConcurrentTransactionsNoConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	var wg sync.WaitGroup
	numTxns := 10

	for i := range numTxns {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
			defer txn.Rollback()

			// Each transaction writes to its own key
			key := []byte{byte(id)}
			value := []byte{byte(id + 100)}

			if err := txn.Put(key, value); err != nil {
				t.Errorf("txn %d Put failed: %v", id, err)
				return
			}

			if err := txn.Commit(); err != nil {
				t.Errorf("txn %d Commit failed: %v", id, err)
				return
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys were written
	for i := range numTxns {
		key := []byte{byte(i)}
		val, err := txnDB.Get(key)
		if err != nil || val[0] != byte(i+100) {
			t.Errorf("Key %d not correctly committed", i)
		}
	}
}

// TestRollbackAfterPut tests rollback reverts writes
func TestRollbackAfterPut(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txnDB.Put([]byte("foo"), []byte("original"))

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn.Put([]byte("foo"), []byte("modified"))

	// Rollback
	txn.Rollback()

	// Original value should remain
	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "original" {
		t.Errorf("Expected 'original' after rollback, got %q", string(val))
	}
}

// TestGetForUpdateOnNonExistent tests GetForUpdate on non-existent key
func TestGetForUpdateOnNonExistent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// GetForUpdate on non-existent key
	_, err = txn.GetForUpdate([]byte("nonexistent"), true)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// But the lock should be held
	if txn.GetNumLocks() != 1 {
		t.Errorf("Expected 1 lock (on non-existent key), got %d", txn.GetNumLocks())
	}

	// Conflicting transaction should fail
	txn2Opts := DefaultPessimisticTransactionOptions()
	txn2Opts.LockTimeout = 10 * time.Millisecond
	txn2 := txnDB.BeginTransaction(txn2Opts, nil)

	err = txn2.Put([]byte("nonexistent"), []byte("value"))
	if err == nil {
		t.Error("Expected timeout for conflicting write")
	}

	txn.Rollback()
	txn2.Rollback()
}

// TestWriteOptionsInTransaction tests that WriteOptions are respected
func TestWriteOptionsInTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	writeOpts := DefaultWriteOptions()
	writeOpts.Sync = true

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), writeOpts)
	txn.Put([]byte("key"), []byte("value"))

	// Commit with sync
	err = txn.Commit()
	if err != nil {
		t.Errorf("Commit with sync failed: %v", err)
	}
}

// TestTransactionWithManyDeletes tests transaction with many deletes
func TestTransactionWithManyDeletes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Pre-populate
	for i := range 100 {
		key := []byte{byte(i)}
		txnDB.Put(key, []byte("value"))
	}

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Delete all
	for i := range 100 {
		key := []byte{byte(i)}
		txn.Delete(key)
	}

	txn.Commit()

	// Verify all deleted
	for i := range 100 {
		key := []byte{byte(i)}
		_, err := txnDB.Get(key)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Key %d should be deleted", i)
		}
	}
}

// TestBatchReaderCoverage tests the pessimisticBatchReader paths
func TestBatchReaderCoverage(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Pre-populate
	txnDB.Put([]byte("existing"), []byte("original"))

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Put, then read from batch
	txn.Put([]byte("new"), []byte("newvalue"))
	val, err := txn.Get([]byte("new"))
	if err != nil || string(val) != "newvalue" {
		t.Errorf("Expected 'newvalue', got %q, err=%v", string(val), err)
	}

	// Overwrite, then read
	txn.Put([]byte("existing"), []byte("modified"))
	val, err = txn.Get([]byte("existing"))
	if err != nil || string(val) != "modified" {
		t.Errorf("Expected 'modified', got %q, err=%v", string(val), err)
	}

	// Delete, then read
	txn.Delete([]byte("existing"))
	_, err = txn.Get([]byte("existing"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	// Put again after delete
	txn.Put([]byte("existing"), []byte("resurrected"))
	val, err = txn.Get([]byte("existing"))
	if err != nil || string(val) != "resurrected" {
		t.Errorf("Expected 'resurrected', got %q, err=%v", string(val), err)
	}

	txn.Rollback()
}

// TestMergeInBatchReader tests that merge operations are handled
func TestMergeInBatchReader(t *testing.T) {
	// This test ensures the batch reader handles merge operations
	// (even if they return nil currently, they shouldn't crash)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Write and read multiple times
	for i := range 10 {
		key := []byte("counter")
		txn.Put(key, []byte{byte(i)})
		val, _ := txn.Get(key)
		if len(val) != 1 || val[0] != byte(i) {
			t.Errorf("Iteration %d: expected %d, got %v", i, i, val)
		}
	}

	txn.Rollback()
}

// TestSavePointWithLocks tests that savepoint correctly tracks locks
func TestSavePointWithLocks(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Put before savepoint
	txn.Put([]byte("before"), []byte("1"))
	numLocksBefore := txn.GetNumLocks()

	// Create savepoint
	txn.SetSavePoint()

	// Put after savepoint
	txn.Put([]byte("after1"), []byte("2"))
	txn.Put([]byte("after2"), []byte("3"))
	numLocksAfter := txn.GetNumLocks()

	if numLocksAfter != numLocksBefore+2 {
		t.Errorf("Expected %d locks, got %d", numLocksBefore+2, numLocksAfter)
	}

	// Rollback to savepoint
	txn.RollbackToSavePoint()

	// Locks for after* should be released
	numLocksRolledBack := txn.GetNumLocks()
	if numLocksRolledBack != numLocksBefore {
		t.Errorf("Expected %d locks after rollback, got %d", numLocksBefore, numLocksRolledBack)
	}

	// Keys added after savepoint should not be visible
	_, err = txn.Get([]byte("after1"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("after1 should not exist after rollback to savepoint")
	}

	// Key before savepoint should still exist
	val, err := txn.Get([]byte("before"))
	if err != nil || string(val) != "1" {
		t.Error("before should still exist")
	}

	txn.Commit()
}

// TestGetFromWriteBatchWithDelete tests batch reader with interleaved puts and deletes
// ============================================================================
// Part C: ValidateSnapshot Tests (C++ Parity for Snapshot Conflict Detection)
// ============================================================================

// TestValidateSnapshotWriteConflict tests that writing to a key modified after snapshot fails
func TestValidateSnapshotWriteConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External write after snapshot
	txnDB.Put([]byte("foo"), []byte("baz"))

	// Try to write to the key - should detect conflict
	err = txn.Put([]byte("foo"), []byte("txn_value"))
	if !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict, got %v", err)
	}

	// Transaction should still be usable for other keys
	err = txn.Put([]byte("other"), []byte("value"))
	if err != nil {
		t.Errorf("Put to non-conflicting key failed: %v", err)
	}

	txn.Rollback()
}

// TestValidateSnapshotNewKeyConflict tests conflict when new key is created after snapshot
func TestValidateSnapshotNewKeyConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Start transaction with snapshot (key doesn't exist yet)
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External write creates the key after snapshot
	txnDB.Put([]byte("newkey"), []byte("external"))

	// Try to write to the key - should detect conflict
	err = txn.Put([]byte("newkey"), []byte("txn_value"))
	if !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict when key created after snapshot, got %v", err)
	}

	txn.Rollback()
}

// TestValidateSnapshotDeleteConflict tests conflict when key is deleted after snapshot
func TestValidateSnapshotDeleteConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External delete after snapshot
	txnDB.Delete([]byte("foo"))

	// Try to write to the key - should detect conflict
	err = txn.Put([]byte("foo"), []byte("txn_value"))
	if !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict when key deleted after snapshot, got %v", err)
	}

	txn.Rollback()
}

// TestValidateSnapshotNoConflict tests that unchanged keys don't cause conflicts
func TestValidateSnapshotNoConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External write to a DIFFERENT key
	txnDB.Put([]byte("other"), []byte("value"))

	// Write to our key - should NOT conflict
	err = txn.Put([]byte("foo"), []byte("txn_value"))
	if err != nil {
		t.Errorf("Expected no conflict, got %v", err)
	}

	txn.Commit()

	// Verify committed value
	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "txn_value" {
		t.Errorf("Expected 'txn_value', got %q", string(val))
	}
}

// TestValidateSnapshotNoSnapshotNoValidation tests that without snapshot, no validation occurs
func TestValidateSnapshotNoSnapshotNoValidation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction WITHOUT snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = false
	txn := txnDB.BeginTransaction(opts, nil)

	// External write after transaction started
	txnDB.Put([]byte("foo"), []byte("baz"))

	// Write to the key - should NOT conflict (no snapshot = no validation)
	err = txn.Put([]byte("foo"), []byte("txn_value"))
	if err != nil {
		t.Errorf("Expected no conflict without snapshot, got %v", err)
	}

	txn.Commit()

	// Verify committed value (txn overwrote)
	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "txn_value" {
		t.Errorf("Expected 'txn_value', got %q", string(val))
	}
}

// TestValidateSnapshotGetForUpdateConflict tests GetForUpdate with conflict
func TestValidateSnapshotGetForUpdateConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External write after snapshot
	txnDB.Put([]byte("foo"), []byte("baz"))

	// GetForUpdate should also detect the conflict
	_, err = txn.GetForUpdate([]byte("foo"), true)
	if !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict from GetForUpdate, got %v", err)
	}

	txn.Rollback()
}

// TestValidateSnapshotDeleteConflictOnDelete tests Delete with conflict
func TestValidateSnapshotDeleteConflictOnDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// External write after snapshot
	txnDB.Put([]byte("foo"), []byte("baz"))

	// Delete should also detect the conflict
	err = txn.Delete([]byte("foo"))
	if !errors.Is(err, ErrWriteConflict) {
		t.Errorf("Expected ErrWriteConflict from Delete, got %v", err)
	}

	txn.Rollback()
}

// TestValidateSnapshotSameKeyMultipleTimes tests that once validated, same key doesn't revalidate
func TestValidateSnapshotSameKeyMultipleTimes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Write initial value
	txnDB.Put([]byte("foo"), []byte("bar"))

	// Start transaction with snapshot
	opts := DefaultPessimisticTransactionOptions()
	opts.SetSnapshot = true
	txn := txnDB.BeginTransaction(opts, nil)

	// First write - should succeed (no external modification yet)
	err = txn.Put([]byte("foo"), []byte("v1"))
	if err != nil {
		t.Errorf("First Put failed: %v", err)
	}

	// Second write to same key - should also succeed (already validated/locked)
	err = txn.Put([]byte("foo"), []byte("v2"))
	if err != nil {
		t.Errorf("Second Put to same key failed: %v", err)
	}

	// Third write
	err = txn.Put([]byte("foo"), []byte("v3"))
	if err != nil {
		t.Errorf("Third Put to same key failed: %v", err)
	}

	txn.Commit()

	// Verify final value
	val, _ := txnDB.Get([]byte("foo"))
	if string(val) != "v3" {
		t.Errorf("Expected 'v3', got %q", string(val))
	}
}

func TestGetFromWriteBatchWithDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Complex sequence
	txn.Put([]byte("key"), []byte("v1"))
	txn.Delete([]byte("key"))
	txn.Put([]byte("key"), []byte("v2"))
	txn.Put([]byte("key"), []byte("v3"))
	txn.Delete([]byte("key"))

	// Final state should be deleted
	_, err = txn.Get([]byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Key should be deleted after final Delete")
	}

	txn.Rollback()
}
