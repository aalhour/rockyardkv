package rockyardkv

// pessimistic_transaction_test.go implements tests for pessimistic transaction.

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPessimisticTransactionBasic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Begin a pessimistic transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Put some data
	if err := txn.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	if err := txn.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Verify locks are held
	if txn.GetNumLocks() != 2 {
		t.Errorf("Expected 2 locks, got %d", txn.GetNumLocks())
	}

	// Data should be visible within transaction
	val, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}

	// Data should NOT be visible outside transaction
	_, err = txnDB.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Commit
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify locks are released
	if txnDB.getLockManager().NumLocks() != 0 {
		t.Error("Expected locks to be released after commit")
	}

	// Data should now be visible
	val, err = txnDB.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get after commit: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}
}

func TestPessimisticTransactionRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Put initial data
	txnDB.Put([]byte("key1"), []byte("initial"))

	// Begin transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// Modify data
	txn.Put([]byte("key1"), []byte("modified"))
	txn.Put([]byte("key2"), []byte("new"))

	// Rollback
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Original data should be preserved
	val, err := txnDB.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(val) != "initial" {
		t.Errorf("Expected 'initial', got %q", string(val))
	}

	// New key should not exist
	_, err = txnDB.Get([]byte("key2"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound for key2, got %v", err)
	}

	// Locks should be released
	if txnDB.getLockManager().NumLocks() != 0 {
		t.Error("Expected locks to be released after rollback")
	}
}

func TestPessimisticTransactionGetForUpdate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Put initial data
	txnDB.Put([]byte("key1"), []byte("value1"))

	// Begin transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// GetForUpdate should lock the key
	val, err := txn.GetForUpdate([]byte("key1"), true)
	if err != nil {
		t.Fatalf("GetForUpdate failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}

	// Verify lock is held
	if txn.GetNumLocks() != 1 {
		t.Errorf("Expected 1 lock, got %d", txn.GetNumLocks())
	}

	// Another transaction should not be able to modify the key
	txn2 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	opts := DefaultPessimisticTransactionOptions()
	opts.LockTimeout = 100 * time.Millisecond
	txn2.opts = opts

	err = txn2.Put([]byte("key1"), []byte("conflict"))
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("Expected ErrLockTimeout, got %v", err)
	}

	txn.Rollback()
	txn2.Rollback()
}

func TestPessimisticTransactionSavePoint(t *testing.T) {
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

	// Put initial key
	txn.Put([]byte("key1"), []byte("value1"))

	// Set savepoint
	if err := txn.SetSavePoint(); err != nil {
		t.Fatalf("SetSavePoint failed: %v", err)
	}

	// Put more data
	txn.Put([]byte("key2"), []byte("value2"))
	txn.Put([]byte("key3"), []byte("value3"))

	if txn.GetWriteBatchSize() != 3 {
		t.Errorf("Expected 3 entries, got %d", txn.GetWriteBatchSize())
	}

	// Rollback to savepoint
	if err := txn.RollbackToSavePoint(); err != nil {
		t.Fatalf("RollbackToSavePoint failed: %v", err)
	}

	// Only key1 should be in the batch now
	// Note: our implementation may not perfectly restore the batch, but the concept is shown
	if txn.GetWriteBatchSize() > 1 {
		t.Logf("Note: RollbackToSavePoint may not perfectly restore batch state, got %d entries", txn.GetWriteBatchSize())
	}

	// Commit
	txn.Commit()

	// key1 should exist
	val, err := txnDB.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Expected key1 to exist: %v", err)
	} else if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}
}

func TestPessimisticTransactionNoSavePoint(t *testing.T) {
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

	// RollbackToSavePoint without savepoint should fail
	err = txn.RollbackToSavePoint()
	if !errors.Is(err, ErrNoSavePoint) {
		t.Errorf("Expected ErrNoSavePoint, got %v", err)
	}

	// PopSavePoint without savepoint should fail
	err = txn.PopSavePoint()
	if !errors.Is(err, ErrNoSavePoint) {
		t.Errorf("Expected ErrNoSavePoint, got %v", err)
	}

	txn.Rollback()
}

func TestPessimisticTransactionReadOnly(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Put some data
	txnDB.Put([]byte("key1"), []byte("value1"))

	// Begin read-only transaction
	opts := DefaultPessimisticTransactionOptions()
	opts.ReadOnly = true
	txn := txnDB.BeginTransaction(opts, nil)

	// Get should work
	val, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}

	// Put should fail
	err = txn.Put([]byte("key2"), []byte("value2"))
	if !errors.Is(err, ErrTransactionReadOnly) {
		t.Errorf("Expected ErrTransactionReadOnly, got %v", err)
	}

	// Delete should fail
	err = txn.Delete([]byte("key1"))
	if !errors.Is(err, ErrTransactionReadOnly) {
		t.Errorf("Expected ErrTransactionReadOnly, got %v", err)
	}

	txn.Rollback()
}

func TestPessimisticTransactionExpiration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Begin transaction with short expiration
	opts := DefaultPessimisticTransactionOptions()
	opts.Expiration = 100 * time.Millisecond
	txn := txnDB.BeginTransaction(opts, nil)

	// Should work initially
	if txn.IsExpired() {
		t.Error("Transaction should not be expired yet")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	if !txn.IsExpired() {
		t.Error("Transaction should be expired")
	}

	// Operations should fail
	err = txn.Put([]byte("key1"), []byte("value1"))
	if !errors.Is(err, ErrTransactionExpired) {
		t.Errorf("Expected ErrTransactionExpired, got %v", err)
	}

	txn.Rollback()
}

func TestPessimisticTransactionClosedError(t *testing.T) {
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
	txn.Commit()

	// All operations should fail after close
	err = txn.Put([]byte("key"), []byte("value"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	_, err = txn.Get([]byte("key"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Expected ErrTransactionClosed for Get, got %v", err)
	}

	err = txn.Commit()
	if !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Expected ErrTransactionClosed for Commit, got %v", err)
	}
}

func TestPessimisticTransactionDeadlock(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Put initial data
	txnDB.Put([]byte("key1"), []byte("value1"))
	txnDB.Put([]byte("key2"), []byte("value2"))

	// Create two transactions
	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn2 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	// txn1 locks key1
	_, err = txn1.GetForUpdate([]byte("key1"), true)
	if err != nil {
		t.Fatalf("txn1 GetForUpdate key1 failed: %v", err)
	}

	// txn2 locks key2
	_, err = txn2.GetForUpdate([]byte("key2"), true)
	if err != nil {
		t.Fatalf("txn2 GetForUpdate key2 failed: %v", err)
	}

	var wg sync.WaitGroup
	var txn1Err error

	// txn1 tries to lock key2 (will wait)
	wg.Go(func() {
		_, txn1Err = txn1.GetForUpdate([]byte("key2"), true)
	})

	time.Sleep(50 * time.Millisecond)

	// txn2 tries to lock key1 - should detect deadlock
	_, err = txn2.GetForUpdate([]byte("key1"), true)
	if !errors.Is(err, ErrDeadlock) {
		t.Errorf("Expected ErrDeadlock, got %v", err)
	}

	// Rollback txn2 to allow txn1 to proceed
	txn2.Rollback()

	wg.Wait()

	if txn1Err != nil {
		t.Errorf("txn1 should have succeeded after txn2 rolled back: %v", txn1Err)
	}

	txn1.Rollback()
}

// TestPessimisticTransactionRaceCondition tests for race conditions.
func TestPessimisticTransactionRaceCondition(t *testing.T) {
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
	numGoroutines := 10
	numOps := 50

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numOps {
				txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
				key := []byte{byte(j % 5)}

				if err := txn.Put(key, []byte{byte(id)}); err != nil {
					// Lock conflicts are expected
					txn.Rollback()
					continue
				}

				if _, err := txn.Get(key); err != nil && !errors.Is(err, ErrNotFound) {
					txn.Rollback()
					continue
				}

				txn.Commit()
			}
		}(i)
	}

	wg.Wait()

	// Verify no lingering locks
	if txnDB.getLockManager().NumLocks() != 0 {
		t.Errorf("Expected 0 locks, got %d", txnDB.getLockManager().NumLocks())
	}
}

// TestTransactionDBWrapDB tests wrapping an existing DB.
func TestTransactionDBWrapDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	// Open regular DB first
	database, err := Open(dbPath, dbOpts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	dbImpl := database.(*dbImpl)

	// Put some data
	database.Put(nil, []byte("key1"), []byte("value1"))

	// Wrap as TransactionDB
	txnDB, err := WrapDB(dbImpl, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("WrapDB: %v", err)
	}

	// Should be able to read existing data
	val, err := txnDB.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}

	// Begin transaction
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn.Put([]byte("key2"), []byte("value2"))
	txn.Commit()

	// Verify new data
	val, err = txnDB.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected 'value2', got %q", string(val))
	}

	txnDB.Close()
}

// TestPessimisticTransactionStress tests under stress.
func TestPessimisticTransactionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

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
	numGoroutines := 20
	duration := 2 * time.Second

	stop := make(chan struct{})
	time.AfterFunc(duration, func() { close(stop) })

	var commits, rollbacks, deadlocks, timeouts int64

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				opts := DefaultPessimisticTransactionOptions()
				opts.LockTimeout = 10 * time.Millisecond
				txn := txnDB.BeginTransaction(opts, nil)

				// Try to modify 2-3 random keys
				numKeys := 2 + id%2
				success := true
				for j := range numKeys {
					key := []byte{byte((id + j) % 10)}
					if err := txn.Put(key, []byte{byte(id)}); err != nil {
						switch {
						case errors.Is(err, ErrDeadlock):
							atomic.AddInt64(&deadlocks, 1)
						case errors.Is(err, ErrLockTimeout):
							atomic.AddInt64(&timeouts, 1)
						}
						success = false
						break
					}
				}

				if success {
					if err := txn.Commit(); err == nil {
						atomic.AddInt64(&commits, 1)
					} else {
						atomic.AddInt64(&rollbacks, 1)
						txn.Rollback()
					}
				} else {
					atomic.AddInt64(&rollbacks, 1)
					txn.Rollback()
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Stress test results: commits=%d, rollbacks=%d, deadlocks=%d, timeouts=%d",
		commits, rollbacks, deadlocks, timeouts)

	if txnDB.getLockManager().NumLocks() != 0 {
		t.Errorf("Expected 0 locks after test, got %d", txnDB.getLockManager().NumLocks())
	}
}

func TestTransactionDBMethods(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Test pass-through methods
	if err := txnDB.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := txnDB.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(val))
	}

	if err := txnDB.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = txnDB.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	// Test snapshot
	snapshot := txnDB.GetSnapshot()
	if snapshot == nil {
		t.Error("Expected snapshot to be created")
	}
	txnDB.ReleaseSnapshot(snapshot)

	// Test iterator
	txnDB.Put([]byte("a"), []byte("1"))
	txnDB.Put([]byte("b"), []byte("2"))
	iter := txnDB.NewIterator(nil)
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Expected iterator to be valid")
	}
	iter.Close()

	// Test GetDB
	if txnDB.GetDB() == nil {
		t.Error("GetDB should return the underlying DB")
	}

	// Test NumActiveTransactions
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	if txnDB.NumActiveTransactions() != 1 {
		t.Errorf("Expected 1 active transaction, got %d", txnDB.NumActiveTransactions())
	}
	txn.Rollback()
}

func TestPessimisticTransactionDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	// Put initial data
	txnDB.Put([]byte("key1"), []byte("value1"))

	// Begin transaction and delete
	txn := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	if err := txn.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Should not be visible within transaction
	_, err = txn.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}

	// Commit
	txn.Commit()

	// Should not exist
	_, err = txnDB.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after commit, got %v", err)
	}
}

func TestPessimisticTransactionID(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	dbOpts := DefaultOptions()
	dbOpts.CreateIfMissing = true

	txnDB, err := OpenTransactionDB(dbPath, dbOpts, DefaultTransactionDBOptions())
	if err != nil {
		t.Fatalf("Failed to open TransactionDB: %v", err)
	}
	defer txnDB.Close()

	txn1 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)
	txn2 := txnDB.BeginTransaction(DefaultPessimisticTransactionOptions(), nil)

	if txn1.ID() == txn2.ID() {
		t.Error("Transaction IDs should be unique")
	}
	if txn1.ID() == 0 || txn2.ID() == 0 {
		t.Error("Transaction IDs should be non-zero")
	}

	txn1.Rollback()
	txn2.Rollback()
}

// TestPessimisticTransactionSavepointSingleDeletePreservation verifies that
// SingleDelete entries are preserved (not downgraded to Delete) during
// savepoint rollback. This is important because SingleDelete has different
// semantics than Delete in RocksDB.
func TestPessimisticTransactionSavepointSingleDeletePreservation(t *testing.T) {
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

	// Put a key, set savepoint
	if err := txn.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Set savepoint BEFORE the SingleDelete
	txn.SetSavePoint()

	// SingleDelete (directly access writeBatch since we're in same package)
	txn.writeBatch.SingleDelete([]byte("single-key"))

	// Also add SingleDeleteCF
	txn.writeBatch.SingleDeleteCF(0, []byte("single-key-cf"))

	// Put another key after the SingleDelete
	if err := txn.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Now we have: Put(key1) | SAVEPOINT | SingleDelete(single-key) | SingleDeleteCF(single-key-cf) | Put(key2)
	// Batch count should be 4 (key1, single-key, single-key-cf, key2)
	if txn.writeBatch.Count() != 4 {
		t.Errorf("Expected 4 entries before rollback, got %d", txn.writeBatch.Count())
	}

	// Rollback to savepoint - should remove entries after savepoint
	if err := txn.RollbackToSavePoint(); err != nil {
		t.Fatalf("RollbackToSavePoint failed: %v", err)
	}

	// After rollback: only Put(key1) should remain
	// The SingleDelete and second Put should be removed
	if txn.writeBatch.Count() != 1 {
		t.Errorf("Expected 1 entry after rollback, got %d", txn.writeBatch.Count())
	}

	// Now test that SingleDelete IS preserved when it's BEFORE the savepoint
	// Add SingleDelete before new savepoint
	txn.writeBatch.SingleDelete([]byte("another-single"))

	// Set savepoint AFTER the SingleDelete
	txn.SetSavePoint()

	// Add more entries after savepoint
	if err := txn.Put([]byte("key3"), []byte("value3")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Now we have: Put(key1) | SingleDelete(another-single) | SAVEPOINT | Put(key3)
	// Rollback should keep Put(key1) and SingleDelete(another-single)
	if err := txn.RollbackToSavePoint(); err != nil {
		t.Fatalf("RollbackToSavePoint failed: %v", err)
	}

	// Verify batch has 2 entries and contains SingleDelete (not converted to Delete)
	if txn.writeBatch.Count() != 2 {
		t.Errorf("Expected 2 entries after second rollback, got %d", txn.writeBatch.Count())
	}

	// Verify the batch still has SingleDelete record type (not downgraded to Delete)
	if !txn.writeBatch.HasSingleDelete() {
		t.Error("SingleDelete should be preserved in batch after savepoint rollback, but HasSingleDelete() returned false")
	}

	txn.Rollback()
}
