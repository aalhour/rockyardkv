package rockyardkv

// transaction_contract_test.go implements tests for transaction contract.


import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// =============================================================================
// Transaction API Contract Tests
//
// These tests verify that the Transaction interface maintains its semantic
// contract for ACID properties. They document expected behavior and prevent
// regressions.
//
// Reference: RocksDB v10.7.5 include/rocksdb/utilities/transaction.h
// =============================================================================

// TestTransaction_Contract_PutGetWithinTransaction verifies that Put and Get
// work correctly within a transaction.
//
// Contract: Get returns the value set by Put within the same transaction.
func TestTransaction_Contract_PutGetWithinTransaction(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)
	defer txn.Rollback()

	// Contract: Put should succeed
	if err := txn.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Contract: Get should return the value set by Put
	value, err := txn.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("Get returned wrong value: got %q, want %q", value, "value")
	}
}

// TestTransaction_Contract_IsolationBeforeCommit verifies that uncommitted
// changes are isolated from other readers.
//
// Contract: Uncommitted transaction changes are not visible to other readers.
func TestTransaction_Contract_IsolationBeforeCommit(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Start a transaction and write a value
	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)

	if err := txn.Put([]byte("key"), []byte("txn_value")); err != nil {
		txn.Rollback()
		t.Fatalf("Put failed: %v", err)
	}

	// Contract: Value should NOT be visible via direct DB read
	_, err := db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		txn.Rollback()
		t.Errorf("Uncommitted value should not be visible: got err=%v", err)
	}

	txn.Rollback()
}

// TestTransaction_Contract_CommitMakesChangesVisible verifies that committed
// changes become visible to other readers.
//
// Contract: After Commit, transaction changes are visible to all readers.
func TestTransaction_Contract_CommitMakesChangesVisible(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)

	if err := txn.Put([]byte("key"), []byte("committed_value")); err != nil {
		txn.Rollback()
		t.Fatalf("Put failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Contract: Value should be visible after commit
	value, err := db.Get(nil, []byte("key"))
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}
	if string(value) != "committed_value" {
		t.Errorf("Wrong value after commit: got %q, want %q", value, "committed_value")
	}
}

// TestTransaction_Contract_RollbackDiscardsChanges verifies that rollback
// discards all transaction changes.
//
// Contract: After Rollback, transaction changes are discarded.
func TestTransaction_Contract_RollbackDiscardsChanges(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("original")); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Start transaction and modify
	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)

	if err := txn.Put([]byte("key"), []byte("modified")); err != nil {
		txn.Rollback()
		t.Fatalf("Put failed: %v", err)
	}

	// Rollback
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Contract: Original value should remain
	value, err := db.Get(nil, []byte("key"))
	if err != nil {
		t.Fatalf("Get after rollback failed: %v", err)
	}
	if string(value) != "original" {
		t.Errorf("Wrong value after rollback: got %q, want %q", value, "original")
	}
}

// TestTransaction_Contract_DeleteWithinTransaction verifies that Delete
// works correctly within a transaction.
//
// Contract: Delete within transaction makes key invisible via Get.
func TestTransaction_Contract_DeleteWithinTransaction(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)
	defer txn.Rollback()

	// Contract: Delete should succeed
	if err := txn.Delete([]byte("key")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Contract: Get should return ErrNotFound within transaction
	_, err := txn.Get([]byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after delete should return ErrNotFound, got: %v", err)
	}
}

// TestTransaction_Contract_SnapshotIsolation verifies that transactions
// with snapshots see a consistent view.
//
// Contract: Transaction snapshot isolates from concurrent writes.
func TestTransaction_Contract_SnapshotIsolation(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("v1")); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Start transaction with snapshot
	opts := DefaultTransactionOptions()
	opts.SetSnapshot = true
	txn := db.BeginTransaction(opts, nil)
	defer txn.Rollback()

	// Modify value outside transaction
	if err := db.Put(nil, []byte("key"), []byte("v2")); err != nil {
		t.Fatalf("External put failed: %v", err)
	}

	// Contract: Transaction should see original value (v1)
	value, err := txn.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get in transaction failed: %v", err)
	}
	if string(value) != "v1" {
		t.Errorf("Transaction should see snapshot value: got %q, want %q", value, "v1")
	}
}

// TestTransaction_Contract_ConflictDetection verifies that conflicting
// transactions are detected at commit time.
//
// Contract: Commit fails with ErrTransactionConflict if key was modified.
func TestTransaction_Contract_ConflictDetection(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("v1")); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Start transaction and read the key (tracks for conflict detection)
	opts := DefaultTransactionOptions()
	opts.SetSnapshot = true
	txn := db.BeginTransaction(opts, nil)

	// Read the key (this tracks it for conflict detection)
	_, err := txn.Get([]byte("key"))
	if err != nil {
		txn.Rollback()
		t.Fatalf("Initial get failed: %v", err)
	}

	// Modify via another write (simulates concurrent transaction)
	if err := db.Put(nil, []byte("key"), []byte("v2")); err != nil {
		txn.Rollback()
		t.Fatalf("Concurrent put failed: %v", err)
	}

	// Try to modify and commit
	if err := txn.Put([]byte("key"), []byte("v3")); err != nil {
		txn.Rollback()
		t.Fatalf("Put failed: %v", err)
	}

	// Contract: Commit should fail due to conflict
	err = txn.Commit()
	if !errors.Is(err, ErrTransactionConflict) {
		t.Errorf("Expected ErrTransactionConflict, got: %v", err)
	}
}

// TestTransaction_Contract_ClosedTransactionReturnsError verifies that
// operations on closed transactions return errors.
//
// Contract: Operations on closed transaction return ErrTransactionClosed.
func TestTransaction_Contract_ClosedTransactionReturnsError(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Contract: Put should fail
	if err := txn.Put([]byte("key"), []byte("value")); !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Put on closed txn should return ErrTransactionClosed, got: %v", err)
	}

	// Contract: Get should fail
	_, err := txn.Get([]byte("key"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Get on closed txn should return ErrTransactionClosed, got: %v", err)
	}

	// Contract: Delete should fail
	if err := txn.Delete([]byte("key")); !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Delete on closed txn should return ErrTransactionClosed, got: %v", err)
	}

	// Contract: Commit should fail
	if err := txn.Commit(); !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Commit on closed txn should return ErrTransactionClosed, got: %v", err)
	}

	// Contract: Rollback should fail
	if err := txn.Rollback(); !errors.Is(err, ErrTransactionClosed) {
		t.Errorf("Rollback on closed txn should return ErrTransactionClosed, got: %v", err)
	}
}

// TestTransaction_Contract_MultipleOperationsAtomic verifies that multiple
// operations in a transaction are applied atomically.
//
// Contract: All or nothing - either all ops apply or none do.
func TestTransaction_Contract_MultipleOperationsAtomic(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)

	// Add multiple operations
	for i := range 10 {
		key := []byte{byte('a' + i)}
		value := []byte{byte('0' + i)}
		if err := txn.Put(key, value); err != nil {
			txn.Rollback()
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Contract: All values should be visible
	for i := range 10 {
		key := []byte{byte('a' + i)}
		expectedValue := []byte{byte('0' + i)}

		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get for key %q failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Wrong value for key %q: got %q, want %q", key, value, expectedValue)
		}
	}
}

// TestTransaction_Contract_GetForUpdateTracksKey verifies that GetForUpdate
// tracks the key for conflict detection.
//
// Contract: GetForUpdate tracks key so concurrent modifications cause conflict.
func TestTransaction_Contract_GetForUpdateTracksKey(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("v1")); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Start transaction and read with GetForUpdate
	opts := DefaultTransactionOptions()
	opts.SetSnapshot = true
	txn := db.BeginTransaction(opts, nil)

	// GetForUpdate tracks the key
	_, err := txn.GetForUpdate([]byte("key"), true)
	if err != nil {
		txn.Rollback()
		t.Fatalf("GetForUpdate failed: %v", err)
	}

	// Modify externally
	if err := db.Put(nil, []byte("key"), []byte("v2")); err != nil {
		txn.Rollback()
		t.Fatalf("External put failed: %v", err)
	}

	// Contract: Commit should fail due to tracked key conflict
	err = txn.Commit()
	if !errors.Is(err, ErrTransactionConflict) {
		t.Errorf("Expected ErrTransactionConflict after GetForUpdate, got: %v", err)
	}
}

// TestTransaction_Contract_SetSnapshotUpdatesView verifies that SetSnapshot
// updates the transaction's view.
//
// Contract: SetSnapshot updates the read view to current database state.
func TestTransaction_Contract_SetSnapshotUpdatesView(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Insert v1
	if err := db.Put(nil, []byte("key"), []byte("v1")); err != nil {
		t.Fatalf("Put v1 failed: %v", err)
	}

	// Start transaction with snapshot
	opts := DefaultTransactionOptions()
	opts.SetSnapshot = true
	txn := db.BeginTransaction(opts, nil)
	defer txn.Rollback()

	// Should see v1
	value, err := txn.Get([]byte("key"))
	if err != nil || string(value) != "v1" {
		t.Fatalf("Expected v1, got %q (err=%v)", value, err)
	}

	// Insert v2
	if err := db.Put(nil, []byte("key"), []byte("v2")); err != nil {
		t.Fatalf("Put v2 failed: %v", err)
	}

	// Update snapshot
	txn.SetSnapshot()

	// Contract: Should now see v2
	value, err = txn.Get([]byte("key"))
	if err != nil || string(value) != "v2" {
		t.Errorf("After SetSnapshot, expected v2, got %q (err=%v)", value, err)
	}
}

// TestTransaction_Contract_ColumnFamilyIsolation verifies that transactions
// work correctly with column families.
//
// Contract: PutCF/GetCF/DeleteCF work within transactions.
func TestTransaction_Contract_ColumnFamilyIsolation(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	// Create a column family
	cf, err := db.CreateColumnFamily(ColumnFamilyOptions{}, "test_cf")
	if err != nil {
		t.Fatalf("CreateColumnFamily failed: %v", err)
	}

	txn := db.BeginTransaction(DefaultTransactionOptions(), nil)
	defer txn.Rollback()

	// Contract: PutCF should succeed
	if err := txn.PutCF(cf, []byte("cf_key"), []byte("cf_value")); err != nil {
		t.Fatalf("PutCF failed: %v", err)
	}

	// Contract: GetCF should return the value
	value, err := txn.GetCF(cf, []byte("cf_key"))
	if err != nil {
		t.Fatalf("GetCF failed: %v", err)
	}
	if string(value) != "cf_value" {
		t.Errorf("Wrong CF value: got %q, want %q", value, "cf_value")
	}

	// Contract: Key should not exist in default CF
	_, err = txn.Get([]byte("cf_key"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Key should not exist in default CF, got err=%v", err)
	}
}

// TestTransaction_Contract_ConcurrentTransactions verifies that multiple
// transactions can run concurrently without interference (unless they conflict).
//
// Contract: Non-conflicting transactions commit successfully.
func TestTransaction_Contract_ConcurrentTransactions(t *testing.T) {
	db, cleanup := createTransactionTestDB(t)
	defer cleanup()

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Start 10 transactions writing to different keys
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			txn := db.BeginTransaction(DefaultTransactionOptions(), nil)

			key := []byte{byte('a' + n)}
			value := []byte{byte('0' + n)}

			if err := txn.Put(key, value); err != nil {
				txn.Rollback()
				errors <- err
				return
			}

			if err := txn.Commit(); err != nil {
				errors <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Contract: No errors for non-conflicting transactions
	for err := range errors {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify all values
	for i := range 10 {
		key := []byte{byte('a' + i)}
		expectedValue := []byte{byte('0' + i)}

		value, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get for key %q failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Wrong value for key %q: got %q, want %q", key, value, expectedValue)
		}
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

func createTransactionTestDB(t *testing.T) (DB, func()) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close DB: %v", err)
		}
	}

	return db, cleanup
}
