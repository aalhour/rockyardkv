package rockyardkv

// transaction_test.go implements tests for transaction.

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestTransactionBasic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Begin a transaction
	txn := database.BeginTransaction(DefaultTransactionOptions(), nil)

	// Put some data
	if err := txn.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put in transaction: %v", err)
	}
	if err := txn.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put in transaction: %v", err)
	}

	// Data should be visible within transaction
	val, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get from transaction: %v", err)
	}
	if string(val) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(val))
	}

	// Data should NOT be visible outside transaction (before commit)
	_, err = database.Get(nil, []byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound before commit, got %v", err)
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now data should be visible
	val, err = database.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get after commit: %v", err)
	}
	if string(val) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(val))
	}
}

func TestTransactionRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Put initial data
	database.Put(nil, []byte("key1"), []byte("initial"))

	// Begin a transaction
	txn := database.BeginTransaction(DefaultTransactionOptions(), nil)

	// Modify the data
	txn.Put([]byte("key1"), []byte("modified"))
	txn.Put([]byte("key2"), []byte("new"))

	// Rollback
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Original data should be preserved
	val, err := database.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get after rollback: %v", err)
	}
	if string(val) != "initial" {
		t.Fatalf("Expected 'initial', got '%s'", string(val))
	}

	// New key should not exist
	_, err = database.Get(nil, []byte("key2"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound after rollback, got %v", err)
	}
}

func TestTransactionConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Put initial data
	database.Put(nil, []byte("key1"), []byte("initial"))

	// Begin transaction 1 and read the key
	txn1 := database.BeginTransaction(DefaultTransactionOptions(), nil)
	_, err = txn1.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("txn1 get failed: %v", err)
	}

	// Modify the key outside the transaction (concurrent write)
	database.Put(nil, []byte("key1"), []byte("concurrent"))

	// Try to commit txn1 - should fail due to conflict
	err = txn1.Commit()
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Expected ErrTransactionConflict, got %v", err)
	}
}

func TestTransactionWriteConflict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Begin two transactions
	txn1 := database.BeginTransaction(DefaultTransactionOptions(), nil)
	txn2 := database.BeginTransaction(DefaultTransactionOptions(), nil)

	// Both write to the same key
	txn1.Put([]byte("key1"), []byte("from_txn1"))
	txn2.Put([]byte("key1"), []byte("from_txn2"))

	// First commit should succeed
	if err := txn1.Commit(); err != nil {
		t.Fatalf("txn1 commit failed: %v", err)
	}

	// Second commit should fail due to write conflict
	err = txn2.Commit()
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("Expected ErrTransactionConflict for txn2, got %v", err)
	}
}

func TestTransactionDelete(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Put initial data
	database.Put(nil, []byte("key1"), []byte("value1"))

	// Begin transaction and delete
	txn := database.BeginTransaction(DefaultTransactionOptions(), nil)
	if err := txn.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Get within transaction should fail
	_, err = txn.Get([]byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound after delete in txn, got %v", err)
	}

	// Commit
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Key should be deleted
	_, err = database.Get(nil, []byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Expected ErrNotFound after commit, got %v", err)
	}
}

func TestTransactionSnapshot(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Put initial data
	database.Put(nil, []byte("key1"), []byte("initial"))

	// Begin transaction with snapshot
	txn := database.BeginTransaction(DefaultTransactionOptions(), nil)

	// Verify snapshot was set
	snap := txn.GetSnapshot()
	if snap == nil {
		t.Fatal("Expected snapshot to be set")
	}

	// Read the key
	val, err := txn.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(val) != "initial" {
		t.Fatalf("Expected 'initial', got '%s'", string(val))
	}

	txn.Rollback()
}

func TestTransactionNoSnapshot(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Begin transaction without snapshot
	txnOpts := TransactionOptions{SetSnapshot: false}
	txn := database.BeginTransaction(txnOpts, nil)

	snap := txn.GetSnapshot()
	if snap != nil {
		t.Fatal("Expected no snapshot")
	}

	// Put data
	txn.Put([]byte("key1"), []byte("value1"))

	// Commit should succeed
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

func TestTransactionClosed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Begin and commit
	txn := database.BeginTransaction(DefaultTransactionOptions(), nil)
	txn.Put([]byte("key1"), []byte("value1"))
	txn.Commit()

	// Operations on closed transaction should fail
	err = txn.Put([]byte("key2"), []byte("value2"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("Expected ErrTransactionClosed, got %v", err)
	}

	_, err = txn.Get([]byte("key1"))
	if !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("Expected ErrTransactionClosed, got %v", err)
	}

	err = txn.Commit()
	if !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransactionConcurrent(t *testing.T) {
	dir, _ := os.MkdirTemp("", "txntest-*")
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Run multiple concurrent transactions
	var wg sync.WaitGroup
	numWorkers := 10
	numOps := 100

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range numOps {
				txn := database.BeginTransaction(DefaultTransactionOptions(), nil)
				key := []byte{byte(workerID), byte(i)}
				val := []byte{byte(workerID), byte(i), 'v'}

				txn.Put(key, val)
				err := txn.Commit()
				if err != nil {
					// Conflicts are expected
					txn.Rollback()
				}
			}
		}(w)
	}

	wg.Wait()

	// Verify some data was written
	count := 0
	for w := range numWorkers {
		for i := range numOps {
			key := []byte{byte(w), byte(i)}
			_, err := database.Get(nil, key)
			if err == nil {
				count++
			}
		}
	}

	if count == 0 {
		t.Fatal("Expected some data to be written")
	}
	t.Logf("Written %d out of %d keys", count, numWorkers*numOps)
}
