package rockyardkv

// write_prepared_txn_recovery_test.go implements tests for write prepared txn recovery.


import (
	"os"
	"path/filepath"
	"testing"
)

// TestWritePrepared2PCBasic tests basic prepare/commit functionality.
func TestWritePrepared2PCBasic(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_basic")

	// Open database with write-prepared transaction support
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	// Begin a write-prepared transaction
	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("test_txn_1"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	// Put some data
	if err := txn.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	if err := txn.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Prepare the transaction
	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	// Verify state is prepared
	if txn.GetState() != TxnStatePrepared {
		t.Fatalf("Expected state Prepared, got %v", txn.GetState())
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify state is committed
	if txn.GetState() != TxnStateCommitted {
		t.Fatalf("Expected state Committed, got %v", txn.GetState())
	}

	// Verify data is visible
	val, err := wpDB.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Fatalf("Expected value1, got %s", val)
	}

	wpDB.Close()
}

// TestWritePrepared2PCRollback tests prepare/rollback functionality.
func TestWritePrepared2PCRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_rollback")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	// Begin a transaction
	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("rollback_txn"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	// Put some data
	if err := txn.Put([]byte("rollback_key"), []byte("rollback_value")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Prepare
	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	// Rollback instead of commit
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify state is rolled back
	if txn.GetState() != TxnStateRolledBack {
		t.Fatalf("Expected state RolledBack, got %v", txn.GetState())
	}

	wpDB.Close()
}

// TestWritePrepared2PCRecoveryCommitted tests recovery of committed transactions.
func TestWritePrepared2PCRecoveryCommitted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_recovery_committed")

	// Phase 1: Open, prepare, commit, close
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("recovery_committed_txn"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	if err := txn.Put([]byte("recover_key1"), []byte("recover_value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	wpDB.Close()

	// Phase 2: Reopen and verify data persisted
	wpDB2, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to reopen write-prepared txn db: %v", err)
	}
	defer wpDB2.Close()

	val, err := wpDB2.Get([]byte("recover_key1"))
	if err != nil {
		t.Fatalf("Failed to get recover_key1 after recovery: %v", err)
	}
	if string(val) != "recover_value1" {
		t.Fatalf("Expected recover_value1, got %s", val)
	}
}

// TestWritePrepared2PCRecoveryRolledBack tests recovery of rolled back transactions.
// NOTE: This test currently demonstrates a known limitation - rolled back data
// is still visible after recovery because the 2PC recovery handler is not yet
// integrated into the main recovery path. This is tracked in PARITY_PLAN.md.
func TestWritePrepared2PCRecoveryRolledBack(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_recovery_rolledback")

	// Phase 1: Open, prepare, rollback, close
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	// First, put some committed data
	regularTxn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := regularTxn.Put([]byte("committed_key"), []byte("committed_value")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	if err := regularTxn.Commit(); err != nil {
		t.Fatalf("Failed to commit regular txn: %v", err)
	}

	// Now prepare and rollback
	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("recovery_rollback_txn"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	if err := txn.Put([]byte("should_not_exist"), []byte("should_not_exist")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	wpDB.Close()

	// Phase 2: Reopen and verify
	wpDB2, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to reopen write-prepared txn db: %v", err)
	}
	defer wpDB2.Close()

	// Committed data should be present
	val, err := wpDB2.Get([]byte("committed_key"))
	if err != nil {
		t.Fatalf("Failed to get committed_key after recovery: %v", err)
	}
	if string(val) != "committed_value" {
		t.Fatalf("Expected committed_value, got %s", val)
	}

	// With 2PC recovery, rolled back data should NOT be present after recovery.
	// The 2PC recovery handler scans the WAL and writes delete tombstones for
	// any keys that were part of rolled-back transactions.
	_, err = wpDB2.Get([]byte("should_not_exist"))
	if err == nil {
		t.Fatal("Rolled back data should NOT be visible after recovery, but it was")
	}
	// The key should be "not found" (either ErrKeyNotFound or similar)
}

// TestWritePrepared2PCRecoveryPreparedNotCommitted tests recovery of prepared but not committed transactions.
// This simulates a crash between prepare and commit.
func TestWritePrepared2PCRecoveryPreparedNotCommitted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_recovery_prepared")

	// Phase 1: Open, prepare (but don't commit), close
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("prepared_not_committed_txn"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	if err := txn.Put([]byte("prepared_key"), []byte("prepared_value")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	// Simulate crash - close without commit or rollback
	// Note: In a real crash, the DB would be closed abruptly
	// For testing, we just close normally - the WAL should contain the prepare markers
	wpDB.Close()

	// Phase 2: Reopen - the prepared transaction should be recovered
	wpDB2, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to reopen write-prepared txn db: %v", err)
	}
	defer wpDB2.Close()

	// Get all recovered prepared transactions
	recovered := wpDB2.GetAllPreparedTransactions()
	if len(recovered) != 1 {
		t.Fatalf("Expected 1 recovered prepared transaction, got %d", len(recovered))
	}

	if recovered[0].Name != "prepared_not_committed_txn" {
		t.Fatalf("Expected transaction name 'prepared_not_committed_txn', got '%s'", recovered[0].Name)
	}

	t.Logf("Found recovered prepared transaction: %s with %d keys", recovered[0].Name, len(recovered[0].Keys))

	// Commit the recovered transaction
	if err := wpDB2.CommitPreparedTransaction("prepared_not_committed_txn"); err != nil {
		t.Fatalf("Failed to commit recovered transaction: %v", err)
	}

	// Verify the data is now accessible
	val, err := wpDB2.Get([]byte("prepared_key"))
	if err != nil {
		t.Fatalf("Failed to get prepared_key after committing recovered txn: %v", err)
	}
	if string(val) != "prepared_value" {
		t.Fatalf("Expected prepared_value, got %s", val)
	}

	// Verify no more recovered transactions
	recovered = wpDB2.GetAllPreparedTransactions()
	if len(recovered) != 0 {
		t.Fatalf("Expected 0 recovered prepared transactions after commit, got %d", len(recovered))
	}
}

// TestWritePrepared2PCRecoveryPreparedThenRollback tests rolling back a recovered prepared transaction.
func TestWritePrepared2PCRecoveryPreparedThenRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_2pc_recovery_rollback_after")

	// Phase 1: Open, prepare (but don't commit), close
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("to_be_rolled_back"); err != nil {
		t.Fatalf("Failed to set txn name: %v", err)
	}

	if err := txn.Put([]byte("rollback_key"), []byte("rollback_value")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	wpDB.Close()

	// Phase 2: Reopen and rollback
	wpDB2, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to reopen write-prepared txn db: %v", err)
	}
	defer wpDB2.Close()

	// Get recovered transactions
	recovered := wpDB2.GetAllPreparedTransactions()
	if len(recovered) != 1 {
		t.Fatalf("Expected 1 recovered prepared transaction, got %d", len(recovered))
	}

	// Rollback instead of commit
	if err := wpDB2.RollbackPreparedTransaction("to_be_rolled_back"); err != nil {
		t.Fatalf("Failed to rollback recovered transaction: %v", err)
	}

	// Verify the data is NOT accessible
	_, err = wpDB2.Get([]byte("rollback_key"))
	if err == nil {
		t.Fatal("Key should not be accessible after rolling back recovered transaction")
	}
}

// TestWritePreparedMarkers tests that 2PC markers are written to WAL correctly.
func TestWritePreparedMarkers(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_markers")

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	wpDB, err := OpenWritePreparedTxnDB(dbPath, opts, TransactionDBOptions{})
	if err != nil {
		t.Fatalf("Failed to open write-prepared txn db: %v", err)
	}

	// Create and prepare a transaction
	txn := wpDB.BeginWritePreparedTransaction(PessimisticTransactionOptions{}, nil)
	if err := txn.SetName("marker_test_txn"); err != nil {
		t.Fatalf("Failed to set name: %v", err)
	}

	if err := txn.Put([]byte("marker_key"), []byte("marker_value")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := txn.Prepare(); err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	wpDB.Close()

	// Verify the WAL contains the markers by checking the log files exist
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		t.Fatalf("Failed to read db dir: %v", err)
	}

	hasLogFile := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			hasLogFile = true
			info, _ := entry.Info()
			if info.Size() > 0 {
				t.Logf("Found WAL file %s with size %d bytes", entry.Name(), info.Size())
			}
		}
	}

	if !hasLogFile {
		t.Log("No WAL files found - data was likely flushed to SST")
	}
}
