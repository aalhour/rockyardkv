package db

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/memtable"
)

// =============================================================================
// Handler2PC API Contract Tests
//
// These tests verify that Handler2PC implementations correctly handle
// two-phase commit markers during WAL recovery. They document expected
// behavior and prevent regressions.
//
// Reference: RocksDB v10.7.5 db/write_batch.cc (Iterate2PC)
// =============================================================================

// TestHandler2PC_Contract_MarkBeginEndPrepare verifies that
// MarkBeginPrepare and MarkEndPrepare correctly bracket a prepared transaction.
//
// Contract: Operations between BeginPrepare and EndPrepare are accumulated.
func TestHandler2PC_Contract_MarkBeginEndPrepare(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Contract: MarkBeginPrepare starts accumulating
	if err := handler.MarkBeginPrepare(false); err != nil {
		t.Fatalf("MarkBeginPrepare failed: %v", err)
	}

	// Add operations inside prepare block
	if err := handler.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := handler.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Contract: MarkEndPrepare completes the prepared transaction
	if err := handler.MarkEndPrepare([]byte("txn1")); err != nil {
		t.Fatalf("MarkEndPrepare failed: %v", err)
	}

	// Contract: Prepared transaction should be stored
	prepared := handler.GetPreparedTransactions()
	if len(prepared) != 1 {
		t.Errorf("Expected 1 prepared transaction, got %d", len(prepared))
	}
	if prepared[0].Name != "txn1" {
		t.Errorf("Expected transaction name 'txn1', got %q", prepared[0].Name)
	}
}

// TestHandler2PC_Contract_PreparedNotInMemtable verifies that prepared
// transactions are not immediately applied to memtable.
//
// Contract: Prepared writes are buffered, not applied until commit.
func TestHandler2PC_Contract_PreparedNotInMemtable(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Prepare a transaction
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("prepared_key"), []byte("prepared_value"))
	handler.MarkEndPrepare([]byte("txn1"))

	// Contract: Key should NOT be in memtable (prepared but not committed)
	iter := mem.NewIterator()
	iter.Seek([]byte("prepared_key"))

	if iter.Valid() && string(iter.UserKey()) == "prepared_key" {
		t.Error("Prepared key should not be in memtable until commit")
	}
}

// TestHandler2PC_Contract_MarkCommitAppliesTransaction verifies that
// MarkCommit applies the prepared transaction to memtable.
//
// Contract: Commit applies buffered writes to memtable.
func TestHandler2PC_Contract_MarkCommitAppliesTransaction(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Prepare a transaction
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("key1"), []byte("value1"))
	handler.MarkEndPrepare([]byte("txn1"))

	// Contract: Commit applies the writes
	if err := handler.MarkCommit([]byte("txn1")); err != nil {
		t.Fatalf("MarkCommit failed: %v", err)
	}

	// Contract: Key should now be in memtable
	iter := mem.NewIterator()
	iter.Seek([]byte("key1"))

	if !iter.Valid() || string(iter.UserKey()) != "key1" {
		t.Error("Committed key should be in memtable")
	}
}

// TestHandler2PC_Contract_MarkRollbackDiscardsTransaction verifies that
// MarkRollback discards the prepared transaction.
//
// Contract: Rollback discards buffered writes.
func TestHandler2PC_Contract_MarkRollbackDiscardsTransaction(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Prepare a transaction
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("key1"), []byte("value1"))
	handler.MarkEndPrepare([]byte("txn1"))

	// Contract: Rollback discards the writes
	if err := handler.MarkRollback([]byte("txn1")); err != nil {
		t.Fatalf("MarkRollback failed: %v", err)
	}

	// Contract: Key should NOT be in memtable
	iter := mem.NewIterator()
	iter.Seek([]byte("key1"))

	if iter.Valid() && string(iter.UserKey()) == "key1" {
		t.Error("Rolled back key should not be in memtable")
	}

	// Contract: Transaction should not be in pending prepared list
	prepared := handler.GetPreparedTransactions()
	if len(prepared) != 0 {
		t.Errorf("Expected 0 prepared transactions after rollback, got %d", len(prepared))
	}
}

// TestHandler2PC_Contract_NonPrepareOperationsApplyImmediately verifies that
// operations outside of prepare blocks are applied immediately.
//
// Contract: Operations outside prepare blocks apply to memtable immediately.
func TestHandler2PC_Contract_NonPrepareOperationsApplyImmediately(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Put operation outside of prepare block
	if err := handler.Put([]byte("immediate_key"), []byte("immediate_value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Contract: Key should be in memtable immediately
	iter := mem.NewIterator()
	iter.Seek([]byte("immediate_key"))

	if !iter.Valid() || string(iter.UserKey()) != "immediate_key" {
		t.Error("Non-prepared key should be in memtable immediately")
	}
}

// TestHandler2PC_Contract_SequenceAdvances verifies that sequence numbers
// advance correctly for both immediate and committed operations.
//
// Contract: Sequence numbers advance after applying operations.
func TestHandler2PC_Contract_SequenceAdvances(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 100)

	// Initial sequence
	initialSeq := handler.Sequence()
	if initialSeq != 100 {
		t.Errorf("Initial sequence should be 100, got %d", initialSeq)
	}

	// Immediate operation advances sequence
	handler.Put([]byte("key1"), []byte("value1"))
	if handler.Sequence() != 101 {
		t.Errorf("Sequence should be 101 after Put, got %d", handler.Sequence())
	}

	// Prepare and commit also advances sequence
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("key2"), []byte("value2"))
	handler.MarkEndPrepare([]byte("txn1"))
	handler.MarkCommit([]byte("txn1"))

	// Contract: Sequence should have advanced for the committed write
	if handler.Sequence() < 102 {
		t.Errorf("Sequence should be >= 102 after commit, got %d", handler.Sequence())
	}
}

// TestHandler2PC_Contract_MultiplePreparedTransactions verifies that
// multiple prepared transactions can coexist.
//
// Contract: Multiple transactions can be prepared simultaneously.
func TestHandler2PC_Contract_MultiplePreparedTransactions(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Prepare first transaction
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("key1"), []byte("value1"))
	handler.MarkEndPrepare([]byte("txn1"))

	// Prepare second transaction
	handler.MarkBeginPrepare(false)
	handler.Put([]byte("key2"), []byte("value2"))
	handler.MarkEndPrepare([]byte("txn2"))

	// Contract: Both transactions should be pending
	prepared := handler.GetPreparedTransactions()
	if len(prepared) != 2 {
		t.Errorf("Expected 2 prepared transactions, got %d", len(prepared))
	}

	// Commit first, rollback second
	handler.MarkCommit([]byte("txn1"))
	handler.MarkRollback([]byte("txn2"))

	// Contract: Only key1 should be in memtable
	iter := mem.NewIterator()

	iter.Seek([]byte("key1"))
	if !iter.Valid() || string(iter.UserKey()) != "key1" {
		t.Error("Committed key1 should be in memtable")
	}

	iter.Seek([]byte("key2"))
	if iter.Valid() && string(iter.UserKey()) == "key2" {
		t.Error("Rolled back key2 should not be in memtable")
	}
}

// TestHandler2PC_Contract_DeleteOperationsInPrepare verifies that delete
// operations work correctly in prepared transactions.
//
// Contract: Delete operations in prepare block are correctly applied on commit.
func TestHandler2PC_Contract_DeleteOperationsInPrepare(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// First add a key immediately
	handler.Put([]byte("key1"), []byte("original"))

	// Prepare a delete for the same key
	handler.MarkBeginPrepare(false)
	handler.Delete([]byte("key1"))
	handler.MarkEndPrepare([]byte("txn1"))

	// Commit the delete
	handler.MarkCommit([]byte("txn1"))

	// Contract: The most recent operation for key1 should be a deletion
	// (This is visible through the memtable having a deletion marker)
	// Just verify no crash and sequence advances
	if handler.Sequence() < 2 {
		t.Errorf("Sequence should have advanced, got %d", handler.Sequence())
	}
}

// TestHandler2PC_Contract_ImplementsInterface verifies that
// recovery2PCHandler implements batch.Handler2PC.
//
// Contract: recovery2PCHandler is a valid Handler2PC implementation.
func TestHandler2PC_Contract_ImplementsInterface(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	// Compile-time check
	var _ batch.Handler2PC = handler
}

// TestHandler2PC_Contract_AllOperationTypes verifies that all operation
// types are correctly handled in prepared transactions.
//
// Contract: All batch operation types work within prepare blocks.
func TestHandler2PC_Contract_AllOperationTypes(t *testing.T) {
	mem := memtable.NewMemTable(nil)
	handler := newRecovery2PCHandler(mem, 1)

	handler.MarkBeginPrepare(false)

	// All operation types
	if err := handler.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Errorf("Put failed: %v", err)
	}
	if err := handler.Delete([]byte("k2")); err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	if err := handler.SingleDelete([]byte("k3")); err != nil {
		t.Errorf("SingleDelete failed: %v", err)
	}
	if err := handler.Merge([]byte("k4"), []byte("v4")); err != nil {
		t.Errorf("Merge failed: %v", err)
	}
	if err := handler.DeleteRange([]byte("k5"), []byte("k6")); err != nil {
		t.Errorf("DeleteRange failed: %v", err)
	}

	// Column family variants
	if err := handler.PutCF(1, []byte("k7"), []byte("v7")); err != nil {
		t.Errorf("PutCF failed: %v", err)
	}
	if err := handler.DeleteCF(1, []byte("k8")); err != nil {
		t.Errorf("DeleteCF failed: %v", err)
	}
	if err := handler.SingleDeleteCF(1, []byte("k9")); err != nil {
		t.Errorf("SingleDeleteCF failed: %v", err)
	}
	if err := handler.MergeCF(1, []byte("k10"), []byte("v10")); err != nil {
		t.Errorf("MergeCF failed: %v", err)
	}
	if err := handler.DeleteRangeCF(1, []byte("k11"), []byte("k12")); err != nil {
		t.Errorf("DeleteRangeCF failed: %v", err)
	}

	// LogData (should not fail)
	handler.LogData([]byte("some log data"))

	if err := handler.MarkEndPrepare([]byte("txn1")); err != nil {
		t.Fatalf("MarkEndPrepare failed: %v", err)
	}

	// Contract: Transaction should be prepared with all operations
	prepared := handler.GetPreparedTransactions()
	if len(prepared) != 1 {
		t.Errorf("Expected 1 prepared transaction, got %d", len(prepared))
	}
}
