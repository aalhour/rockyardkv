package db

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/batch"
)

// =============================================================================
// WriteBatch API Contract Tests
//
// These tests verify that the public WriteBatch API maintains its semantic
// contract. They exist to prevent regressions like:
// - SingleDelete being silently converted to Delete
// - Record types being lost during batch operations
//
// When adding new WriteBatch methods, add corresponding contract tests here.
// =============================================================================

// TestWriteBatch_RecordTypePreservation verifies that each WriteBatch method
// produces the expected record type in the underlying batch. This prevents
// semantic drift where one operation type is silently converted to another.
func TestWriteBatch_RecordTypePreservation(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	endKey := []byte("test-key-end")

	tests := []struct {
		name      string
		operation func(wb *WriteBatch)
		check     func(internal *batch.WriteBatch) bool
		checkName string
	}{
		{
			name:      "Put produces TypeValue",
			operation: func(wb *WriteBatch) { wb.Put(key, value) },
			check:     func(b *batch.WriteBatch) bool { return b.HasPut() },
			checkName: "HasPut",
		},
		{
			name:      "Delete produces TypeDeletion",
			operation: func(wb *WriteBatch) { wb.Delete(key) },
			check:     func(b *batch.WriteBatch) bool { return b.HasDelete() },
			checkName: "HasDelete",
		},
		{
			name:      "SingleDelete produces TypeSingleDeletion",
			operation: func(wb *WriteBatch) { wb.SingleDelete(key) },
			check:     func(b *batch.WriteBatch) bool { return b.HasSingleDelete() },
			checkName: "HasSingleDelete",
		},
		{
			name:      "Merge produces TypeMerge",
			operation: func(wb *WriteBatch) { wb.Merge(key, value) },
			check:     func(b *batch.WriteBatch) bool { return b.HasMerge() },
			checkName: "HasMerge",
		},
		{
			name:      "DeleteRange produces TypeRangeDeletion",
			operation: func(wb *WriteBatch) { wb.DeleteRange(key, endKey) },
			check:     func(b *batch.WriteBatch) bool { return b.HasDeleteRange() },
			checkName: "HasDeleteRange",
		},
		// Column Family variants
		{
			name:      "PutCF produces TypeColumnFamilyValue",
			operation: func(wb *WriteBatch) { wb.PutCF(1, key, value) },
			check:     func(b *batch.WriteBatch) bool { return b.HasPut() },
			checkName: "HasPut (CF variant)",
		},
		{
			name:      "DeleteCF produces TypeColumnFamilyDeletion",
			operation: func(wb *WriteBatch) { wb.DeleteCF(1, key) },
			check:     func(b *batch.WriteBatch) bool { return b.HasDelete() },
			checkName: "HasDelete (CF variant)",
		},
		{
			name:      "SingleDeleteCF produces TypeColumnFamilySingleDeletion",
			operation: func(wb *WriteBatch) { wb.SingleDeleteCF(1, key) },
			check:     func(b *batch.WriteBatch) bool { return b.HasSingleDelete() },
			checkName: "HasSingleDelete (CF variant)",
		},
		{
			name:      "MergeCF produces TypeColumnFamilyMerge",
			operation: func(wb *WriteBatch) { wb.MergeCF(1, key, value) },
			check:     func(b *batch.WriteBatch) bool { return b.HasMerge() },
			checkName: "HasMerge (CF variant)",
		},
		{
			name:      "DeleteRangeCF produces TypeColumnFamilyRangeDeletion",
			operation: func(wb *WriteBatch) { wb.DeleteRangeCF(1, key, endKey) },
			check:     func(b *batch.WriteBatch) bool { return b.HasDeleteRange() },
			checkName: "HasDeleteRange (CF variant)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wb := NewWriteBatch()
			tc.operation(wb)

			internal := wb.internalBatch()
			if !tc.check(internal) {
				t.Errorf("%s() returned false after %s operation", tc.checkName, tc.name)
			}

			// Verify count is 1
			if internal.Count() != 1 {
				t.Errorf("Expected count=1, got %d", internal.Count())
			}
		})
	}
}

// TestWriteBatch_ClearResetsState verifies that Clear() properly resets
// all batch state, including record type flags.
func TestWriteBatch_ClearResetsState(t *testing.T) {
	wb := NewWriteBatch()
	key := []byte("key")
	value := []byte("value")

	// Add various operations
	wb.Put(key, value)
	wb.Delete(key)
	wb.SingleDelete(key)
	wb.Merge(key, value)
	wb.DeleteRange(key, value)

	internal := wb.internalBatch()
	if internal.Count() != 5 {
		t.Fatalf("Expected count=5 before clear, got %d", internal.Count())
	}

	// Clear and verify
	wb.Clear()

	if internal.Count() != 0 {
		t.Errorf("Expected count=0 after clear, got %d", internal.Count())
	}
	if internal.HasPut() {
		t.Error("HasPut() should be false after Clear()")
	}
	if internal.HasDelete() {
		t.Error("HasDelete() should be false after Clear()")
	}
	if internal.HasSingleDelete() {
		t.Error("HasSingleDelete() should be false after Clear()")
	}
	if internal.HasMerge() {
		t.Error("HasMerge() should be false after Clear()")
	}
	if internal.HasDeleteRange() {
		t.Error("HasDeleteRange() should be false after Clear()")
	}
}

// =============================================================================
// Batch Handler Compliance Tests
//
// Any implementation of batch.Handler that copies/rebuilds batches MUST
// preserve record types. These tests verify compliance for known handlers.
// =============================================================================

// batchRecordTypeChecker is a test handler that records which record types
// it receives during iteration.
type batchRecordTypeChecker struct {
	hasPut          bool
	hasDelete       bool
	hasSingleDelete bool
	hasMerge        bool
	hasDeleteRange  bool
}

func (c *batchRecordTypeChecker) Put(key, value []byte) error {
	c.hasPut = true
	return nil
}

func (c *batchRecordTypeChecker) PutCF(cfID uint32, key, value []byte) error {
	c.hasPut = true
	return nil
}

func (c *batchRecordTypeChecker) Delete(key []byte) error {
	c.hasDelete = true
	return nil
}

func (c *batchRecordTypeChecker) DeleteCF(cfID uint32, key []byte) error {
	c.hasDelete = true
	return nil
}

func (c *batchRecordTypeChecker) SingleDelete(key []byte) error {
	c.hasSingleDelete = true
	return nil
}

func (c *batchRecordTypeChecker) SingleDeleteCF(cfID uint32, key []byte) error {
	c.hasSingleDelete = true
	return nil
}

func (c *batchRecordTypeChecker) Merge(key, value []byte) error {
	c.hasMerge = true
	return nil
}

func (c *batchRecordTypeChecker) MergeCF(cfID uint32, key, value []byte) error {
	c.hasMerge = true
	return nil
}

func (c *batchRecordTypeChecker) DeleteRange(startKey, endKey []byte) error {
	c.hasDeleteRange = true
	return nil
}

func (c *batchRecordTypeChecker) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	c.hasDeleteRange = true
	return nil
}

func (c *batchRecordTypeChecker) LogData(blob []byte) {}

// TestBatchIterate_PreservesRecordTypes verifies that iterating over a batch
// correctly invokes the corresponding handler method for each record type.
func TestBatchIterate_PreservesRecordTypes(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	endKey := []byte("end")

	// Create a batch with all record types
	wb := NewWriteBatch()
	wb.Put(key, value)
	wb.Delete(key)
	wb.SingleDelete(key)
	wb.Merge(key, value)
	wb.DeleteRange(key, endKey)

	// Iterate and check that all record types are received
	checker := &batchRecordTypeChecker{}
	if err := wb.internalBatch().Iterate(checker); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if !checker.hasPut {
		t.Error("Handler did not receive Put")
	}
	if !checker.hasDelete {
		t.Error("Handler did not receive Delete")
	}
	if !checker.hasSingleDelete {
		t.Error("Handler did not receive SingleDelete")
	}
	if !checker.hasMerge {
		t.Error("Handler did not receive Merge")
	}
	if !checker.hasDeleteRange {
		t.Error("Handler did not receive DeleteRange")
	}
}

// TestBatchCopier_PreservesAllRecordTypes verifies that batchCopier (used by
// pessimistic transaction savepoint rollback) preserves all record types.
// This is a regression test for the SingleDelete → Delete bug.
func TestBatchCopier_PreservesAllRecordTypes(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	endKey := []byte("end")

	// Create source batch with all record types
	src := batch.New()
	src.Put(key, value)
	src.Delete(key)
	src.SingleDelete(key)
	src.Merge(key, value)
	src.DeleteRange(key, endKey)

	// Also test CF variants
	src.PutCF(1, key, value)
	src.DeleteCF(1, key)
	src.SingleDeleteCF(1, key)
	src.MergeCF(1, key, value)
	src.DeleteRangeCF(1, key, endKey)

	// Copy using batchCopier (copy all 10 entries)
	dst := batch.New()
	copier := &batchCopier{
		target:   dst,
		maxCount: 10,
	}

	if err := src.Iterate(copier); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// Verify destination has all record types
	if !dst.HasPut() {
		t.Error("Copied batch missing Put records")
	}
	if !dst.HasDelete() {
		t.Error("Copied batch missing Delete records")
	}
	if !dst.HasSingleDelete() {
		t.Error("Copied batch missing SingleDelete records (regression: was converted to Delete)")
	}
	if !dst.HasMerge() {
		t.Error("Copied batch missing Merge records")
	}
	if !dst.HasDeleteRange() {
		t.Error("Copied batch missing DeleteRange records")
	}

	// Verify count matches
	if dst.Count() != src.Count() {
		t.Errorf("Count mismatch: src=%d, dst=%d", src.Count(), dst.Count())
	}
}

// TestBatchCopier_PartialCopyPreservesRecordTypes verifies that partial
// copies (used by savepoint rollback) also preserve record types.
func TestBatchCopier_PartialCopyPreservesRecordTypes(t *testing.T) {
	key := []byte("key")
	value := []byte("value")

	// Create source batch: Put, SingleDelete, Delete, Merge
	src := batch.New()
	src.Put(key, value)        // 1 - will be copied
	src.SingleDelete(key)      // 2 - will be copied
	src.Delete(key)            // 3 - will NOT be copied
	src.Merge(key, value)      // 4 - will NOT be copied

	// Copy only first 2 entries
	dst := batch.New()
	copier := &batchCopier{
		target:   dst,
		maxCount: 2,
	}

	if err := src.Iterate(copier); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// Verify destination has Put and SingleDelete
	if !dst.HasPut() {
		t.Error("Partial copy missing Put")
	}
	if !dst.HasSingleDelete() {
		t.Error("Partial copy missing SingleDelete")
	}

	// Verify destination does NOT have Delete and Merge
	if dst.HasDelete() {
		t.Error("Partial copy should not have Delete (was beyond maxCount)")
	}
	if dst.HasMerge() {
		t.Error("Partial copy should not have Merge (was beyond maxCount)")
	}

	// Verify count
	if dst.Count() != 2 {
		t.Errorf("Expected count=2, got %d", dst.Count())
	}
}

// =============================================================================
// WriteBatch Wrapper Invariants
// =============================================================================

// TestWriteBatch_CountMatchesOperations verifies that Count() accurately
// reflects the number of operations added to the batch.
func TestWriteBatch_CountMatchesOperations(t *testing.T) {
	wb := NewWriteBatch()

	if wb.Count() != 0 {
		t.Errorf("New batch should have count=0, got %d", wb.Count())
	}

	wb.Put([]byte("k1"), []byte("v1"))
	if wb.Count() != 1 {
		t.Errorf("After 1 Put, count should be 1, got %d", wb.Count())
	}

	wb.Delete([]byte("k2"))
	if wb.Count() != 2 {
		t.Errorf("After Put+Delete, count should be 2, got %d", wb.Count())
	}

	wb.SingleDelete([]byte("k3"))
	if wb.Count() != 3 {
		t.Errorf("After Put+Delete+SingleDelete, count should be 3, got %d", wb.Count())
	}

	wb.Merge([]byte("k4"), []byte("v4"))
	if wb.Count() != 4 {
		t.Errorf("After 4 ops, count should be 4, got %d", wb.Count())
	}

	wb.DeleteRange([]byte("a"), []byte("z"))
	if wb.Count() != 5 {
		t.Errorf("After 5 ops, count should be 5, got %d", wb.Count())
	}
}

// TestWriteBatch_DataNotEmpty verifies that Data() returns non-empty bytes
// after operations are added (it should contain at least the header).
func TestWriteBatch_DataNotEmpty(t *testing.T) {
	wb := NewWriteBatch()

	// Even empty batch has a header
	if len(wb.Data()) == 0 {
		t.Error("Empty batch should have header bytes")
	}

	initialLen := len(wb.Data())

	wb.Put([]byte("key"), []byte("value"))

	if len(wb.Data()) <= initialLen {
		t.Error("Data() should grow after Put")
	}
}

// TestNewWriteBatchFromInternal_PreservesData verifies that wrapping an
// internal batch preserves all its data.
func TestNewWriteBatchFromInternal_PreservesData(t *testing.T) {
	// Create internal batch with data
	internal := batch.New()
	internal.Put([]byte("k1"), []byte("v1"))
	internal.SingleDelete([]byte("k2"))
	internal.Merge([]byte("k3"), []byte("v3"))

	// Wrap it
	wb := newWriteBatchFromInternal(internal)

	// Verify the wrapper sees the same data
	if wb.Count() != 3 {
		t.Errorf("Wrapped batch should have count=3, got %d", wb.Count())
	}

	// Verify we can add more operations through the wrapper
	wb.Delete([]byte("k4"))
	if wb.Count() != 4 {
		t.Errorf("After adding via wrapper, count should be 4, got %d", wb.Count())
	}

	// Verify the internal batch also sees the new operation
	if internal.Count() != 4 {
		t.Errorf("Internal batch should also have count=4, got %d", internal.Count())
	}
}

