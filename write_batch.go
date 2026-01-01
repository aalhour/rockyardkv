// write_batch.go implements the public WriteBatch API for atomic writes.
//
// Reference: RocksDB v10.7.5 include/rocksdb/write_batch.h
package rockyardkv

import (
	"github.com/aalhour/rockyardkv/internal/batch"
)

// WriteBatch holds a collection of writes to be applied atomically.
// Keys and values are copied, so you can modify them after calling Put/Delete.
//
// A WriteBatch can be reused by calling Clear() after Write().
//
// Example:
//
//	wb := db.NewWriteBatch()
//	wb.Put([]byte("key1"), []byte("value1"))
//	wb.Put([]byte("key2"), []byte("value2"))
//	wb.Delete([]byte("key3"))
//	err := database.Write(writeOpts, wb)
//	wb.Clear() // Reuse the batch
type WriteBatch struct {
	internal *batch.WriteBatch
}

// NewWriteBatch creates a new empty WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		internal: batch.New(),
	}
}

// Put adds a key-value pair to the batch.
func (wb *WriteBatch) Put(key, value []byte) {
	wb.internal.Put(key, value)
}

// PutCF adds a key-value pair to the batch for the specified column family.
func (wb *WriteBatch) PutCF(cfID uint32, key, value []byte) {
	wb.internal.PutCF(cfID, key, value)
}

// Delete adds a deletion for the key to the batch.
func (wb *WriteBatch) Delete(key []byte) {
	wb.internal.Delete(key)
}

// DeleteCF adds a deletion for the key to the batch for the specified column family.
func (wb *WriteBatch) DeleteCF(cfID uint32, key []byte) {
	wb.internal.DeleteCF(cfID, key)
}

// DeleteRange adds a range deletion [startKey, endKey) to the batch.
func (wb *WriteBatch) DeleteRange(startKey, endKey []byte) {
	wb.internal.DeleteRange(startKey, endKey)
}

// DeleteRangeCF adds a range deletion to the batch for the specified column family.
func (wb *WriteBatch) DeleteRangeCF(cfID uint32, startKey, endKey []byte) {
	wb.internal.DeleteRangeCF(cfID, startKey, endKey)
}

// Merge adds a merge operand for the key to the batch.
func (wb *WriteBatch) Merge(key, value []byte) {
	wb.internal.Merge(key, value)
}

// MergeCF adds a merge operand to the batch for the specified column family.
func (wb *WriteBatch) MergeCF(cfID uint32, key, value []byte) {
	wb.internal.MergeCF(cfID, key, value)
}

// SingleDelete adds a single deletion for the key to the batch.
// SingleDelete is more efficient than Delete when the key has only one version.
func (wb *WriteBatch) SingleDelete(key []byte) {
	wb.internal.SingleDelete(key)
}

// SingleDeleteCF adds a single deletion to the batch for the specified column family.
func (wb *WriteBatch) SingleDeleteCF(cfID uint32, key []byte) {
	wb.internal.SingleDeleteCF(cfID, key)
}

// Clear resets the batch to empty, allowing it to be reused.
func (wb *WriteBatch) Clear() {
	wb.internal.Clear()
}

// Count returns the number of operations in the batch.
func (wb *WriteBatch) Count() uint32 {
	return wb.internal.Count()
}

// Data returns the raw batch data (for advanced use only).
func (wb *WriteBatch) Data() []byte {
	return wb.internal.Data()
}

// internalBatch returns the underlying batch for use by DB.Write().
// This is not part of the public API.
func (wb *WriteBatch) internalBatch() *batch.WriteBatch {
	return wb.internal
}

// newWriteBatchFromInternal wraps an internal batch.
// This is not part of the public API - used by internal Put/Delete operations.
func newWriteBatchFromInternal(internal *batch.WriteBatch) *WriteBatch {
	return &WriteBatch{internal: internal}
}
