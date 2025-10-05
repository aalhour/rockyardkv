package batch

import (
	"bytes"
	"testing"
)

// -----------------------------------------------------------------------------
// Batch Edge Case Tests
// These tests verify edge cases and error handling in WriteBatch.
// -----------------------------------------------------------------------------

// TestBatchCorruptedTruncated tests detection of truncated batch data.
func TestBatchCorruptedTruncated(t *testing.T) {
	// Create a valid batch
	wb := New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	data := wb.Data()

	// Truncate the data at various points
	truncations := []int{0, 5, 10, HeaderSize - 1, HeaderSize, HeaderSize + 1, len(data) - 1}
	for _, n := range truncations {
		if n > len(data) {
			continue
		}
		truncated := data[:n]

		_, err := NewFromData(truncated)
		if n < HeaderSize && err == nil {
			t.Errorf("Truncated at %d should fail (< header size)", n)
		}
	}
}

// TestBatchVeryLarge tests handling of very large batches.
func TestBatchVeryLarge(t *testing.T) {
	wb := New()

	// Add many entries
	numEntries := 10000
	for i := range numEntries {
		key := []byte{byte(i >> 8), byte(i)}
		value := []byte{byte(i)}
		wb.Put(key, value)
	}

	if wb.Count() != uint32(numEntries) {
		t.Errorf("Count = %d, want %d", wb.Count(), numEntries)
	}

	// Verify we can iterate all entries
	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.puts != numEntries {
		t.Errorf("Iterated %d puts, want %d", handler.puts, numEntries)
	}
}

// TestBatchEmptyKey tests handling of empty keys.
func TestBatchEmptyKey(t *testing.T) {
	wb := New()
	wb.Put([]byte{}, []byte("value_for_empty_key"))
	wb.Delete([]byte{})

	if wb.Count() != 2 {
		t.Errorf("Count = %d, want 2", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.puts != 1 || handler.deletes != 1 {
		t.Errorf("Got puts=%d, deletes=%d; want 1, 1", handler.puts, handler.deletes)
	}
}

// TestBatchEmptyValue tests handling of empty values.
func TestBatchEmptyValue(t *testing.T) {
	wb := New()
	wb.Put([]byte("key1"), []byte{})
	wb.Put([]byte("key2"), []byte{})

	if wb.Count() != 2 {
		t.Errorf("Count = %d, want 2", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.puts != 2 {
		t.Errorf("Got puts=%d, want 2", handler.puts)
	}
}

// TestBatchBinaryKeyValue tests keys and values with null bytes.
func TestBatchBinaryKeyValue(t *testing.T) {
	wb := New()

	binaryKey := []byte{0x00, 0x01, 0x00, 0x02, 0x00}
	binaryValue := []byte{0xFF, 0x00, 0xFF, 0x00, 0xFF}

	wb.Put(binaryKey, binaryValue)

	handler := &edgeCaseCapturingHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(handler.entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(handler.entries))
	}

	if !bytes.Equal(handler.entries[0].key, binaryKey) {
		t.Errorf("Key mismatch: got %x, want %x", handler.entries[0].key, binaryKey)
	}
	if !bytes.Equal(handler.entries[0].value, binaryValue) {
		t.Errorf("Value mismatch: got %x, want %x", handler.entries[0].value, binaryValue)
	}
}

// TestBatchMergeOperations tests merge operations in batch.
func TestBatchMergeOperations(t *testing.T) {
	wb := New()
	wb.Merge([]byte("counter"), []byte("1"))
	wb.Merge([]byte("counter"), []byte("2"))
	wb.Merge([]byte("counter"), []byte("3"))

	if wb.Count() != 3 {
		t.Errorf("Count = %d, want 3", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.merges != 3 {
		t.Errorf("Got merges=%d, want 3", handler.merges)
	}
}

// TestBatchSingleDelete tests single delete operations.
func TestBatchSingleDelete(t *testing.T) {
	wb := New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.SingleDelete([]byte("key1"))

	if wb.Count() != 2 {
		t.Errorf("Count = %d, want 2", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.puts != 1 || handler.singleDeletes != 1 {
		t.Errorf("Got puts=%d, singleDeletes=%d; want 1, 1", handler.puts, handler.singleDeletes)
	}
}

// TestBatchDeleteRange tests delete range operations.
func TestBatchDeleteRange(t *testing.T) {
	wb := New()
	wb.DeleteRange([]byte("a"), []byte("z"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.deleteRanges != 1 {
		t.Errorf("Got deleteRanges=%d, want 1", handler.deleteRanges)
	}
}

// TestBatchColumnFamily tests column family operations.
func TestBatchColumnFamily(t *testing.T) {
	wb := New()

	// Default CF
	wb.Put([]byte("key1"), []byte("value1"))

	// CF 1
	wb.PutCF(1, []byte("cf1_key"), []byte("cf1_value"))
	wb.DeleteCF(1, []byte("cf1_key2"))

	// CF 2
	wb.PutCF(2, []byte("cf2_key"), []byte("cf2_value"))

	if wb.Count() != 4 {
		t.Errorf("Count = %d, want 4", wb.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// 1 default put + 2 CF puts
	if handler.puts+handler.cfPuts != 3 {
		t.Errorf("Got puts=%d, cfPuts=%d; want total 3", handler.puts, handler.cfPuts)
	}
}

// TestBatchAppendBatches tests appending two batches.
func TestBatchAppendBatches(t *testing.T) {
	wb1 := New()
	wb1.Put([]byte("key1"), []byte("value1"))
	wb1.Put([]byte("key2"), []byte("value2"))

	wb2 := New()
	wb2.Put([]byte("key3"), []byte("value3"))
	wb2.Delete([]byte("key4"))

	// Append wb2 to wb1
	wb1.Append(wb2)

	if wb1.Count() != 4 {
		t.Errorf("Count after append = %d, want 4", wb1.Count())
	}

	handler := &edgeCaseTestHandler{}
	if err := wb1.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.puts != 3 || handler.deletes != 1 {
		t.Errorf("Got puts=%d, deletes=%d; want 3, 1", handler.puts, handler.deletes)
	}
}

// TestBatchClearBatch tests clearing a batch.
func TestBatchClearBatch(t *testing.T) {
	wb := New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))

	if wb.Count() != 2 {
		t.Errorf("Count before clear = %d, want 2", wb.Count())
	}

	wb.Clear()

	if wb.Count() != 0 {
		t.Errorf("Count after clear = %d, want 0", wb.Count())
	}

	// Should be able to add more after clear
	wb.Put([]byte("key3"), []byte("value3"))
	if wb.Count() != 1 {
		t.Errorf("Count after re-add = %d, want 1", wb.Count())
	}
}

// TestBatchSequenceNumber tests sequence number handling.
func TestBatchSequenceNumberEdge(t *testing.T) {
	wb := New()
	wb.SetSequence(12345)

	if wb.Sequence() != 12345 {
		t.Errorf("Sequence = %d, want 12345", wb.Sequence())
	}

	// Verify sequence is in encoded data
	data := wb.Data()
	// Sequence is in first 8 bytes, little-endian
	seq := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
		uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56

	if seq != 12345 {
		t.Errorf("Encoded sequence = %d, want 12345", seq)
	}
}

// TestBatchHasMethods tests Has* query methods.
func TestBatchHasMethodsEdge(t *testing.T) {
	wb := New()

	if wb.HasPut() || wb.HasDelete() || wb.HasMerge() || wb.HasSingleDelete() || wb.HasDeleteRange() {
		t.Error("Empty batch should have no operations")
	}

	wb.Put([]byte("k"), []byte("v"))
	if !wb.HasPut() {
		t.Error("Should have put")
	}

	wb.Delete([]byte("k"))
	if !wb.HasDelete() {
		t.Error("Should have delete")
	}

	wb.Merge([]byte("k"), []byte("v"))
	if !wb.HasMerge() {
		t.Error("Should have merge")
	}

	wb.SingleDelete([]byte("k"))
	if !wb.HasSingleDelete() {
		t.Error("Should have single delete")
	}

	wb.DeleteRange([]byte("a"), []byte("z"))
	if !wb.HasDeleteRange() {
		t.Error("Should have delete range")
	}
}

// TestBatchLogData tests log data records.
func TestBatchLogDataEdge(t *testing.T) {
	wb := New()
	wb.PutLogData([]byte("log entry 1"))
	wb.Put([]byte("key"), []byte("value"))
	wb.PutLogData([]byte("log entry 2"))

	handler := &edgeCaseTestHandler{}
	if err := wb.Iterate(handler); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if handler.logData != 2 {
		t.Errorf("Got logData=%d, want 2", handler.logData)
	}
}

// TestBatchRoundTrip tests serialization round-trip.
func TestBatchRoundTripEdge(t *testing.T) {
	original := New()
	original.SetSequence(999)
	original.Put([]byte("key1"), []byte("value1"))
	original.Delete([]byte("key2"))
	original.Merge([]byte("key3"), []byte("operand"))
	original.PutCF(5, []byte("cf_key"), []byte("cf_value"))

	data := original.Data()

	restored, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	if restored.Sequence() != original.Sequence() {
		t.Errorf("Sequence mismatch: got %d, want %d", restored.Sequence(), original.Sequence())
	}

	if restored.Count() != original.Count() {
		t.Errorf("Count mismatch: got %d, want %d", restored.Count(), original.Count())
	}
}

// edgeCaseTestHandler counts different operation types.
type edgeCaseTestHandler struct {
	puts          int
	deletes       int
	singleDeletes int
	merges        int
	deleteRanges  int
	logData       int
	cfPuts        int
	cfDeletes     int
	cfMerges      int
}

func (h *edgeCaseTestHandler) Put(key, value []byte) error {
	h.puts++
	return nil
}

func (h *edgeCaseTestHandler) Delete(key []byte) error {
	h.deletes++
	return nil
}

func (h *edgeCaseTestHandler) SingleDelete(key []byte) error {
	h.singleDeletes++
	return nil
}

func (h *edgeCaseTestHandler) Merge(key, value []byte) error {
	h.merges++
	return nil
}

func (h *edgeCaseTestHandler) DeleteRange(startKey, endKey []byte) error {
	h.deleteRanges++
	return nil
}

func (h *edgeCaseTestHandler) LogData(blob []byte) {
	h.logData++
}

func (h *edgeCaseTestHandler) PutCF(cfID uint32, key, value []byte) error {
	h.cfPuts++
	return nil
}

func (h *edgeCaseTestHandler) DeleteCF(cfID uint32, key []byte) error {
	h.cfDeletes++
	return nil
}

func (h *edgeCaseTestHandler) MergeCF(cfID uint32, key, value []byte) error {
	h.cfMerges++
	return nil
}

func (h *edgeCaseTestHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	h.singleDeletes++
	return nil
}

func (h *edgeCaseTestHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	h.deleteRanges++
	return nil
}

// edgeCaseCapturingHandler captures entries for verification.
type edgeCaseCapturingHandler struct {
	entries []struct {
		key   []byte
		value []byte
	}
}

func (h *edgeCaseCapturingHandler) Put(key, value []byte) error {
	h.entries = append(h.entries, struct {
		key   []byte
		value []byte
	}{
		key:   append([]byte{}, key...),
		value: append([]byte{}, value...),
	})
	return nil
}

func (h *edgeCaseCapturingHandler) Delete(key []byte) error                   { return nil }
func (h *edgeCaseCapturingHandler) SingleDelete(key []byte) error             { return nil }
func (h *edgeCaseCapturingHandler) Merge(key, value []byte) error             { return nil }
func (h *edgeCaseCapturingHandler) DeleteRange(startKey, endKey []byte) error { return nil }
func (h *edgeCaseCapturingHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	return nil
}
func (h *edgeCaseCapturingHandler) LogData(blob []byte)                          {}
func (h *edgeCaseCapturingHandler) PutCF(cfID uint32, key, value []byte) error   { return nil }
func (h *edgeCaseCapturingHandler) DeleteCF(cfID uint32, key []byte) error       { return nil }
func (h *edgeCaseCapturingHandler) SingleDeleteCF(cfID uint32, key []byte) error { return nil }
func (h *edgeCaseCapturingHandler) MergeCF(cfID uint32, key, value []byte) error { return nil }
