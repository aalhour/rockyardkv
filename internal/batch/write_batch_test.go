package batch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
)

// testHandler records all operations for verification.
type testHandler struct {
	puts    []kvPair
	deletes [][]byte
	merges  []kvPair
	ranges  []kvPair
	singles [][]byte
	logData [][]byte
}

type kvPair struct {
	cfID  uint32
	key   []byte
	value []byte
}

func (h *testHandler) Put(key, value []byte) error {
	h.puts = append(h.puts, kvPair{0, dup(key), dup(value)})
	return nil
}

func (h *testHandler) PutCF(cfID uint32, key, value []byte) error {
	h.puts = append(h.puts, kvPair{cfID, dup(key), dup(value)})
	return nil
}

func (h *testHandler) Delete(key []byte) error {
	h.deletes = append(h.deletes, dup(key))
	return nil
}

func (h *testHandler) DeleteCF(cfID uint32, key []byte) error {
	h.deletes = append(h.deletes, dup(key))
	return nil
}

func (h *testHandler) SingleDelete(key []byte) error {
	h.singles = append(h.singles, dup(key))
	return nil
}

func (h *testHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	h.singles = append(h.singles, dup(key))
	return nil
}

func (h *testHandler) Merge(key, value []byte) error {
	h.merges = append(h.merges, kvPair{0, dup(key), dup(value)})
	return nil
}

func (h *testHandler) MergeCF(cfID uint32, key, value []byte) error {
	h.merges = append(h.merges, kvPair{cfID, dup(key), dup(value)})
	return nil
}

func (h *testHandler) DeleteRange(start, end []byte) error {
	h.ranges = append(h.ranges, kvPair{0, dup(start), dup(end)})
	return nil
}

func (h *testHandler) DeleteRangeCF(cfID uint32, start, end []byte) error {
	h.ranges = append(h.ranges, kvPair{cfID, dup(start), dup(end)})
	return nil
}

func (h *testHandler) LogData(blob []byte) {
	h.logData = append(h.logData, dup(blob))
}

func dup(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

func TestWriteBatchEmpty(t *testing.T) {
	wb := New()

	if wb.Count() != 0 {
		t.Errorf("Count = %d, want 0", wb.Count())
	}
	if wb.Size() != HeaderSize {
		t.Errorf("Size = %d, want %d", wb.Size(), HeaderSize)
	}
}

func TestWriteBatchPut(t *testing.T) {
	wb := New()
	wb.Put([]byte("key1"), []byte("value1"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 1 {
		t.Fatalf("Expected 1 put, got %d", len(h.puts))
	}
	if !bytes.Equal(h.puts[0].key, []byte("key1")) {
		t.Errorf("Key = %q, want 'key1'", h.puts[0].key)
	}
	if !bytes.Equal(h.puts[0].value, []byte("value1")) {
		t.Errorf("Value = %q, want 'value1'", h.puts[0].value)
	}
}

func TestWriteBatchDelete(t *testing.T) {
	wb := New()
	wb.Delete([]byte("key1"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.deletes) != 1 {
		t.Fatalf("Expected 1 delete, got %d", len(h.deletes))
	}
	if !bytes.Equal(h.deletes[0], []byte("key1")) {
		t.Errorf("Key = %q, want 'key1'", h.deletes[0])
	}
}

func TestWriteBatchMultipleOperations(t *testing.T) {
	wb := New()
	wb.Put([]byte("k1"), []byte("v1"))
	wb.Delete([]byte("k2"))
	wb.Put([]byte("k3"), []byte("v3"))
	wb.Merge([]byte("k4"), []byte("v4"))

	if wb.Count() != 4 {
		t.Errorf("Count = %d, want 4", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 2 {
		t.Errorf("Expected 2 puts, got %d", len(h.puts))
	}
	if len(h.deletes) != 1 {
		t.Errorf("Expected 1 delete, got %d", len(h.deletes))
	}
	if len(h.merges) != 1 {
		t.Errorf("Expected 1 merge, got %d", len(h.merges))
	}
}

func TestWriteBatchClear(t *testing.T) {
	wb := New()
	wb.Put([]byte("k1"), []byte("v1"))
	wb.Put([]byte("k2"), []byte("v2"))

	if wb.Count() != 2 {
		t.Errorf("Count before clear = %d, want 2", wb.Count())
	}

	wb.Clear()

	if wb.Count() != 0 {
		t.Errorf("Count after clear = %d, want 0", wb.Count())
	}
	if wb.Size() != HeaderSize {
		t.Errorf("Size after clear = %d, want %d", wb.Size(), HeaderSize)
	}
}

func TestWriteBatchSequence(t *testing.T) {
	wb := New()

	if wb.Sequence() != 0 {
		t.Errorf("Initial sequence = %d, want 0", wb.Sequence())
	}

	wb.SetSequence(12345)
	if wb.Sequence() != 12345 {
		t.Errorf("Sequence = %d, want 12345", wb.Sequence())
	}

	wb.SetSequence(0xFFFFFFFFFFFFFFFF)
	if wb.Sequence() != 0xFFFFFFFFFFFFFFFF {
		t.Errorf("Sequence = %d, want max uint64", wb.Sequence())
	}
}

func TestWriteBatchColumnFamily(t *testing.T) {
	wb := New()
	wb.PutCF(1, []byte("k1"), []byte("v1"))
	wb.PutCF(0, []byte("k2"), []byte("v2")) // Should be regular Put
	wb.DeleteCF(2, []byte("k3"))

	if wb.Count() != 3 {
		t.Errorf("Count = %d, want 3", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 2 {
		t.Fatalf("Expected 2 puts, got %d", len(h.puts))
	}

	// First put should have cfID=1
	if h.puts[0].cfID != 1 {
		t.Errorf("First put cfID = %d, want 1", h.puts[0].cfID)
	}
	// Second put should have cfID=0 (default)
	if h.puts[1].cfID != 0 {
		t.Errorf("Second put cfID = %d, want 0", h.puts[1].cfID)
	}
}

func TestWriteBatchSingleDelete(t *testing.T) {
	wb := New()
	wb.SingleDelete([]byte("key1"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.singles) != 1 {
		t.Fatalf("Expected 1 single delete, got %d", len(h.singles))
	}
	if !bytes.Equal(h.singles[0], []byte("key1")) {
		t.Errorf("Key = %q, want 'key1'", h.singles[0])
	}
}

func TestWriteBatchDeleteRange(t *testing.T) {
	wb := New()
	wb.DeleteRange([]byte("a"), []byte("z"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.ranges) != 1 {
		t.Fatalf("Expected 1 range delete, got %d", len(h.ranges))
	}
	if !bytes.Equal(h.ranges[0].key, []byte("a")) {
		t.Errorf("Start = %q, want 'a'", h.ranges[0].key)
	}
	if !bytes.Equal(h.ranges[0].value, []byte("z")) {
		t.Errorf("End = %q, want 'z'", h.ranges[0].value)
	}
}

func TestWriteBatchFromData(t *testing.T) {
	// Create a batch and get its data
	wb1 := New()
	wb1.SetSequence(999)
	wb1.Put([]byte("key"), []byte("value"))

	// Create a new batch from the same data
	wb2, err := NewFromData(wb1.Data())
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	if wb2.Sequence() != 999 {
		t.Errorf("Sequence = %d, want 999", wb2.Sequence())
	}
	if wb2.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb2.Count())
	}

	h := &testHandler{}
	if err := wb2.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 1 {
		t.Fatalf("Expected 1 put, got %d", len(h.puts))
	}
}

func TestWriteBatchTooSmall(t *testing.T) {
	_, err := NewFromData(make([]byte, 5))
	if !errors.Is(err, ErrTooSmall) {
		t.Errorf("Expected ErrTooSmall, got %v", err)
	}
}

func TestWriteBatchEmptyKey(t *testing.T) {
	wb := New()
	wb.Put([]byte{}, []byte("value"))

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 1 {
		t.Fatalf("Expected 1 put, got %d", len(h.puts))
	}
	if len(h.puts[0].key) != 0 {
		t.Errorf("Key should be empty")
	}
}

func TestWriteBatchEmptyValue(t *testing.T) {
	wb := New()
	wb.Put([]byte("key"), []byte{})

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 1 {
		t.Fatalf("Expected 1 put, got %d", len(h.puts))
	}
	if len(h.puts[0].value) != 0 {
		t.Errorf("Value should be empty")
	}
}

func TestWriteBatchLargeData(t *testing.T) {
	wb := New()

	// Create a 1KB key and 10KB value
	key := make([]byte, 1024)
	value := make([]byte, 10*1024)
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte(i % 256)
	}

	wb.Put(key, value)

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if !bytes.Equal(h.puts[0].key, key) {
		t.Error("Key mismatch")
	}
	if !bytes.Equal(h.puts[0].value, value) {
		t.Error("Value mismatch")
	}
}

// -----------------------------------------------------------------------------
// Additional tests for C++ parity
// Based on db/write_batch_test.cc
// -----------------------------------------------------------------------------

func TestWriteBatchAppend(t *testing.T) {
	// Test appending batches (matches C++ Append test)
	wb1 := New()
	wb1.Put([]byte("a"), []byte("va"))
	wb1.Put([]byte("b"), []byte("vb"))

	wb2 := New()
	wb2.Put([]byte("c"), []byte("vc"))
	wb2.Delete([]byte("d"))

	wb1.Append(wb2)

	if wb1.Count() != 4 {
		t.Errorf("Count = %d, want 4", wb1.Count())
	}

	h := &testHandler{}
	if err := wb1.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 3 {
		t.Errorf("Expected 3 puts, got %d", len(h.puts))
	}
	if len(h.deletes) != 1 {
		t.Errorf("Expected 1 delete, got %d", len(h.deletes))
	}
}

func TestWriteBatchAppendEmpty(t *testing.T) {
	wb1 := New()
	wb1.Put([]byte("a"), []byte("va"))

	wb2 := New() // Empty

	countBefore := wb1.Count()
	wb1.Append(wb2)

	if wb1.Count() != countBefore {
		t.Errorf("Count should not change when appending empty batch")
	}
}

func TestWriteBatchLogData(t *testing.T) {
	wb := New()
	wb.Put([]byte("k1"), []byte("v1"))
	wb.PutLogData([]byte("log blob 1"))
	wb.Put([]byte("k2"), []byte("v2"))
	wb.PutLogData([]byte("log blob 2"))

	// LogData doesn't increment count
	if wb.Count() != 2 {
		t.Errorf("Count = %d, want 2 (LogData should not be counted)", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 2 {
		t.Errorf("Expected 2 puts, got %d", len(h.puts))
	}
	if len(h.logData) != 2 {
		t.Errorf("Expected 2 log data, got %d", len(h.logData))
	}
	if string(h.logData[0]) != "log blob 1" {
		t.Errorf("LogData[0] = %q, want 'log blob 1'", h.logData[0])
	}
}

func TestWriteBatchCorruptionTruncatedKey(t *testing.T) {
	wb := New()
	wb.Put([]byte("key"), []byte("value"))

	// Truncate the data to corrupt it
	wb.data = wb.data[:len(wb.data)-3]

	h := &testHandler{}
	err := wb.Iterate(h)
	if err == nil {
		t.Error("Expected error for truncated batch")
	}
}

func TestWriteBatchCorruptionBadVarint(t *testing.T) {
	// Create a batch with an invalid varint
	data := make([]byte, HeaderSize+5)
	binary.LittleEndian.PutUint64(data[0:8], 0)  // sequence
	binary.LittleEndian.PutUint32(data[8:12], 1) // count

	// Add a Put tag followed by invalid varint (all high bits set)
	data[HeaderSize] = TypeValue
	data[HeaderSize+1] = 0xFF
	data[HeaderSize+2] = 0xFF
	data[HeaderSize+3] = 0xFF
	data[HeaderSize+4] = 0xFF

	wb, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	h := &testHandler{}
	err = wb.Iterate(h)
	if err == nil {
		t.Error("Expected error for bad varint")
	}
}

func TestWriteBatchCorruptionUnknownTag(t *testing.T) {
	// Create a batch with an unknown tag
	data := make([]byte, HeaderSize+1)
	binary.LittleEndian.PutUint64(data[0:8], 0)  // sequence
	binary.LittleEndian.PutUint32(data[8:12], 1) // count

	// Add an unknown tag (0xFF is not a valid type)
	data[HeaderSize] = 0xFF

	wb, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	h := &testHandler{}
	err = wb.Iterate(h)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Expected ErrCorrupted, got %v", err)
	}
}

func TestWriteBatchNoop(t *testing.T) {
	// Create a batch with a Noop record
	wb := New()
	// Manually add a Noop
	wb.data = append(wb.data, TypeNoop)

	// Iterate should skip noop
	h := &testHandler{}
	err := wb.Iterate(h)
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}
}

func TestWriteBatchHasOperations(t *testing.T) {
	wb := New()

	if wb.HasPut() {
		t.Error("Empty batch should not have Put")
	}
	if wb.HasDelete() {
		t.Error("Empty batch should not have Delete")
	}
	if wb.HasMerge() {
		t.Error("Empty batch should not have Merge")
	}

	wb.Put([]byte("k"), []byte("v"))
	if !wb.HasPut() {
		t.Error("Batch with Put should have Put")
	}

	wb2 := New()
	wb2.Delete([]byte("k"))
	if !wb2.HasDelete() {
		t.Error("Batch with Delete should have Delete")
	}

	wb3 := New()
	wb3.Merge([]byte("k"), []byte("v"))
	if !wb3.HasMerge() {
		t.Error("Batch with Merge should have Merge")
	}
}

func TestWriteBatchManyOperations(t *testing.T) {
	wb := New()

	// Add many operations
	for i := range 1000 {
		key := []byte(string(rune('a' + (i % 26))))
		value := []byte("value")
		wb.Put(key, value)
	}

	if wb.Count() != 1000 {
		t.Errorf("Count = %d, want 1000", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(h.puts) != 1000 {
		t.Errorf("Expected 1000 puts, got %d", len(h.puts))
	}
}

func TestWriteBatchBinaryData(t *testing.T) {
	// Test with binary data including null bytes
	wb := New()

	key := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	value := []byte{0xFF, 0x00, 0x00, 0xFF, 0x01}

	wb.Put(key, value)

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if !bytes.Equal(h.puts[0].key, key) {
		t.Errorf("Key mismatch with binary data")
	}
	if !bytes.Equal(h.puts[0].value, value) {
		t.Errorf("Value mismatch with binary data")
	}
}

func TestWriteBatchDeleteRangeCF(t *testing.T) {
	wb := New()
	wb.DeleteRangeCF(5, []byte("a"), []byte("z"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}

	// Verify by checking the raw bytes contain the CF tag
	found := slices.Contains(wb.data[HeaderSize:], TypeColumnFamilyRangeDeletion)
	if !found {
		t.Error("Expected TypeColumnFamilyRangeDeletion in batch data")
	}
}

func TestWriteBatchSingleDeleteCF(t *testing.T) {
	wb := New()
	// Need to implement SingleDeleteCF if not present
	wb.deleteRecord(TypeColumnFamilySingleDeletion, 3, []byte("key"))

	if wb.Count() != 1 {
		t.Errorf("Count = %d, want 1", wb.Count())
	}
}

func TestWriteBatchMultipleColumnFamilies(t *testing.T) {
	wb := New()
	wb.PutCF(0, []byte("k0"), []byte("v0")) // Default CF
	wb.PutCF(1, []byte("k1"), []byte("v1"))
	wb.PutCF(2, []byte("k2"), []byte("v2"))
	wb.PutCF(100, []byte("k100"), []byte("v100"))

	if wb.Count() != 4 {
		t.Errorf("Count = %d, want 4", wb.Count())
	}

	h := &testHandler{}
	if err := wb.Iterate(h); err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// Check CF IDs
	expectedCFs := []uint32{0, 1, 2, 100}
	for i, put := range h.puts {
		if put.cfID != expectedCFs[i] {
			t.Errorf("Put[%d] cfID = %d, want %d", i, put.cfID, expectedCFs[i])
		}
	}
}

func TestWriteBatchDataConsistency(t *testing.T) {
	// Create a batch, get data, create another batch from it, verify equality
	wb1 := New()
	wb1.SetSequence(12345)
	wb1.Put([]byte("key1"), []byte("value1"))
	wb1.Delete([]byte("key2"))
	wb1.Merge([]byte("key3"), []byte("value3"))

	data := wb1.Data()

	wb2, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	if wb2.Sequence() != 12345 {
		t.Errorf("Sequence = %d, want 12345", wb2.Sequence())
	}
	if wb2.Count() != 3 {
		t.Errorf("Count = %d, want 3", wb2.Count())
	}

	// Iterate both and compare
	h1 := &testHandler{}
	h2 := &testHandler{}
	wb1.Iterate(h1)
	wb2.Iterate(h2)

	if len(h1.puts) != len(h2.puts) {
		t.Error("Put counts don't match")
	}
	if len(h1.deletes) != len(h2.deletes) {
		t.Error("Delete counts don't match")
	}
	if len(h1.merges) != len(h2.merges) {
		t.Error("Merge counts don't match")
	}
}

func TestWriteBatchSequenceWrap(t *testing.T) {
	wb := New()

	// Test max sequence number
	maxSeq := uint64(0xFFFFFFFFFFFFFFFF)
	wb.SetSequence(maxSeq)

	if wb.Sequence() != maxSeq {
		t.Errorf("Sequence = %d, want max uint64", wb.Sequence())
	}
}

func TestWriteBatchIterateError(t *testing.T) {
	// Handler that returns error
	errorHandler := &errorReturningHandler{
		errorOnPut: true,
	}

	wb := New()
	wb.Put([]byte("k1"), []byte("v1"))
	wb.Put([]byte("k2"), []byte("v2"))

	err := wb.Iterate(errorHandler)
	if err == nil {
		t.Error("Expected error from handler")
	}

	// Only first put should have been processed
	if errorHandler.putCount != 1 {
		t.Errorf("putCount = %d, want 1 (should stop after first error)", errorHandler.putCount)
	}
}

type errorReturningHandler struct {
	testHandler
	errorOnPut bool
	putCount   int
}

func (h *errorReturningHandler) Put(key, value []byte) error {
	h.putCount++
	if h.errorOnPut {
		return errors.New("intentional error")
	}
	return nil
}

func (h *errorReturningHandler) PutCF(cfID uint32, key, value []byte) error {
	return h.Put(key, value)
}

// Benchmark tests
func BenchmarkWriteBatchPut(b *testing.B) {
	key := []byte("key")
	value := []byte("value")

	for b.Loop() {
		wb := New()
		wb.Put(key, value)
	}
}

func BenchmarkWriteBatchIterate(b *testing.B) {
	wb := New()
	for range 100 {
		wb.Put([]byte("key"), []byte("value"))
	}

	h := &testHandler{}

	for b.Loop() {
		h.puts = h.puts[:0]
		wb.Iterate(h)
	}
}
