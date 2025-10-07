package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

func TestMemTableEmpty(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	if !mt.Empty() {
		t.Error("New memtable should be empty")
	}

	if mt.Count() != 0 {
		t.Errorf("Count = %d, want 0", mt.Count())
	}

	// Get on empty table
	_, found, _ := mt.Get([]byte("key"), 100)
	if found {
		t.Error("Should not find key in empty table")
	}
}

func TestMemTableAdd(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("value1"))

	if mt.Empty() {
		t.Error("Memtable should not be empty after Add")
	}

	if mt.Count() != 1 {
		t.Errorf("Count = %d, want 1", mt.Count())
	}

	// Get should find the key
	value, found, deleted := mt.Get([]byte("key1"), 100)
	if !found {
		t.Error("Should find key1")
	}
	if deleted {
		t.Error("key1 should not be deleted")
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Value = %q, want 'value1'", value)
	}
}

func TestMemTableMultipleAdds(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("value1"))
	mt.Add(2, dbformat.TypeValue, []byte("key2"), []byte("value2"))
	mt.Add(3, dbformat.TypeValue, []byte("key3"), []byte("value3"))

	if mt.Count() != 3 {
		t.Errorf("Count = %d, want 3", mt.Count())
	}

	for i := 1; i <= 3; i++ {
		key := fmt.Appendf(nil, "key%d", i)
		expectedValue := fmt.Appendf(nil, "value%d", i)

		value, found, deleted := mt.Get(key, 100)
		if !found {
			t.Errorf("Should find %s", key)
		}
		if deleted {
			t.Errorf("%s should not be deleted", key)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value for %s = %q, want %q", key, value, expectedValue)
		}
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add a key then delete it
	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("value1"))
	mt.Add(2, dbformat.TypeDeletion, []byte("key1"), nil)

	// Get with seq=100 should see the deletion
	value, found, deleted := mt.Get([]byte("key1"), 100)
	if !found {
		t.Error("Should find key1 (as deleted)")
	}
	if !deleted {
		t.Error("key1 should be deleted")
	}
	if value != nil {
		t.Errorf("Deleted key value should be nil, got %q", value)
	}
}

func TestMemTableSequenceNumber(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add same key with different sequence numbers
	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("v1"))
	mt.Add(2, dbformat.TypeValue, []byte("key1"), []byte("v2"))
	mt.Add(3, dbformat.TypeValue, []byte("key1"), []byte("v3"))

	// Get with seq=3 should see v3
	value, found, _ := mt.Get([]byte("key1"), 3)
	if !found {
		t.Error("Should find key1 at seq=3")
	}
	if !bytes.Equal(value, []byte("v3")) {
		t.Errorf("Value at seq=3 = %q, want 'v3'", value)
	}

	// Get with seq=2 should see v2
	value, found, _ = mt.Get([]byte("key1"), 2)
	if !found {
		t.Error("Should find key1 at seq=2")
	}
	if !bytes.Equal(value, []byte("v2")) {
		t.Errorf("Value at seq=2 = %q, want 'v2'", value)
	}

	// Get with seq=1 should see v1
	value, found, _ = mt.Get([]byte("key1"), 1)
	if !found {
		t.Error("Should find key1 at seq=1")
	}
	if !bytes.Equal(value, []byte("v1")) {
		t.Errorf("Value at seq=1 = %q, want 'v1'", value)
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Insert in random order
	mt.Add(1, dbformat.TypeValue, []byte("d"), []byte("vd"))
	mt.Add(2, dbformat.TypeValue, []byte("b"), []byte("vb"))
	mt.Add(3, dbformat.TypeValue, []byte("f"), []byte("vf"))
	mt.Add(4, dbformat.TypeValue, []byte("a"), []byte("va"))
	mt.Add(5, dbformat.TypeValue, []byte("e"), []byte("ve"))
	mt.Add(6, dbformat.TypeValue, []byte("c"), []byte("vc"))

	iter := mt.NewIterator()
	iter.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e", "f"}
	i := 0
	for iter.Valid() && i < len(expected) {
		if string(iter.UserKey()) != expected[i] {
			t.Errorf("Key[%d] = %q, want %q", i, iter.UserKey(), expected[i])
		}
		i++
		iter.Next()
	}

	if i != len(expected) {
		t.Errorf("Iterated %d keys, want %d", i, len(expected))
	}
}

func TestMemTableIteratorSeek(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	mt.Add(1, dbformat.TypeValue, []byte("b"), []byte("vb"))
	mt.Add(2, dbformat.TypeValue, []byte("d"), []byte("vd"))
	mt.Add(3, dbformat.TypeValue, []byte("f"), []byte("vf"))

	iter := mt.NewIterator()

	// Seek to exact key - build proper internal key with max sequence number
	seekKey := buildInternalKey([]byte("d"), dbformat.MaxSequenceNumber, dbformat.ValueTypeForSeek)
	iter.Seek(seekKey)

	if !iter.Valid() {
		t.Fatal("Iterator should be valid after Seek")
	}
	if string(iter.UserKey()) != "d" {
		t.Errorf("UserKey = %q, want 'd'", iter.UserKey())
	}
}

// buildInternalKey builds an internal key for testing.
func buildInternalKey(userKey []byte, seq dbformat.SequenceNumber, typ dbformat.ValueType) []byte {
	key := make([]byte, len(userKey)+8)
	copy(key, userKey)
	trailer := dbformat.PackSequenceAndType(seq, typ)
	binary.LittleEndian.PutUint64(key[len(userKey):], trailer)
	return key
}

func TestMemTableMemoryUsage(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	initialUsage := mt.ApproximateMemoryUsage()
	if initialUsage != 0 {
		t.Errorf("Initial memory usage = %d, want 0", initialUsage)
	}

	// Add some entries
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		value := fmt.Appendf(nil, "value%03d", i)
		mt.Add(dbformat.SequenceNumber(i), dbformat.TypeValue, key, value)
	}

	usage := mt.ApproximateMemoryUsage()
	if usage <= 0 {
		t.Error("Memory usage should be positive after adding entries")
	}
	t.Logf("Memory usage after 100 entries: %d bytes", usage)
}

func TestMemTableRefCounting(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Initial ref count should be 1
	mt.Ref() // Now 2
	mt.Ref() // Now 3

	if mt.Unref() {
		t.Error("Unref should return false when refs > 1")
	}
	if mt.Unref() {
		t.Error("Unref should return false when refs > 1")
	}
	if !mt.Unref() {
		t.Error("Unref should return true when last ref removed")
	}
}

func TestMemTableBinaryKeys(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Keys with null bytes
	key1 := []byte{0x00, 0x01, 0x02}
	key2 := []byte{0xFF, 0xFE, 0xFD}
	value1 := []byte("value1")
	value2 := []byte("value2")

	mt.Add(1, dbformat.TypeValue, key1, value1)
	mt.Add(2, dbformat.TypeValue, key2, value2)

	v, found, _ := mt.Get(key1, 100)
	if !found || !bytes.Equal(v, value1) {
		t.Error("Failed to get key with null bytes")
	}

	v, found, _ = mt.Get(key2, 100)
	if !found || !bytes.Equal(v, value2) {
		t.Error("Failed to get key with 0xFF bytes")
	}
}

func TestMemTableEmptyValue(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	mt.Add(1, dbformat.TypeValue, []byte("key"), []byte{})

	value, found, _ := mt.Get([]byte("key"), 100)
	if !found {
		t.Error("Should find key with empty value")
	}
	if len(value) != 0 {
		t.Errorf("Value should be empty, got %q", value)
	}
}

func TestMemTableLargeValue(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	largeValue := make([]byte, 10*1024) // 10KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	mt.Add(1, dbformat.TypeValue, []byte("key"), largeValue)

	value, found, _ := mt.Get([]byte("key"), 100)
	if !found {
		t.Error("Should find key with large value")
	}
	if !bytes.Equal(value, largeValue) {
		t.Error("Large value mismatch")
	}
}

func TestMemTableSingleDelete(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("value1"))
	mt.Add(2, dbformat.TypeSingleDeletion, []byte("key1"), nil)

	_, found, deleted := mt.Get([]byte("key1"), 100)
	if !found {
		t.Error("Should find key1")
	}
	if !deleted {
		t.Error("key1 should be marked as deleted")
	}
}

// Benchmarks
func BenchmarkMemTableAdd(b *testing.B) {
	mt := NewMemTable(BytewiseComparator)
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := range b.N {
		keys[i] = fmt.Appendf(nil, "key%010d", i)
		values[i] = fmt.Appendf(nil, "value%010d", i)
	}

	b.ResetTimer()
	for i := range b.N {
		mt.Add(dbformat.SequenceNumber(i), dbformat.TypeValue, keys[i], values[i])
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mt := NewMemTable(BytewiseComparator)
	n := 10000
	for i := range n {
		key := fmt.Appendf(nil, "key%05d", i)
		value := fmt.Appendf(nil, "value%05d", i)
		mt.Add(dbformat.SequenceNumber(i), dbformat.TypeValue, key, value)
	}

	keys := make([][]byte, b.N)
	for i := range b.N {
		keys[i] = fmt.Appendf(nil, "key%05d", i%n)
	}

	b.ResetTimer()
	for i := range b.N {
		mt.Get(keys[i], 100000)
	}
}

func BenchmarkMemTableIterate(b *testing.B) {
	mt := NewMemTable(BytewiseComparator)
	for i := range 10000 {
		key := fmt.Appendf(nil, "key%05d", i)
		value := fmt.Appendf(nil, "value%05d", i)
		mt.Add(dbformat.SequenceNumber(i), dbformat.TypeValue, key, value)
	}

	for b.Loop() {
		iter := mt.NewIterator()
		iter.SeekToFirst()
		for iter.Valid() {
			_ = iter.UserKey()
			_ = iter.Value()
			iter.Next()
		}
	}
}

// =============================================================================
// Merge Operand Collection Tests
// =============================================================================

func TestCollectMergeOperandsEmpty(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	if foundBase {
		t.Error("Should not find base in empty memtable")
	}
	if deleted {
		t.Error("Should not be deleted in empty memtable")
	}
	if baseValue != nil {
		t.Errorf("Base value should be nil, got %v", baseValue)
	}
	if len(operands) != 0 {
		t.Errorf("Operands should be empty, got %d", len(operands))
	}
}

func TestCollectMergeOperandsSingleMerge(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add a single merge operand
	mt.Add(1, dbformat.TypeMerge, []byte("key"), []byte("op1"))

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	if foundBase {
		t.Error("Should not find base (only merge operand)")
	}
	if deleted {
		t.Error("Should not be deleted")
	}
	if baseValue != nil {
		t.Errorf("Base value should be nil, got %v", baseValue)
	}
	if len(operands) != 1 {
		t.Errorf("Should have 1 operand, got %d", len(operands))
	}
	if len(operands) > 0 && !bytes.Equal(operands[0], []byte("op1")) {
		t.Errorf("Operand = %q, want %q", operands[0], "op1")
	}
}

func TestCollectMergeOperandsMultipleMerges(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add multiple merge operands in order (newer seq = higher number)
	mt.Add(1, dbformat.TypeMerge, []byte("key"), []byte("op1"))
	mt.Add(2, dbformat.TypeMerge, []byte("key"), []byte("op2"))
	mt.Add(3, dbformat.TypeMerge, []byte("key"), []byte("op3"))

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	if foundBase {
		t.Error("Should not find base (only merge operands)")
	}
	if deleted {
		t.Error("Should not be deleted")
	}
	if baseValue != nil {
		t.Errorf("Base value should be nil, got %v", baseValue)
	}
	// Operands should be in reverse chronological order (newest first)
	if len(operands) != 3 {
		t.Fatalf("Should have 3 operands, got %d", len(operands))
	}
	expected := []string{"op3", "op2", "op1"}
	for i, exp := range expected {
		if !bytes.Equal(operands[i], []byte(exp)) {
			t.Errorf("Operand[%d] = %q, want %q", i, operands[i], exp)
		}
	}
}

func TestCollectMergeOperandsWithBase(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add base value first, then merge operands
	mt.Add(1, dbformat.TypeValue, []byte("key"), []byte("base"))
	mt.Add(2, dbformat.TypeMerge, []byte("key"), []byte("op1"))
	mt.Add(3, dbformat.TypeMerge, []byte("key"), []byte("op2"))

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	if !foundBase {
		t.Error("Should find base value")
	}
	if deleted {
		t.Error("Should not be deleted")
	}
	if !bytes.Equal(baseValue, []byte("base")) {
		t.Errorf("Base value = %q, want %q", baseValue, "base")
	}
	// Should have 2 merge operands (newest first)
	if len(operands) != 2 {
		t.Fatalf("Should have 2 operands, got %d", len(operands))
	}
	expected := []string{"op2", "op1"}
	for i, exp := range expected {
		if !bytes.Equal(operands[i], []byte(exp)) {
			t.Errorf("Operand[%d] = %q, want %q", i, operands[i], exp)
		}
	}
}

func TestCollectMergeOperandsWithDeletion(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Base value, then merge operands, then deletion, then more merges
	mt.Add(1, dbformat.TypeValue, []byte("key"), []byte("base"))
	mt.Add(2, dbformat.TypeMerge, []byte("key"), []byte("op1"))
	mt.Add(3, dbformat.TypeDeletion, []byte("key"), nil)
	mt.Add(4, dbformat.TypeMerge, []byte("key"), []byte("op2"))
	mt.Add(5, dbformat.TypeMerge, []byte("key"), []byte("op3"))

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	// Should stop at deletion - only operands after deletion are collected
	if foundBase {
		t.Error("Should not find base (deleted)")
	}
	if !deleted {
		t.Error("Should be deleted")
	}
	if baseValue != nil {
		t.Errorf("Base value should be nil after deletion, got %v", baseValue)
	}
	// Should only have operands after the deletion (op2, op3)
	if len(operands) != 2 {
		t.Fatalf("Should have 2 operands after deletion, got %d", len(operands))
	}
	expected := []string{"op3", "op2"}
	for i, exp := range expected {
		if !bytes.Equal(operands[i], []byte(exp)) {
			t.Errorf("Operand[%d] = %q, want %q", i, operands[i], exp)
		}
	}
}

func TestCollectMergeOperandsSequenceFiltering(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add entries with various sequence numbers
	mt.Add(1, dbformat.TypeValue, []byte("key"), []byte("base"))
	mt.Add(2, dbformat.TypeMerge, []byte("key"), []byte("op1"))
	mt.Add(3, dbformat.TypeMerge, []byte("key"), []byte("op2"))
	mt.Add(4, dbformat.TypeMerge, []byte("key"), []byte("op3"))
	mt.Add(5, dbformat.TypeMerge, []byte("key"), []byte("op4"))

	// Query with seq=3 should only see entries with seq <= 3
	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 3)
	if !foundBase {
		t.Error("Should find base value")
	}
	if deleted {
		t.Error("Should not be deleted")
	}
	if !bytes.Equal(baseValue, []byte("base")) {
		t.Errorf("Base value = %q, want %q", baseValue, "base")
	}
	// Should only have op1 and op2 (seq 2 and 3)
	if len(operands) != 2 {
		t.Fatalf("Should have 2 operands, got %d", len(operands))
	}
	expected := []string{"op2", "op1"}
	for i, exp := range expected {
		if !bytes.Equal(operands[i], []byte(exp)) {
			t.Errorf("Operand[%d] = %q, want %q", i, operands[i], exp)
		}
	}
}

func TestCollectMergeOperandsOnlyBase(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add only a base value (no merges)
	mt.Add(1, dbformat.TypeValue, []byte("key"), []byte("base"))

	baseValue, operands, foundBase, deleted := mt.CollectMergeOperands([]byte("key"), 100)
	if !foundBase {
		t.Error("Should find base value")
	}
	if deleted {
		t.Error("Should not be deleted")
	}
	if !bytes.Equal(baseValue, []byte("base")) {
		t.Errorf("Base value = %q, want %q", baseValue, "base")
	}
	if len(operands) != 0 {
		t.Errorf("Should have no operands, got %d", len(operands))
	}
}

func TestCollectMergeOperandsDifferentKeys(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add entries for different keys
	mt.Add(1, dbformat.TypeValue, []byte("key1"), []byte("base1"))
	mt.Add(2, dbformat.TypeMerge, []byte("key1"), []byte("op1-1"))
	mt.Add(3, dbformat.TypeMerge, []byte("key2"), []byte("op2-1"))
	mt.Add(4, dbformat.TypeMerge, []byte("key1"), []byte("op1-2"))
	mt.Add(5, dbformat.TypeValue, []byte("key2"), []byte("base2"))

	// Query key1
	baseValue, operands, foundBase, _ := mt.CollectMergeOperands([]byte("key1"), 100)
	if !foundBase {
		t.Error("Should find base for key1")
	}
	if !bytes.Equal(baseValue, []byte("base1")) {
		t.Errorf("Base value = %q, want %q", baseValue, "base1")
	}
	if len(operands) != 2 {
		t.Fatalf("Should have 2 operands for key1, got %d", len(operands))
	}
	expected := []string{"op1-2", "op1-1"}
	for i, exp := range expected {
		if !bytes.Equal(operands[i], []byte(exp)) {
			t.Errorf("Operand[%d] = %q, want %q", i, operands[i], exp)
		}
	}

	// Query key2
	baseValue, operands, foundBase, _ = mt.CollectMergeOperands([]byte("key2"), 100)
	if !foundBase {
		t.Error("Should find base for key2")
	}
	if !bytes.Equal(baseValue, []byte("base2")) {
		t.Errorf("Base value = %q, want %q", baseValue, "base2")
	}
	// key2's merge operand came before the base, so it won't be collected
	if len(operands) != 0 {
		t.Errorf("Should have 0 operands for key2 (merge before base), got %d", len(operands))
	}
}

func TestCollectMergeOperandsEmptyOperand(t *testing.T) {
	mt := NewMemTable(BytewiseComparator)

	// Add merge operand with empty value
	mt.Add(1, dbformat.TypeMerge, []byte("key"), []byte{})

	_, operands, _, _ := mt.CollectMergeOperands([]byte("key"), 100)
	if len(operands) != 1 {
		t.Fatalf("Should have 1 operand, got %d", len(operands))
	}
	if len(operands[0]) != 0 {
		t.Errorf("Operand should be empty, got %q", operands[0])
	}
}
