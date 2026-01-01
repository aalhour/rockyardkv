// merge_operator_contract_test.go implements tests for merge operator contract.
package rockyardkv

import (
	"path/filepath"
	"testing"
)

// =============================================================================
// MergeOperator API Contract Tests
//
// These tests verify that the MergeOperator interface maintains its semantic
// contract. They document expected behavior and prevent regressions.
//
// Reference: RocksDB v10.7.5 include/rocksdb/merge_operator.h
// =============================================================================

// TestMergeOperator_Contract_FullMergeWithExistingValue verifies that
// FullMerge correctly combines an existing value with operands.
//
// Contract: FullMerge(key, existing, operands) produces the merged result.
func TestMergeOperator_Contract_FullMergeWithExistingValue(t *testing.T) {
	op := &UInt64AddOperator{}

	existing := encodeUint64(100)
	operands := [][]byte{
		encodeUint64(10),
		encodeUint64(20),
		encodeUint64(30),
	}

	result, ok := op.FullMerge([]byte("key"), existing, operands)

	// Contract: Merge should succeed
	if !ok {
		t.Fatal("FullMerge should succeed")
	}

	// Contract: Result should be 100 + 10 + 20 + 30 = 160
	if decodeUint64(result) != 160 {
		t.Errorf("Expected 160, got %d", decodeUint64(result))
	}
}

// TestMergeOperator_Contract_FullMergeWithNilExisting verifies that
// FullMerge works when no existing value exists.
//
// Contract: FullMerge(key, nil, operands) uses identity element.
func TestMergeOperator_Contract_FullMergeWithNilExisting(t *testing.T) {
	op := &UInt64AddOperator{}

	operands := [][]byte{
		encodeUint64(10),
		encodeUint64(20),
	}

	result, ok := op.FullMerge([]byte("key"), nil, operands)

	// Contract: Merge should succeed
	if !ok {
		t.Fatal("FullMerge should succeed")
	}

	// Contract: Result should be 0 + 10 + 20 = 30
	if decodeUint64(result) != 30 {
		t.Errorf("Expected 30, got %d", decodeUint64(result))
	}
}

// TestMergeOperator_Contract_PartialMergeCombinesOperands verifies that
// PartialMerge correctly combines two operands.
//
// Contract: PartialMerge(key, left, right) combines two operands into one.
func TestMergeOperator_Contract_PartialMergeCombinesOperands(t *testing.T) {
	op := &UInt64AddOperator{}

	left := encodeUint64(100)
	right := encodeUint64(50)

	result, ok := op.PartialMerge([]byte("key"), left, right)

	// Contract: Partial merge should succeed
	if !ok {
		t.Fatal("PartialMerge should succeed")
	}

	// Contract: Result should be 100 + 50 = 150
	if decodeUint64(result) != 150 {
		t.Errorf("Expected 150, got %d", decodeUint64(result))
	}
}

// TestMergeOperator_Contract_NameReturnsConsistentValue verifies that
// Name() returns the same value on every call.
//
// Contract: Name() returns a consistent, non-empty string.
func TestMergeOperator_Contract_NameReturnsConsistentValue(t *testing.T) {
	operators := []MergeOperator{
		&UInt64AddOperator{},
		&StringAppendOperator{Delimiter: ","},
		&MaxOperator{},
	}

	for _, op := range operators {
		name1 := op.Name()
		name2 := op.Name()

		// Contract: Name should be non-empty
		if name1 == "" {
			t.Errorf("Name() returned empty string for %T", op)
		}

		// Contract: Name should be consistent
		if name1 != name2 {
			t.Errorf("Name() not consistent: %q != %q", name1, name2)
		}
	}
}

// TestMergeOperator_Contract_StringAppendDelimiter verifies that
// StringAppendOperator correctly uses the delimiter.
//
// Contract: StringAppendOperator joins values with delimiter.
func TestMergeOperator_Contract_StringAppendDelimiter(t *testing.T) {
	op := &StringAppendOperator{Delimiter: ","}

	existing := []byte("a")
	operands := [][]byte{
		[]byte("b"),
		[]byte("c"),
	}

	result, ok := op.FullMerge([]byte("key"), existing, operands)

	// Contract: Merge should succeed
	if !ok {
		t.Fatal("FullMerge should succeed")
	}

	// Contract: Result should be "a,b,c"
	if string(result) != "a,b,c" {
		t.Errorf("Expected 'a,b,c', got %q", result)
	}
}

// TestMergeOperator_Contract_MaxOperatorKeepsMax verifies that
// MaxOperator keeps the maximum value.
//
// Contract: MaxOperator returns the lexicographically largest value.
func TestMergeOperator_Contract_MaxOperatorKeepsMax(t *testing.T) {
	op := &MaxOperator{}

	existing := []byte("banana")
	operands := [][]byte{
		[]byte("apple"),
		[]byte("cherry"),
		[]byte("apricot"),
	}

	result, ok := op.FullMerge([]byte("key"), existing, operands)

	// Contract: Merge should succeed
	if !ok {
		t.Fatal("FullMerge should succeed")
	}

	// Contract: Result should be "cherry" (max)
	if string(result) != "cherry" {
		t.Errorf("Expected 'cherry', got %q", result)
	}
}

// TestMergeOperator_Contract_InvalidOperandReturnsFailure verifies that
// merge operators return ok=false for invalid operands.
//
// Contract: Invalid operands cause FullMerge/PartialMerge to return ok=false.
func TestMergeOperator_Contract_InvalidOperandReturnsFailure(t *testing.T) {
	op := &UInt64AddOperator{}

	// Invalid operand (wrong length)
	operands := [][]byte{
		[]byte("not8bytes"),
	}

	_, ok := op.FullMerge([]byte("key"), nil, operands)

	// Contract: Merge should fail with invalid operand
	if ok {
		t.Error("FullMerge should fail with invalid operand")
	}
}

// TestMergeOperator_Contract_IntegrationWithDB verifies that merge
// operations work correctly through the database interface.
//
// Contract: DB.Merge() applies merge operator on Get().
func TestMergeOperator_Contract_IntegrationWithDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("counter")

	// Initial merge (no existing value)
	if err := db.Merge(nil, key, encodeUint64(10)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Second merge
	if err := db.Merge(nil, key, encodeUint64(20)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Third merge
	if err := db.Merge(nil, key, encodeUint64(30)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Contract: Get should return merged value (10 + 20 + 30 = 60)
	value, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if decodeUint64(value) != 60 {
		t.Errorf("Expected 60, got %d", decodeUint64(value))
	}
}

// TestMergeOperator_Contract_MergeAfterPut verifies that merge
// works correctly after an existing Put.
//
// Contract: Merge on existing value combines with Put value.
func TestMergeOperator_Contract_MergeAfterPut(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &UInt64AddOperator{}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	key := []byte("counter")

	// Put initial value
	if err := db.Put(nil, key, encodeUint64(100)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Merge adds to existing value
	if err := db.Merge(nil, key, encodeUint64(50)); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Contract: Get should return 100 + 50 = 150
	value, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if decodeUint64(value) != 150 {
		t.Errorf("Expected 150, got %d", decodeUint64(value))
	}
}

// TestMergeOperator_Contract_AssociativeAdapter verifies that
// AssociativeMergeOperatorAdapter correctly wraps an associative operator.
//
// Contract: Adapter converts AssociativeMergeOperator to MergeOperator.
func TestMergeOperator_Contract_AssociativeAdapter(t *testing.T) {
	// Simple counter operator
	counter := &simpleCounter{}
	adapter := &AssociativeMergeOperatorAdapter{Op: counter}

	operands := [][]byte{
		encodeUint64(1),
		encodeUint64(2),
		encodeUint64(3),
	}

	result, ok := adapter.FullMerge([]byte("key"), nil, operands)

	// Contract: Adapter should work
	if !ok {
		t.Fatal("FullMerge should succeed")
	}

	// Contract: Result should be 1 + 2 + 3 = 6
	if decodeUint64(result) != 6 {
		t.Errorf("Expected 6, got %d", decodeUint64(result))
	}
}

// simpleCounter is a test AssociativeMergeOperator
type simpleCounter struct{}

func (c *simpleCounter) Name() string { return "simpleCounter" }

func (c *simpleCounter) Merge(key []byte, existingValue, value []byte) ([]byte, bool) {
	var existing uint64
	if existingValue != nil {
		if len(existingValue) != 8 {
			return nil, false
		}
		existing = decodeUint64(existingValue)
	}

	if len(value) != 8 {
		return nil, false
	}
	add := decodeUint64(value)

	return encodeUint64(existing + add), true
}
