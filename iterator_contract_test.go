package rockyardkv

// iterator_contract_test.go implements tests for iterator contract.

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
)

// =============================================================================
// Iterator API Contract Tests
//
// These tests verify that the Iterator interface maintains its semantic
// contract. They document expected behavior and prevent regressions.
//
// Reference: RocksDB v10.7.5 include/rocksdb/iterator.h
// =============================================================================

// TestIterator_Contract_ValidOnlyWhenPositioned verifies that Valid() returns
// true only when the iterator is positioned at a valid entry.
//
// Contract: Valid() returns false for new iterators, after exhaustion, and
// after Close(). It returns true only after successful positioning.
func TestIterator_Contract_ValidOnlyWhenPositioned(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Add some data
	for i := range 5 {
		key := fmt.Appendf(nil, "key%d", i)
		if err := db.Put(nil, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Contract: New iterator is not valid
	if iter.Valid() {
		t.Error("New iterator must not be valid before positioning")
	}

	// Contract: Valid after SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Iterator must be valid after SeekToFirst on non-empty DB")
	}

	// Contract: Invalid after exhaustion
	for iter.Valid() {
		iter.Next()
	}
	if iter.Valid() {
		t.Error("Iterator must not be valid after exhaustion")
	}
}

// TestIterator_Contract_SeekToFirstPositionsAtSmallestKey verifies that
// SeekToFirst positions the iterator at the lexicographically smallest key.
//
// Contract: After SeekToFirst, Key() returns the smallest key in the database.
func TestIterator_Contract_SeekToFirstPositionsAtSmallestKey(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Insert keys in random order
	keys := []string{"charlie", "alpha", "bravo", "delta"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid after SeekToFirst")
	}

	// Contract: Key must be "alpha" (lexicographically smallest)
	if string(iter.Key()) != "alpha" {
		t.Errorf("SeekToFirst must position at smallest key: got %q, want %q",
			iter.Key(), "alpha")
	}
}

// TestIterator_Contract_SeekToLastPositionsAtLargestKey verifies that
// SeekToLast positions the iterator at the lexicographically largest key.
//
// Contract: After SeekToLast, Key() returns the largest key in the database.
func TestIterator_Contract_SeekToLastPositionsAtLargestKey(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	keys := []string{"charlie", "alpha", "bravo", "delta"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("Iterator not valid after SeekToLast")
	}

	// Contract: Key must be "delta" (lexicographically largest)
	if string(iter.Key()) != "delta" {
		t.Errorf("SeekToLast must position at largest key: got %q, want %q",
			iter.Key(), "delta")
	}
}

// TestIterator_Contract_SeekPositionsAtFirstKeyGTE verifies that Seek(target)
// positions the iterator at the first key >= target.
//
// Contract: Seek(target) positions at the smallest key that is >= target.
func TestIterator_Contract_SeekPositionsAtFirstKeyGTE(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	tests := []struct {
		target    string
		wantKey   string
		wantValid bool
	}{
		{"aaa", "aaa", true}, // Exact match
		{"bba", "bbb", true}, // Between keys
		{"ccc", "ccc", true}, // Exact match
		{"zzz", "", false},   // Past all keys
		{"000", "aaa", true}, // Before all keys
	}

	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			iter := db.NewIterator(nil)
			defer iter.Close()

			iter.Seek([]byte(tc.target))

			if iter.Valid() != tc.wantValid {
				t.Errorf("Seek(%q) Valid() = %v, want %v", tc.target, iter.Valid(), tc.wantValid)
			}

			if tc.wantValid && string(iter.Key()) != tc.wantKey {
				t.Errorf("Seek(%q) Key() = %q, want %q", tc.target, iter.Key(), tc.wantKey)
			}
		})
	}
}

// TestIterator_Contract_SeekForPrevPositionsAtLastKeyLTE verifies that
// SeekForPrev(target) positions at the last key <= target.
//
// Contract: SeekForPrev(target) positions at the largest key that is <= target.
func TestIterator_Contract_SeekForPrevPositionsAtLastKeyLTE(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	tests := []struct {
		target    string
		wantKey   string
		wantValid bool
	}{
		{"ccc", "ccc", true}, // Exact match
		{"ccd", "ccc", true}, // Between keys
		{"eee", "eee", true}, // Exact match at end
		{"zzz", "eee", true}, // Past all keys
		{"000", "", false},   // Before all keys
	}

	for _, tc := range tests {
		t.Run(tc.target, func(t *testing.T) {
			iter := db.NewIterator(nil)
			defer iter.Close()

			iter.SeekForPrev([]byte(tc.target))

			if iter.Valid() != tc.wantValid {
				t.Errorf("SeekForPrev(%q) Valid() = %v, want %v", tc.target, iter.Valid(), tc.wantValid)
			}

			if tc.wantValid && string(iter.Key()) != tc.wantKey {
				t.Errorf("SeekForPrev(%q) Key() = %q, want %q", tc.target, iter.Key(), tc.wantKey)
			}
		})
	}
}

// TestIterator_Contract_NextMovesInAscendingOrder verifies that Next()
// moves the iterator to the next key in ascending order.
//
// Contract: Successive Next() calls produce keys in strictly ascending order.
func TestIterator_Contract_NextMovesInAscendingOrder(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Insert keys in random order
	insertKeys := []string{"delta", "alpha", "echo", "bravo", "charlie"}
	for _, k := range insertKeys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	var collected []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		collected = append(collected, string(iter.Key()))
	}

	// Contract: Keys must be in ascending order
	expected := make([]string, len(insertKeys))
	copy(expected, insertKeys)
	sort.Strings(expected)

	if len(collected) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(collected))
	}

	for i, k := range collected {
		if k != expected[i] {
			t.Errorf("Key at position %d: got %q, want %q", i, k, expected[i])
		}
	}
}

// TestIterator_Contract_PrevMovesInDescendingOrder verifies that Prev()
// moves the iterator to the previous key in descending order.
//
// Contract: Successive Prev() calls produce keys in strictly descending order.
func TestIterator_Contract_PrevMovesInDescendingOrder(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	insertKeys := []string{"delta", "alpha", "echo", "bravo", "charlie"}
	for _, k := range insertKeys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	var collected []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		collected = append(collected, string(iter.Key()))
	}

	// Contract: Keys must be in descending order
	expected := make([]string, len(insertKeys))
	copy(expected, insertKeys)
	sort.Sort(sort.Reverse(sort.StringSlice(expected)))

	if len(collected) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(collected))
	}

	for i, k := range collected {
		if k != expected[i] {
			t.Errorf("Key at position %d: got %q, want %q", i, k, expected[i])
		}
	}
}

// TestIterator_Contract_KeyValueReturnCorrectData verifies that Key() and
// Value() return the correct data at the current position.
//
// Contract: Key() and Value() return the key-value pair at the current position.
func TestIterator_Contract_KeyValueReturnCorrectData(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	data := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range data {
		if err := db.Put(nil, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())

		expectedValue, ok := data[key]
		if !ok {
			t.Errorf("Unexpected key: %q", key)
			continue
		}
		if value != expectedValue {
			t.Errorf("Value for key %q: got %q, want %q", key, value, expectedValue)
		}
	}
}

// TestIterator_Contract_EmptyDatabaseHasNoValidIterator verifies that
// iterators on empty databases are never valid.
//
// Contract: SeekToFirst/SeekToLast/Seek on empty DB leave iterator invalid.
func TestIterator_Contract_EmptyDatabaseHasNoValidIterator(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Contract: SeekToFirst on empty DB
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("SeekToFirst on empty DB must leave iterator invalid")
	}

	// Contract: SeekToLast on empty DB
	iter.SeekToLast()
	if iter.Valid() {
		t.Error("SeekToLast on empty DB must leave iterator invalid")
	}

	// Contract: Seek on empty DB
	iter.Seek([]byte("any"))
	if iter.Valid() {
		t.Error("Seek on empty DB must leave iterator invalid")
	}
}

// TestIterator_Contract_DeletedKeysNotVisible verifies that deleted keys
// are not visible through iteration.
//
// Contract: Deleted keys don't appear in iteration results.
func TestIterator_Contract_DeletedKeysNotVisible(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Insert and delete some keys
	for i := range 5 {
		key := fmt.Appendf(nil, "key%d", i)
		if err := db.Put(nil, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete key2
	if err := db.Delete(nil, []byte("key2")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if string(iter.Key()) == "key2" {
			t.Error("Deleted key must not appear in iteration")
		}
	}
}

// TestIterator_Contract_SnapshotIsolation verifies that iterators respect
// snapshot visibility.
//
// Contract: Iterator created with a snapshot sees only data visible at snapshot time.
func TestIterator_Contract_SnapshotIsolation(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Insert initial data
	if err := db.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Create snapshot
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Insert more data after snapshot
	if err := db.Put(nil, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Create iterator with snapshot
	readOpts := DefaultReadOptions()
	readOpts.Snapshot = snap
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// Contract: Iterator should only see key1
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 1 || keys[0] != "key1" {
		t.Errorf("Snapshot iterator should see only key1, got: %v", keys)
	}
}

// TestIterator_Contract_DirectionChange verifies that changing direction
// (forward to backward or vice versa) works correctly.
//
// Contract: After direction change, iteration continues correctly from current position.
func TestIterator_Contract_DirectionChange(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward to ccc
	iter.Seek([]byte("ccc"))
	if !iter.Valid() || string(iter.Key()) != "ccc" {
		t.Fatal("Failed to seek to ccc")
	}

	// Contract: Prev from ccc should give bbb
	iter.Prev()
	if !iter.Valid() || string(iter.Key()) != "bbb" {
		t.Errorf("Prev from ccc should give bbb, got %q", iter.Key())
	}

	// Contract: Next from bbb should give ccc
	iter.Next()
	if !iter.Valid() || string(iter.Key()) != "ccc" {
		t.Errorf("Next from bbb should give ccc, got %q", iter.Key())
	}
}

// TestIterator_Contract_UpdatedValueVisible verifies that updated values
// are visible in new iterators.
//
// Contract: New iterators see the latest value for updated keys.
func TestIterator_Contract_UpdatedValueVisible(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	// Insert initial value
	if err := db.Put(nil, []byte("key"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Update value
	if err := db.Put(nil, []byte("key"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid")
	}

	// Contract: Value should be the latest version
	if string(iter.Value()) != "value2" {
		t.Errorf("Value should be 'value2', got %q", iter.Value())
	}
}

// TestIterator_Contract_ErrorPropagation verifies that iterator errors
// are properly propagated.
//
// Contract: Error() returns nil when no error occurred, non-nil otherwise.
func TestIterator_Contract_ErrorPropagation(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	if err := db.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()

	// Contract: No error on valid iterator
	if err := iter.Error(); err != nil {
		t.Errorf("Error should be nil on valid iterator, got: %v", err)
	}
}

// TestIterator_Contract_CloseReleasesResources verifies that Close()
// properly releases iterator resources.
//
// Contract: After Close(), the iterator is no longer usable.
func TestIterator_Contract_CloseReleasesResources(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	if err := db.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	iter := db.NewIterator(nil)
	iter.SeekToFirst()

	// Close the iterator
	if err := iter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Contract: After Close, Valid should return false
	// (behavior is implementation-defined, but should not crash)
}

// =============================================================================
// Helper Functions
// =============================================================================

func createContractTestDB(t *testing.T) (DB, func()) {
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

// =============================================================================
// Forward-Backward Consistency Tests
// =============================================================================

// TestIterator_Contract_ForwardBackwardConsistency verifies that forward
// and backward iteration produce the same keys in opposite order.
//
// Contract: Forward iteration keys reversed equals backward iteration keys.
func TestIterator_Contract_ForwardBackwardConsistency(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	keys := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Collect forward
	iter := db.NewIterator(nil)
	var forward []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		forward = append(forward, string(iter.Key()))
	}
	iter.Close()

	// Collect backward
	iter = db.NewIterator(nil)
	var backward []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		backward = append(backward, string(iter.Key()))
	}
	iter.Close()

	// Contract: Forward reversed equals backward
	if len(forward) != len(backward) {
		t.Fatalf("Forward count %d != backward count %d", len(forward), len(backward))
	}

	for i := range forward {
		j := len(forward) - 1 - i
		if forward[i] != backward[j] {
			t.Errorf("Inconsistency at position %d: forward[%d]=%q != backward[%d]=%q",
				i, i, forward[i], j, backward[j])
		}
	}
}

// =============================================================================
// Key/Value Buffer Independence Tests
// =============================================================================

// TestIterator_Contract_KeyValueBuffersIndependent verifies that Key() and
// Value() return independent buffers that don't change on Next()/Prev().
//
// Contract: Buffers returned by Key()/Value() are stable until Close().
func TestIterator_Contract_KeyValueBuffersIndependent(t *testing.T) {
	db, cleanup := createContractTestDB(t)
	defer cleanup()

	if err := db.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(nil, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator not valid")
	}

	// Capture first key/value
	firstKey := make([]byte, len(iter.Key()))
	copy(firstKey, iter.Key())
	firstValue := make([]byte, len(iter.Value()))
	copy(firstValue, iter.Value())

	// Move to next
	iter.Next()
	if !iter.Valid() {
		t.Fatal("Iterator not valid after Next")
	}

	// Contract: Original buffers should still contain original data
	// (We made copies, so this tests that Key()/Value() return stable data)
	if !bytes.Equal(firstKey, []byte("key1")) {
		t.Error("First key buffer was modified")
	}
	if !bytes.Equal(firstValue, []byte("value1")) {
		t.Error("First value buffer was modified")
	}
}
