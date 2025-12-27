package db

import (
	"bytes"
	"fmt"
	"testing"
)

// =============================================================================
// Iterator Tests (matching C++ RocksDB db/db_iter_test.cc)
// =============================================================================

// TestIteratorPrevNext tests alternating Prev and Next operations.
func TestIteratorPrevNext(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert ordered keys
	for i := range 10 {
		key := fmt.Appendf(nil, "key%02d", i)
		value := fmt.Appendf(nil, "value%02d", i)
		db.Put(nil, key, value)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to middle
	iter.Seek([]byte("key05"))
	if !iter.Valid() {
		t.Fatal("Seek to key05 failed")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("key05")) {
		t.Errorf("After Seek: key = %q, want key05", iter.Key())
	}

	// Go forward
	iter.Next()
	if !iter.Valid() {
		t.Fatal("Next failed")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("key06")) {
		t.Errorf("After Next: key = %q, want key06", iter.Key())
	}

	// Go backward
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("Prev failed")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("key05")) {
		t.Errorf("After Prev: key = %q, want key05", iter.Key())
	}

	// Keep going backward
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("Second Prev failed")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("key04")) {
		t.Errorf("After second Prev: key = %q, want key04", iter.Key())
	}

	// Forward again
	iter.Next()
	iter.Next()
	if !iter.Valid() {
		t.Fatal("Forward after backward failed")
	}
	if !bytes.HasPrefix(iter.Key(), []byte("key06")) {
		t.Errorf("After forward: key = %q, want key06", iter.Key())
	}
}

// TestIteratorEmptyDB tests iterator behavior on empty database.
func TestIteratorEmptyDB(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	iter := db.NewIterator(nil)
	defer iter.Close()

	// SeekToFirst on empty DB
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("SeekToFirst on empty DB should not be valid")
	}

	// SeekToLast on empty DB
	iter.SeekToLast()
	if iter.Valid() {
		t.Error("SeekToLast on empty DB should not be valid")
	}

	// Seek on empty DB
	iter.Seek([]byte("key"))
	if iter.Valid() {
		t.Error("Seek on empty DB should not be valid")
	}

	// SeekForPrev on empty DB
	iter.SeekForPrev([]byte("key"))
	if iter.Valid() {
		t.Error("SeekForPrev on empty DB should not be valid")
	}
}

// TestIteratorOneKey tests iterator with single key.
func TestIteratorOneKey(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("only_key"), []byte("only_value"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// SeekToFirst
	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("SeekToFirst should be valid")
	}
	if !bytes.Equal(iter.Key(), []byte("only_key")) {
		t.Errorf("Key = %q, want only_key", iter.Key())
	}

	// Next should invalidate
	iter.Next()
	if iter.Valid() {
		t.Error("Next after only key should not be valid")
	}

	// SeekToLast
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if !bytes.Equal(iter.Key(), []byte("only_key")) {
		t.Errorf("Key = %q, want only_key", iter.Key())
	}

	// Prev should invalidate
	iter.Prev()
	if iter.Valid() {
		t.Error("Prev after only key should not be valid")
	}
}

// TestIteratorSeekBeyondRange tests seeking beyond the key range.
func TestIteratorSeekBeyondRange(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys from "b" to "y"
	for _, key := range []string{"b", "d", "f", "h", "j", "l", "n", "p", "r", "t", "v", "x"} {
		db.Put(nil, []byte(key), []byte(key))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek before first key
	iter.Seek([]byte("a"))
	if !iter.Valid() {
		t.Fatal("Seek to 'a' should find 'b'")
	}
	if !bytes.Equal(iter.Key(), []byte("b")) {
		t.Errorf("Seek to 'a' found %q, want 'b'", iter.Key())
	}

	// Seek after last key
	iter.Seek([]byte("z"))
	if iter.Valid() {
		t.Error("Seek to 'z' should not be valid (beyond all keys)")
	}

	// SeekForPrev after last key
	iter.SeekForPrev([]byte("z"))
	if !iter.Valid() {
		t.Fatal("SeekForPrev to 'z' should find 'x'")
	}
	if !bytes.Equal(iter.Key(), []byte("x")) {
		t.Errorf("SeekForPrev to 'z' found %q, want 'x'", iter.Key())
	}

	// SeekForPrev before first key
	iter.SeekForPrev([]byte("a"))
	if iter.Valid() {
		t.Error("SeekForPrev to 'a' should not be valid (before all keys)")
	}
}

// TestIteratorFullScan tests forward and backward full scans.
func TestIteratorFullScan(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		db.Put(nil, []byte(key), []byte(key))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward scan
	var forwardKeys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		forwardKeys = append(forwardKeys, string(iter.Key()))
	}

	if len(forwardKeys) != len(keys) {
		t.Errorf("Forward scan found %d keys, want %d", len(forwardKeys), len(keys))
	}

	for i, key := range forwardKeys {
		if key != keys[i] {
			t.Errorf("Forward scan[%d] = %q, want %q", i, key, keys[i])
		}
	}

	// Backward scan
	var backwardKeys []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		backwardKeys = append(backwardKeys, string(iter.Key()))
	}

	if len(backwardKeys) != len(keys) {
		t.Errorf("Backward scan found %d keys, want %d", len(backwardKeys), len(keys))
	}

	// Should be in reverse order
	for i, key := range backwardKeys {
		expected := keys[len(keys)-1-i]
		if key != expected {
			t.Errorf("Backward scan[%d] = %q, want %q", i, key, expected)
		}
	}
}

// TestIteratorSeekExact tests seeking to exact keys.
func TestIteratorSeekExact(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	keys := []string{"a", "c", "e", "g", "i"}
	for _, key := range keys {
		db.Put(nil, []byte(key), []byte(key))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to existing keys
	for _, key := range keys {
		iter.Seek([]byte(key))
		if !iter.Valid() {
			t.Errorf("Seek to %q should be valid", key)
			continue
		}
		if !bytes.Equal(iter.Key(), []byte(key)) {
			t.Errorf("Seek to %q found %q", key, iter.Key())
		}
	}

	// Seek to non-existing keys (should find next)
	testCases := []struct {
		seek     string
		expected string
	}{
		{"b", "c"},
		{"d", "e"},
		{"f", "g"},
		{"h", "i"},
	}

	for _, tc := range testCases {
		iter.Seek([]byte(tc.seek))
		if !iter.Valid() {
			t.Errorf("Seek to %q should be valid", tc.seek)
			continue
		}
		if !bytes.Equal(iter.Key(), []byte(tc.expected)) {
			t.Errorf("Seek to %q found %q, want %q", tc.seek, iter.Key(), tc.expected)
		}
	}
}

// TestIteratorSeekForPrevExact tests SeekForPrev to exact keys.
func TestIteratorSeekForPrevExact(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	keys := []string{"a", "c", "e", "g", "i"}
	for _, key := range keys {
		db.Put(nil, []byte(key), []byte(key))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// SeekForPrev to existing keys
	for _, key := range keys {
		iter.SeekForPrev([]byte(key))
		if !iter.Valid() {
			t.Errorf("SeekForPrev to %q should be valid", key)
			continue
		}
		if !bytes.Equal(iter.Key(), []byte(key)) {
			t.Errorf("SeekForPrev to %q found %q", key, iter.Key())
		}
	}

	// SeekForPrev to non-existing keys (should find previous)
	testCases := []struct {
		seek     string
		expected string
	}{
		{"b", "a"},
		{"d", "c"},
		{"f", "e"},
		{"h", "g"},
	}

	for _, tc := range testCases {
		iter.SeekForPrev([]byte(tc.seek))
		if !iter.Valid() {
			t.Errorf("SeekForPrev to %q should be valid", tc.seek)
			continue
		}
		if !bytes.Equal(iter.Key(), []byte(tc.expected)) {
			t.Errorf("SeekForPrev to %q found %q, want %q", tc.seek, iter.Key(), tc.expected)
		}
	}
}

// TestIteratorWithDeletes tests iterator behavior with deleted keys.
func TestIteratorWithDeletes(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	for i := range 10 {
		key := fmt.Appendf(nil, "key%02d", i)
		db.Put(nil, key, []byte("value"))
	}

	// Delete even keys
	for i := 0; i < 10; i += 2 {
		key := fmt.Appendf(nil, "key%02d", i)
		db.Delete(nil, key)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Count remaining keys
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	// Should have 5 keys (odd numbers)
	if count != 5 {
		t.Errorf("Expected 5 keys, got %d", count)
	}
}

// TestIteratorWithUpdates tests iterator sees latest values.
func TestIteratorWithUpdates(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert initial values
	db.Put(nil, []byte("key"), []byte("value1"))

	// Update
	db.Put(nil, []byte("key"), []byte("value2"))
	db.Put(nil, []byte("key"), []byte("value3"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Fatal("Iterator should be valid")
	}

	if !bytes.Equal(iter.Value(), []byte("value3")) {
		t.Errorf("Value = %q, want value3", iter.Value())
	}
}

// TestIteratorAfterFlushMerged tests iterator works with flushed and memtable data.
func TestIteratorAfterFlushMerged(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert and flush
	for i := range 50 {
		key := fmt.Appendf(nil, "key%04d", i)
		db.Put(nil, key, []byte("value"))
	}
	db.Flush(nil)

	// Insert more (in memtable)
	for i := 50; i < 100; i++ {
		key := fmt.Appendf(nil, "key%04d", i)
		db.Put(nil, key, []byte("value"))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Count all keys
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 keys, got %d", count)
	}
}

// TestIteratorSnapshotIsolation tests iterator with snapshots.
func TestIteratorSnapshotIsolation(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Initial data
	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("b"), []byte("2"))

	// Take snapshot
	snapshot := db.GetSnapshot()
	defer db.ReleaseSnapshot(snapshot)

	// Modify data after snapshot
	db.Put(nil, []byte("a"), []byte("updated"))
	db.Put(nil, []byte("c"), []byte("3"))
	db.Delete(nil, []byte("b"))

	// Iterator with snapshot should see old data
	readOpts := DefaultReadOptions()
	readOpts.Snapshot = snapshot
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	var keys []string
	var values []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
	}

	if len(keys) != 2 {
		t.Errorf("Expected 2 keys with snapshot, got %d", len(keys))
	}
	if len(values) != 2 || values[0] != "1" || values[1] != "2" {
		t.Errorf("Values = %v, want [1, 2]", values)
	}

	// Iterator without snapshot should see new data
	iter2 := db.NewIterator(nil)
	defer iter2.Close()

	keys = nil
	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		keys = append(keys, string(iter2.Key()))
	}

	if len(keys) != 2 { // a (updated) and c (b was deleted)
		t.Errorf("Expected 2 keys without snapshot, got %d: %v", len(keys), keys)
	}
}

// TestIteratorRangeWithFlush tests iterator range after flush.
func TestIteratorRangeWithFlush(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert batch 1 and flush
	for i := range 10 {
		key := fmt.Appendf(nil, "a%02d", i)
		db.Put(nil, key, []byte("batch1"))
	}
	db.Flush(nil)

	// Insert batch 2 and flush
	for i := range 10 {
		key := fmt.Appendf(nil, "b%02d", i)
		db.Put(nil, key, []byte("batch2"))
	}
	db.Flush(nil)

	// Insert batch 3 (in memtable)
	for i := range 10 {
		key := fmt.Appendf(nil, "c%02d", i)
		db.Put(nil, key, []byte("batch3"))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to middle batch
	iter.Seek([]byte("b"))
	if !iter.Valid() {
		t.Fatal("Seek to 'b' should be valid")
	}

	// Count keys from b* onwards
	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
	}

	// Should have 10 (b*) + 10 (c*) = 20 keys
	if count != 20 {
		t.Errorf("Expected 20 keys from 'b' onwards, got %d", count)
	}
}

// TestIteratorStressTest performs a stress test on iterator operations.
func TestIteratorStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	const numKeys = 1000

	// Insert keys
	for i := range numKeys {
		key := fmt.Appendf(nil, "key%06d", i)
		value := fmt.Appendf(nil, "value%06d", i)
		db.Put(nil, key, value)
	}

	// Flush half and keep half in memtable
	db.Flush(nil)
	for i := numKeys; i < numKeys*2; i++ {
		key := fmt.Appendf(nil, "key%06d", i)
		value := fmt.Appendf(nil, "value%06d", i)
		db.Put(nil, key, value)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward scan
	forwardCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		forwardCount++
	}

	if forwardCount != numKeys*2 {
		t.Errorf("Forward count = %d, want %d", forwardCount, numKeys*2)
	}

	// Backward scan
	backwardCount := 0
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		backwardCount++
	}

	if backwardCount != numKeys*2 {
		t.Errorf("Backward count = %d, want %d", backwardCount, numKeys*2)
	}

	// Random seeks
	for i := range 100 {
		key := fmt.Sprintf("key%06d", i*20)
		iter.Seek([]byte(key))
		if !iter.Valid() {
			t.Errorf("Seek to %s should be valid", key)
		}
	}
}

// =============================================================================
// Edge Case Tests (ported from C++ RocksDB db/db_iterator_test.cc)
// =============================================================================

// TestIteratorSeekBeforePrev tests Seek then Prev pattern across multiple flushes.
// Port of: TEST_P(DBIteratorTest, IterSeekBeforePrev)
func TestIteratorSeekBeforePrev(t *testing.T) {

	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("b"))
	db.Put(nil, []byte("c"), []byte("d"))
	db.Flush(nil)

	db.Put(nil, []byte("0"), []byte("f"))
	db.Put(nil, []byte("1"), []byte("h"))
	db.Flush(nil)

	db.Put(nil, []byte("2"), []byte("j"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to c, then Prev, then Seek to a, then Prev
	iter.Seek([]byte("c"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Seek to 'c' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev from 'c' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}

	iter.Seek([]byte("a"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Seek to 'a' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("2")) {
		t.Fatalf("Prev from 'a' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}
}

// TestIteratorSeekForPrevBeforeNext tests SeekForPrev then Next pattern.
// Port of: TEST_P(DBIteratorTest, IterSeekForPrevBeforeNext)
func TestIteratorSeekForPrevBeforeNext(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("b"))
	db.Put(nil, []byte("c"), []byte("d"))
	db.Flush(nil)

	db.Put(nil, []byte("0"), []byte("f"))
	db.Put(nil, []byte("1"), []byte("h"))
	db.Flush(nil)

	db.Put(nil, []byte("2"), []byte("j"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	iter.SeekForPrev([]byte("0"))
	if iter.Valid() {
		t.Logf("SeekForPrev to '0' found: key=%q", iter.Key())
	}

	iter.Next()
	if iter.Valid() {
		t.Logf("Next found: key=%q", iter.Key())
	}

	iter.SeekForPrev([]byte("1"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("1")) {
		t.Fatalf("SeekForPrev to '1' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("2")) {
		t.Fatalf("Next from '1' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}
}

// TestIteratorLongKeys tests iterator with keys of varying lengths.
// Port of: TEST_P(DBIteratorTest, IterLongKeys)
func TestIteratorLongKeys(t *testing.T) {

	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	makeLongKey := func(length int, c byte) []byte {
		key := make([]byte, length)
		for i := range key {
			key[i] = c
		}
		return key
	}

	// Insert keys of varying lengths
	db.Put(nil, makeLongKey(20, 0), []byte("0"))
	db.Put(nil, makeLongKey(32, 2), []byte("2"))
	db.Put(nil, []byte("a"), []byte("b"))
	db.Flush(nil)

	db.Put(nil, makeLongKey(50, 1), []byte("1"))
	db.Put(nil, makeLongKey(127, 3), []byte("3"))
	db.Put(nil, makeLongKey(64, 4), []byte("4"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward iteration from key with length 20
	iter.Seek(makeLongKey(20, 0))
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("0")) {
		t.Fatalf("Seek to 20-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("1")) {
		t.Fatalf("Next to 50-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("2")) {
		t.Fatalf("Next to 32-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("3")) {
		t.Fatalf("Next to 127-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("4")) {
		t.Fatalf("Next to 64-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	// Backward iteration
	iter.SeekForPrev(makeLongKey(127, 3))
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("3")) {
		t.Fatalf("SeekForPrev to 127-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("2")) {
		t.Fatalf("Prev to 32-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Value(), []byte("1")) {
		t.Fatalf("Prev to 50-byte key failed: valid=%v, value=%q", iter.Valid(), iter.Value())
	}
}

// TestIteratorUpperBound tests iterate_upper_bound behavior.
// Port of: TEST_P(DBIteratorTest, DBIteratorBoundTest)
func TestIteratorUpperBound(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	db.Put(nil, []byte("a"), []byte("a"))
	db.Put(nil, []byte("b"), []byte("b"))
	db.Put(nil, []byte("c"), []byte("c"))
	db.Put(nil, []byte("d"), []byte("d"))
	db.Put(nil, []byte("e"), []byte("e"))
	db.Flush(nil)

	// Test with upper bound
	readOpts := DefaultReadOptions()
	readOpts.IterateUpperBound = []byte("d")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// Forward scan should stop before "d"
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 keys with upper bound 'd', got %d: %v", len(keys), keys)
	}

	for _, k := range keys {
		if k >= "d" {
			t.Errorf("Key %q should not be >= upper bound 'd'", k)
		}
	}

	// Seek to "b" and iterate
	iter.Seek([]byte("b"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Seek to 'b' failed")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Next from 'b' should find 'c'")
	}

	iter.Next()
	if iter.Valid() {
		t.Errorf("Next from 'c' should be invalid (upper bound 'd'), got key=%q", iter.Key())
	}
}

// TestIteratorLowerBound tests iterate_lower_bound behavior.
func TestIteratorLowerBound(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	db.Put(nil, []byte("a"), []byte("a"))
	db.Put(nil, []byte("b"), []byte("b"))
	db.Put(nil, []byte("c"), []byte("c"))
	db.Put(nil, []byte("d"), []byte("d"))
	db.Put(nil, []byte("e"), []byte("e"))
	db.Flush(nil)

	// Test with lower bound
	readOpts := DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("c")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// SeekToFirst should go to lower bound
	iter.SeekToFirst()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("SeekToFirst with lower bound 'c' should find 'c', got key=%q", iter.Key())
	}

	// Backward scan from end should stop at lower bound
	var keys []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 keys with lower bound 'c', got %d: %v", len(keys), keys)
	}

	for _, k := range keys {
		if k < "c" {
			t.Errorf("Key %q should not be < lower bound 'c'", k)
		}
	}
}

// TestIteratorBothBounds tests both upper and lower bounds together.
func TestIteratorBothBounds(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	for i := range 26 {
		key := []byte{byte('a' + i)}
		db.Put(nil, key, key)
	}
	db.Flush(nil)

	// Test with both bounds: [e, p)
	readOpts := DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("e")
	readOpts.IterateUpperBound = []byte("p")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	// Should have e,f,g,h,i,j,k,l,m,n,o = 11 keys
	if len(keys) != 11 {
		t.Errorf("Expected 11 keys in [e,p), got %d: %v", len(keys), keys)
	}

	if len(keys) > 0 && keys[0] != "e" {
		t.Errorf("First key should be 'e', got %q", keys[0])
	}
	if len(keys) > 0 && keys[len(keys)-1] != "o" {
		t.Errorf("Last key should be 'o', got %q", keys[len(keys)-1])
	}
}

// TestIteratorDirectionChange tests rapid direction changes.
// Port of: TEST_F(DBIteratorTest, ReseekUponDirectionChange)
func TestIteratorDirectionChange(t *testing.T) {

	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys across multiple flushes
	db.Put(nil, []byte("a"), []byte("a"))
	db.Put(nil, []byte("b"), []byte("b"))
	db.Flush(nil)

	db.Put(nil, []byte("c"), []byte("c"))
	db.Put(nil, []byte("d"), []byte("d"))
	db.Flush(nil)

	db.Put(nil, []byte("e"), []byte("e"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to middle, then change direction multiple times
	iter.Seek([]byte("c"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Initial seek failed")
	}

	// Next, then Prev, then Next
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("Next to 'd' failed: key=%q", iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Prev to 'c' failed: key=%q", iter.Key())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("Next to 'd' (again) failed: key=%q", iter.Key())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("e")) {
		t.Fatalf("Next to 'e' failed: key=%q", iter.Key())
	}

	// Now go all the way back
	iter.Prev()
	iter.Prev()
	iter.Prev()
	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Multiple Prev to 'a' failed: key=%q", iter.Key())
	}

	// And forward again
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Next to 'b' failed: key=%q", iter.Key())
	}
}

// TestIteratorPrevMaxSkip tests iterator behavior when many versions of a key exist.
// Port of: TEST_P(DBIteratorTest, IterPrevMaxSkip)
func TestIteratorPrevMaxSkip(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	db.Put(nil, []byte("a"), []byte("a"))
	db.Put(nil, []byte("c"), []byte("c"))

	// Create many versions of "b" (simulating max_sequential_skip_in_iterations)
	for range 20 {
		db.Put(nil, []byte("b"), []byte("b"))
	}

	db.Flush(nil)

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Start at "c" and go backward
	iter.Seek([]byte("c"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Seek to 'c' failed")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Prev to 'b' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev to 'a' failed: valid=%v, key=%q", iter.Valid(), iter.Key())
	}
}

// TestIteratorDeletedKeyNotVisible tests that deleted keys are not visible.
// BUG: SeekForPrev to a deleted key returns the deleted key instead of previous.
func TestIteratorDeletedKeyNotVisible(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("b"), []byte("2"))
	db.Put(nil, []byte("c"), []byte("3"))
	db.Flush(nil)

	// Delete middle key
	db.Delete(nil, []byte("b"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward scan should skip "b"
	iter.SeekToFirst()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("SeekToFirst failed")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Next should skip deleted 'b', got key=%q", iter.Key())
	}

	// Backward scan should also skip "b"
	iter.SeekToLast()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("SeekToLast failed")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev should skip deleted 'b', got key=%q", iter.Key())
	}

	// Seek directly to deleted key should go to next
	iter.Seek([]byte("b"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Seek to deleted 'b' should find 'c', got key=%q", iter.Key())
	}

	// SeekForPrev to deleted key should find previous valid key
	iter.SeekForPrev([]byte("b"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("SeekForPrev to deleted 'b' should find 'a', got key=%q", iter.Key())
	}
}

// TestIteratorCrossingMultipleFlushes tests iterator across many flushes.
func TestIteratorCrossingMultipleFlushes(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Create data across 5 flushes
	for flush := range 5 {
		for i := range 10 {
			key := fmt.Appendf(nil, "key%d%d", flush, i)
			db.Put(nil, key, fmt.Appendf(nil, "flush%d", flush))
		}
		db.Flush(nil)
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Count total keys
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 50 {
		t.Errorf("Expected 50 keys across 5 flushes, got %d", count)
	}

	// Verify backward scan finds same count
	backCount := 0
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		backCount++
	}

	if backCount != 50 {
		t.Errorf("Backward scan: expected 50 keys, got %d", backCount)
	}
}

// TestIteratorEmptyRange tests iterator with no keys in range.
func TestIteratorEmptyRange(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("z"), []byte("2"))
	db.Flush(nil)

	// Set bounds that exclude all keys: [b, c)
	readOpts := DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("b")
	readOpts.IterateUpperBound = []byte("c")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	iter.SeekToFirst()
	if iter.Valid() {
		t.Errorf("Expected no valid keys in range [b,c), got key=%q", iter.Key())
	}

	iter.SeekToLast()
	if iter.Valid() {
		t.Errorf("Expected no valid keys in range [b,c) for SeekToLast, got key=%q", iter.Key())
	}
}

// TestIteratorSeekToNonExistent tests seeking to keys that don't exist.
func TestIteratorSeekToNonExistent(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("e"), []byte("2"))
	db.Put(nil, []byte("i"), []byte("3"))
	db.Flush(nil)

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Seek to non-existent key between existing keys
	iter.Seek([]byte("c"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("e")) {
		t.Fatalf("Seek to 'c' should find 'e', got key=%q", iter.Key())
	}

	iter.Seek([]byte("g"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("i")) {
		t.Fatalf("Seek to 'g' should find 'i', got key=%q", iter.Key())
	}

	// SeekForPrev to non-existent key
	iter.SeekForPrev([]byte("c"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("SeekForPrev to 'c' should find 'a', got key=%q", iter.Key())
	}

	iter.SeekForPrev([]byte("g"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("e")) {
		t.Fatalf("SeekForPrev to 'g' should find 'e', got key=%q", iter.Key())
	}
}

// =============================================================================
// Additional Tests for Specific Scenarios
// =============================================================================

// TestIteratorSmallAndLargeMix tests iterator with mixed small and large values.
// Port of: TEST_P(DBIteratorTest, IterSmallAndLargeMix)
func TestIteratorSmallAndLargeMix(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	largeB := bytes.Repeat([]byte("b"), 100000)
	largeD := bytes.Repeat([]byte("d"), 100000)
	largeE := bytes.Repeat([]byte("e"), 100000)

	db.Put(nil, []byte("a"), []byte("va"))
	db.Put(nil, []byte("b"), largeB)
	db.Put(nil, []byte("c"), []byte("vc"))
	db.Put(nil, []byte("d"), largeD)
	db.Put(nil, []byte("e"), largeE)

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward scan
	iter.SeekToFirst()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("SeekToFirst: expected a")
	}
	if !bytes.Equal(iter.Value(), []byte("va")) {
		t.Fatalf("Value for a: expected va")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Next: expected b")
	}
	if !bytes.Equal(iter.Value(), largeB) {
		t.Fatalf("Value for b: wrong size, got %d, want 100000", len(iter.Value()))
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Next: expected c")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("Next: expected d")
	}
	if !bytes.Equal(iter.Value(), largeD) {
		t.Fatalf("Value for d: wrong size")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("e")) {
		t.Fatalf("Next: expected e")
	}

	iter.Next()
	if iter.Valid() {
		t.Fatal("Expected invalid after last key")
	}

	// Backward scan
	iter.SeekToLast()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("e")) {
		t.Fatalf("SeekToLast: expected e")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("Prev: expected d")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Prev: expected c")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Prev: expected b")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev: expected a")
	}

	iter.Prev()
	if iter.Valid() {
		t.Fatal("Expected invalid before first key")
	}

	// Seek to middle and verify large value
	iter.Seek([]byte("d"))
	if !iter.Valid() || !bytes.Equal(iter.Value(), largeD) {
		t.Fatalf("Seek to d: wrong value")
	}
}

// TestIteratorUpperBoundWithDirectionChange tests upper bound with direction change.
// Port of: TEST_P(DBIteratorTest, UpperBoundWithChangeDirection)
func TestIteratorUpperBoundWithDirectionChange(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Write keys across flush
	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("y"), []byte("1"))
	db.Put(nil, []byte("y1"), []byte("1"))
	db.Put(nil, []byte("y2"), []byte("1"))
	db.Put(nil, []byte("y3"), []byte("1"))
	db.Put(nil, []byte("z"), []byte("1"))
	db.Flush(nil)

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("z"), []byte("1"))
	db.Put(nil, []byte("bar"), []byte("1"))
	db.Put(nil, []byte("foo"), []byte("1"))

	// Iterator with upper bound "x"
	readOpts := DefaultReadOptions()
	readOpts.IterateUpperBound = []byte("x")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	iter.Seek([]byte("foo"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("foo")) {
		t.Fatalf("Seek to foo: expected foo, got %q", iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("bar")) {
		t.Fatalf("Prev from foo: expected bar, got %q", iter.Key())
	}
}

// TestIteratorSeekForPrevCrossingFiles tests SeekForPrev crossing multiple files.
// Port of: TEST_P(DBIteratorTest, IterSeekForPrevCrossingFiles)
func TestIteratorSeekForPrevCrossingFiles(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("b"), []byte("2"))
	db.Flush(nil)

	db.Put(nil, []byte("c"), []byte("3"))
	db.Put(nil, []byte("d"), []byte("4"))
	db.Flush(nil)

	db.Put(nil, []byte("e"), []byte("5"))
	db.Put(nil, []byte("f"), []byte("6"))

	iter := db.NewIterator(nil)
	defer iter.Close()

	// SeekForPrev to key in middle file
	iter.SeekForPrev([]byte("d"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("SeekForPrev to d: expected d, got %q", iter.Key())
	}

	// Go backward across file boundary
	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Prev: expected c, got %q", iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Prev: expected b, got %q", iter.Key())
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev: expected a, got %q", iter.Key())
	}

	// Go forward all the way
	for iter.Valid() {
		iter.Next()
	}

	// SeekForPrev to last key in memtable
	iter.SeekForPrev([]byte("f"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("f")) {
		t.Fatalf("SeekForPrev to f: expected f, got %q", iter.Key())
	}

	// Traverse backward through all files
	count := 0
	for iter.Valid() {
		count++
		iter.Prev()
	}
	if count != 6 {
		t.Fatalf("Expected 6 keys in backward traversal, got %d", count)
	}
}

// TestIteratorPrevKeyCrossingBlocks tests Prev across block boundaries.
// Port of: TEST_P(DBIteratorTest, IterPrevKeyCrossingBlocks)
func TestIteratorPrevKeyCrossingBlocks(t *testing.T) {
	opts := DefaultOptions()
	// Small block size to force multiple blocks
	opts.BlockSize = 100
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert enough keys to span multiple blocks
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		value := fmt.Appendf(nil, "value%03d", i)
		db.Put(nil, key, value)
	}
	db.Flush(nil)

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Start from end and go backward
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}

	// Count backward
	count := 0
	for iter.Valid() {
		count++
		iter.Prev()
	}
	if count != 100 {
		t.Fatalf("Expected 100 keys in backward scan, got %d", count)
	}

	// Forward scan for verification
	count = 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if count != 100 {
		t.Fatalf("Expected 100 keys in forward scan, got %d", count)
	}
}

// TestIteratorBoundMultiSeek tests multiple seeks with bounds.
// Port of: TEST_P(DBIteratorTest, DBIteratorBoundMultiSeek)
func TestIteratorBoundMultiSeek(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	// Insert keys
	for i := range 10 {
		key := fmt.Appendf(nil, "key%d", i)
		db.Put(nil, key, key)
	}
	db.Flush(nil)

	// Iterator with bounds [key3, key7)
	readOpts := DefaultReadOptions()
	readOpts.IterateLowerBound = []byte("key3")
	readOpts.IterateUpperBound = []byte("key7")

	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// Seek to each key within bounds
	for i := 3; i < 7; i++ {
		key := fmt.Appendf(nil, "key%d", i)
		iter.Seek(key)
		if !iter.Valid() || !bytes.Equal(iter.Key(), key) {
			t.Fatalf("Seek to %s: expected %s, got %q", key, key, iter.Key())
		}
	}

	// Seek before lower bound should go to lower bound
	iter.Seek([]byte("key0"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("key3")) {
		t.Fatalf("Seek before lower bound: expected key3, got %q", iter.Key())
	}

	// Seek at or after upper bound should be invalid
	iter.Seek([]byte("key7"))
	if iter.Valid() {
		t.Fatalf("Seek at upper bound should be invalid, got %q", iter.Key())
	}

	iter.Seek([]byte("key9"))
	if iter.Valid() {
		t.Fatalf("Seek past upper bound should be invalid")
	}
}

// TestIteratorDeleteMultiWithDelete tests iterator with interleaved deletes.
// Port of: TEST_P(DBIteratorTest, IterMultiWithDelete)
func TestIteratorDeleteMultiWithDelete(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	db.Put(nil, []byte("a"), []byte("va"))
	db.Put(nil, []byte("b"), []byte("vb"))
	db.Put(nil, []byte("c"), []byte("vc"))
	db.Delete(nil, []byte("b"))
	db.Put(nil, []byte("d"), []byte("vd"))
	db.Flush(nil) // Flush to apply delete

	iter := db.NewIterator(nil)
	defer iter.Close()

	// Forward should skip b
	iter.SeekToFirst()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("SeekToFirst: expected a")
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Next should skip b: expected c, got %q", iter.Key())
	}

	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("Next: expected d")
	}

	iter.Next()
	if iter.Valid() {
		t.Fatal("Expected invalid after last key")
	}

	// Backward should also skip b
	iter.SeekToLast()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("d")) {
		t.Fatalf("SeekToLast: expected d")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Prev: expected c")
	}

	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev should skip deleted 'b': expected a, got %q", iter.Key())
	}

	iter.Prev()
	if iter.Valid() {
		t.Fatal("Expected invalid before first key")
	}
}
