// db_compaction_test.go - Compaction behavior tests
//
// These tests verify compaction correctness: deletion markers, L0 ordering,
// overlapping files, and the interaction between compaction and reads.

package db

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// =============================================================================
// L0 File Ordering Tests
// L0 files can overlap, so ordering by file number (newest first) is critical.
// =============================================================================

func TestGetLevel0Ordering(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to trigger flushes

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("ordering_key")

	// Write and flush multiple times with different values
	for i := range 5 {
		db.Put(nil, key, fmt.Appendf(nil, "v%d", i))
		db.Flush(nil)
	}

	// Should see the latest value
	val, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(val) != "v4" {
		t.Errorf("Get = %s, want v4 (latest)", val)
	}
}

func TestGetOrderedByLevels(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("level_key")

	// Write, flush, overwrite, flush multiple times
	db.Put(nil, key, []byte("first"))
	db.Flush(nil)

	db.Put(nil, key, []byte("second"))
	db.Flush(nil)

	db.Put(nil, key, []byte("third"))
	db.Flush(nil)

	// Should see latest
	val, _ := db.Get(nil, key)
	if string(val) != "third" {
		t.Errorf("Get = %s, want third", val)
	}
}

func TestOverlapInLevel0(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Create overlapping key ranges in L0
	// File 1: a, b, c
	db.Put(nil, []byte("a"), []byte("file1"))
	db.Put(nil, []byte("b"), []byte("file1"))
	db.Put(nil, []byte("c"), []byte("file1"))
	db.Flush(nil)

	// File 2: b, c, d (overlaps with file 1 on b, c)
	db.Put(nil, []byte("b"), []byte("file2"))
	db.Put(nil, []byte("c"), []byte("file2"))
	db.Put(nil, []byte("d"), []byte("file2"))
	db.Flush(nil)

	// Should see file2's values for b, c (newer)
	valB, _ := db.Get(nil, []byte("b"))
	valC, _ := db.Get(nil, []byte("c"))
	if string(valB) != "file2" || string(valC) != "file2" {
		t.Errorf("b=%s, c=%s, want file2 for both", valB, valC)
	}

	// Should see file1's value for a (only in file1)
	valA, _ := db.Get(nil, []byte("a"))
	if string(valA) != "file1" {
		t.Errorf("a=%s, want file1", valA)
	}
}

// =============================================================================
// Deletion Marker Tests
// These verify that deletion markers correctly shadow older values.
// =============================================================================

func TestDeletionMarkers1(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Put, flush, delete
	db.Put(nil, []byte("key"), []byte("value"))
	db.Flush(nil)
	db.Delete(nil, []byte("key"))

	// Key should not be found
	_, err := db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Key should be deleted")
	}
}

func TestDeletionMarkers2(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Put, flush, delete, flush
	db.Put(nil, []byte("key"), []byte("value"))
	db.Flush(nil)
	db.Delete(nil, []byte("key"))
	db.Flush(nil)

	// Key should not be found
	_, err := db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Key should be deleted after flush")
	}
}

func TestDeletionMarkersWithSnapshot(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("value"))
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Delete(nil, []byte("key"))

	// Current view: deleted
	_, err := db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Current view should see deletion")
	}

	// Snapshot view: still exists
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, _ := db.Get(snapOpts, []byte("key"))
	if string(val) != "value" {
		t.Error("Snapshot should still see value")
	}
}

func TestDeletionMarkersIterator(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Create: a, b, c
	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("b"), []byte("2"))
	db.Put(nil, []byte("c"), []byte("3"))
	db.Flush(nil)

	// Delete b
	db.Delete(nil, []byte("b"))

	// Iterator should skip b
	iter := db.NewIterator(nil)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Errorf("Iterator keys = %v, want [a c]", keys)
	}
}

// =============================================================================
// Compaction Trigger Tests
// =============================================================================

func TestCompactionAfterMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer

	db, _ := Open(dir, opts)
	defer db.Close()

	// Generate many flushes to trigger compaction
	for round := range 10 {
		for i := range 50 {
			key := fmt.Appendf(nil, "key_%03d_%03d", round, i)
			db.Put(nil, key, bytes.Repeat([]byte("v"), 100))
		}
		db.Flush(nil)
	}

	// Data should still be accessible
	for round := range 10 {
		for i := range 50 {
			key := fmt.Appendf(nil, "key_%03d_%03d", round, i)
			_, err := db.Get(nil, key)
			if err != nil {
				t.Errorf("key_%03d_%03d not found", round, i)
			}
		}
	}
}

func TestCompactionWithOverwrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("overwrite_key")

	// Many overwrites with flushes
	for i := range 20 {
		db.Put(nil, key, fmt.Appendf(nil, "v%d", i))
		if i%5 == 0 {
			db.Flush(nil)
		}
	}

	// Should see latest
	val, _ := db.Get(nil, key)
	if string(val) != "v19" {
		t.Errorf("Get = %s, want v19", val)
	}
}

func TestCompactionWithDeletes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Create keys
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "del_key%03d", i), []byte("value"))
	}
	db.Flush(nil)

	// Delete even keys (0, 2, 4, ..., 98)
	for i := 0; i < 100; i += 2 {
		db.Delete(nil, fmt.Appendf(nil, "del_key%03d", i))
	}
	// Don't flush the deletions - keep them in memtable

	// Count remaining - should see 50 odd keys
	iter := db.NewIterator(nil)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 50 {
		t.Errorf("Count = %d, want 50", count)
	}
}

// =============================================================================
// Hidden Values Tests
// These verify that old versions are properly hidden by newer writes.
// =============================================================================

func TestHiddenValuesAreRemoved(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("hidden_key")

	// Write v1, take snapshot, write v2, delete
	db.Put(nil, key, []byte("v1"))
	snap := db.GetSnapshot()

	db.Put(nil, key, []byte("v2"))
	db.Delete(nil, key)

	// Current: not found
	_, err := db.Get(nil, []byte("hidden_key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Current view should not see deleted key")
	}

	// Snapshot: sees v1
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, _ := db.Get(snapOpts, []byte("hidden_key"))
	if string(val) != "v1" {
		t.Errorf("Snapshot = %s, want v1", val)
	}

	db.ReleaseSnapshot(snap)
}

func TestGetPicksCorrectFile(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Create non-overlapping files at different key ranges
	// File 1: a-m
	for c := 'a'; c <= 'm'; c++ {
		db.Put(nil, []byte{byte(c)}, []byte("file1"))
	}
	db.Flush(nil)

	// File 2: n-z
	for c := 'n'; c <= 'z'; c++ {
		db.Put(nil, []byte{byte(c)}, []byte("file2"))
	}
	db.Flush(nil)

	// Get should find the right file
	valA, _ := db.Get(nil, []byte("a"))
	valZ, _ := db.Get(nil, []byte("z"))

	if string(valA) != "file1" {
		t.Errorf("a = %s, want file1", valA)
	}
	if string(valZ) != "file2" {
		t.Errorf("z = %s, want file2", valZ)
	}
}

// =============================================================================
// Ported from C++ RocksDB: db/db_compaction_test.cc
// Reference: RocksDB v10.7.5
// =============================================================================

// TestUserKeyCrossFile1 verifies that a deletion marker properly shadows
// a key that exists in a different SST file after compaction.
//
// Reference: db/db_compaction_test.cc - TEST_F(DBCompactionTest, UserKeyCrossFile1)
func TestUserKeyCrossFile1(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Level0FileNumCompactionTrigger = 3

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Create first file and flush to L0
	if err := db.Put(nil, []byte("4"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(nil, []byte("3"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Second file: write "2", delete "3"
	if err := db.Put(nil, []byte("2"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Delete(nil, []byte("3")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Key "3" should NOT be found (deleted in second file)
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound before compaction, got %v", err)
	}

	// Force compaction to move files to L1
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Key "3" should still NOT be found after compaction
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound after compaction, got %v", err)
	}

	// Generate more L0 files to trigger another compaction
	for range 3 {
		if err := db.Put(nil, []byte("2"), []byte("B")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := db.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	// Wait for compaction
	if err := db.WaitForCompact(nil); err != nil {
		t.Fatalf("WaitForCompact failed: %v", err)
	}

	// Key "3" should STILL not be found
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound after second compaction, got %v", err)
	}
}

// TestUserKeyCrossFile2 is similar to TestUserKeyCrossFile1 but uses SingleDelete.
// SingleDelete is an optimization for keys that are only written once.
//
// Reference: db/db_compaction_test.cc - TEST_F(DBCompactionTest, UserKeyCrossFile2)
func TestUserKeyCrossFile2(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Level0FileNumCompactionTrigger = 3

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Create first file and flush to L0
	if err := db.Put(nil, []byte("4"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Put(nil, []byte("3"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Second file: write "2", SingleDelete "3"
	if err := db.Put(nil, []byte("2"), []byte("A")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.SingleDelete(nil, []byte("3")); err != nil {
		t.Fatalf("SingleDelete failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Key "3" should NOT be found
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound before compaction, got %v", err)
	}

	// Force compaction
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Key "3" should still NOT be found
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound after compaction, got %v", err)
	}

	// Generate more L0 files
	for range 3 {
		if err := db.Put(nil, []byte("2"), []byte("B")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := db.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	// Wait for compaction
	if err := db.WaitForCompact(nil); err != nil {
		t.Fatalf("WaitForCompact failed: %v", err)
	}

	// Key "3" should STILL not be found
	_, err = db.Get(nil, []byte("3"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(3) should return ErrNotFound after second compaction, got %v", err)
	}
}

// TestManualCompactionBasic tests that manual CompactRange works correctly.
//
// Reference: db/db_compaction_test.cc - TEST_P(DBCompactionTestWithParam, ManualCompaction)
func TestManualCompactionBasic(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Level0FileNumCompactionTrigger = 10 // High trigger to prevent auto-compaction

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write data and flush multiple times to create L0 files
	for round := range 5 {
		for i := range 20 {
			key := fmt.Appendf(nil, "key_%03d_%03d", round, i)
			if err := db.Put(nil, key, bytes.Repeat([]byte("v"), 100)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := db.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	// Trigger manual compaction of full range
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Verify all data is still accessible
	for round := range 5 {
		for i := range 20 {
			key := fmt.Appendf(nil, "key_%03d_%03d", round, i)
			_, err := db.Get(nil, key)
			if err != nil {
				t.Errorf("Get(%s) failed: %v", key, err)
			}
		}
	}
}

// TestManualCompactionWithRange tests CompactRange with specific key ranges.
//
// Reference: db/db_compaction_test.cc - manual compaction with begin/end keys
func TestManualCompactionWithRange(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Create data with keys: a, b, c, d, e
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := db.Put(nil, []byte(k), []byte("value_"+k)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Create more data for overlap
	for _, k := range []string{"b", "c", "d"} {
		if err := db.Put(nil, []byte(k), []byte("updated_"+k)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Compact only range [b, d]
	begin := []byte("b")
	end := []byte("d")
	if err := db.CompactRange(nil, begin, end); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Verify data
	val, err := db.Get(nil, []byte("b"))
	if err != nil || string(val) != "updated_b" {
		t.Errorf("Get(b) = %s, %v; want updated_b", val, err)
	}
	val, err = db.Get(nil, []byte("c"))
	if err != nil || string(val) != "updated_c" {
		t.Errorf("Get(c) = %s, %v; want updated_c", val, err)
	}
}

// TestCompactionPreservesDeleteMarkers verifies that delete markers are
// correctly handled during compaction when there are snapshots.
//
// Reference: Compaction should not drop delete markers visible to snapshots
func TestCompactionPreservesDeleteMarkers(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Write initial data
	if err := db.Put(nil, []byte("key"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Take snapshot before delete
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Delete the key
	if err := db.Delete(nil, []byte("key")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Trigger compaction
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Current view should see the deletion
	_, err = db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Current view should see deletion, got %v", err)
	}

	// Snapshot view should still see the original value
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, err := db.Get(snapOpts, []byte("key"))
	if err != nil {
		t.Errorf("Snapshot view should see value, got %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Snapshot value = %s, want value1", val)
	}
}

// TestCompactionWithMergeOperator verifies merge operations survive compaction.
func TestCompactionWithMergeOperator(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &StringAppendOperator{Delimiter: ","}

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("merge_key")

	// Initial value
	if err := db.Put(nil, key, []byte("a")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	val, _ := db.Get(nil, key)
	t.Logf("After Put(a) + Flush: %s", val)

	// Merge operations
	for _, v := range []string{"b", "c", "d"} {
		if err := db.Merge(nil, key, []byte(v)); err != nil {
			t.Fatalf("Merge failed: %v", err)
		}
		if err := db.Flush(nil); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		val, _ := db.Get(nil, key)
		t.Logf("After Merge(%s) + Flush: %s", v, val)
	}

	// Compact
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Verify merged value
	val, err = db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "a,b,c,d" {
		t.Errorf("Merged value = %s, want a,b,c,d", val)
	}
}

// TestL0CompactionBugIssue44a is a regression test from LevelDB.
// It tests a specific sequence of operations that caused a bug.
//
// Reference: db/db_compaction_test.cc - TEST_F(DBCompactionTest, L0_CompactionBug_Issue44_a)
func TestL0CompactionBugIssue44a(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	// Sequence: Put b, reopen, delete b, delete a, reopen, delete a, reopen, put a, reopen
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Put(nil, []byte("b"), []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	db.Close()

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen 1 failed: %v", err)
	}
	if err := db.Delete(nil, []byte("b")); err != nil {
		t.Fatalf("Delete b failed: %v", err)
	}
	if err := db.Delete(nil, []byte("a")); err != nil {
		t.Fatalf("Delete a failed: %v", err)
	}
	db.Close()

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen 2 failed: %v", err)
	}
	if err := db.Delete(nil, []byte("a")); err != nil {
		t.Fatalf("Delete a 2 failed: %v", err)
	}
	db.Close()

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen 3 failed: %v", err)
	}
	if err := db.Put(nil, []byte("a"), []byte("v")); err != nil {
		t.Fatalf("Put a failed: %v", err)
	}
	db.Close()

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Reopen 4 failed: %v", err)
	}
	defer db.Close()

	// Wait for any compaction
	db.WaitForCompact(nil)

	// Verify: should see only "a" -> "v"
	val, err := db.Get(nil, []byte("a"))
	if err != nil {
		t.Errorf("Get(a) failed: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("Get(a) = %s, want v", val)
	}

	_, err = db.Get(nil, []byte("b"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(b) should be not found, got %v", err)
	}
}

// TestCompactionEmptyKeyValue tests compaction with empty keys and values.
func TestCompactionEmptyKeyValue(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Empty key with value
	if err := db.Put(nil, []byte(""), []byte("empty_key")); err != nil {
		t.Fatalf("Put empty key failed: %v", err)
	}
	// Key with empty value
	if err := db.Put(nil, []byte("empty_value"), []byte("")); err != nil {
		t.Fatalf("Put empty value failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Update them
	if err := db.Put(nil, []byte(""), []byte("updated")); err != nil {
		t.Fatalf("Put empty key 2 failed: %v", err)
	}
	if err := db.Put(nil, []byte("empty_value"), []byte("now_has_value")); err != nil {
		t.Fatalf("Put empty value 2 failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Compact
	if err := db.CompactRange(nil, nil, nil); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Verify
	val, err := db.Get(nil, []byte(""))
	if err != nil || string(val) != "updated" {
		t.Errorf("Empty key = %s, %v; want updated", val, err)
	}
	val, err = db.Get(nil, []byte("empty_value"))
	if err != nil || string(val) != "now_has_value" {
		t.Errorf("empty_value key = %s, %v; want now_has_value", val, err)
	}
}
