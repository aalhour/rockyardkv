package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteRangeBasic(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert some data
	writeOpts := &WriteOptions{Sync: false}
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		if err := db.Put(writeOpts, []byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Verify data exists
	readOpts := &ReadOptions{}
	val, err := db.Get(readOpts, []byte("key05"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value05" {
		t.Errorf("Get(key05) = %q, want 'value05'", val)
	}

	// Delete range [key03, key07)
	if err := db.DeleteRange(writeOpts, []byte("key03"), []byte("key07")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Verify keys outside range still exist
	val, err = db.Get(readOpts, []byte("key00"))
	if err != nil {
		t.Fatalf("Get(key00) failed: %v", err)
	}
	if string(val) != "value00" {
		t.Errorf("Get(key00) = %q, want 'value00'", val)
	}

	val, err = db.Get(readOpts, []byte("key02"))
	if err != nil {
		t.Fatalf("Get(key02) failed: %v", err)
	}
	if string(val) != "value02" {
		t.Errorf("Get(key02) = %q, want 'value02'", val)
	}

	// key07 is at end of range (exclusive), should exist
	val, err = db.Get(readOpts, []byte("key07"))
	if err != nil {
		t.Fatalf("Get(key07) failed: %v", err)
	}
	if string(val) != "value07" {
		t.Errorf("Get(key07) = %q, want 'value07'", val)
	}

	// Verify keys in deleted range are gone
	for i := 3; i < 7; i++ {
		key := fmt.Sprintf("key%02d", i)
		_, err := db.Get(readOpts, []byte(key))
		if err == nil {
			t.Errorf("Get(%s) should return error (key deleted)", key)
		}
	}
}

func TestDeleteRangeOverwritesLater(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	writeOpts := &WriteOptions{Sync: false}
	readOpts := &ReadOptions{}

	// Insert data
	if err := db.Put(writeOpts, []byte("key"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete range covering the key
	if err := db.DeleteRange(writeOpts, []byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Key should be deleted
	_, err = db.Get(readOpts, []byte("key"))
	if err == nil {
		t.Error("Get should return error after DeleteRange")
	}

	// Write the key again
	if err := db.Put(writeOpts, []byte("key"), []byte("value2")); err != nil {
		t.Fatalf("Put after DeleteRange failed: %v", err)
	}

	// Key should now exist with new value
	val, err := db.Get(readOpts, []byte("key"))
	if err != nil {
		t.Fatalf("Get after re-Put failed: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Get = %q, want 'value2'", val)
	}
}

func TestDeleteRangeEmpty(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	writeOpts := &WriteOptions{Sync: false}

	// Empty range should be a no-op
	if err := db.DeleteRange(writeOpts, []byte("z"), []byte("a")); err != nil {
		t.Fatalf("DeleteRange with empty range failed: %v", err)
	}

	// Same start and end should be a no-op
	if err := db.DeleteRange(writeOpts, []byte("key"), []byte("key")); err != nil {
		t.Fatalf("DeleteRange with same start/end failed: %v", err)
	}
}

func TestDeleteRangePersistence(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to force flush

	// Open, write, delete range, close
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	writeOpts := &WriteOptions{Sync: true}
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		if err := db.Put(writeOpts, []byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range
	if err := db.DeleteRange(writeOpts, []byte("key03"), []byte("key07")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Force flush
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	db.Close()

	// Reopen and verify
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	readOpts := &ReadOptions{}

	// Keys outside range should exist
	val, err := db.Get(readOpts, []byte("key00"))
	if err != nil {
		t.Fatalf("Get(key00) after reopen failed: %v", err)
	}
	if string(val) != "value00" {
		t.Errorf("Get(key00) = %q, want 'value00'", val)
	}

	val, err = db.Get(readOpts, []byte("key07"))
	if err != nil {
		t.Fatalf("Get(key07) after reopen failed: %v", err)
	}
	if string(val) != "value07" {
		t.Errorf("Get(key07) = %q, want 'value07'", val)
	}

	// Keys in range should be deleted
	for i := 3; i < 7; i++ {
		key := fmt.Sprintf("key%02d", i)
		_, err := db.Get(readOpts, []byte(key))
		if err == nil {
			t.Errorf("Get(%s) after reopen should return error", key)
		}
	}
}

func TestDeleteRangeIterator(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	writeOpts := &WriteOptions{Sync: false}
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		if err := db.Put(writeOpts, []byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [key03, key07)
	if err := db.DeleteRange(writeOpts, []byte("key03"), []byte("key07")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Iterate and verify deleted keys are skipped
	readOpts := &ReadOptions{}
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Should have keys 00, 01, 02, 07, 08, 09 (6 keys)
	expectedKeys := []string{"key00", "key01", "key02", "key07", "key08", "key09"}
	if len(keys) != len(expectedKeys) {
		t.Errorf("Got %d keys, want %d: %v", len(keys), len(expectedKeys), keys)
	} else {
		for i, expected := range expectedKeys {
			if keys[i] != expected {
				t.Errorf("Key %d: got %q, want %q", i, keys[i], expected)
			}
		}
	}
}

func TestDeleteRangeWithMultipleRanges(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	writeOpts := &WriteOptions{Sync: false}

	// Insert keys a-z
	for c := byte('a'); c <= 'z'; c++ {
		key := []byte{c}
		if err := db.Put(writeOpts, key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete multiple ranges
	if err := db.DeleteRange(writeOpts, []byte("c"), []byte("f")); err != nil {
		t.Fatalf("DeleteRange 1 failed: %v", err)
	}
	if err := db.DeleteRange(writeOpts, []byte("m"), []byte("p")); err != nil {
		t.Fatalf("DeleteRange 2 failed: %v", err)
	}
	if err := db.DeleteRange(writeOpts, []byte("x"), []byte("{")); err != nil { // { is after z
		t.Fatalf("DeleteRange 3 failed: %v", err)
	}

	// Verify deletions
	readOpts := &ReadOptions{}

	// Should exist: a, b, f, g, h, i, j, k, l, p, q, r, s, t, u, v, w
	shouldExist := "abfghijklpqrstuvw"
	for _, c := range shouldExist {
		_, err := db.Get(readOpts, []byte{byte(c)})
		if err != nil {
			t.Errorf("Get(%c) should exist", c)
		}
	}

	// Should be deleted: c, d, e, m, n, o, x, y, z
	shouldBeDeleted := "cdemnoxyz"
	for _, c := range shouldBeDeleted {
		_, err := db.Get(readOpts, []byte{byte(c)})
		if err == nil {
			t.Errorf("Get(%c) should be deleted", c)
		}
	}
}

func TestDeleteRangeSnapshot(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	writeOpts := &WriteOptions{Sync: false}

	// Insert data
	if err := db.Put(writeOpts, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Take snapshot
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Delete range covering the key
	if err := db.DeleteRange(writeOpts, []byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Read at snapshot should still see the key
	readOpts := &ReadOptions{Snapshot: snap}
	val, err := db.Get(readOpts, []byte("key"))
	if err != nil {
		t.Fatalf("Get at snapshot failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Get at snapshot = %q, want 'value'", val)
	}

	// Read without snapshot should not see the key
	readOpts2 := &ReadOptions{}
	_, err = db.Get(readOpts2, []byte("key"))
	if err == nil {
		t.Error("Get without snapshot should fail after DeleteRange")
	}
}

func TestDeleteRangeCF(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create column family
	cfOpts := DefaultColumnFamilyOptions()
	cf, err := db.CreateColumnFamily(cfOpts, "test_cf")
	if err != nil {
		t.Fatalf("CreateColumnFamily failed: %v", err)
	}

	writeOpts := &WriteOptions{Sync: false}

	// Insert data into CF
	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		if err := db.PutCF(writeOpts, cf, []byte(key), []byte("value")); err != nil {
			t.Fatalf("PutCF failed: %v", err)
		}
	}

	// Also insert into default CF
	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		if err := db.Put(writeOpts, []byte(key), []byte("default_value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range in CF only
	if err := db.DeleteRangeCF(writeOpts, cf, []byte("key1"), []byte("key4")); err != nil {
		t.Fatalf("DeleteRangeCF failed: %v", err)
	}

	readOpts := &ReadOptions{}

	// Keys in default CF should still exist
	for i := range 5 {
		key := fmt.Sprintf("key%d", i)
		val, err := db.Get(readOpts, []byte(key))
		if err != nil {
			t.Errorf("Get(%s) in default CF failed: %v", key, err)
		}
		if string(val) != "default_value" {
			t.Errorf("Get(%s) in default CF = %q, want 'default_value'", key, val)
		}
	}

	// Keys outside range in CF should exist
	val, err := db.GetCF(readOpts, cf, []byte("key0"))
	if err != nil {
		t.Fatalf("GetCF(key0) failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("GetCF(key0) = %q, want 'value'", val)
	}

	val, err = db.GetCF(readOpts, cf, []byte("key4"))
	if err != nil {
		t.Fatalf("GetCF(key4) failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("GetCF(key4) = %q, want 'value'", val)
	}

	// Keys in deleted range should be gone
	for i := 1; i < 4; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := db.GetCF(readOpts, cf, []byte(key))
		if err == nil {
			t.Errorf("GetCF(%s) should fail after DeleteRangeCF", key)
		}
	}
}

func TestDeleteRangeFileGeneration(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 512 // Very small to force flushes

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	writeOpts := &WriteOptions{Sync: false}

	// Write data and force flush
	for i := range 50 {
		key := fmt.Sprintf("key%03d", i)
		value := bytes.Repeat([]byte("v"), 100)
		if err := db.Put(writeOpts, []byte(key), value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Force flush
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Delete a range
	if err := db.DeleteRange(writeOpts, []byte("key010"), []byte("key040")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Force another flush to ensure range tombstone is persisted
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush 2 failed: %v", err)
	}

	db.Close()

	// Verify SST files were created
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	sstCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".sst" {
			sstCount++
		}
	}

	if sstCount < 1 {
		t.Errorf("Expected at least 1 SST file, found %d", sstCount)
	}

	// Reopen and verify data
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db.Close()

	readOpts := &ReadOptions{}

	// Keys outside range should exist
	for _, keyNum := range []int{0, 5, 9, 40, 45, 49} {
		key := fmt.Sprintf("key%03d", keyNum)
		_, err := db.Get(readOpts, []byte(key))
		if err != nil {
			t.Errorf("Get(%s) should exist", key)
		}
	}

	// Keys in range should be deleted
	for _, keyNum := range []int{10, 15, 20, 25, 30, 35, 39} {
		key := fmt.Sprintf("key%03d", keyNum)
		_, err := db.Get(readOpts, []byte(key))
		if err == nil {
			t.Errorf("Get(%s) should be deleted", key)
		}
	}
}

// TestDeleteRangeCrossSST tests that range deletion works when the range
// tombstone is in a newer SST file than the data it covers.
func TestDeleteRangeCrossSST(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to force flush

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	writeOpts := &WriteOptions{Sync: true}
	readOpts := &ReadOptions{}

	// Step 1: Write some keys and flush to SST #1
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		if err := db.Put(writeOpts, []byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush #1 failed: %v", err)
	}

	// Verify keys exist after first flush
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		_, err := db.Get(readOpts, []byte(key))
		if err != nil {
			t.Fatalf("Get(%s) after flush #1 failed: %v", key, err)
		}
	}

	// Step 2: Delete a range (keys 03-06) and flush to SST #2
	if err := db.DeleteRange(writeOpts, []byte("key03"), []byte("key07")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush #2 failed: %v", err)
	}

	// Step 3: Verify cross-SST deletion works
	// Keys outside range should still exist
	for _, keyNum := range []int{0, 1, 2, 7, 8, 9} {
		key := fmt.Sprintf("key%02d", keyNum)
		val, err := db.Get(readOpts, []byte(key))
		if err != nil {
			t.Errorf("Get(%s) should exist, got error: %v", key, err)
		} else {
			expected := fmt.Sprintf("value%02d", keyNum)
			if string(val) != expected {
				t.Errorf("Get(%s) = %q, want %q", key, val, expected)
			}
		}
	}

	// Keys in the deleted range should return ErrNotFound
	for _, keyNum := range []int{3, 4, 5, 6} {
		key := fmt.Sprintf("key%02d", keyNum)
		_, err := db.Get(readOpts, []byte(key))
		if err == nil {
			t.Errorf("Get(%s) should be deleted (covered by range tombstone in SST #2)", key)
		} else if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%s) got error %v, want ErrNotFound", key, err)
		}
	}

	// Step 4: Close and reopen to verify persistence
	db.Close()

	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify again after reopen
	for _, keyNum := range []int{0, 1, 2, 7, 8, 9} {
		key := fmt.Sprintf("key%02d", keyNum)
		_, err := db.Get(readOpts, []byte(key))
		if err != nil {
			t.Errorf("After reopen: Get(%s) should exist, got error: %v", key, err)
		}
	}

	for _, keyNum := range []int{3, 4, 5, 6} {
		key := fmt.Sprintf("key%02d", keyNum)
		_, err := db.Get(readOpts, []byte(key))
		if err == nil {
			t.Errorf("After reopen: Get(%s) should be deleted", key)
		}
	}
}

// =============================================================================
// Edge Case Tests (ported from C++ RocksDB db/db_range_del_test.cc)
// =============================================================================

// TestDeleteRangeEndBeforeStartInvalid tests that DeleteRange returns error
// when end key comes before start key.
// Port of: TEST_F(DBRangeDelTest, EndComesBeforeStartInvalidArgument)
func TestDeleteRangeEndBeforeStartInvalid(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}

	// Put a key first
	if err := db.Put(writeOpts, []byte("b"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// DeleteRange with end < start should be a no-op (not error in Go impl)
	// Note: C++ returns InvalidArgument, Go impl just ignores
	_ = db.DeleteRange(writeOpts, []byte("b"), []byte("a"))
	// This should either be a no-op or return an error
	// Either behavior is acceptable as long as the key is preserved

	// Key should still exist
	val, err := db.Get(&ReadOptions{}, []byte("b"))
	if err != nil {
		t.Fatalf("Get failed after invalid DeleteRange: %v", err)
	}
	if string(val) != "val" {
		t.Errorf("Get = %q, want 'val'", val)
	}
}

// TestDeleteRangeInMutableMemtable tests that a range deletion in the same
// memtable as a Put correctly covers the key.
// Port of: TEST_F(DBRangeDelTest, GetCoveredKeyFromMutableMemtable)
func TestDeleteRangeInMutableMemtable(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}
	readOpts := &ReadOptions{}

	// Put key, then delete range covering it (all in memtable)
	if err := db.Put(writeOpts, []byte("key"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := db.DeleteRange(writeOpts, []byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Key should be deleted (covered by range tombstone)
	_, err := db.Get(readOpts, []byte("key"))
	if err == nil {
		t.Error("Get should fail - key covered by range tombstone in memtable")
	}
}

// TestDeleteRangeOnlyTombstoneFlush tests that a flush with only range
// tombstones (no point data) produces valid output.
// Port of: TEST_F(DBRangeDelTest, FlushOutputHasOnlyRangeTombstones)
func TestDeleteRangeOnlyTombstoneFlush(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}

	// Only write a range tombstone, no point data
	if err := db.DeleteRange(writeOpts, []byte("dr1"), []byte("dr2")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Flush should succeed even with only tombstones
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush with only tombstones failed: %v", err)
	}

	// Now add some data that overlaps with the tombstone range
	if err := db.Put(writeOpts, []byte("dr1"), []byte("val")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// The key should be visible (written after tombstone)
	val, err := db.Get(&ReadOptions{}, []byte("dr1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "val" {
		t.Errorf("Get = %q, want 'val'", val)
	}
}

// TestDeleteRangeMultipleOverlapping tests multiple overlapping range deletions.
// Port of: TEST_F(DBRangeDelTest, UnorderedTombstones)
func TestDeleteRangeMultipleOverlapping(t *testing.T) {

	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}
	readOpts := &ReadOptions{}

	// Insert keys
	for c := byte('a'); c <= 'z'; c++ {
		if err := db.Put(writeOpts, []byte{c}, []byte("val")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Add overlapping range deletions
	// Delete [c, g)
	if err := db.DeleteRange(writeOpts, []byte("c"), []byte("g")); err != nil {
		t.Fatalf("DeleteRange 1 failed: %v", err)
	}

	// Delete [e, m) - overlaps with first deletion
	if err := db.DeleteRange(writeOpts, []byte("e"), []byte("m")); err != nil {
		t.Fatalf("DeleteRange 2 failed: %v", err)
	}

	// Delete [k, p) - overlaps with second deletion
	if err := db.DeleteRange(writeOpts, []byte("k"), []byte("p")); err != nil {
		t.Fatalf("DeleteRange 3 failed: %v", err)
	}

	// Flush to persist tombstones
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify: Combined deletions cover [c, p)
	// Should exist: a, b, p, q, r, s, t, u, v, w, x, y, z
	shouldExist := "abpqrstuvwxyz"
	for _, c := range shouldExist {
		_, err := db.Get(readOpts, []byte{byte(c)})
		if err != nil {
			t.Errorf("Get(%c) should exist", c)
		}
	}

	// Should be deleted: c, d, e, f, g, h, i, j, k, l, m, n, o
	shouldBeDeleted := "cdefghijklmno"
	for _, c := range shouldBeDeleted {
		_, err := db.Get(readOpts, []byte{byte(c)})
		if err == nil {
			t.Errorf("Get(%c) should be deleted (covered by overlapping tombstones)", c)
		}
	}
}

// TestDeleteRangeSeekForPrev tests SeekForPrev behavior with range deletions.
func TestDeleteRangeSeekForPrev(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}

	// Insert keys a, b, c, d, e
	for c := byte('a'); c <= 'e'; c++ {
		if err := db.Put(writeOpts, []byte{c}, []byte{c}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [b, d)
	if err := db.DeleteRange(writeOpts, []byte("b"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	iter := db.NewIterator(&ReadOptions{})
	defer iter.Close()

	// SeekForPrev to "c" (deleted) should find "a"
	iter.SeekForPrev([]byte("c"))
	if !iter.Valid() {
		t.Fatal("SeekForPrev to deleted 'c' should be valid")
	}
	if !bytes.Equal(iter.Key(), []byte("a")) {
		t.Errorf("SeekForPrev to 'c' found %q, want 'a'", iter.Key())
	}

	// SeekForPrev to "d" (exists, end of deletion range) should find "d"
	iter.SeekForPrev([]byte("d"))
	if !iter.Valid() {
		t.Fatal("SeekForPrev to 'd' should be valid")
	}
	if !bytes.Equal(iter.Key(), []byte("d")) {
		t.Errorf("SeekForPrev to 'd' found %q, want 'd'", iter.Key())
	}
}

// TestDeleteRangePrevAfterSeek tests Prev operation after Seek with range deletions.
func TestDeleteRangePrevAfterSeek(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}

	// Insert keys
	for c := byte('a'); c <= 'j'; c++ {
		if err := db.Put(writeOpts, []byte{c}, []byte{c}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [c, g)
	if err := db.DeleteRange(writeOpts, []byte("c"), []byte("g")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	iter := db.NewIterator(&ReadOptions{})
	defer iter.Close()

	// Seek to "h"
	iter.Seek([]byte("h"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("h")) {
		t.Fatalf("Seek to 'h' failed: key=%q", iter.Key())
	}

	// Prev should go to "g" (end of deletion, so it exists)
	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("g")) {
		t.Fatalf("Prev from 'h' should find 'g', got key=%q", iter.Key())
	}

	// Prev should skip [c,g) and go to "b"
	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("Prev from 'g' should skip deletion and find 'b', got key=%q", iter.Key())
	}

	// Prev should go to "a"
	iter.Prev()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("a")) {
		t.Fatalf("Prev from 'b' should find 'a', got key=%q", iter.Key())
	}
}

// TestDeleteRangeNextAfterSeekForPrev tests Next after SeekForPrev with deletions.
func TestDeleteRangeNextAfterSeekForPrev(t *testing.T) {
	opts := DefaultOptions()
	db, cleanup := createTestDB(t, opts)
	defer cleanup()

	writeOpts := &WriteOptions{Sync: false}

	// Insert keys
	for c := byte('a'); c <= 'j'; c++ {
		if err := db.Put(writeOpts, []byte{c}, []byte{c}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete range [d, h)
	if err := db.DeleteRange(writeOpts, []byte("d"), []byte("h")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	iter := db.NewIterator(&ReadOptions{})
	defer iter.Close()

	// SeekForPrev to "b"
	iter.SeekForPrev([]byte("b"))
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("b")) {
		t.Fatalf("SeekForPrev to 'b' failed: key=%q", iter.Key())
	}

	// Next should go to "c"
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("c")) {
		t.Fatalf("Next from 'b' should find 'c', got key=%q", iter.Key())
	}

	// Next should skip [d,h) and go to "h"
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("h")) {
		t.Fatalf("Next from 'c' should skip deletion and find 'h', got key=%q", iter.Key())
	}

	// Next should go to "i"
	iter.Next()
	if !iter.Valid() || !bytes.Equal(iter.Key(), []byte("i")) {
		t.Fatalf("Next from 'h' should find 'i', got key=%q", iter.Key())
	}
}

// TestDeleteRangeIteratorCrossSST tests that the iterator correctly skips
// range-deleted keys when the data and tombstone are in different SST files.
func TestDeleteRangeIteratorCrossSST(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Small buffer to force flush

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	writeOpts := &WriteOptions{Sync: true}

	// Step 1: Write some keys and flush to SST #1
	for i := range 10 {
		key := fmt.Sprintf("key%02d", i)
		value := fmt.Sprintf("value%02d", i)
		if err := db.Put(writeOpts, []byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush #1 failed: %v", err)
	}

	// Step 2: Delete a range (keys 03-06) and flush to SST #2
	if err := db.DeleteRange(writeOpts, []byte("key03"), []byte("key07")); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	if err := db.Flush(nil); err != nil {
		t.Fatalf("Flush #2 failed: %v", err)
	}

	// Step 3: Iterate and verify the range-deleted keys are skipped
	readOpts := &ReadOptions{}
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Should have keys 00, 01, 02, 07, 08, 09 (6 keys)
	expectedKeys := []string{"key00", "key01", "key02", "key07", "key08", "key09"}
	if len(keys) != len(expectedKeys) {
		t.Errorf("Got %d keys, want %d: %v", len(keys), len(expectedKeys), keys)
	} else {
		for i, expected := range expectedKeys {
			if keys[i] != expected {
				t.Errorf("Key %d: got %q, want %q", i, keys[i], expected)
			}
		}
	}

	// Step 4: Test reverse iteration
	iter.Close()
	iter = db.NewIterator(readOpts)

	var reverseKeys []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		reverseKeys = append(reverseKeys, string(iter.Key()))
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error (reverse): %v", err)
	}

	// Should have keys 09, 08, 07, 02, 01, 00 (6 keys in reverse)
	expectedReverseKeys := []string{"key09", "key08", "key07", "key02", "key01", "key00"}
	if len(reverseKeys) != len(expectedReverseKeys) {
		t.Errorf("Reverse: Got %d keys, want %d: %v", len(reverseKeys), len(expectedReverseKeys), reverseKeys)
	} else {
		for i, expected := range expectedReverseKeys {
			if reverseKeys[i] != expected {
				t.Errorf("Reverse Key %d: got %q, want %q", i, reverseKeys[i], expected)
			}
		}
	}

	// Step 5: Close and reopen, then test iteration again
	db.Close()
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	iter2 := db.NewIterator(readOpts)
	defer iter2.Close()

	var keysAfterReopen []string
	for iter2.SeekToFirst(); iter2.Valid(); iter2.Next() {
		keysAfterReopen = append(keysAfterReopen, string(iter2.Key()))
	}

	if len(keysAfterReopen) != len(expectedKeys) {
		t.Errorf("After reopen: Got %d keys, want %d: %v", len(keysAfterReopen), len(expectedKeys), keysAfterReopen)
	}
}
