// db_snapshot_test.go - Snapshot isolation and consistency tests
//
// These tests verify that snapshots provide a consistent point-in-time view
// of the database, isolating readers from concurrent writes.

package db

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// =============================================================================
// Basic Snapshot Tests
// =============================================================================

func TestSnapshotBasic(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("v1"))

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Put(nil, []byte("key"), []byte("v2"))

	// Current view sees v2
	val, _ := db.Get(nil, []byte("key"))
	if string(val) != "v2" {
		t.Errorf("Current view = %s, want v2", val)
	}

	// Snapshot sees v1
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, _ = db.Get(snapOpts, []byte("key"))
	if string(val) != "v1" {
		t.Errorf("Snapshot view = %s, want v1", val)
	}
}

func TestSnapshotWithDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("before"))

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Delete(nil, []byte("key"))

	// Current view: deleted
	_, err := db.Get(nil, []byte("key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Current view should be NotFound")
	}

	// Snapshot view: still exists
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, err := db.Get(snapOpts, []byte("key"))
	if err != nil {
		t.Fatalf("Snapshot get error: %v", err)
	}
	if string(val) != "before" {
		t.Errorf("Snapshot should see 'before', got %s", val)
	}
}

func TestSnapshotMultiple(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("v1"))
	snap1 := db.GetSnapshot()

	db.Put(nil, []byte("key"), []byte("v2"))
	snap2 := db.GetSnapshot()

	db.Put(nil, []byte("key"), []byte("v3"))
	snap3 := db.GetSnapshot()

	// Each snapshot sees its version
	opts1 := DefaultReadOptions()
	opts1.Snapshot = snap1
	v1, _ := db.Get(opts1, []byte("key"))

	opts2 := DefaultReadOptions()
	opts2.Snapshot = snap2
	v2, _ := db.Get(opts2, []byte("key"))

	opts3 := DefaultReadOptions()
	opts3.Snapshot = snap3
	v3, _ := db.Get(opts3, []byte("key"))

	if string(v1) != "v1" || string(v2) != "v2" || string(v3) != "v3" {
		t.Errorf("Snapshot values: v1=%s, v2=%s, v3=%s", v1, v2, v3)
	}

	db.ReleaseSnapshot(snap1)
	db.ReleaseSnapshot(snap2)
	db.ReleaseSnapshot(snap3)
}

func TestSnapshotAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("v1"))
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Flush(nil)
	db.Put(nil, []byte("key"), []byte("v2"))

	// Snapshot still sees v1 even after flush
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, _ := db.Get(snapOpts, []byte("key"))
	if string(val) != "v1" {
		t.Errorf("Snapshot after flush = %s, want v1", val)
	}
}

// =============================================================================
// Snapshot Iterator Tests
// =============================================================================

func TestSnapshotIterator(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("a"), []byte("1"))
	db.Put(nil, []byte("b"), []byte("2"))

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Put(nil, []byte("c"), []byte("3"))
	db.Delete(nil, []byte("a"))

	// Snapshot iterator sees old state
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	iter := db.NewIterator(snapOpts)
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("Snapshot iterator keys = %v, want [a b]", keys)
	}
}

func TestSnapshotIteratorWithUpdates(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	for i := range 10 {
		db.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("v1"))
	}

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Modify data
	for i := range 10 {
		db.Put(nil, fmt.Appendf(nil, "key%d", i), []byte("v2"))
	}

	// Snapshot iterator sees v1 for all
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	iter := db.NewIterator(snapOpts)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if string(iter.Value()) != "v1" {
			t.Errorf("Key %s: got %s, want v1", iter.Key(), iter.Value())
		}
	}
}

// =============================================================================
// Concurrent Snapshot Tests
// =============================================================================

func TestSnapshotConcurrent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("value"))

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			snap := db.GetSnapshot()
			// Read through snapshot
			snapOpts := DefaultReadOptions()
			snapOpts.Snapshot = snap
			_, _ = db.Get(snapOpts, []byte("key"))
			db.ReleaseSnapshot(snap)
		})
	}
	wg.Wait()
}

func TestSnapshotDuringWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("key"), []byte("initial"))
	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Concurrent writes
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			db.Put(nil, []byte("key"), fmt.Appendf(nil, "v%d", n))
		}(i)
	}
	wg.Wait()

	// Snapshot still sees initial
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	val, _ := db.Get(snapOpts, []byte("key"))
	if string(val) != "initial" {
		t.Errorf("Snapshot = %s, want initial", val)
	}
}

// =============================================================================
// Snapshot Edge Cases
// =============================================================================

func TestSnapshotReleaseTwice(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	snap := db.GetSnapshot()
	db.ReleaseSnapshot(snap)

	// Second release should be safe (no-op)
	db.ReleaseSnapshot(snap)
}

func TestSnapshotNoNewKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	db.Put(nil, []byte("new_key"), []byte("new_value"))

	// Snapshot should not see new key
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	_, err := db.Get(snapOpts, []byte("new_key"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("Snapshot should not see key added after snapshot")
	}
}

func TestSnapshotConsistentManyKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Initial data
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "key%03d", i), []byte("v1"))
	}

	snap := db.GetSnapshot()
	defer db.ReleaseSnapshot(snap)

	// Modify all keys
	for i := range 100 {
		db.Put(nil, fmt.Appendf(nil, "key%03d", i), []byte("v2"))
	}

	// Snapshot should see all v1
	snapOpts := DefaultReadOptions()
	snapOpts.Snapshot = snap
	for i := range 100 {
		val, _ := db.Get(snapOpts, fmt.Appendf(nil, "key%03d", i))
		if string(val) != "v1" {
			t.Errorf("key%03d = %s, want v1", i, val)
		}
	}
}
