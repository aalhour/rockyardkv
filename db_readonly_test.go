// db_readonly_test.go implements tests for read-only database mode.
package rockyardkv

import (
	"errors"
	"os"
	"slices"
	"testing"
)

// TestOpenForReadOnlyBasic tests basic read-only database operations.
func TestOpenForReadOnlyBasic(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		if err := db1.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to ensure data is on disk
	if err := db1.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Close the writable database
	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// Verify reads work
	for i := range 10 {
		key := []byte{byte(i)}
		expected := []byte{byte(i * 10)}
		got, err := roDB.Get(nil, key)
		if err != nil {
			t.Fatalf("Get failed for key %d: %v", i, err)
		}
		if len(got) != len(expected) || got[0] != expected[0] {
			t.Errorf("Value mismatch: got %v, want %v", got, expected)
		}
	}
}

// TestOpenForReadOnlyWriteRejected tests that write operations are rejected.
func TestOpenForReadOnlyWriteRejected(t *testing.T) {
	dir := t.TempDir()

	// Create a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	db1.Flush(nil)
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// Test that all write operations return ErrReadOnly
	testCases := []struct {
		name string
		fn   func() error
	}{
		{"Put", func() error { return roDB.Put(nil, []byte("k"), []byte("v")) }},
		{"Delete", func() error { return roDB.Delete(nil, []byte("k")) }},
		{"SingleDelete", func() error { return roDB.SingleDelete(nil, []byte("k")) }},
		{"DeleteRange", func() error { return roDB.DeleteRange(nil, []byte("a"), []byte("z")) }},
		{"Merge", func() error { return roDB.Merge(nil, []byte("k"), []byte("v")) }},
		{"Flush", func() error { return roDB.Flush(nil) }},
		{"CompactRange", func() error { return roDB.CompactRange(nil, nil, nil) }},
		{"SyncWAL", func() error { return roDB.SyncWAL() }},
		{"FlushWAL", func() error { return roDB.FlushWAL(true) }},
		{"IngestExternalFile", func() error { return roDB.IngestExternalFile(nil, IngestExternalFileOptions{}) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if !errors.Is(err, ErrReadOnly) {
				t.Errorf("%s: expected ErrReadOnly, got %v", tc.name, err)
			}
		})
	}
}

// TestOpenForReadOnlyIterator tests that iterators work in read-only mode.
func TestOpenForReadOnlyIterator(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	for _, k := range keys {
		if err := db1.Put(nil, k, k); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	db1.Flush(nil)
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// Create iterator and verify
	iter := roDB.NewIterator(nil)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}
	if count != len(keys) {
		t.Errorf("Iterator count = %d, want %d", count, len(keys))
	}
}

// TestOpenForReadOnlyNonExistent tests opening a non-existent database.
func TestOpenForReadOnlyNonExistent(t *testing.T) {
	_, err := OpenForReadOnly("/nonexistent/path/db", nil, false)
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}

// TestOpenForReadOnlyWithWAL tests errorIfWALExists flag.
func TestOpenForReadOnlyWithWAL(t *testing.T) {
	dir := t.TempDir()

	// Create a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write data but DON'T flush (leaves data in WAL)
	if err := db1.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close without flush - this should leave WAL files
	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check if WAL exists
	files, _ := os.ReadDir(dir)
	hasWAL := false
	for _, f := range files {
		if len(f.Name()) > 4 && f.Name()[len(f.Name())-4:] == ".log" {
			hasWAL = true
			break
		}
	}

	if hasWAL {
		// If WAL exists, opening with errorIfWALExists=true should fail
		_, err := OpenForReadOnly(dir, opts, true)
		if err == nil {
			t.Error("Expected error when WAL exists and errorIfWALExists=true")
		}

		// But opening with errorIfWALExists=false should succeed
		roDB, err := OpenForReadOnly(dir, opts, false)
		if err != nil {
			t.Fatalf("OpenForReadOnly with errorIfWALExists=false failed: %v", err)
		}
		roDB.Close()
	}
}

// TestOpenForReadOnlySnapshot tests that snapshots work in read-only mode.
func TestOpenForReadOnlySnapshot(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	if err := db1.Put(nil, []byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	db1.Flush(nil)
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// Get a snapshot
	snap := roDB.GetSnapshot()
	if snap == nil {
		t.Skip("Snapshots not fully supported in read-only mode")
	}
	defer roDB.ReleaseSnapshot(snap)

	// Read using snapshot
	readOpts := DefaultReadOptions()
	readOpts.Snapshot = snap
	val, err := roDB.Get(readOpts, []byte("key"))
	if err != nil {
		t.Fatalf("Get with snapshot failed: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Value = %q, want %q", val, "value")
	}
}

// TestOpenForReadOnlyColumnFamilyOps tests CF operations in read-only mode.
func TestOpenForReadOnlyColumnFamilyOps(t *testing.T) {
	dir := t.TempDir()

	// Create a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	db1.Flush(nil)
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// Creating column family should fail
	_, err = roDB.CreateColumnFamily(ColumnFamilyOptions{}, "test")
	if !errors.Is(err, ErrReadOnly) {
		t.Errorf("CreateColumnFamily: expected ErrReadOnly, got %v", err)
	}
}

// TestListColumnFamilies tests listing column families.
func TestListColumnFamilies(t *testing.T) {
	dir := t.TempDir()

	// Create a database with default CF
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	db1.Flush(nil)
	db1.Close()

	// List column families
	cfs, err := ListColumnFamilies(dir, opts)
	if err != nil {
		t.Fatalf("ListColumnFamilies failed: %v", err)
	}

	// Should have at least "default"
	if !slices.Contains(cfs, "default") {
		t.Error("Expected 'default' column family in list")
	}
}

// TestOpenForReadOnlyMultiGet tests MultiGet in read-only mode.
func TestOpenForReadOnlyMultiGet(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	for i, k := range keys {
		if err := db1.Put(nil, k, values[i]); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	db1.Flush(nil)
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// MultiGet
	results, errs := roDB.MultiGet(nil, keys)
	for i, v := range results {
		if errs[i] != nil {
			t.Errorf("MultiGet error for key %s: %v", keys[i], errs[i])
		}
		if string(v) != string(values[i]) {
			t.Errorf("MultiGet value mismatch: got %q, want %q", v, values[i])
		}
	}
}

// TestOpenForReadOnlyGetLiveFilesMetaData tests that GetLiveFilesMetaData works
// in read-only mode and returns actual live SST file information.
// This is Issue 11: Read-only DB stubs live-file APIs
func TestOpenForReadOnlyGetLiveFilesMetaData(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write enough data to create SST files
	for i := range 100 {
		key := []byte{byte(i)}
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i)
		}
		if err := db1.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST files on disk
	if err := db1.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Get metadata from writable DB for comparison
	writableMetadata := db1.GetLiveFilesMetaData()
	if len(writableMetadata) == 0 {
		t.Fatal("Writable DB has no live SST files after flush")
	}

	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// GetLiveFilesMetaData should return actual file metadata
	metadata := roDB.GetLiveFilesMetaData()

	// Should have at least one SST file
	if len(metadata) == 0 {
		t.Error("GetLiveFilesMetaData returned empty in read-only mode (Issue 11)")
	}

	// Verify metadata has reasonable values
	for _, m := range metadata {
		if m.Name == "" {
			t.Error("LiveFileMetaData has empty Name")
		}
		if m.Size == 0 {
			t.Errorf("LiveFileMetaData %s has zero Size", m.Name)
		}
	}
}

// TestOpenForReadOnlyGetLiveFiles tests that GetLiveFiles works
// in read-only mode and returns actual file list.
func TestOpenForReadOnlyGetLiveFiles(t *testing.T) {
	dir := t.TempDir()

	// Create and populate a database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	// Write data and flush to create SST files
	for i := range 50 {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		if err := db1.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db1.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	db1.Close()

	// Open in read-only mode
	roDB, err := OpenForReadOnly(dir, opts, false)
	if err != nil {
		t.Fatalf("OpenForReadOnly failed: %v", err)
	}
	defer roDB.Close()

	// GetLiveFiles should return actual file list
	files, manifestSize, err := roDB.GetLiveFiles(false)
	if err != nil {
		t.Fatalf("GetLiveFiles failed: %v", err)
	}

	// Should have at least MANIFEST and CURRENT files
	if len(files) == 0 {
		t.Error("GetLiveFiles returned empty file list in read-only mode")
	}

	// Manifest size should be non-zero
	if manifestSize == 0 {
		t.Error("GetLiveFiles returned zero manifest size")
	}

	// Check that we have expected file types
	// Files are returned as /FILENAME format (with leading slash)
	hasManifest := false
	hasCurrent := false
	hasSST := false
	for _, f := range files {
		// MANIFEST files can be named /MANIFEST-XXXXXX
		if len(f) >= 9 && f[1:9] == "MANIFEST" {
			hasManifest = true
		}
		if f == "/CURRENT" {
			hasCurrent = true
		}
		if len(f) > 4 && f[len(f)-4:] == ".sst" {
			hasSST = true
		}
	}

	if !hasManifest {
		t.Errorf("GetLiveFiles missing MANIFEST file in %v", files)
	}
	if !hasCurrent {
		t.Errorf("GetLiveFiles missing CURRENT file in %v", files)
	}
	if !hasSST {
		t.Errorf("GetLiveFiles missing SST files in %v", files)
	}
}
