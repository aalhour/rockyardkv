package rockyardkv

// db_secondary_test.go implements tests for secondary instance mode.

import (
	"errors"
	"testing"
)

// TestOpenAsSecondaryBasic tests basic secondary instance operations.
func TestOpenAsSecondaryBasic(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create and populate a primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}

	// Write some data
	for i := range 10 {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		if err := primary.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to ensure data is on disk
	if err := primary.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		primary.Close()
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()

	// Verify reads work from secondary
	for i := range 10 {
		key := []byte{byte(i)}
		expected := []byte{byte(i * 10)}
		got, err := secondary.Get(nil, key)
		if err != nil {
			t.Fatalf("Secondary Get failed for key %d: %v", i, err)
		}
		if len(got) != len(expected) || got[0] != expected[0] {
			t.Errorf("Value mismatch: got %v, want %v", got, expected)
		}
	}

	// Close primary
	if err := primary.Close(); err != nil {
		t.Fatalf("Close primary failed: %v", err)
	}
}

// TestOpenAsSecondaryWriteRejected tests that write operations are rejected.
func TestOpenAsSecondaryWriteRejected(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create a primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}
	primary.Flush(nil)
	primary.Close()

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()

	// Test that all write operations return ErrReadOnly
	testCases := []struct {
		name string
		fn   func() error
	}{
		{"Put", func() error { return secondary.Put(nil, []byte("k"), []byte("v")) }},
		{"Delete", func() error { return secondary.Delete(nil, []byte("k")) }},
		{"SingleDelete", func() error { return secondary.SingleDelete(nil, []byte("k")) }},
		{"DeleteRange", func() error { return secondary.DeleteRange(nil, []byte("a"), []byte("z")) }},
		{"Merge", func() error { return secondary.Merge(nil, []byte("k"), []byte("v")) }},
		{"Flush", func() error { return secondary.Flush(nil) }},
		{"CompactRange", func() error { return secondary.CompactRange(nil, nil, nil) }},
		{"SyncWAL", func() error { return secondary.SyncWAL() }},
		{"FlushWAL", func() error { return secondary.FlushWAL(true) }},
		{"IngestExternalFile", func() error { return secondary.IngestExternalFile(nil, IngestExternalFileOptions{}) }},
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

// TestOpenAsSecondaryCatchUp tests TryCatchUpWithPrimary.
func TestOpenAsSecondaryCatchUp(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}

	// Write initial data
	if err := primary.Put(nil, []byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := primary.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Open secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		primary.Close()
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}

	// Verify secondary can read initial data
	val, err := secondary.Get(nil, []byte("key1"))
	if err != nil {
		t.Fatalf("Secondary Get key1 failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("key1 value = %q, want %q", val, "value1")
	}

	// Write new data to primary
	if err := primary.Put(nil, []byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}
	if err := primary.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Catch up secondary
	secDB := secondary.(*dbImplSecondary)
	if err := secDB.TryCatchUpWithPrimary(); err != nil {
		t.Fatalf("TryCatchUpWithPrimary failed: %v", err)
	}

	// Verify secondary can now read new data
	val, err = secondary.Get(nil, []byte("key2"))
	if err != nil {
		t.Fatalf("Secondary Get key2 after catchup failed: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("key2 value = %q, want %q", val, "value2")
	}

	secondary.Close()
	primary.Close()
}

// TestOpenAsSecondaryIterator tests iterators on secondary instance.
func TestOpenAsSecondaryIterator(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create and populate a primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	for _, k := range keys {
		if err := primary.Put(nil, k, k); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	primary.Flush(nil)

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		primary.Close()
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()
	defer primary.Close()

	// Create iterator and verify
	iter := secondary.NewIterator(nil)
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

// TestOpenAsSecondaryNonExistent tests opening a non-existent primary.
func TestOpenAsSecondaryNonExistent(t *testing.T) {
	secondaryDir := t.TempDir()
	_, err := OpenAsSecondary("/nonexistent/path/db", secondaryDir, nil)
	if err == nil {
		t.Error("Expected error for non-existent primary database")
	}
}

// TestOpenAsSecondaryProperties tests secondary-specific properties.
func TestOpenAsSecondaryProperties(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}
	primary.Flush(nil)
	defer primary.Close()

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()

	// Get secondary-specific properties
	secDB := secondary.(*dbImplSecondary)

	primaryPath, ok := secDB.GetProperty("rocksdb.secondary.primary-path")
	if !ok {
		t.Error("Expected primary-path property to exist")
	}
	if primaryPath != primaryDir {
		t.Errorf("primary-path = %q, want %q", primaryPath, primaryDir)
	}

	secondaryPath, ok := secDB.GetProperty("rocksdb.secondary.secondary-path")
	if !ok {
		t.Error("Expected secondary-path property to exist")
	}
	if secondaryPath != secondaryDir {
		t.Errorf("secondary-path = %q, want %q", secondaryPath, secondaryDir)
	}
}

// TestOpenAsSecondaryColumnFamilyOps tests CF operations on secondary.
func TestOpenAsSecondaryColumnFamilyOps(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create a primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}
	primary.Flush(nil)
	primary.Close()

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()

	// Creating column family should fail
	_, err = secondary.CreateColumnFamily(ColumnFamilyOptions{}, "test")
	if !errors.Is(err, ErrReadOnly) {
		t.Errorf("CreateColumnFamily: expected ErrReadOnly, got %v", err)
	}
}

// TestOpenAsSecondaryMultiGet tests MultiGet on secondary instance.
func TestOpenAsSecondaryMultiGet(t *testing.T) {
	primaryDir := t.TempDir()
	secondaryDir := t.TempDir()

	// Create and populate a primary database
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	primary, err := Open(primaryDir, opts)
	if err != nil {
		t.Fatalf("Failed to open primary: %v", err)
	}

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	values := [][]byte{[]byte("1"), []byte("2"), []byte("3")}
	for i, k := range keys {
		if err := primary.Put(nil, k, values[i]); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	primary.Flush(nil)

	// Open as secondary
	secondary, err := OpenAsSecondary(primaryDir, secondaryDir, opts)
	if err != nil {
		primary.Close()
		t.Fatalf("OpenAsSecondary failed: %v", err)
	}
	defer secondary.Close()
	defer primary.Close()

	// MultiGet from secondary
	results, errs := secondary.MultiGet(nil, keys)
	for i, v := range results {
		if errs[i] != nil {
			t.Errorf("MultiGet error for key %s: %v", keys[i], errs[i])
		}
		if string(v) != string(values[i]) {
			t.Errorf("MultiGet value mismatch: got %q, want %q", v, values[i])
		}
	}
}
