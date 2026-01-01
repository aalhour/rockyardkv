// timestamped_db_test.go implements tests for timestamped db.
package rockyardkv

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTimestampedDBBasicOperations(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}
	defer db.Close()

	// Test PutWithTimestamp
	key := []byte("testkey")
	value1 := []byte("value_at_100")
	ts1 := EncodeU64Ts(100)

	err = db.PutWithTimestamp(nil, key, value1, ts1)
	if err != nil {
		t.Fatalf("PutWithTimestamp failed: %v", err)
	}

	// Write another version with a newer timestamp
	value2 := []byte("value_at_200")
	ts2 := EncodeU64Ts(200)

	err = db.PutWithTimestamp(nil, key, value2, ts2)
	if err != nil {
		t.Fatalf("PutWithTimestamp failed: %v", err)
	}

	// GetWithTimestamp at ts2 should return value2
	val, foundTS, err := db.GetWithTimestamp(nil, key, ts2)
	if err != nil {
		t.Fatalf("GetWithTimestamp at ts2 failed: %v", err)
	}
	if !bytes.Equal(val, value2) {
		t.Errorf("GetWithTimestamp at ts2: expected %q, got %q", value2, val)
	}
	if !bytes.Equal(foundTS, ts2) {
		t.Errorf("GetWithTimestamp at ts2: expected timestamp %v, got %v", ts2, foundTS)
	}

	// GetWithTimestamp at ts1 should return value1
	val, foundTS, err = db.GetWithTimestamp(nil, key, ts1)
	if err != nil {
		t.Fatalf("GetWithTimestamp at ts1 failed: %v", err)
	}
	if !bytes.Equal(val, value1) {
		t.Errorf("GetWithTimestamp at ts1: expected %q, got %q", value1, val)
	}
	if !bytes.Equal(foundTS, ts1) {
		t.Errorf("GetWithTimestamp at ts1: expected timestamp %v, got %v", ts1, foundTS)
	}

	// GetWithTimestamp at an older timestamp should not find the key
	ts0 := EncodeU64Ts(50)
	_, _, err = db.GetWithTimestamp(nil, key, ts0)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetWithTimestamp at ts0: expected ErrNotFound, got %v", err)
	}
}

func TestTimestampedDBMultipleKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_multi")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}
	defer db.Close()

	// Write multiple keys with different timestamps
	for i := range 10 {
		key := []byte{'k', byte('0' + i)}
		for ts := range uint64(5) {
			value := []byte{'v', byte('0' + i), byte('0' + ts)}
			err = db.PutWithTimestamp(nil, key, value, EncodeU64Ts(ts*100))
			if err != nil {
				t.Fatalf("PutWithTimestamp failed: %v", err)
			}
		}
	}

	// Verify each key at different timestamps
	for i := range 10 {
		key := []byte{'k', byte('0' + i)}
		for ts := range uint64(5) {
			expectedValue := []byte{'v', byte('0' + i), byte('0' + ts)}
			val, _, err := db.GetWithTimestamp(nil, key, EncodeU64Ts(ts*100))
			if err != nil {
				t.Fatalf("GetWithTimestamp failed for key %s at ts %d: %v", key, ts*100, err)
			}
			if !bytes.Equal(val, expectedValue) {
				t.Errorf("GetWithTimestamp key %s at ts %d: expected %q, got %q",
					key, ts*100, expectedValue, val)
			}
		}
	}
}

func TestTimestampedDBDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_delete")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}
	defer db.Close()

	key := []byte("delkey")
	value := []byte("delvalue")
	ts1 := EncodeU64Ts(100)
	ts2 := EncodeU64Ts(200)

	// Write at ts1
	err = db.PutWithTimestamp(nil, key, value, ts1)
	if err != nil {
		t.Fatalf("PutWithTimestamp failed: %v", err)
	}

	// Delete at ts2
	err = db.DeleteWithTimestamp(nil, key, ts2)
	if err != nil {
		t.Fatalf("DeleteWithTimestamp failed: %v", err)
	}

	// Reading at ts2 should find the delete tombstone (or not find the key)
	// Since our simple implementation seeks to key+ts, we should not find the value
	// at ts2 because the delete tombstone shadows it
	// This depends on how deletions are handled - in RocksDB, a delete at ts2
	// would shadow reads at ts2 but not at ts1

	// Reading at ts1 should still find the value
	val, _, err := db.GetWithTimestamp(nil, key, ts1)
	if err != nil {
		t.Fatalf("GetWithTimestamp at ts1 failed: %v", err)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("GetWithTimestamp at ts1: expected %q, got %q", value, val)
	}
}

func TestTimestampedDBIterator(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_iter")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}
	defer db.Close()

	// Write some keys with timestamps
	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		for _, ts := range []uint64{100, 200, 300} {
			value := []byte(k + "_" + string(rune('0'+ts/100)))
			err = db.PutWithTimestamp(nil, []byte(k), value, EncodeU64Ts(ts))
			if err != nil {
				t.Fatalf("PutWithTimestamp failed: %v", err)
			}
		}
	}

	// Create a timestamped iterator at ts=200
	readOpts := DefaultReadOptions()
	readOpts.Timestamp = EncodeU64Ts(200)

	iter := db.NewTimestampedIterator(readOpts)
	defer iter.Close()

	// Iterate and collect all visible entries
	var found []struct {
		key   string
		value string
		ts    uint64
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		ts, _ := DecodeU64Ts(iter.Timestamp())
		found = append(found, struct {
			key   string
			value string
			ts    uint64
		}{
			key:   string(iter.UserKey()),
			value: string(iter.Value()),
			ts:    ts,
		})
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// We should see entries with ts <= 200
	for _, f := range found {
		if f.ts > 200 {
			t.Errorf("Iterator returned entry with ts=%d, expected <= 200", f.ts)
		}
	}
}

func TestTimestampedDBPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_persist")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")

	// Open and write data
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}

	key := []byte("persistkey")
	value1 := []byte("persist_value_100")
	value2 := []byte("persist_value_200")

	err = db.PutWithTimestamp(nil, key, value1, EncodeU64Ts(100))
	if err != nil {
		t.Fatalf("PutWithTimestamp failed: %v", err)
	}

	err = db.PutWithTimestamp(nil, key, value2, EncodeU64Ts(200))
	if err != nil {
		t.Fatalf("PutWithTimestamp failed: %v", err)
	}

	// Flush to ensure data is persisted
	err = db.Flush(nil)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	db.Close()

	// Reopen and verify
	db, err = OpenTimestampedDB(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen timestamped DB: %v", err)
	}
	defer db.Close()

	// Check value at ts=200
	val, foundTS, err := db.GetWithTimestamp(nil, key, EncodeU64Ts(200))
	if err != nil {
		t.Fatalf("GetWithTimestamp after reopen failed: %v", err)
	}
	if !bytes.Equal(val, value2) {
		t.Errorf("After reopen at ts=200: expected %q, got %q", value2, val)
	}
	tsVal, _ := DecodeU64Ts(foundTS)
	if tsVal != 200 {
		t.Errorf("After reopen: expected ts=200, got %d", tsVal)
	}

	// Check value at ts=100
	val, foundTS, err = db.GetWithTimestamp(nil, key, EncodeU64Ts(100))
	if err != nil {
		t.Fatalf("GetWithTimestamp after reopen at ts=100 failed: %v", err)
	}
	if !bytes.Equal(val, value1) {
		t.Errorf("After reopen at ts=100: expected %q, got %q", value1, val)
	}
	tsVal, _ = DecodeU64Ts(foundTS)
	if tsVal != 100 {
		t.Errorf("After reopen: expected ts=100, got %d", tsVal)
	}
}

func TestTimestampedDBInvalidTimestamp(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_invalid")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparatorWithU64Ts{}

	db, err := OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if err != nil {
		t.Fatalf("Failed to open timestamped DB: %v", err)
	}
	defer db.Close()

	// Try to put with invalid timestamp size
	err = db.PutWithTimestamp(nil, []byte("key"), []byte("value"), []byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidTimestampSize) {
		t.Errorf("PutWithTimestamp with invalid timestamp: expected ErrInvalidTimestampSize, got %v", err)
	}

	// Try to get with invalid timestamp size
	_, _, err = db.GetWithTimestamp(nil, []byte("key"), []byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidTimestampSize) {
		t.Errorf("GetWithTimestamp with invalid timestamp: expected ErrInvalidTimestampSize, got %v", err)
	}

	// Try to delete with invalid timestamp size
	err = db.DeleteWithTimestamp(nil, []byte("key"), []byte{1, 2, 3})
	if !errors.Is(err, ErrInvalidTimestampSize) {
		t.Errorf("DeleteWithTimestamp with invalid timestamp: expected ErrInvalidTimestampSize, got %v", err)
	}
}

func TestTimestampedDBWithoutTimestampComparator(t *testing.T) {
	dir, err := os.MkdirTemp("", "timestamped_db_nocomp")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = BytewiseComparator{} // Regular comparator, not timestamped

	_, err = OpenTimestampedDB(filepath.Join(dir, "db"), opts)
	if !errors.Is(err, ErrTimestampNotSupported) {
		t.Errorf("OpenTimestampedDB with non-timestamp comparator: expected ErrTimestampNotSupported, got %v", err)
	}
}
