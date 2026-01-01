package rockyardkv

// db_options_test.go implements db_options_test.go - Options validation and effects tests.
//
// These tests verify that database options are correctly validated and
// applied, affecting database behavior as expected.


import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// =============================================================================
// Options Validation Tests
// =============================================================================

func TestOptionsDefaults(t *testing.T) {
	opts := DefaultOptions()

	if opts.WriteBufferSize == 0 {
		t.Error("WriteBufferSize should not be zero")
	}
	if opts.MaxOpenFiles == 0 {
		t.Error("MaxOpenFiles should not be zero")
	}
}

func TestReadOptionsDefaults(t *testing.T) {
	opts := DefaultReadOptions()

	if opts.FillCache != true {
		t.Error("FillCache should default to true")
	}
}

func TestWriteOptionsDefaults(t *testing.T) {
	opts := DefaultWriteOptions()

	if opts.DisableWAL != false {
		t.Error("DisableWAL should default to false")
	}
}

// =============================================================================
// WriteBufferSize Tests
// =============================================================================

func TestOptionsWriteBufferSize(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.WriteBufferSize = 1024 // Very small - should trigger flushes

	db, _ := Open(dir, opts)
	defer db.Close()

	// Write enough data to trigger memtable switches
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		value := bytes.Repeat([]byte("v"), 100)
		db.Put(nil, key, value)
	}

	// Data should still be accessible
	for i := range 100 {
		_, err := db.Get(nil, fmt.Appendf(nil, "key%03d", i))
		if err != nil {
			t.Errorf("key%03d not found", i)
		}
	}
}

// =============================================================================
// Sync Option Tests
// =============================================================================

func TestOptionsSync(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Write with Sync=true
	woSync := DefaultWriteOptions()
	woSync.Sync = true

	if err := db.Put(woSync, []byte("sync_key"), []byte("sync_value")); err != nil {
		t.Fatalf("Put with Sync error: %v", err)
	}

	// Verify write succeeded
	val, err := db.Get(nil, []byte("sync_key"))
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(val) != "sync_value" {
		t.Errorf("Get = %s, want sync_value", val)
	}
}

// =============================================================================
// DisableWAL Tests
// =============================================================================

func TestOptionsDisableWAL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)

	// Write with DisableWAL=true
	woNoWAL := DefaultWriteOptions()
	woNoWAL.DisableWAL = true

	db.Put(woNoWAL, []byte("nowal_key"), []byte("nowal_value"))

	// Flush to persist
	db.Flush(nil)

	// Verify write succeeded
	val, _ := db.Get(nil, []byte("nowal_key"))
	if string(val) != "nowal_value" {
		t.Errorf("Get = %s, want nowal_value", val)
	}

	db.Close()

	// Reopen - data should be there (was flushed)
	db2, _ := Open(dir, opts)
	defer db2.Close()

	val, _ = db2.Get(nil, []byte("nowal_key"))
	if string(val) != "nowal_value" {
		t.Errorf("After reopen = %s, want nowal_value", val)
	}
}

// =============================================================================
// CreateIfMissing / ErrorIfExists Tests
// =============================================================================

func TestOptionsCreateIfMissing(t *testing.T) {
	dir := t.TempDir()

	// Without CreateIfMissing, should fail
	opts := DefaultOptions()
	opts.CreateIfMissing = false

	_, err := Open(dir, opts)
	if !errors.Is(err, ErrDBNotFound) {
		t.Errorf("Open without CreateIfMissing: %v, want ErrDBNotFound", err)
	}

	// With CreateIfMissing, should succeed
	opts.CreateIfMissing = true
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open with CreateIfMissing: %v", err)
	}
	db.Close()
}

func TestOptionsErrorIfExists(t *testing.T) {
	dir := t.TempDir()

	// Create database
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	db1, _ := Open(dir, opts)
	db1.Close()

	// With ErrorIfExists, should fail
	opts.ErrorIfExists = true
	_, err := Open(dir, opts)
	if !errors.Is(err, ErrDBExists) {
		t.Errorf("Open with ErrorIfExists: %v, want ErrDBExists", err)
	}
}

// =============================================================================
// Comparator Tests
// =============================================================================

func TestOptionsComparator(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Default bytewise comparator
	keys := []string{"c", "a", "b"}
	for _, k := range keys {
		db.Put(nil, []byte(k), []byte(k))
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	var result []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		result = append(result, string(iter.Key()))
	}

	expected := []string{"a", "b", "c"}
	for i, k := range result {
		if k != expected[i] {
			t.Errorf("Position %d: got %s, want %s", i, k, expected[i])
		}
	}
}

// =============================================================================
// BlockSize Tests
// =============================================================================

func TestOptionsBlockSize(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.BlockSize = 256 // Very small blocks

	db, _ := Open(dir, opts)
	defer db.Close()

	// Write data
	for i := range 100 {
		key := fmt.Appendf(nil, "block_key%03d", i)
		value := bytes.Repeat([]byte("x"), 50)
		db.Put(nil, key, value)
	}
	db.Flush(nil)

	// Verify data accessible
	for i := range 100 {
		key := fmt.Appendf(nil, "block_key%03d", i)
		val, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("key %d not found", i)
			continue
		}
		if len(val) != 50 {
			t.Errorf("key %d value length = %d, want 50", i, len(val))
		}
	}
}

// =============================================================================
// FlushOptions Tests
// =============================================================================

func TestFlushOptionsWait(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("flush_key"), []byte("flush_value"))

	// Flush with Wait=true (default)
	flushOpts := &FlushOptions{Wait: true}
	if err := db.Flush(flushOpts); err != nil {
		t.Errorf("Flush with Wait error: %v", err)
	}

	// Data should be persisted
	val, _ := db.Get(nil, []byte("flush_key"))
	if string(val) != "flush_value" {
		t.Errorf("After flush = %s, want flush_value", val)
	}
}

// =============================================================================
// ParanoidChecks Tests
// =============================================================================

func TestOptionsParanoidChecks(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.ParanoidChecks = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open with ParanoidChecks error: %v", err)
	}
	defer db.Close()

	// Basic operations should still work
	db.Put(nil, []byte("paranoid_key"), []byte("value"))
	val, _ := db.Get(nil, []byte("paranoid_key"))
	if string(val) != "value" {
		t.Error("ParanoidChecks should not break basic operations")
	}
}

// =============================================================================
// VerifyChecksums Tests
// =============================================================================

func TestReadOptionsVerifyChecksums(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	db.Put(nil, []byte("checksum_key"), []byte("value"))
	db.Flush(nil)

	// Read with VerifyChecksums=true
	readOpts := DefaultReadOptions()
	readOpts.VerifyChecksums = true

	val, err := db.Get(readOpts, []byte("checksum_key"))
	if err != nil {
		t.Fatalf("Get with VerifyChecksums error: %v", err)
	}
	if string(val) != "value" {
		t.Error("VerifyChecksums should not break reads")
	}
}
