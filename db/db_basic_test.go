// db_basic_test.go - Core database operations: Open/Close, Put/Get/Delete, key-value edge cases
//
// These tests verify the fundamental correctness of basic database operations.
// They should pass before any other tests are considered.

package db

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/internal/batch"
)

// =============================================================================
// Open/Close Tests
// =============================================================================

func TestOpenCreate(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Open() returned nil db")
	}
}

func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("First Open() error = %v", err)
	}
	db1.Close()

	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Second Open() error = %v", err)
	}
	defer db2.Close()
}

func TestOpenNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = false

	_, err := Open(dir, opts)
	if !errors.Is(err, ErrDBNotFound) {
		t.Errorf("Open() error = %v, want ErrDBNotFound", err)
	}
}

func TestOpenErrorIfExists(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db1, _ := Open(dir, opts)
	db1.Close()

	opts.ErrorIfExists = true
	_, err := Open(dir, opts)
	if !errors.Is(err, ErrDBExists) {
		t.Errorf("Open() error = %v, want ErrDBExists", err)
	}
}

func TestRepeatedOpenClose(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	for i := range 5 {
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("Open %d error: %v", i, err)
		}
		db.Put(nil, fmt.Appendf(nil, "key_%d", i), []byte("value"))
		db.Flush(nil)
		db.Close()
	}

	// Verify all keys persist
	db, _ := Open(dir, opts)
	defer db.Close()
	for i := range 5 {
		_, err := db.Get(nil, fmt.Appendf(nil, "key_%d", i))
		if err != nil {
			t.Errorf("key_%d not found after reopen", i)
		}
	}
}

// =============================================================================
// Put/Get/Delete Tests
// =============================================================================

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("key1")
	value := []byte("value1")

	if err := db.Put(nil, key, value); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	got, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("Get() = %s, want %s", got, value)
	}
}

func TestGetNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	_, err := db.Get(nil, []byte("nonexistent"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("delete_key")
	db.Put(nil, key, []byte("value"))

	if err := db.Delete(nil, key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, err := db.Get(nil, key)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Deleting non-existent key should not error
	if err := db.Delete(nil, []byte("nonexistent")); err != nil {
		t.Errorf("Delete non-existent error: %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("overwrite_key")
	db.Put(nil, key, []byte("v1"))
	db.Put(nil, key, []byte("v2"))

	got, _ := db.Get(nil, key)
	if string(got) != "v2" {
		t.Errorf("Get() = %s, want v2", got)
	}
}

func TestPutDeleteGet(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("pdg_key")

	db.Put(nil, key, []byte("v1"))
	val, _ := db.Get(nil, key)
	if string(val) != "v1" {
		t.Error("After put, should see v1")
	}

	db.Delete(nil, key)
	_, err := db.Get(nil, key)
	if !errors.Is(err, ErrNotFound) {
		t.Error("After delete, should be NotFound")
	}

	db.Put(nil, key, []byte("v2"))
	val, _ = db.Get(nil, key)
	if string(val) != "v2" {
		t.Error("After re-put, should see v2")
	}
}

// =============================================================================
// Key/Value Edge Cases
// =============================================================================

func TestEmptyKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	emptyKey := []byte{}
	db.Put(nil, emptyKey, []byte("empty_key_value"))

	val, err := db.Get(nil, emptyKey)
	if err != nil {
		t.Fatalf("Get empty key error: %v", err)
	}
	if string(val) != "empty_key_value" {
		t.Errorf("Get empty key = %s, want empty_key_value", val)
	}
}

func TestEmptyValue(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	key := []byte("empty_value_key")
	db.Put(nil, key, []byte{})

	val, err := db.Get(nil, key)
	if err != nil {
		t.Fatalf("Get empty value error: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("Get empty value length = %d, want 0", len(val))
	}
}

func TestLongKey(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	longKey := bytes.Repeat([]byte("k"), 10000)
	db.Put(nil, longKey, []byte("value"))

	val, err := db.Get(nil, longKey)
	if err != nil {
		t.Fatalf("Get long key error: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Get long key = %s, want value", val)
	}
}

func TestLongValue(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	longValue := bytes.Repeat([]byte("v"), 1024*1024) // 1MB
	db.Put(nil, []byte("long_value_key"), longValue)

	val, err := db.Get(nil, []byte("long_value_key"))
	if err != nil {
		t.Fatalf("Get long value error: %v", err)
	}
	if !bytes.Equal(val, longValue) {
		t.Errorf("Get long value length = %d, want %d", len(val), len(longValue))
	}
}

func TestBinaryKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Keys with null bytes, high bytes, etc.
	binaryKey := []byte{0x00, 0xFF, 0x7F, 0x80, 0x01}
	binaryValue := make([]byte, 256)
	for i := range binaryValue {
		binaryValue[i] = byte(i)
	}

	db.Put(nil, binaryKey, binaryValue)

	val, err := db.Get(nil, binaryKey)
	if err != nil {
		t.Fatalf("Get binary error: %v", err)
	}
	if !bytes.Equal(val, binaryValue) {
		t.Error("Binary value mismatch")
	}
}

func TestSpecialCharacterKeys(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	specialKeys := [][]byte{
		[]byte("\x00\x01\x02"),
		[]byte("key with spaces"),
		[]byte("key\twith\ttabs"),
		[]byte("key\nwith\nnewlines"),
		[]byte("日本語"),
		[]byte("emoji🎉"),
	}

	for _, key := range specialKeys {
		db.Put(nil, key, []byte("value"))
		val, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get special key %q error: %v", key, err)
			continue
		}
		if string(val) != "value" {
			t.Errorf("Get special key %q = %s, want value", key, val)
		}
	}
}

// =============================================================================
// Batch Write Tests
// =============================================================================

func TestWriteBatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	wb := batch.New()
	wb.Put([]byte("batch_key1"), []byte("batch_value1"))
	wb.Put([]byte("batch_key2"), []byte("batch_value2"))
	wb.Delete([]byte("batch_key3"))

	if err := db.Write(nil, wb); err != nil {
		t.Fatalf("Write batch error: %v", err)
	}

	v1, _ := db.Get(nil, []byte("batch_key1"))
	v2, _ := db.Get(nil, []byte("batch_key2"))
	if string(v1) != "batch_value1" || string(v2) != "batch_value2" {
		t.Error("Batch write failed")
	}
}

func TestWriteEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	wb := batch.New()
	// Empty batch should succeed
	if err := db.Write(nil, wb); err != nil {
		t.Errorf("Write empty batch error: %v", err)
	}
}

func TestBatchAtomicity(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Pre-populate
	db.Put(nil, []byte("key1"), []byte("old1"))

	// Batch with multiple ops
	wb := batch.New()
	wb.Delete([]byte("key1"))
	wb.Put([]byte("key2"), []byte("new2"))
	wb.Put([]byte("key3"), []byte("new3"))
	db.Write(nil, wb)

	// All should have happened
	_, err := db.Get(nil, []byte("key1"))
	if !errors.Is(err, ErrNotFound) {
		t.Error("key1 should be deleted")
	}
	v2, _ := db.Get(nil, []byte("key2"))
	v3, _ := db.Get(nil, []byte("key3"))
	if string(v2) != "new2" || string(v3) != "new3" {
		t.Error("Batch writes failed")
	}
}

// =============================================================================
// Many Keys Tests
// =============================================================================

func TestManyPuts(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 1000
	for i := range n {
		key := fmt.Appendf(nil, "key_%05d", i)
		value := fmt.Appendf(nil, "value_%05d", i)
		db.Put(nil, key, value)
	}

	// Verify all
	for i := range n {
		key := fmt.Appendf(nil, "key_%05d", i)
		expected := fmt.Appendf(nil, "value_%05d", i)
		got, err := db.Get(nil, key)
		if err != nil {
			t.Errorf("Get %s error: %v", key, err)
			continue
		}
		if !bytes.Equal(got, expected) {
			t.Errorf("Get %s = %s, want %s", key, got, expected)
		}
	}
}

func TestKeyOrdering(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CreateIfMissing = true

	db, _ := Open(dir, opts)
	defer db.Close()

	// Insert in random order
	keys := []string{"zebra", "apple", "mango", "banana", "kiwi"}
	for _, k := range keys {
		db.Put(nil, []byte(k), []byte(k))
	}

	// Iterate - should be sorted
	iter := db.NewIterator(nil)
	defer iter.Close()

	var result []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		result = append(result, string(iter.Key()))
	}

	expected := []string{"apple", "banana", "kiwi", "mango", "zebra"}
	for i, k := range result {
		if k != expected[i] {
			t.Errorf("Position %d: got %s, want %s", i, k, expected[i])
		}
	}
}
