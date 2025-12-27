// db_test.go - Database round-trip tests (Go tests).
//
// These tests verify that Go can correctly write and read back databases.
package main

import (
	"fmt"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

// TestDatabaseRoundTrip_Basic tests basic database write and read.
func TestDatabaseRoundTrip_Basic(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Write data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := database.Put(nil, []byte(k), []byte(v)); err != nil {
			database.Close()
			t.Fatalf("put failed: %v", err)
		}
	}

	// Read back while still open
	for k, expectedV := range testData {
		v, err := database.Get(nil, []byte(k))
		if err != nil {
			database.Close()
			t.Fatalf("get failed for %q: %v", k, err)
		}
		if string(v) != expectedV {
			database.Close()
			t.Fatalf("value mismatch for %q: got %q, want %q", k, v, expectedV)
		}
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// TestDatabaseRoundTrip_WithFlush tests database with explicit flush.
func TestDatabaseRoundTrip_WithFlush(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Write data
	for i := range 100 {
		key := fmt.Sprintf("flush_key_%05d", i)
		value := fmt.Sprintf("flush_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			t.Fatalf("put failed: %v", err)
		}
	}

	// Flush to SST
	if err := database.Flush(nil); err != nil {
		database.Close()
		t.Fatalf("flush failed: %v", err)
	}

	// Read back from SST
	for i := range 100 {
		key := fmt.Sprintf("flush_key_%05d", i)
		expectedValue := fmt.Sprintf("flush_value_%05d", i)

		v, err := database.Get(nil, []byte(key))
		if err != nil {
			database.Close()
			t.Fatalf("get failed for %q: %v", key, err)
		}
		if string(v) != expectedValue {
			database.Close()
			t.Fatalf("value mismatch for %q: got %q, want %q", key, v, expectedValue)
		}
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// TestDatabaseRoundTrip_WithReopen tests database reopen.
func TestDatabaseRoundTrip_WithReopen(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	// First session: write and close
	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	for i := range 50 {
		key := fmt.Sprintf("reopen_key_%05d", i)
		value := fmt.Sprintf("reopen_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			t.Fatalf("put failed: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		database.Close()
		t.Fatalf("flush failed: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Second session: reopen and read
	opts.CreateIfMissing = false
	database, err = db.Open(dir, opts)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}

	for i := range 50 {
		key := fmt.Sprintf("reopen_key_%05d", i)
		expectedValue := fmt.Sprintf("reopen_value_%05d", i)

		v, err := database.Get(nil, []byte(key))
		if err != nil {
			database.Close()
			t.Fatalf("get failed after reopen for %q: %v", key, err)
		}
		if string(v) != expectedValue {
			database.Close()
			t.Fatalf("value mismatch after reopen for %q: got %q, want %q", key, v, expectedValue)
		}
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// TestDatabaseRoundTrip_ColumnFamilies tests column family isolation.
func TestDatabaseRoundTrip_ColumnFamilies(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Create column family
	cfOpts := db.DefaultColumnFamilyOptions()
	cf1, err := database.CreateColumnFamily(cfOpts, "test_cf")
	if err != nil {
		database.Close()
		t.Fatalf("create CF failed: %v", err)
	}

	// Write to default CF
	writeOpts := db.DefaultWriteOptions()
	if err := database.Put(writeOpts, []byte("default_key"), []byte("default_value")); err != nil {
		database.Close()
		t.Fatalf("put to default failed: %v", err)
	}

	// Write to test_cf
	if err := database.PutCF(writeOpts, cf1, []byte("cf_key"), []byte("cf_value")); err != nil {
		database.Close()
		t.Fatalf("put to CF failed: %v", err)
	}

	// Verify isolation: cf_key should not be in default
	_, err = database.Get(nil, []byte("cf_key"))
	if err == nil {
		database.Close()
		t.Fatal("cf_key should not be visible in default CF")
	}
	// Key not found is expected - don't close DB on this expected error

	// Verify cf_key is in test_cf
	v, err := database.GetCF(nil, cf1, []byte("cf_key"))
	if err != nil {
		database.Close()
		t.Fatalf("get from CF failed: %v", err)
	}
	if string(v) != "cf_value" {
		database.Close()
		t.Fatalf("CF value mismatch: got %q, want %q", v, "cf_value")
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// TestDatabaseRoundTrip_Iterator tests iterator round-trip.
func TestDatabaseRoundTrip_Iterator(t *testing.T) {
	dir := t.TempDir()

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Write data
	testData := []struct {
		key, value string
	}{
		{"aaa", "1"},
		{"bbb", "2"},
		{"ccc", "3"},
		{"ddd", "4"},
		{"eee", "5"},
	}

	for _, td := range testData {
		if err := database.Put(nil, []byte(td.key), []byte(td.value)); err != nil {
			database.Close()
			t.Fatalf("put failed: %v", err)
		}
	}

	// Flush to SST
	if err := database.Flush(nil); err != nil {
		database.Close()
		t.Fatalf("flush failed: %v", err)
	}

	// Iterate and verify order
	iter := database.NewIterator(nil)
	defer iter.Close()

	idx := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if idx >= len(testData) {
			t.Fatalf("too many entries")
		}

		if string(iter.Key()) != testData[idx].key {
			t.Errorf("key mismatch at %d: got %q, want %q", idx, iter.Key(), testData[idx].key)
		}
		if string(iter.Value()) != testData[idx].value {
			t.Errorf("value mismatch at %d: got %q, want %q", idx, iter.Value(), testData[idx].value)
		}
		idx++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	if idx != len(testData) {
		t.Errorf("entry count mismatch: got %d, want %d", idx, len(testData))
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}
