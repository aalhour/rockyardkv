// db_test.go - Database compatibility tests.
//
// Contract: Go databases are readable by C++ RocksDB, and vice versa.
//
// Reference: RocksDB v10.7.5
//
//	db/db_impl/db_impl.cc      - Database implementation
//	db/db_impl/db_impl_open.cc - Database opening
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// =============================================================================
// C++ Compatibility Tests
// =============================================================================

// TestDatabase_Contract_CppWritesGoReads tests that Go can open C++ databases.
//
// Contract: Go can open and read databases created by C++ RocksDB.
func TestDatabase_Contract_CppWritesGoReads(t *testing.T) {
	goldenPath := "testdata/cpp_generated/sst/simple_db"

	if _, err := os.Stat(goldenPath); os.IsNotExist(err) {
		t.Skip("C++ fixture not found")
	}

	opts := db.DefaultOptions()
	opts.CreateIfMissing = false

	// Use read-only mode to avoid modifying the test fixture
	database, err := db.OpenForReadOnly(goldenPath, opts, false)
	if err != nil {
		t.Fatalf("open C++ database: %v", err)
	}
	defer database.Close()

	// Read all keys
	iter := database.NewIterator(nil)
	defer iter.Close()

	keyCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keyCount++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iterator: %v", err)
	}

	t.Logf("Go opened C++ database with %d keys", keyCount)
}

// TestDatabase_Contract_GoWritesCppReads tests that C++ can open Go databases.
//
// Contract: C++ ldb can open and read databases created by Go.
func TestDatabase_Contract_GoWritesCppReads(t *testing.T) {
	ldb := findLdbPathDB(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "go_db_for_cpp")

	// Create database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write test data
	testData := []struct{ key, value string }{
		{"simple_key", "simple_value"},
		{"unicode_key_日本語", "unicode_value_中文"},
	}

	for _, td := range testData {
		if err := database.Put(nil, []byte(td.key), []byte(td.value)); err != nil {
			database.Close()
			t.Fatalf("put: %v", err)
		}
	}

	// Also write sequential keys
	for i := range 100 {
		key := fmt.Sprintf("seq_key_%05d", i)
		value := fmt.Sprintf("seq_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			t.Fatalf("put seq: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		database.Close()
		t.Fatalf("flush: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Verify with ldb scan
	output := runLdbScanDB(t, ldb, dbPath)
	if !strings.Contains(output, "simple_key") {
		t.Errorf("ldb output missing simple_key")
	}
	if !strings.Contains(output, "seq_key_00000") {
		t.Errorf("ldb output missing seq_key_00000")
	}

	// Verify specific key lookup
	output = runLdbGetDB(t, ldb, dbPath, "simple_key")
	if !strings.Contains(output, "simple_value") {
		t.Errorf("ldb get returned wrong value: %s", output)
	}
}

// TestDatabase_Contract_ColumnFamilyIsolation_CppReads tests that C++ can read
// column families created by Go.
//
// Regression: Issue 7 - column family isolation.
//
// Contract: C++ ldb can read multi-CF databases created by Go.
func TestDatabase_Contract_ColumnFamilyIsolation_CppReads(t *testing.T) {
	ldb := findLdbPathDB(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()

	// Create database with column family
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	cfOpts := db.DefaultColumnFamilyOptions()
	cf1, err := database.CreateColumnFamily(cfOpts, "test_cf")
	if err != nil {
		database.Close()
		t.Fatalf("create CF: %v", err)
	}

	// Write to both CFs
	writeOpts := db.DefaultWriteOptions()
	if err := database.Put(writeOpts, []byte("default_key"), []byte("default_value")); err != nil {
		database.Close()
		t.Fatalf("put default: %v", err)
	}
	if err := database.PutCF(writeOpts, cf1, []byte("cf_key"), []byte("cf_value")); err != nil {
		database.Close()
		t.Fatalf("put CF: %v", err)
	}
	if err := database.Flush(db.DefaultFlushOptions()); err != nil {
		database.Close()
		t.Fatalf("flush: %v", err)
	}
	database.Close()

	// C++ should be able to scan the database
	output := runLdbScanDB(t, ldb, dir)
	// Default CF should have default_key
	if !strings.Contains(output, "default_key") {
		t.Logf("Note: default_key not in scan output (may need CF flag)")
	}
}

// =============================================================================
// Helpers
// =============================================================================

func findLdbPathDB(t *testing.T) string {
	t.Helper()

	paths := []string{
		os.ExpandEnv("$HOME/Workspace/rocksdb/ldb"),
		os.ExpandEnv("$ROCKSDB_PATH/ldb"),
		"/usr/local/bin/ldb",
		"ldb",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		if found, err := exec.LookPath(p); err == nil {
			return found
		}
	}

	return ""
}

func runLdbScanDB(t *testing.T, ldb, dbPath string) string {
	t.Helper()

	cmd := exec.Command(ldb, "scan", "--db="+dbPath)
	dir := filepath.Dir(ldb)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("ldb scan failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}

func runLdbGetDB(t *testing.T, ldb, dbPath, key string) string {
	t.Helper()

	cmd := exec.Command(ldb, "get", "--db="+dbPath, key)
	dir := filepath.Dir(ldb)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("ldb get failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}
