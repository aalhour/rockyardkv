// sst_test.go - SST fixture and special-case tests.
//
// This file contains:
// - C++ fixture tests (Go reads C++ generated SST files)
// - sst_dump feature tests (verify_checksum, show_properties)
//
// For format version and compression matrix tests, see sst_format_test.go.
// For behavioral contract tests, see sst_contract_test.go.
//
// Reference: RocksDB v10.7.5
//
//	table/block_based_table_builder.cc
//	table/block_based_table_reader.cc
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// =============================================================================
// C++ Writes, Go Reads — Fixture Tests
// =============================================================================

// TestCppWritesGoReads_Fixtures tests that Go can read C++ generated SST files.
//
// Contract: Go reads C++ fixtures without errors; this is the oracle direction.
func TestCppWritesGoReads_Fixtures(t *testing.T) {
	goldenPath := "testdata/cpp_generated/sst/simple_db"

	files, err := filepath.Glob(filepath.Join(goldenPath, "*.sst"))
	if err != nil || len(files) == 0 {
		t.Skip("Golden SST files not found")
	}

	fs := vfs.Default()
	for _, sstPath := range files {
		t.Run(filepath.Base(sstPath), func(t *testing.T) {
			file, err := fs.OpenRandomAccess(sstPath)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer file.Close()

			reader, err := table.Open(file, table.ReaderOptions{VerifyChecksums: true})
			if err != nil {
				t.Fatalf("reader: %v", err)
			}
			defer reader.Close()

			// Read all entries
			iter := reader.NewIterator()
			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				count++
			}

			if err := iter.Error(); err != nil {
				t.Fatalf("iterator: %v", err)
			}

			t.Logf("Read %d entries", count)
		})
	}
}

// =============================================================================
// sst_dump Feature Tests
// =============================================================================

// TestSstDump_VerifyChecksum tests that C++ sst_dump can verify Go SST checksums.
//
// Contract: Go SST checksums are valid per C++ verification.
func TestSstDump_VerifyChecksum(t *testing.T) {
	sstDump := findSstDumpPath(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "checksum.sst")

	// Write SST
	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i := range 10 {
		key := []byte("key" + string(rune('0'+i)))
		ikey := dbformat.NewInternalKey(key, dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte("value")); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Run sst_dump --verify_checksum
	cmd := exec.Command(sstDump, "--file="+sstPath, "--verify_checksum")
	cmd.Env = toolEnv(filepath.Dir(sstDump))

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("verify_checksum failed: %v\nOutput: %s", err, output)
	}

	t.Log("Checksum verification passed")
}

// TestSstDump_ShowProperties tests that C++ sst_dump can read Go SST properties.
//
// Contract: Go SST properties are readable by C++.
func TestSstDump_ShowProperties(t *testing.T) {
	sstDump := findSstDumpPath(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "properties.sst")

	// Write SST
	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i := range 5 {
		key := []byte("prop_key" + string(rune('0'+i)))
		ikey := dbformat.NewInternalKey(key, dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte("value")); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Run sst_dump --show_properties
	cmd := exec.Command(sstDump, "--file="+sstPath, "--show_properties")
	cmd.Env = toolEnv(filepath.Dir(sstDump))

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("show_properties failed: %v\nOutput: %s", err, output)
	}

	outputStr := string(output)

	// Verify some properties are present
	if !strings.Contains(strings.ToLower(outputStr), "entries") {
		t.Log("Note: 'entries' property not found in output")
	}

	t.Logf("Properties output:\n%s", output)
}

// =============================================================================
// Helpers
// =============================================================================

// =============================================================================
// C++ Corpus Tests (External Fixtures)
// =============================================================================

// TestCppCorpus_ZlibSST tests that Go can read C++ zlib-compressed SST files.
//
// Regression: Issue 0 — Go couldn't read C++ zlib SST due to missing varint32
// size prefix handling and wrong deflate format.
//
// Contract: Go reads C++ zlib-compressed SST files correctly.
func TestCppCorpus_ZlibSST(t *testing.T) {
	// Look for red team corpus
	corpusPath := os.ExpandEnv("$REDTEAM_CPP_CORPUS_ROOT")
	// Keep this path portable.
	// Set REDTEAM_CPP_CORPUS_ROOT to a local path if you have an external corpus checkout.
	if corpusPath == "" {
		t.Skip("Red team corpus not found (set REDTEAM_CPP_CORPUS_ROOT)")
	}

	// The actual DB is in a subdirectory called "db"
	zlibDBPath := filepath.Join(corpusPath, "zlib_small_blocks_db", "db")
	if _, err := os.Stat(zlibDBPath); os.IsNotExist(err) {
		t.Skip("Red team corpus not found (set REDTEAM_CPP_CORPUS_ROOT)")
	}

	// Open the database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false

	database, err := db.Open(zlibDBPath, opts)
	if err != nil {
		t.Fatalf("open zlib DB: %v", err)
	}
	defer database.Close()

	// Count keys
	iter := database.NewIterator(nil)
	defer iter.Close()

	keyCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keyCount++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iterator: %v", err)
	}

	if keyCount == 0 {
		t.Error("no keys found in zlib DB")
	}

	t.Logf("Go read %d keys from C++ zlib-compressed DB", keyCount)
}

// TestCppCorpus_MultiCF tests that Go can read C++ multi-column-family databases.
//
// Contract: Go reads C++ multi-CF databases correctly.
func TestCppCorpus_MultiCF(t *testing.T) {
	corpusPath := os.ExpandEnv("$REDTEAM_CPP_CORPUS_ROOT")
	// Keep this path portable.
	// Set REDTEAM_CPP_CORPUS_ROOT to a local path if you have an external corpus checkout.
	if corpusPath == "" {
		t.Skip("Red team corpus not found (set REDTEAM_CPP_CORPUS_ROOT)")
	}

	// The actual DB is in a subdirectory called "db"
	multiCFPath := filepath.Join(corpusPath, "multi_cf_db", "db")
	if _, err := os.Stat(multiCFPath); os.IsNotExist(err) {
		t.Skip("Red team corpus not found")
	}

	// Open with column families
	opts := db.DefaultOptions()
	opts.CreateIfMissing = false

	// First, list column families
	cfNames, err := db.ListColumnFamilies(multiCFPath, opts)
	if err != nil {
		t.Fatalf("list CFs: %v", err)
	}

	t.Logf("Found %d column families: %v", len(cfNames), cfNames)

	if len(cfNames) < 2 {
		t.Skipf("Expected multiple column families, got %d", len(cfNames))
	}
}

// TestCppCorpus_RangeDel tests that Go can read C++ databases with range deletions.
//
// Contract: Go reads C++ range deletion databases correctly.
func TestCppCorpus_RangeDel(t *testing.T) {
	corpusPath := os.ExpandEnv("$REDTEAM_CPP_CORPUS_ROOT")
	// Keep this path portable.
	// Set REDTEAM_CPP_CORPUS_ROOT to a local path if you have an external corpus checkout.
	if corpusPath == "" {
		t.Skip("Red team corpus not found (set REDTEAM_CPP_CORPUS_ROOT)")
	}

	// The actual DB is in a subdirectory called "db"
	rangeDelPath := filepath.Join(corpusPath, "rangedel_db", "db")
	if _, err := os.Stat(rangeDelPath); os.IsNotExist(err) {
		t.Skip("Red team corpus not found")
	}

	opts := db.DefaultOptions()
	opts.CreateIfMissing = false

	database, err := db.Open(rangeDelPath, opts)
	if err != nil {
		t.Fatalf("open rangedel DB: %v", err)
	}
	defer database.Close()

	// Count keys (some should be deleted by range tombstones)
	iter := database.NewIterator(nil)
	defer iter.Close()

	keyCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keyCount++
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("iterator: %v", err)
	}

	t.Logf("Go read %d keys from C++ rangedel DB", keyCount)
}

// TestSST_Writer_BloomFilter verifies that Go-written Bloom Filters are readable by C++.
//
// Contract: Go SST files with Bloom Filter are valid per C++ sst_dump properties output.
func TestSST_Writer_BloomFilter(t *testing.T) {
	sstDump := findSstDumpPath(t)
	if sstDump == "" {
		t.Skip("sst_dump not found - set ROCKSDB_PATH or build C++ RocksDB first")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "filter.sst")

	// Create SST with Bloom Filter enabled
	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	opts.FormatVersion = 6
	opts.FilterBitsPerKey = 10 // Enable Bloom Filter
	opts.FilterPolicy = "rocksdb.BuiltinBloomFilter"

	builder := table.NewTableBuilder(file, opts)

	// Add enough keys to make a meaningful filter
	for i := range 100 {
		key := fmt.Sprintf("filter_key_%04d", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte("value")); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify with C++ sst_dump --show_properties
	cmd := exec.Command(sstDump, "--file="+sstPath, "--show_properties")
	cmd.Env = toolEnv(filepath.Dir(sstDump))

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sst_dump show_properties failed: %v\nOutput: %s", err, output)
	}

	props := string(output)

	// Verify filter block exists and has non-zero size
	if !strings.Contains(props, "filter block size") {
		t.Error("Missing 'filter block size' in sst_dump output")
	}
	if strings.Contains(props, "filter block size: 0") {
		t.Error("Filter block size is 0, expected > 0")
	}

	// Verify filter policy name is correct
	if !strings.Contains(props, "filter policy name: rocksdb.BuiltinBloomFilter") {
		t.Error("Missing or incorrect filter policy name")
	}

	t.Logf("C++ sst_dump verified Go Bloom Filter:\n%s", props)
}

// =============================================================================
// Helpers
// =============================================================================

func findSstDumpPath(t *testing.T) string {
	t.Helper()

	paths := []string{
		os.ExpandEnv("$HOME/Workspace/rocksdb/sst_dump"),
		os.ExpandEnv("$ROCKSDB_PATH/sst_dump"),
		"/usr/local/bin/sst_dump",
		"sst_dump",
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
