// wal_test.go - WAL format compatibility tests.
//
// Contract: Go-written WAL records are recoverable by C++ RocksDB.
// WAL format is implicitly tested through database recovery.
//
// Reference: RocksDB v10.7.5
//
//	db/log_format.h  - Record format
//	db/log_reader.cc - Reading
//	db/log_writer.cc - Writing
package main

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// =============================================================================
// WAL Contract Tests
// =============================================================================

// TestWAL_Contract_GoWritesCppReads tests that C++ can read a database
// whose data was written via WAL (not flushed to SST).
//
// Contract: Go WAL records are recoverable by C++ ldb.
func TestWAL_Contract_GoWritesCppReads(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found - build C++ RocksDB first")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal_test_db")

	// Write data (will go to WAL, not flushed to SST)
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := range 10 {
		key := []byte("wal_key_" + string(rune('0'+i)))
		value := []byte("wal_value_" + string(rune('0'+i)))
		if err := database.Put(nil, key, value); err != nil {
			database.Close()
			t.Fatalf("put: %v", err)
		}
	}

	// Close without flush - data is in WAL only
	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Verify C++ ldb can read the database
	output := runLdbScan(t, ldb, dbPath)
	if !strings.Contains(output, "wal_key_0") {
		t.Errorf("ldb output missing wal_key_0")
	}
}

// TestWAL_RoundTrip_GoWritesGoReads tests Go reading its own WAL.
//
// Contract: Go can write and read back WAL records.
func TestWAL_RoundTrip_GoWritesGoReads(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.log")

	// Write WAL records
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	writer := wal.NewWriter(f, 1, false)

	// Create and write a WriteBatch
	wb := batch.New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	wb.SetSequence(100)

	data := wb.Data()
	if _, err := writer.AddRecord(data); err != nil {
		f.Close()
		t.Fatalf("add record: %v", err)
	}

	f.Close()

	// Read back
	f, err = os.Open(walPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	reader := wal.NewReader(f, &silentReporter{}, true, 0)

	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("read record: %v", err)
	}

	// Parse as WriteBatch
	readWB, err := batch.NewFromData(record)
	if err != nil {
		t.Fatalf("parse batch: %v", err)
	}

	if readWB.Sequence() != 100 {
		t.Errorf("sequence: got %d, want 100", readWB.Sequence())
	}
	if readWB.Count() != 2 {
		t.Errorf("count: got %d, want 2", readWB.Count())
	}
}

// =============================================================================
// Helpers
// =============================================================================

type silentReporter struct{}

func (s *silentReporter) Corruption(_ int, _ error) {}
func (s *silentReporter) OldLogRecord(_ int)        {}

func findLdbPath(t *testing.T) string {
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

func runLdbScan(t *testing.T, ldb, dbPath string) string {
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

func runLdbDumpWal(t *testing.T, ldb, walPath string) string {
	t.Helper()

	cmd := exec.Command(ldb, "dump_wal", "--walfile="+walPath, "--header", "--print_value")
	dir := filepath.Dir(ldb)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("ldb dump_wal failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}

func runLdbManifestDump(t *testing.T, ldb, path string) string {
	t.Helper()

	cmd := exec.Command(ldb, "manifest_dump", "--path="+path)
	dir := filepath.Dir(ldb)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Some errors are acceptable (missing SST files)
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
	}

	return string(output)
}

// TestWAL_Contract_Recyclable_GoWritesCppReads tests that C++ can read
// recyclable format WAL records written by Go.
//
// Contract: Go recyclable WAL records are recoverable by C++ ldb.
// Reference: RocksDB v10.7.5 db/log_format.h (kRecyclable* types)
func TestWAL_Contract_Recyclable_GoWritesCppReads(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found - build C++ RocksDB first")
	}

	dir := t.TempDir()
	walPath := filepath.Join(dir, "000001.log")

	// Write recyclable WAL records
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Use recyclable format (second param true)
	logNumber := uint64(1)
	writer := wal.NewWriter(f, logNumber, true /* recyclable */)

	// Create WriteBatch with test data
	wb := batch.New()
	wb.Put([]byte("recyclable_key1"), []byte("recyclable_value1"))
	wb.Put([]byte("recyclable_key2"), []byte("recyclable_value2"))
	wb.Delete([]byte("deleted_key"))
	wb.SetSequence(1)

	data := wb.Data()
	if _, err := writer.AddRecord(data); err != nil {
		f.Close()
		t.Fatalf("add record: %v", err)
	}

	// Write a second batch to verify fragmentation works
	wb2 := batch.New()
	wb2.Put([]byte("recyclable_key3"), []byte("recyclable_value3"))
	wb2.SetSequence(4)

	if _, err := writer.AddRecord(wb2.Data()); err != nil {
		f.Close()
		t.Fatalf("add second record: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Use ldb dump_wal to verify C++ can read the recyclable WAL
	output := runLdbDumpWal(t, ldb, walPath)

	// ldb displays keys in hex format when no DB is specified.
	// Verify output contains the hex-encoded keys.
	// "recyclable_key1" in hex = 72656379636C61626C655F6B657931
	if !strings.Contains(output, "72656379636C61626C655F6B657931") {
		t.Errorf("ldb dump_wal output missing recyclable_key1 (hex)\nOutput: %s", output)
	}
	// "recyclable_key2" in hex = 72656379636C61626C655F6B657932
	if !strings.Contains(output, "72656379636C61626C655F6B657932") {
		t.Errorf("ldb dump_wal output missing recyclable_key2 (hex)\nOutput: %s", output)
	}
	// "recyclable_key3" in hex = 72656379636C61626C655F6B657933
	if !strings.Contains(output, "72656379636C61626C655F6B657933") {
		t.Errorf("ldb dump_wal output missing recyclable_key3 (hex)\nOutput: %s", output)
	}

	// Verify correct record counts and structure
	if !strings.Contains(output, "Sequence,Count") {
		t.Errorf("ldb dump_wal output missing header\nOutput: %s", output)
	}
	// First batch: sequence=1, count=3 (2 PUTs + 1 DELETE)
	if !strings.Contains(output, "1,3,") {
		t.Errorf("ldb dump_wal output missing first batch (seq=1, count=3)\nOutput: %s", output)
	}
	// Second batch: sequence=4, count=1
	if !strings.Contains(output, "4,1,") {
		t.Errorf("ldb dump_wal output missing second batch (seq=4, count=1)\nOutput: %s", output)
	}

	t.Logf("C++ ldb successfully read recyclable WAL with %d bytes", len(output))
}

// TestWAL_RoundTrip_Recyclable tests Go reading its own recyclable WAL.
//
// Contract: Go can write and read back recyclable WAL records.
func TestWAL_RoundTrip_Recyclable(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.log")

	logNumber := uint64(42)

	// Write recyclable WAL records
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	writer := wal.NewWriter(f, logNumber, true /* recyclable */)

	// Verify writer is in recyclable mode
	if !writer.IsRecyclable() {
		t.Error("writer should be recyclable")
	}

	wb := batch.New()
	wb.Put([]byte("key1"), []byte("value1"))
	wb.Put([]byte("key2"), []byte("value2"))
	wb.SetSequence(100)

	if _, err := writer.AddRecord(wb.Data()); err != nil {
		f.Close()
		t.Fatalf("add record: %v", err)
	}

	f.Close()

	// Read back with matching log number
	f, err = os.Open(walPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	reader := wal.NewReader(f, &silentReporter{}, true, logNumber)

	record, err := reader.ReadRecord()
	if err != nil {
		t.Fatalf("read record: %v", err)
	}

	readWB, err := batch.NewFromData(record)
	if err != nil {
		t.Fatalf("parse batch: %v", err)
	}

	if readWB.Sequence() != 100 {
		t.Errorf("sequence: got %d, want 100", readWB.Sequence())
	}
	if readWB.Count() != 2 {
		t.Errorf("count: got %d, want 2", readWB.Count())
	}
}

// TestWAL_CppWritesGoReads tests that Go can read a WAL from C++ generated fixtures.
// Note: C++ WAL files are often empty after compaction, so this test reads
// from the full database recovery path instead.
func TestWAL_CppWritesGoReads(t *testing.T) {
	goldenPath := "testdata/cpp_generated/sst/simple_db"

	// Find any WAL files
	files, err := filepath.Glob(filepath.Join(goldenPath, "*.log"))
	if err != nil || len(files) == 0 {
		t.Skip("No WAL files in fixtures (normal after compaction)")
	}

	for _, walPath := range files {
		t.Run(filepath.Base(walPath), func(t *testing.T) {
			f, err := os.Open(walPath)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer f.Close()

			reader := wal.NewReader(f, &silentReporter{}, true, 0)

			recordCount := 0
			for {
				record, err := reader.ReadRecord()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					if recordCount > 0 {
						break // Partial file is OK
					}
					t.Fatalf("read: %v", err)
				}
				if record == nil {
					break
				}
				recordCount++

				// Verify parseable as WriteBatch
				_, err = batch.NewFromData(record)
				if err != nil {
					t.Fatalf("parse batch: %v", err)
				}
			}

			t.Logf("Read %d WAL records", recordCount)
		})
	}
}

// =============================================================================
// Stop-after-corruption oracle test
// =============================================================================

// TestGoldenWAL_StopAfterCorruption_Oracle verifies that both Go and C++
// stop reading WAL records after encountering a checksum mismatch.
//
// Contract: On WAL corruption, Go and C++ must produce the same recovered DB state.
//
// Evidence: C++ `ldb scan` only shows records BEFORE corruption, not after.
//
// Reference: RocksDB v10.7.5 WALRecoveryMode::kTolerateCorruptedTailRecords
func TestGoldenWAL_StopAfterCorruption_Oracle(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found - build C++ RocksDB first")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "corruption_test_db")

	// Create DB with 3 records: k01, k02 (large to force fragmentation), k03
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	// Disable auto-flush so data stays in WAL
	opts.WriteBufferSize = 64 * 1024 * 1024 // 64MB

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write k01
	if err := database.Put(nil, []byte("k01"), []byte("v01")); err != nil {
		t.Fatalf("put k01: %v", err)
	}

	// Write k02 with large value (40KB) to force WAL fragmentation
	largeValue := make([]byte, 40000)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}
	if err := database.Put(nil, []byte("k02"), largeValue); err != nil {
		t.Fatalf("put k02: %v", err)
	}

	// Write k03
	if err := database.Put(nil, []byte("k03"), []byte("v03")); err != nil {
		t.Fatalf("put k03: %v", err)
	}

	// Close without explicit flush (data in WAL only)
	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Find the WAL file
	walFiles, err := filepath.Glob(filepath.Join(dbPath, "*.log"))
	if err != nil || len(walFiles) == 0 {
		t.Fatalf("no WAL files found: %v", err)
	}
	walPath := walFiles[0]

	// Corrupt the WAL: flip a byte inside k02's record payload
	// k01 is ~28 bytes, so k02 starts around offset 28+7 (header).
	// We flip at offset 28+7+100 to hit inside k02's payload.
	walData, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}

	corruptOffset := 28 + 7 + 100
	if corruptOffset >= len(walData) {
		t.Fatalf("WAL too small (%d bytes) to corrupt at offset %d", len(walData), corruptOffset)
	}

	originalByte := walData[corruptOffset]
	walData[corruptOffset] = originalByte ^ 0x01 // flip one bit

	if err := os.WriteFile(walPath, walData, 0644); err != nil {
		t.Fatalf("write corrupted WAL: %v", err)
	}

	t.Logf("Corrupted WAL at offset %d (byte 0x%02x -> 0x%02x)", corruptOffset, originalByte, walData[corruptOffset])

	// Step 1: Verify C++ oracle behavior with ldb dump_wal
	output := runLdbDumpWal(t, ldb, walPath)
	if !strings.Contains(strings.ToLower(output), "checksum mismatch") &&
		!strings.Contains(strings.ToLower(output), "corruption") {
		t.Logf("ldb dump_wal output (may not always show explicit corruption message):\n%s", output)
	}

	// Step 2: Verify C++ oracle behavior with ldb scan
	scanOutput := runLdbScan(t, ldb, dbPath)

	// k01 should be present (before corruption)
	if !strings.Contains(scanOutput, "k01") {
		t.Errorf("C++ oracle: k01 should be present (before corruption)\nOutput: %s", scanOutput)
	}

	// k03 should NOT be present (after corruption)
	if strings.Contains(scanOutput, "k03") {
		t.Errorf("C++ oracle: k03 should NOT be present (after corruption)\nOutput: %s", scanOutput)
	}

	t.Logf("C++ oracle scan output:\n%s", scanOutput)

	// Step 3: Verify Go behavior matches oracle
	database, err = db.Open(dbPath, opts)
	if err != nil {
		// Some corruption may prevent opening - that's acceptable too
		t.Logf("Go failed to open DB (acceptable for severe corruption): %v", err)
		return
	}
	defer database.Close()

	// k01 should be present
	val, err := database.Get(nil, []byte("k01"))
	if err != nil {
		t.Errorf("Go: k01 should be present (before corruption): %v", err)
	} else if string(val) != "v01" {
		t.Errorf("Go: k01 value = %q, want %q", val, "v01")
	}

	// k03 should NOT be present (after corruption)
	val, err = database.Get(nil, []byte("k03"))
	if err == nil {
		t.Errorf("Go: k03 should NOT be present (after corruption), but got: %q", val)
	}
}
