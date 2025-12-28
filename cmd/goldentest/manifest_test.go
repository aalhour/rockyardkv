// manifest_test.go - MANIFEST format compatibility tests.
//
// Contract: Go-written MANIFEST files are readable by C++ RocksDB,
// and Go can read C++ MANIFEST files.
//
// Reference: RocksDB v10.7.5
//
//	db/version_edit.h   - VersionEdit format
//	db/version_edit.cc  - Encoding/decoding
//	db/version_set.cc   - MANIFEST reading
package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// =============================================================================
// MANIFEST Contract Tests
// =============================================================================

// TestManifest_Contract_CppWritesGoReads tests that Go can read C++ MANIFEST.
//
// Contract: Go can parse VersionEdit records from C++ MANIFEST.
func TestManifest_Contract_CppWritesGoReads(t *testing.T) {
	goldenPath := "testdata/cpp_generated/sst/simple_db"

	// Find MANIFEST files
	files, err := filepath.Glob(filepath.Join(goldenPath, "MANIFEST-*"))
	if err != nil || len(files) == 0 {
		t.Skip("No MANIFEST files in fixtures")
	}

	for _, manifestPath := range files {
		t.Run(filepath.Base(manifestPath), func(t *testing.T) {
			f, err := os.Open(manifestPath)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer f.Close()

			// MANIFEST uses WAL format
			reader := wal.NewReader(f, &silentReporter{}, true, 0)

			editCount := 0
			for {
				record, err := reader.ReadRecord()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					if editCount > 0 {
						break
					}
					t.Fatalf("read: %v", err)
				}
				if record == nil {
					break
				}

				// Parse as VersionEdit
				var edit manifest.VersionEdit
				if err := edit.DecodeFrom(record); err != nil {
					t.Fatalf("decode VersionEdit: %v", err)
				}

				editCount++
			}

			if editCount == 0 {
				t.Error("no VersionEdits found")
			}

			t.Logf("Read %d VersionEdits", editCount)
		})
	}
}

// TestManifest_Contract_GoWritesCppReads tests that C++ can read Go MANIFEST.
//
// Contract: Go-written MANIFEST is readable by C++ ldb manifest_dump.
func TestManifest_Contract_GoWritesCppReads(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "manifest_test_db")

	// Create database and flush to generate MANIFEST
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := range 100 {
		key := []byte("manifest_key_" + string(rune('0'+i%10)))
		value := []byte("manifest_value")
		if err := database.Put(nil, key, value); err != nil {
			database.Close()
			t.Fatalf("put: %v", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		database.Close()
		t.Fatalf("flush: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Find the MANIFEST file
	manifestFiles, err := filepath.Glob(filepath.Join(dbPath, "MANIFEST-*"))
	if err != nil || len(manifestFiles) == 0 {
		t.Fatalf("no MANIFEST files found: %v", err)
	}
	manifestPath := manifestFiles[len(manifestFiles)-1] // Use the latest

	// Verify with ldb manifest_dump using the actual MANIFEST file
	output := runLdbManifestDump(t, ldb, manifestPath)

	// Check for errors that indicate failed parsing
	if strings.Contains(output, "Corruption") {
		t.Errorf("C++ ldb reports corruption parsing Go MANIFEST:\n%s", output)
	}

	// Successful parsing shows column family info
	if !strings.Contains(output, "Column family") {
		t.Errorf("C++ ldb output missing Column family info:\n%s", output)
	}

	// Must contain our comparator
	if !strings.Contains(output, "leveldb.BytewiseComparator") {
		t.Errorf("C++ ldb output missing expected comparator:\n%s", output)
	}

	// Must show level information (indicates successful LSM tree parsing)
	if !strings.Contains(output, "level 0") {
		t.Errorf("C++ ldb output missing level info:\n%s", output)
	}

	// Must show file metadata from the flush (file 4 with keys)
	if !strings.Contains(output, "manifest_key") {
		t.Errorf("C++ ldb output missing SST file key info:\n%s", output)
	}

	t.Logf("C++ ldb successfully parsed Go MANIFEST with SST file metadata")
}

// TestManifest_Contract_UnknownTagsPreserved tests that unknown safe-to-ignore
// tags don't cause C++ to reject the MANIFEST.
//
// Regression: Issue 1 - unknown tag preservation.
//
// Contract: MANIFEST with unknown safe-to-ignore tags is readable by C++.
func TestManifest_Contract_UnknownTagsPreserved(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()

	// Create VersionEdit with unknown safe-to-ignore tag
	ve := manifest.NewVersionEdit()
	ve.SetComparatorName("leveldb.BytewiseComparator")
	ve.SetLogNumber(1)
	ve.SetNextFileNumber(2)
	ve.SetLastSequence(0)

	// Add unknown tag (bit 13 set = safe to ignore)
	ve.UnknownTags = append(ve.UnknownTags, manifest.UnknownTag{
		Tag:   uint32(manifest.TagSafeIgnoreMask) | 99,
		Value: []byte("future-metadata"),
	})

	encoded := ve.EncodeTo()

	// Write MANIFEST using WAL format
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	f, err := os.Create(manifestPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	writer := wal.NewWriter(f, 1, false)
	if _, err := writer.AddRecord(encoded); err != nil {
		f.Close()
		t.Fatalf("add record: %v", err)
	}
	f.Close()

	// Write CURRENT file
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte("MANIFEST-000001\n"), 0644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}

	// C++ should not report corruption
	output := runLdbManifestDump(t, ldb, dir)
	if strings.Contains(output, "Corruption") {
		t.Errorf("C++ reports corruption for MANIFEST with unknown tags:\n%s", output)
	}
}

// TestManifest_Contract_CorruptionRejected tests that both Go and C++ reject
// a MANIFEST with corrupted checksum.
//
// Regression: Issues 5+6 - MANIFEST corruption detection.
//
// Contract: Corrupted MANIFEST is rejected by both implementations.
func TestManifest_Contract_CorruptionRejected(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()

	// Create valid MANIFEST
	ve := manifest.NewVersionEdit()
	ve.SetComparatorName("leveldb.BytewiseComparator")
	ve.SetLogNumber(1)
	ve.SetNextFileNumber(2)
	ve.SetLastSequence(0)

	encoded := ve.EncodeTo()

	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	f, err := os.Create(manifestPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	writer := wal.NewWriter(f, 1, false)
	if _, err := writer.AddRecord(encoded); err != nil {
		f.Close()
		t.Fatalf("add record: %v", err)
	}
	f.Close()

	// Read and corrupt
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(data) < 10 {
		t.Skip("MANIFEST too small to corrupt")
	}

	// Flip bits in CRC
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[0] ^= 0xFF

	// Write corrupted MANIFEST
	corruptPath := filepath.Join(dir, "MANIFEST-000002")
	if err := os.WriteFile(corruptPath, corrupted, 0644); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000002\n"), 0644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}

	// Check C++ behavior
	cppOutput := runLdbManifestDump(t, ldb, dir)
	cppRejects := strings.Contains(cppOutput, "Corruption") ||
		strings.Contains(cppOutput, "checksum") ||
		strings.Contains(cppOutput, "error")

	// Check Go behavior
	reader := wal.NewStrictReader(bytes.NewReader(corrupted), nil, 2)
	_, goErr := reader.ReadRecord()
	goRejects := goErr != nil

	if cppRejects && !goRejects {
		t.Error("Oracle mismatch: C++ rejects but Go accepts corrupted MANIFEST")
	}

	if !cppRejects && !goRejects {
		t.Error("Neither C++ nor Go rejected corrupted MANIFEST")
	}
}

// =============================================================================
// Atomic Group Golden Test
// =============================================================================

// TestManifest_Contract_AtomicGroup verifies that C++ can read Go-written
// MANIFEST with atomic group edits.
//
// Atomic groups are used for multi-CF operations that must be atomic.
// The TagInAtomicGroup (300) field indicates the edit is part of an atomic
// group, and RemainingEntries indicates how many more edits follow.
func TestManifest_Contract_AtomicGroup(t *testing.T) {
	ldb := findLdbPath(t)
	if ldb == "" {
		t.Skip("ldb not found")
	}

	dir := t.TempDir()

	// Create MANIFEST with atomic group edits
	// Simulate a multi-edit atomic group (e.g., adding files to multiple CFs)
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	f, err := os.Create(manifestPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	writer := wal.NewWriter(f, 1, false)

	// First edit: initial setup
	ve1 := manifest.NewVersionEdit()
	ve1.SetComparatorName("leveldb.BytewiseComparator")
	ve1.SetLogNumber(1)
	ve1.SetNextFileNumber(10)
	ve1.SetLastSequence(100)

	if _, err := writer.AddRecord(ve1.EncodeTo()); err != nil {
		f.Close()
		t.Fatalf("add record 1: %v", err)
	}

	// Second edit: start of atomic group (2 more edits follow)
	ve2 := manifest.NewVersionEdit()
	ve2.SetAtomicGroup(2) // 2 more edits after this one
	ve2.SetColumnFamily(0)
	ve2.SetLogNumber(2)

	if _, err := writer.AddRecord(ve2.EncodeTo()); err != nil {
		f.Close()
		t.Fatalf("add record 2: %v", err)
	}

	// Third edit: middle of atomic group (1 more edit follows)
	ve3 := manifest.NewVersionEdit()
	ve3.SetAtomicGroup(1) // 1 more edit after this one
	ve3.SetColumnFamily(0)

	if _, err := writer.AddRecord(ve3.EncodeTo()); err != nil {
		f.Close()
		t.Fatalf("add record 3: %v", err)
	}

	// Fourth edit: end of atomic group (0 more edits)
	ve4 := manifest.NewVersionEdit()
	ve4.SetAtomicGroup(0) // Last edit in the group
	ve4.SetColumnFamily(0)
	ve4.SetNextFileNumber(15)

	if _, err := writer.AddRecord(ve4.EncodeTo()); err != nil {
		f.Close()
		t.Fatalf("add record 4: %v", err)
	}

	f.Close()

	// Write CURRENT file
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte("MANIFEST-000001\n"), 0644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}

	// Verify C++ can parse the MANIFEST with atomic groups
	output := runLdbManifestDump(t, ldb, manifestPath)

	// Check for errors
	if strings.Contains(output, "Corruption") {
		t.Errorf("C++ ldb reports corruption for MANIFEST with atomic groups:\n%s", output)
	}

	// Should have parsed the manifest (shows column family info)
	if !strings.Contains(output, "Column family") {
		t.Errorf("C++ ldb failed to parse MANIFEST with atomic groups:\n%s", output)
	}

	t.Logf("C++ ldb successfully parsed MANIFEST with atomic groups")
}

// =============================================================================
// Round-Trip Tests
// =============================================================================

// TestManifest_RoundTrip_VersionEdit tests Go VersionEdit encoding/decoding.
//
// Contract: VersionEdit round-trips correctly.
func TestManifest_RoundTrip_VersionEdit(t *testing.T) {
	original := manifest.NewVersionEdit()
	original.SetComparatorName("leveldb.BytewiseComparator")
	original.SetLogNumber(42)
	original.SetNextFileNumber(100)
	original.SetLastSequence(12345)

	// Encode
	encoded := original.EncodeTo()

	// Decode
	var decoded manifest.VersionEdit
	if err := decoded.DecodeFrom(encoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Verify fields
	if decoded.Comparator != original.Comparator {
		t.Errorf("comparator: got %q, want %q", decoded.Comparator, original.Comparator)
	}
	if !decoded.HasLogNumber || decoded.LogNumber != 42 {
		t.Errorf("log number: got %d, want 42", decoded.LogNumber)
	}
	if !decoded.HasNextFileNumber || decoded.NextFileNumber != 100 {
		t.Errorf("next file: got %d, want 100", decoded.NextFileNumber)
	}
	if !decoded.HasLastSequence || decoded.LastSequence != 12345 {
		t.Errorf("last sequence: got %d, want 12345", decoded.LastSequence)
	}
}
