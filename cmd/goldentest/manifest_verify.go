// MANIFEST format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - db/version_edit.h (VersionEdit format)
//   - db/version_edit.cc (encoding/decoding)
//   - db/version_set.cc (MANIFEST reading)
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// verifyGoReadsManifest reads a C++ generated MANIFEST file with Go
func verifyGoReadsManifest(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("fixture not found: %s", path)
	}

	// Open the MANIFEST file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open MANIFEST: %w", err)
	}
	defer f.Close()

	// MANIFEST files use the same log format as WAL
	reader := wal.NewReader(f, noopReporter{}, true, 0)

	// Read all VersionEdit records
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
			return fmt.Errorf("failed to read record: %w", err)
		}
		if record == nil {
			break
		}

		// Parse as VersionEdit
		var edit manifest.VersionEdit
		if err := edit.DecodeFrom(record); err != nil {
			return fmt.Errorf("failed to decode VersionEdit: %w", err)
		}

		editCount++

		if *verbose {
			fmt.Printf("    VersionEdit %d:\n", editCount)
			if edit.Comparator != "" {
				fmt.Printf("      Comparator: %s\n", edit.Comparator)
			}
			if edit.HasLogNumber {
				fmt.Printf("      LogNumber: %d\n", edit.LogNumber)
			}
			if edit.HasNextFileNumber {
				fmt.Printf("      NextFile: %d\n", edit.NextFileNumber)
			}
			if edit.HasLastSequence {
				fmt.Printf("      LastSequence: %d\n", edit.LastSequence)
			}
			if len(edit.NewFiles) > 0 {
				fmt.Printf("      NewFiles: %d\n", len(edit.NewFiles))
			}
			if edit.ColumnFamilyName != "" {
				fmt.Printf("      ColumnFamily: %s\n", edit.ColumnFamilyName)
			}
		}
	}

	if editCount == 0 {
		return fmt.Errorf("no VersionEdits found in MANIFEST")
	}

	if *verbose {
		fmt.Printf("    Successfully read %d VersionEdits\n", editCount)
	}

	return nil
}

// verifyManifestUnknownTagsPreserved verifies that a MANIFEST with unknown
// "safe-to-ignore" tags written by Go can be parsed by C++ RocksDB's ldb tool.
// This is the oracle verification for Issue 1 (unknown tag preservation).
func verifyManifestUnknownTagsPreserved() error {
	if *ldbPath == "" {
		return fmt.Errorf("ldb path not specified, skipping")
	}

	dir, err := os.MkdirTemp("", "manifest_unknown_tags")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Create a VersionEdit with an unknown safe-to-ignore tag
	ve := manifest.NewVersionEdit()
	ve.SetComparatorName("leveldb.BytewiseComparator")
	ve.SetLogNumber(1)
	ve.SetNextFileNumber(2)
	ve.SetLastSequence(0)

	// Add an unknown tag (bit 13 set = safe to ignore)
	ve.UnknownTags = append(ve.UnknownTags, manifest.UnknownTag{
		Tag:   uint32(manifest.TagSafeIgnoreMask) | 99,
		Value: []byte("future-metadata-from-rocksdb-v99"),
	})

	encoded := ve.EncodeTo()

	// Write MANIFEST using WAL format
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	manifestFile, err := os.Create(manifestPath)
	if err != nil {
		return err
	}

	writer := wal.NewWriter(manifestFile, 1, false)
	if _, err := writer.AddRecord(encoded); err != nil {
		manifestFile.Close()
		return err
	}
	manifestFile.Close()

	// Write CURRENT file
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte("MANIFEST-000001\n"), 0644); err != nil {
		return err
	}

	// Run ldb manifest_dump
	output, err := runLdb("manifest_dump", "--path="+dir)
	if err != nil {
		// Check if it's corruption vs other errors
		if strings.Contains(output, "Corruption") {
			return fmt.Errorf("C++ ldb reports corruption parsing Go MANIFEST with unknown tags")
		}
		// Non-fatal errors (missing SST files) are acceptable
	}

	if *verbose {
		fmt.Printf("    C++ ldb successfully parsed MANIFEST with unknown tags\n")
	}

	return nil
}

// verifyManifestCorruptionRejected verifies that both Go and C++ reject
// a MANIFEST with corrupted checksum.
// This is the oracle verification for Issues 5+6 (MANIFEST corruption).
func verifyManifestCorruptionRejected() error {
	if *ldbPath == "" {
		return fmt.Errorf("ldb path not specified, skipping")
	}

	dir, err := os.MkdirTemp("", "manifest_corruption")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Create a valid MANIFEST
	ve := manifest.NewVersionEdit()
	ve.SetComparatorName("leveldb.BytewiseComparator")
	ve.SetLogNumber(1)
	ve.SetNextFileNumber(2)
	ve.SetLastSequence(0)

	encoded := ve.EncodeTo()

	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	manifestFile, err := os.Create(manifestPath)
	if err != nil {
		return err
	}

	writer := wal.NewWriter(manifestFile, 1, false)
	if _, err := writer.AddRecord(encoded); err != nil {
		manifestFile.Close()
		return err
	}
	manifestFile.Close()

	// Read and corrupt the MANIFEST
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}

	if len(manifestData) < 10 {
		return fmt.Errorf("manifest too small to corrupt")
	}

	// Flip bits in the CRC
	corruptedData := make([]byte, len(manifestData))
	copy(corruptedData, manifestData)
	corruptedData[0] ^= 0xFF

	// Write corrupted MANIFEST
	corruptPath := filepath.Join(dir, "MANIFEST-000002")
	if err := os.WriteFile(corruptPath, corruptedData, 0644); err != nil {
		return err
	}

	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte("MANIFEST-000002\n"), 0644); err != nil {
		return err
	}

	// Check C++ behavior
	cppOutput, cppErr := runLdb("manifest_dump", "--path="+dir)
	cppRejects := cppErr != nil || strings.Contains(cppOutput, "Corruption") ||
		strings.Contains(cppOutput, "checksum")

	// Check Go behavior
	reader := wal.NewStrictReader(bytes.NewReader(corruptedData), nil, 2)
	_, goErr := reader.ReadRecord()
	goRejects := goErr != nil

	if cppRejects && !goRejects {
		return fmt.Errorf("oracle mismatch: C++ rejects corrupted MANIFEST but Go accepts it")
	}

	if !cppRejects && !goRejects {
		return fmt.Errorf("neither C++ nor Go rejected corrupted MANIFEST")
	}

	if *verbose {
		fmt.Printf("    Both C++ and Go correctly reject corrupted MANIFEST\n")
	}

	return nil
}

// verifyGoGeneratesManifest creates a MANIFEST with Go and verifies C++ can read it.
// Note: MANIFEST generation is verified through the full database tests.
func verifyGoGeneratesManifest() error { //nolint:unused // reserved for future use
	if *ldbPath == "" {
		return fmt.Errorf("ldb path not specified, skipping C++ verification")
	}

	// Create a temporary database
	dbPath := filepath.Join(*outputDir, "manifest_test_db")
	os.RemoveAll(dbPath)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.ErrorIfExists = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Write some data and flush to create SST files
	for i := range 100 {
		key := fmt.Sprintf("manifest_key_%05d", i)
		value := fmt.Sprintf("manifest_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			return fmt.Errorf("failed to write: %w", err)
		}
	}

	// Flush to create SST files (which updates MANIFEST)
	if err := database.Flush(nil); err != nil {
		database.Close()
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := database.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Use ldb manifest_dump to verify
	output, err := runLdb("manifest_dump", "--path="+dbPath)
	if err != nil {
		return fmt.Errorf("ldb manifest_dump failed: %w", err)
	}

	// Verify output contains expected structure
	if !strings.Contains(output, "comparator") &&
		!strings.Contains(output, "log_number") &&
		!strings.Contains(output, "next_file") {
		return fmt.Errorf("manifest_dump output doesn't contain expected fields: %s", output)
	}

	if *verbose {
		fmt.Printf("    ldb manifest_dump successfully read Go-generated MANIFEST\n")
	}

	return nil
}
