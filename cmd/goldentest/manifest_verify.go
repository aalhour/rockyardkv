// MANIFEST format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - db/version_edit.h (VersionEdit format)
//   - db/version_edit.cc (encoding/decoding)
//   - db/version_set.cc (MANIFEST reading)
package main

import (
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
