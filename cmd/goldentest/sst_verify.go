// SST format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - table/block_based/block_based_table_builder.cc (SST building)
//   - table/block_based/block_based_table_reader.cc (SST reading)
//   - table/format.h (footer format)
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// verifyGoReadsSST reads SST files from a C++ database
func verifyGoReadsSST(dbPath string) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("fixture not found: %s", dbPath)
	}

	// Find .sst files in the database directory
	files, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil {
		return fmt.Errorf("failed to list SST files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no SST files found in %s", dbPath)
	}

	fs := vfs.Default()

	for _, sstPath := range files {
		if *verbose {
			fmt.Printf("    Reading SST: %s\n", filepath.Base(sstPath))
		}

		// Open the SST file for random access
		file, err := fs.OpenRandomAccess(sstPath)
		if err != nil {
			return fmt.Errorf("failed to open SST %s: %w", sstPath, err)
		}

		// Create table reader
		opts := table.ReaderOptions{
			VerifyChecksums: true,
		}

		reader, err := table.Open(file, opts)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to open table %s: %w", sstPath, err)
		}

		// Iterate through all entries
		iter := reader.NewIterator()
		entryCount := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			entryCount++
			if *verbose && entryCount <= 5 {
				fmt.Printf("      Entry: key=%q, value=%q\n", iter.Key(), iter.Value())
			}
		}

		if err := iter.Error(); err != nil {
			reader.Close()
			return fmt.Errorf("iterator error in %s: %w", sstPath, err)
		}

		reader.Close()

		if *verbose {
			fmt.Printf("      Read %d entries\n", entryCount)
		}
	}

	if *verbose {
		fmt.Printf("    Successfully read %d SST files\n", len(files))
	}

	return nil
}

// verifyGoGeneratesSST creates SST files with Go and verifies C++ can read them
func verifyGoGeneratesSST() error {
	if *sstDumpPath == "" {
		return fmt.Errorf("sst_dump path not specified, skipping C++ verification")
	}

	// Create a temporary database
	dbPath := filepath.Join(*outputDir, "sst_test_db")
	os.RemoveAll(dbPath)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.ErrorIfExists = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Write data and flush to create SST
	for i := range 100 {
		key := fmt.Sprintf("sst_key_%05d", i)
		value := fmt.Sprintf("sst_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			return fmt.Errorf("failed to write: %w", err)
		}
	}

	if err := database.Flush(nil); err != nil {
		database.Close()
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := database.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Find SST files
	files, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no SST files created")
	}

	// Verify each SST with sst_dump
	for _, sstPath := range files {
		_, err := runSstDump("--file="+sstPath, "--command=check")
		if err != nil {
			return fmt.Errorf("sst_dump failed on %s: %w", sstPath, err)
		}

		if *verbose {
			fmt.Printf("    sst_dump verified: %s\n", filepath.Base(sstPath))
		}

		// Also try to scan the file
		output, err := runSstDump("--file="+sstPath, "--command=scan")
		if err != nil {
			return fmt.Errorf("sst_dump scan failed on %s: %w", sstPath, err)
		}

		if !strings.Contains(output, "sst_key_00000") {
			return fmt.Errorf("sst_dump output doesn't contain expected keys")
		}
	}

	if *verbose {
		fmt.Printf("    sst_dump successfully verified %d Go-generated SST files\n", len(files))
	}

	return nil
}
