// Full database compatibility tests
//
// Reference: RocksDB v10.7.5
//   - db/db_impl/db_impl.cc (database implementation)
//   - db/db_impl/db_impl_open.cc (database opening)
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalhour/rockyardkv/db"
)

// verifyGoOpensDatabase opens a C++ created database with Go
func verifyGoOpensDatabase(dbPath string) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("fixture not found: %s", dbPath)
	}

	opts := db.DefaultOptions()
	opts.CreateIfMissing = false
	opts.ErrorIfExists = false

	database, err := db.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open C++ database: %w", err)
	}
	defer database.Close()

	// Read all keys
	iter := database.NewIterator(nil)
	defer iter.Close()

	keyCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keyCount++
		if *verbose && keyCount <= 5 {
			fmt.Printf("    Key: %q, Value: %q\n", iter.Key(), iter.Value())
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	if *verbose {
		fmt.Printf("    Go successfully opened C++ database with %d keys\n", keyCount)
	}

	return nil
}

// verifyCppOpensGoDatabase creates a database with Go and opens it with C++ ldb
func verifyCppOpensGoDatabase() error {
	if *ldbPath == "" {
		return fmt.Errorf("ldb path not specified, skipping C++ verification")
	}

	// Create a database with Go
	dbPath := filepath.Join(*outputDir, "go_db_for_cpp")
	os.RemoveAll(dbPath)

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.ErrorIfExists = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Write various types of data
	testData := []struct {
		key, value string
	}{
		{"simple_key", "simple_value"},
		{"unicode_key_日本語", "unicode_value_中文"},
	}

	for _, td := range testData {
		if err := database.Put(nil, []byte(td.key), []byte(td.value)); err != nil {
			database.Close()
			return fmt.Errorf("failed to write %q: %w", td.key, err)
		}
	}

	// Also write a range of sequential keys
	for i := range 1000 {
		key := fmt.Sprintf("seq_key_%08d", i)
		value := fmt.Sprintf("seq_value_%08d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			return fmt.Errorf("failed to write sequential key: %w", err)
		}
	}

	// Flush to ensure SST files are created
	if err := database.Flush(nil); err != nil {
		database.Close()
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := database.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Verify with ldb scan
	output, err := runLdb("scan", "--db="+dbPath)
	if err != nil {
		return fmt.Errorf("ldb scan failed: %w", err)
	}

	// Verify output contains expected keys
	if !strings.Contains(output, "simple_key") {
		return fmt.Errorf("ldb output missing simple_key: %s", output)
	}
	if !strings.Contains(output, "seq_key_00000000") {
		return fmt.Errorf("ldb output missing seq_key_00000000: %s", output)
	}

	// Verify specific key lookup
	output, err = runLdb("get", "--db="+dbPath, "simple_key")
	if err != nil {
		return fmt.Errorf("ldb get failed: %w", err)
	}
	if !strings.Contains(output, "simple_value") {
		return fmt.Errorf("ldb get returned wrong value: %s", output)
	}

	if *verbose {
		fmt.Printf("    C++ ldb successfully opened and queried Go-generated database\n")
	}

	return nil
}
