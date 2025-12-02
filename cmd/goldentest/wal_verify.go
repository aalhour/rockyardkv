// WAL format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - db/log_format.h (record format)
//   - db/log_reader.cc (reading)
//   - db/log_writer.cc (writing)
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// noopReporter is a Reporter that ignores all events
type noopReporter struct{}

func (n noopReporter) Corruption(bytes int, err error) {}
func (n noopReporter) OldLogRecord(bytes int)          {}

// verifyGoReadsWAL reads a C++ generated WAL file with Go.
// Note: Not called directly because after compaction WAL is often empty.
// WAL format is verified through the full database open/recovery path.
func verifyGoReadsWAL(path string) error { //nolint:unused // reserved for future use
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("fixture not found: %s", path)
	}

	// Open the WAL file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %w", err)
	}
	defer f.Close()

	// Create a WAL reader with checksum verification
	reader := wal.NewReader(f, noopReporter{}, true, 0)

	// Read all records
	recordCount := 0
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Some errors are expected for partial files
			if recordCount > 0 {
				break
			}
			return fmt.Errorf("failed to read record: %w", err)
		}
		if record == nil {
			break
		}
		recordCount++

		// Verify we can parse this as a WriteBatch
		wb, err := batch.NewFromData(record)
		if err != nil {
			return fmt.Errorf("failed to parse record as WriteBatch: %w", err)
		}

		if *verbose {
			fmt.Printf("    Record %d: %d bytes, seq=%d, count=%d\n",
				recordCount, len(record), wb.Sequence(), wb.Count())
		}
	}

	if recordCount == 0 {
		return fmt.Errorf("no records found in WAL")
	}

	if *verbose {
		fmt.Printf("    Successfully read %d records\n", recordCount)
	}

	return nil
}

// verifyGoGeneratesWAL creates a WAL with Go and verifies C++ can read it
func verifyGoGeneratesWAL() error {
	if *ldbPath == "" {
		return fmt.Errorf("ldb path not specified, skipping C++ verification")
	}

	// Create a temporary database directory
	dbPath := filepath.Join(*outputDir, "wal_test_db")
	os.RemoveAll(dbPath)

	// Open a database and write some data
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.ErrorIfExists = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Write some data
	for i := range 10 {
		key := fmt.Sprintf("wal_test_key_%05d", i)
		value := fmt.Sprintf("wal_test_value_%05d", i)
		if err := database.Put(nil, []byte(key), []byte(value)); err != nil {
			database.Close()
			return fmt.Errorf("failed to write: %w", err)
		}
	}

	// Close to ensure WAL is written
	if err := database.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Use ldb to scan the database
	output, err := runLdb("scan", "--db="+dbPath)
	if err != nil {
		return fmt.Errorf("ldb failed to read Go-generated database: %w", err)
	}

	// Verify output contains our keys
	if !strings.Contains(output, "wal_test_key_00000") {
		return fmt.Errorf("ldb output doesn't contain expected keys: %s", output)
	}

	if *verbose {
		fmt.Printf("    ldb successfully read Go-generated database\n")
	}

	return nil
}
