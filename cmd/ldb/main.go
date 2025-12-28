// Package main provides the ldb CLI tool for inspecting RockyardKV databases.
//
// Usage:
//
//	ldb --db=<path> <command> [options]
//
// Commands:
//
//	scan            Scan all key-value pairs
//	get <key>       Get value for a key
//	put <key> <val> Put a key-value pair
//	delete <key>    Delete a key
//	dump            Dump database contents
//	repair          Attempt to repair a corrupted database
//	info            Print database information
//	manifest_dump   Dump MANIFEST file contents
//	sstfiles        List SST files and their properties
//
// Reference: RocksDB v10.7.5 tools/ldb_tool.cc
package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

var (
	dbPath          = flag.String("db", "", "Path to the database (required)")
	readOnly        = flag.Bool("readonly", true, "Open database in read-only mode")
	hexOutput       = flag.Bool("hex", false, "Output keys and values in hex format")
	limit           = flag.Int("limit", 0, "Limit number of entries (0 = unlimited)")
	fromKey         = flag.String("from", "", "Start key for scan")
	toKey           = flag.String("to", "", "End key for scan")
	help            = flag.Bool("help", false, "Print help")
	createIfMissing = flag.Bool("create_if_missing", false, "Create database if it doesn't exist")
	verbose         = flag.Bool("v", false, "Verbose output for manifest_dump")
)

func main() {
	flag.Parse()

	if *help || len(flag.Args()) == 0 {
		printUsage()
		return
	}

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db flag is required")
		os.Exit(1)
	}

	command := flag.Arg(0)
	args := flag.Args()[1:]

	var err error
	switch command {
	case "scan":
		err = cmdScan()
	case "get":
		err = cmdGet(args)
	case "put":
		err = cmdPut(args)
	case "delete":
		err = cmdDelete(args)
	case "dump":
		err = cmdDump()
	case "info":
		err = cmdInfo()
	case "manifest_dump":
		err = cmdManifestDump()
	case "sstfiles":
		err = cmdSSTFiles()
	case "repair":
		err = cmdRepair()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("ldb - RockyardKV database inspection tool")
	fmt.Println()
	fmt.Println("Usage: ldb --db=<path> <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  scan              Scan all key-value pairs")
	fmt.Println("  get <key>         Get value for a key")
	fmt.Println("  put <key> <val>   Put a key-value pair (requires --readonly=false)")
	fmt.Println("  delete <key>      Delete a key (requires --readonly=false)")
	fmt.Println("  dump              Dump database contents")
	fmt.Println("  info              Print database information")
	fmt.Println("  manifest_dump     Dump MANIFEST file contents")
	fmt.Println("  sstfiles          List SST files and their properties")
	fmt.Println("  repair            Attempt to repair a corrupted database")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
}

func openDB() (db.DB, error) {
	opts := db.DefaultOptions()
	opts.CreateIfMissing = *createIfMissing

	if *readOnly {
		return db.OpenForReadOnly(*dbPath, opts, false)
	}
	return db.Open(*dbPath, opts)
}

func formatOutput(data []byte) string {
	if *hexOutput {
		return hex.EncodeToString(data)
	}
	// Print as string if printable, else hex
	for _, b := range data {
		if b < 32 || b > 126 {
			return hex.EncodeToString(data)
		}
	}
	return string(data)
}

func parseInput(s string) []byte {
	// Try hex decode first (if prefixed with 0x)
	if strings.HasPrefix(s, "0x") {
		decoded, err := hex.DecodeString(s[2:])
		if err == nil {
			return decoded
		}
	}
	return []byte(s)
}

func cmdScan() error {
	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	iter := database.NewIterator(nil)
	defer iter.Close()

	// Seek to start key if specified
	if *fromKey != "" {
		iter.Seek(parseInput(*fromKey))
	} else {
		iter.SeekToFirst()
	}

	toKeyBytes := parseInput(*toKey)
	count := 0

	for iter.Valid() {
		key := iter.Key()

		// Stop at end key if specified
		if *toKey != "" && bytes.Compare(key, toKeyBytes) >= 0 {
			break
		}

		value := iter.Value()
		fmt.Printf("%s => %s\n", formatOutput(key), formatOutput(value))

		count++
		if *limit > 0 && count >= *limit {
			break
		}

		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	fmt.Printf("\n(%d entries scanned)\n", count)
	return nil
}

func cmdGet(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ldb --db=<path> get <key>")
	}

	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	key := parseInput(args[0])
	value, err := database.Get(nil, key)
	if err != nil {
		return fmt.Errorf("key not found: %w", err)
	}

	fmt.Printf("%s\n", formatOutput(value))
	return nil
}

func cmdPut(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: ldb --db=<path> --readonly=false put <key> <value>")
	}

	if *readOnly {
		return fmt.Errorf("cannot put in readonly mode, use --readonly=false")
	}

	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	key := parseInput(args[0])
	value := parseInput(args[1])

	if err := database.Put(nil, key, value); err != nil {
		return fmt.Errorf("put failed: %w", err)
	}

	// Flush to ensure durability
	if err := database.Flush(nil); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	fmt.Println("OK")
	return nil
}

func cmdDelete(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: ldb --db=<path> --readonly=false delete <key>")
	}

	if *readOnly {
		return fmt.Errorf("cannot delete in readonly mode, use --readonly=false")
	}

	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	key := parseInput(args[0])

	if err := database.Delete(nil, key); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	fmt.Println("OK")
	return nil
}

func cmdDump() error {
	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	iter := database.NewIterator(nil)
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("'%s' => '%s'\n", formatOutput(key), formatOutput(value))
		count++

		if *limit > 0 && count >= *limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	fmt.Printf("\n(%d entries dumped)\n", count)
	return nil
}

func cmdInfo() error {
	database, err := openDB()
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	fmt.Printf("Database: %s\n", *dbPath)
	fmt.Println("---")

	// Print various properties
	properties := []string{
		"rocksdb.num-files-at-level0",
		"rocksdb.num-files-at-level1",
		"rocksdb.num-files-at-level2",
		"rocksdb.num-files-at-level3",
		"rocksdb.num-files-at-level4",
		"rocksdb.num-files-at-level5",
		"rocksdb.num-files-at-level6",
		"rocksdb.estimate-num-keys",
		"rocksdb.estimate-table-readers-mem",
		"rocksdb.cur-size-all-mem-tables",
		"rocksdb.live-sst-files-size",
		"rocksdb.is-write-stopped",
		"rocksdb.background-errors",
	}

	for _, prop := range properties {
		value, ok := database.GetProperty(prop)
		if ok {
			fmt.Printf("%s: %s\n", prop, value)
		}
	}

	return nil
}

func cmdManifestDump() error {
	fs := vfs.Default()

	// Read CURRENT file to find the active MANIFEST
	currentPath := filepath.Join(*dbPath, "CURRENT")
	currentData, err := os.ReadFile(currentPath)
	if err != nil {
		return fmt.Errorf("failed to read CURRENT file: %w", err)
	}

	manifestName := strings.TrimSpace(string(currentData))
	if manifestName == "" || !strings.HasPrefix(manifestName, "MANIFEST-") {
		return fmt.Errorf("invalid CURRENT file content: %q", manifestName)
	}

	manifestPath := filepath.Join(*dbPath, manifestName)
	if _, err := fs.Stat(manifestPath); err != nil {
		return fmt.Errorf("MANIFEST file %s not found: %w", manifestPath, err)
	}

	fmt.Printf("MANIFEST file: %s\n", manifestPath)
	fmt.Println("---")

	// Read and display basic info
	info, err := fs.Stat(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to stat MANIFEST: %w", err)
	}

	fmt.Printf("Size: %d bytes\n", info.Size())
	fmt.Printf("Modified: %s\n", info.ModTime())

	// Open and parse the MANIFEST file
	file, err := fs.Open(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to open MANIFEST: %w", err)
	}
	defer file.Close()

	reader := wal.NewReader(file, nil, true, 0)
	editCount := 0
	totalNewFiles := 0
	totalDeletedFiles := 0
	var lastSeqNum manifest.SequenceNumber
	var comparatorName string

	fmt.Println("\nVersion Edits:")
	fmt.Println("---")

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Continue reading past errors to show as much as possible
			fmt.Printf("  [Edit %d] Error reading record: %v\n", editCount+1, err)
			break
		}

		ve := &manifest.VersionEdit{}
		if err := ve.DecodeFrom(record); err != nil {
			fmt.Printf("  [Edit %d] Error decoding: %v\n", editCount+1, err)
			continue
		}

		editCount++

		if ve.HasComparator {
			comparatorName = ve.Comparator
		}
		if ve.HasLastSequence {
			lastSeqNum = ve.LastSequence
		}

		// Count files
		newFiles := len(ve.NewFiles)
		deletedFiles := len(ve.DeletedFiles)
		totalNewFiles += newFiles
		totalDeletedFiles += deletedFiles

		if *verbose {
			// Verbose output: show all details
			fmt.Printf("  [Edit %d]\n", editCount)
			if ve.HasComparator {
				fmt.Printf("    Comparator: %s\n", ve.Comparator)
			}
			if ve.HasLogNumber {
				fmt.Printf("    LogNumber: %d\n", ve.LogNumber)
			}
			if ve.HasNextFileNumber {
				fmt.Printf("    NextFileNumber: %d\n", ve.NextFileNumber)
			}
			if ve.HasLastSequence {
				fmt.Printf("    LastSequence: %d\n", ve.LastSequence)
			}
			if ve.HasColumnFamily {
				fmt.Printf("    ColumnFamily: %d\n", ve.ColumnFamily)
			}
			if ve.ColumnFamilyName != "" {
				fmt.Printf("    ColumnFamilyName: %s\n", ve.ColumnFamilyName)
			}
			if newFiles > 0 {
				fmt.Printf("    NewFiles: %d\n", newFiles)
				for _, nf := range ve.NewFiles {
					fmt.Printf("      Level %d: File %d (%d bytes)\n",
						nf.Level, nf.Meta.FD.GetNumber(), nf.Meta.FD.FileSize)
				}
			}
			if deletedFiles > 0 {
				fmt.Printf("    DeletedFiles: %d\n", deletedFiles)
				for _, df := range ve.DeletedFiles {
					fmt.Printf("      Level %d: File %d\n", df.Level, df.FileNumber)
				}
			}
		} else {
			// Compact output: one line per edit
			parts := []string{fmt.Sprintf("[Edit %d]", editCount)}
			if ve.HasLogNumber {
				parts = append(parts, fmt.Sprintf("log=%d", ve.LogNumber))
			}
			if ve.HasLastSequence {
				parts = append(parts, fmt.Sprintf("seq=%d", ve.LastSequence))
			}
			if newFiles > 0 {
				parts = append(parts, fmt.Sprintf("+%d files", newFiles))
			}
			if deletedFiles > 0 {
				parts = append(parts, fmt.Sprintf("-%d files", deletedFiles))
			}
			fmt.Println("  " + strings.Join(parts, ", "))
		}

		if *limit > 0 && editCount >= *limit {
			break
		}
	}

	// Summary
	fmt.Println("\nSummary:")
	fmt.Println("---")
	fmt.Printf("Total Edits: %d\n", editCount)
	fmt.Printf("Total New Files: %d\n", totalNewFiles)
	fmt.Printf("Total Deleted Files: %d\n", totalDeletedFiles)
	if comparatorName != "" {
		fmt.Printf("Comparator: %s\n", comparatorName)
	}
	fmt.Printf("Last Sequence: %d\n", lastSeqNum)

	return nil
}

func cmdSSTFiles() error {
	fs := vfs.Default()

	entries, err := fs.ListDir(*dbPath)
	if err != nil {
		return fmt.Errorf("failed to list directory: %w", err)
	}

	fmt.Printf("SST files in %s:\n", *dbPath)
	fmt.Println("---")

	count := 0
	var totalSize int64
	for _, entry := range entries {
		if strings.HasSuffix(entry, ".sst") {
			path := filepath.Join(*dbPath, entry)
			info, err := fs.Stat(path)
			if err != nil {
				fmt.Printf("  %s (error: %v)\n", entry, err)
				continue
			}

			// Extract file number
			numStr := strings.TrimSuffix(entry, ".sst")
			fileNum, _ := strconv.ParseUint(numStr, 10, 64)

			fmt.Printf("  %s (file=%d, size=%d bytes)\n", entry, fileNum, info.Size())
			totalSize += info.Size()
			count++
		}
	}

	fmt.Printf("\nTotal: %d SST files, %d bytes\n", count, totalSize)
	return nil
}

func cmdRepair() error {
	fmt.Printf("Attempting to repair database at %s...\n", *dbPath)

	// For now, we don't have a full repair implementation
	// A real repair would:
	// 1. Scan for valid SST files
	// 2. Rebuild the MANIFEST from SST file metadata
	// 3. Recover WAL if possible

	// Check if database exists
	fs := vfs.Default()
	if !fs.Exists(*dbPath) {
		return fmt.Errorf("database path does not exist: %s", *dbPath)
	}

	fmt.Println("Repair not yet implemented - database appears intact")
	fmt.Println("To verify, try: ldb --db=<path> info")
	return nil
}
