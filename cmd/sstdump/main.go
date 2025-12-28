// Package main provides the sstdump CLI tool for inspecting SST files.
//
// Usage:
//
//	sst_dump --file=<path> [options]
//
// Commands:
//
//	raw             Show raw data blocks
//	properties      Show SST file properties
//	scan            Scan all key-value pairs
//	check           Verify SST file integrity
//
// Reference: RocksDB v10.7.5 tools/sst_dump_tool.cc
package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

var (
	filePath        = flag.String("file", "", "Path to the SST file (required)")
	dbDir           = flag.String("db", "", "Path to DB directory containing CURRENT/MANIFEST and SSTs (for --command=collision-check)")
	command         = flag.String("command", "scan", "Command: scan, properties, check, raw, collision-check")
	hexOutput       = flag.Bool("hex", false, "Output keys and values in hex format")
	limit           = flag.Int("limit", 0, "Limit number of entries (0 = unlimited)")
	fromKey         = flag.String("from", "", "Start key for scan")
	toKey           = flag.String("to", "", "End key for scan")
	showValues      = flag.Bool("values", true, "Show values in scan output")
	help            = flag.Bool("help", false, "Print help")
	showSummary     = flag.Bool("summary", true, "Show summary statistics")
	verifyChecksums = flag.Bool("verify_checksums", true, "Verify block checksums during check")
	verbose         = flag.Bool("v", false, "Verbose output during check")
)

func main() {
	flag.Parse()

	if *help {
		printUsage()
		return
	}

	if *command != "collision-check" && *filePath == "" {
		fmt.Fprintln(os.Stderr, "Error: --file flag is required")
		printUsage()
		os.Exit(1)
	}

	var err error
	switch *command {
	case "scan":
		err = cmdScan()
	case "properties":
		err = cmdProperties()
	case "check":
		err = cmdCheck()
	case "raw":
		err = cmdRaw()
	case "collision-check":
		err = cmdCollisionCheck()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", *command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		var ce *collisionError
		if errorsAs(err, &ce) {
			fmt.Fprintln(os.Stderr, ce.Error())
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("sst_dump - RockyardKV SST file inspection tool")
	fmt.Println()
	fmt.Println("Usage: sst_dump --file=<path> [--command=<cmd>] [options]")
	fmt.Println()
	fmt.Println("Commands (--command):")
	fmt.Println("  scan        Scan all key-value pairs (default)")
	fmt.Println("  properties  Show SST file properties")
	fmt.Println("  check       Verify SST file integrity")
	fmt.Println("  raw         Show raw block information")
	fmt.Println("  collision-check  Scan live SSTs (from CURRENT/MANIFEST) for internal-key collisions")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
}

func openSST() (*table.Reader, error) {
	return openSSTWithOptions(false)
}

func openSSTWithOptions(verifyChecksum bool) (*table.Reader, error) {
	fs := vfs.Default()

	file, err := fs.OpenRandomAccess(*filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	opts := table.ReaderOptions{
		VerifyChecksums: verifyChecksum,
	}
	reader, err := table.Open(file, opts)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open SST: %w", err)
	}

	return reader, nil
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

func cmdScan() error {
	reader, err := openSST()
	if err != nil {
		return err
	}

	fmt.Printf("SST file: %s\n", *filePath)
	fmt.Println("---")

	iter := reader.NewIterator()

	// Seek to start key if specified
	if *fromKey != "" {
		iter.Seek([]byte(*fromKey))
	} else {
		iter.SeekToFirst()
	}

	count := 0
	var totalKeyBytes, totalValueBytes int64

	for iter.Valid() {
		key := iter.Key()

		// Stop at end key if specified
		if *toKey != "" && string(extractUserKey(key)) >= *toKey {
			break
		}

		value := iter.Value()

		if *showValues {
			fmt.Printf("%s => %s\n", formatOutput(key), formatOutput(value))
		} else {
			fmt.Printf("%s\n", formatOutput(key))
		}

		totalKeyBytes += int64(len(key))
		totalValueBytes += int64(len(value))
		count++

		if *limit > 0 && count >= *limit {
			break
		}

		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	if *showSummary {
		fmt.Println("---")
		fmt.Printf("Total entries: %d\n", count)
		fmt.Printf("Total key bytes: %d\n", totalKeyBytes)
		fmt.Printf("Total value bytes: %d\n", totalValueBytes)
	}

	return nil
}

func cmdProperties() error {
	fs := vfs.Default()

	info, err := fs.Stat(*filePath)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	reader, err := openSST()
	if err != nil {
		return err
	}

	fmt.Printf("SST file: %s\n", *filePath)
	fmt.Println("---")
	fmt.Printf("File size: %d bytes\n", info.Size())
	fmt.Printf("File name: %s\n", filepath.Base(*filePath))
	_ = reader // Reader used below

	// Count entries
	iter := reader.NewIterator()
	count := 0
	var minKey, maxKey []byte
	var totalKeyBytes, totalValueBytes int64

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if count == 0 {
			minKey = append([]byte{}, key...)
		}
		maxKey = append(maxKey[:0], key...)

		totalKeyBytes += int64(len(key))
		totalValueBytes += int64(len(value))
		count++
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	fmt.Printf("Number of entries: %d\n", count)
	fmt.Printf("Total key bytes: %d\n", totalKeyBytes)
	fmt.Printf("Total value bytes: %d\n", totalValueBytes)

	if count > 0 {
		fmt.Printf("Average key size: %.1f bytes\n", float64(totalKeyBytes)/float64(count))
		fmt.Printf("Average value size: %.1f bytes\n", float64(totalValueBytes)/float64(count))
		fmt.Printf("Smallest key: %s\n", formatOutput(minKey))
		fmt.Printf("Largest key: %s\n", formatOutput(maxKey))
	}

	// Extract user keys
	if len(minKey) >= 8 {
		fmt.Printf("Smallest user key: %s\n", formatOutput(extractUserKey(minKey)))
	}
	if len(maxKey) >= 8 {
		fmt.Printf("Largest user key: %s\n", formatOutput(extractUserKey(maxKey)))
	}

	return nil
}

func cmdCheck() error {
	// Open with checksum verification enabled
	reader, err := openSSTWithOptions(*verifyChecksums)
	if err != nil {
		return err
	}

	fmt.Printf("Checking SST file: %s\n", *filePath)
	if *verifyChecksums {
		fmt.Println("Block checksum verification: ENABLED")
	} else {
		fmt.Println("Block checksum verification: DISABLED")
	}
	fmt.Println("---")

	// Scan all entries to verify integrity
	// This will read all data blocks, triggering checksum verification
	iter := reader.NewIterator()
	count := 0
	checksumErrors := 0
	formatErrors := 0
	blocksVerified := 0
	lastBlock := -1

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		_ = iter.Value()

		// Estimate block (every ~16 entries for tracking)
		currentBlock := count / 16
		if currentBlock != lastBlock {
			blocksVerified++
			lastBlock = currentBlock
			if *verbose {
				fmt.Printf("  Verifying block %d...\n", blocksVerified)
			}
		}

		// Verify key has at least 8 bytes (internal key format)
		if len(key) < 8 {
			fmt.Printf("Warning: Key %d has invalid format (len=%d)\n", count, len(key))
			formatErrors++
		}

		count++
	}

	if err := iter.Error(); err != nil {
		if strings.Contains(err.Error(), "checksum") {
			fmt.Printf("Checksum error: %v\n", err)
			checksumErrors++
		} else {
			fmt.Printf("Iterator error: %v\n", err)
			formatErrors++
		}
	}

	// Print results
	fmt.Println("---")
	fmt.Printf("Total entries scanned: %d\n", count)
	fmt.Printf("Blocks verified: ~%d\n", blocksVerified)

	if *verifyChecksums {
		if checksumErrors == 0 {
			fmt.Println("Checksum verification: ✓ PASSED")
		} else {
			fmt.Printf("Checksum verification: ✗ FAILED (%d errors)\n", checksumErrors)
		}
	}

	if formatErrors > 0 {
		fmt.Printf("Format errors: %d\n", formatErrors)
	}

	totalErrors := checksumErrors + formatErrors
	if totalErrors > 0 {
		return fmt.Errorf("file has %d errors", totalErrors)
	}

	fmt.Println("✓ SST file is valid")
	return nil
}

func cmdRaw() error {
	fs := vfs.Default()

	info, err := fs.Stat(*filePath)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	reader, err := openSST()
	if err != nil {
		return err
	}

	fmt.Printf("SST file: %s\n", *filePath)
	fmt.Printf("File size: %d bytes\n", info.Size())
	fmt.Println("---")

	// For a full implementation, we would parse the block handles
	// and show individual data blocks, meta blocks, etc.
	// This is a simplified version.

	// Count entries per "block" (estimated based on position)
	iter := reader.NewIterator()
	count := 0
	blockCount := 0
	entriesInBlock := 0
	lastBlockStart := 0

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		entriesInBlock++
		count++

		// Estimate block boundary (every ~16 entries for visibility)
		if entriesInBlock >= 16 || !iter.Valid() {
			if entriesInBlock > 0 {
				fmt.Printf("Block %d: %d entries (offset ~%d)\n",
					blockCount, entriesInBlock, lastBlockStart)
			}
			blockCount++
			lastBlockStart = count
			entriesInBlock = 0
		}
	}

	// Final partial block
	if entriesInBlock > 0 {
		fmt.Printf("Block %d: %d entries (offset ~%d)\n",
			blockCount, entriesInBlock, lastBlockStart)
	}

	fmt.Println("---")
	fmt.Printf("Total entries: %d\n", count)
	fmt.Printf("Estimated blocks: %d\n", blockCount+1)

	return nil
}

// extractUserKey extracts the user key portion from an internal key.
func extractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}

// isHexString checks if a string looks like hex.
// Note: Reserved for future hex key parsing support.
func isHexString(s string) bool { //nolint:unused // reserved for future use
	return strings.HasPrefix(s, "0x")
}

// =============================================================================
// collision-check
// =============================================================================

type collisionError struct {
	internalKeyHex string
	file1          string
	value1Hex      string
	file2          string
	value2Hex      string
}

func (e *collisionError) Error() string {
	return fmt.Sprintf(
		"internal_key_hex=%s\nsst1=%s value1_hex=%s\nsst2=%s value2_hex=%s",
		e.internalKeyHex, e.file1, e.value1Hex, e.file2, e.value2Hex,
	)
}

// errorsAs is a tiny wrapper to avoid importing errors just for As in older Go toolchains.
func errorsAs(err error, target interface{}) bool { //nolint:unparam
	// Go stdlib errors.As is fine; keep this wrapper so callers above stay small.
	type aser interface{ As(target interface{}) bool }
	if err == nil {
		return false
	}
	// If the error implements As, try it first.
	if a, ok := err.(aser); ok && a.As(target) {
		return true
	}
	// Fallback: direct type assertion for *collisionError.
	ce, ok := err.(*collisionError)
	if !ok {
		return false
	}
	ptr, ok := target.(**collisionError)
	if !ok {
		return false
	}
	*ptr = ce
	return true
}

func cmdCollisionCheck() error {
	if *dbDir == "" {
		return fmt.Errorf("--db is required for --command=collision-check")
	}

	fs := vfs.Default()

	live, err := liveSSTFilesFromCurrent(*dbDir)
	if err != nil {
		return err
	}
	sort.Strings(live)

	type seen struct {
		valueHex string
		file     string
	}
	byKey := make(map[string]seen, 1024)

	for _, sstPath := range live {
		file, err := fs.OpenRandomAccess(sstPath)
		if err != nil {
			return fmt.Errorf("open sst %s: %w", sstPath, err)
		}
		reader, err := table.Open(file, table.ReaderOptions{})
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("open table %s: %w", sstPath, err)
		}

		it := reader.NewIterator()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			khex := hex.EncodeToString(it.Key())
			vhex := hex.EncodeToString(it.Value())
			if prev, ok := byKey[khex]; ok {
				if prev.valueHex != vhex {
					_ = file.Close()
					return &collisionError{
						internalKeyHex: khex,
						file1:          filepath.Base(prev.file),
						value1Hex:      prev.valueHex,
						file2:          filepath.Base(sstPath),
						value2Hex:      vhex,
					}
				}
				continue
			}
			byKey[khex] = seen{valueHex: vhex, file: sstPath}
		}
		if err := it.Error(); err != nil {
			_ = file.Close()
			return fmt.Errorf("iterate %s: %w", sstPath, err)
		}
		_ = file.Close()
	}

	fmt.Println("NO_COLLISIONS_FOUND")
	return nil
}

func liveSSTFilesFromCurrent(dbDir string) ([]string, error) {
	currentPath := filepath.Join(dbDir, "CURRENT")
	curBytes, err := os.ReadFile(currentPath)
	if err != nil {
		return nil, fmt.Errorf("read CURRENT: %w", err)
	}
	manifestName := strings.TrimSpace(string(curBytes))
	if manifestName == "" {
		return nil, fmt.Errorf("CURRENT is empty")
	}
	manifestPath := filepath.Join(dbDir, manifestName)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read MANIFEST %s: %w", manifestName, err)
	}

	reader := wal.NewStrictReader(bytes.NewReader(manifestData), nil, 0)
	live := make(map[uint64]bool)
	for {
		rec, err := reader.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial manifest is possible in crash tests; treat it as fatal for this tool.
			return nil, fmt.Errorf("read MANIFEST record: %w", err)
		}
		var ve manifest.VersionEdit
		if err := ve.DecodeFrom(rec); err != nil {
			return nil, fmt.Errorf("decode VersionEdit: %w", err)
		}
		for _, nf := range ve.NewFiles {
			live[nf.Meta.FD.GetNumber()] = true
		}
		for _, df := range ve.DeletedFiles {
			delete(live, df.FileNumber)
		}
	}

	out := make([]string, 0, len(live))
	for num := range live {
		out = append(out, filepath.Join(dbDir, fmt.Sprintf("%06d.sst", num)))
	}
	return out, nil
}
