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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

var (
	filePath        = flag.String("file", "", "Path to the SST file (required)")
	command         = flag.String("command", "scan", "Command: scan, properties, check, raw, collision-check")
	dirPath         = flag.String("dir", "", "Directory containing SST files (required for command=collision-check)")
	maxCollisions   = flag.Int("max-collisions", 1, "Stop after finding N collisions (command=collision-check)")
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

	var err error
	switch *command {
	case "scan":
		if *filePath == "" {
			fmt.Fprintln(os.Stderr, "Error: --file flag is required for --command=scan")
			printUsage()
			os.Exit(1)
		}
		err = cmdScan()
	case "properties":
		if *filePath == "" {
			fmt.Fprintln(os.Stderr, "Error: --file flag is required for --command=properties")
			printUsage()
			os.Exit(1)
		}
		err = cmdProperties()
	case "check":
		if *filePath == "" {
			fmt.Fprintln(os.Stderr, "Error: --file flag is required for --command=check")
			printUsage()
			os.Exit(1)
		}
		err = cmdCheck()
	case "raw":
		if *filePath == "" {
			fmt.Fprintln(os.Stderr, "Error: --file flag is required for --command=raw")
			printUsage()
			os.Exit(1)
		}
		err = cmdRaw()
	case "collision-check":
		if *dirPath == "" {
			fmt.Fprintln(os.Stderr, "Error: --dir flag is required for --command=collision-check")
			printUsage()
			os.Exit(1)
		}
		if *maxCollisions <= 0 {
			fmt.Fprintln(os.Stderr, "Error: --max-collisions must be > 0")
			os.Exit(1)
		}
		err = cmdCollisionCheck()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", *command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
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
	fmt.Println("  collision-check  Scan a directory of SSTs for internal-key collisions (same internal key, different values)")
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

type seenEntry struct {
	file string
	sha  [32]byte
	len  int
}

func cmdCollisionCheck() error {
	fs := vfs.Default()

	entries, err := fs.ListDir(*dirPath)
	if err != nil {
		return fmt.Errorf("list dir: %w", err)
	}

	var sstFiles []string
	for _, name := range entries {
		if strings.HasSuffix(name, ".sst") {
			sstFiles = append(sstFiles, filepath.Join(*dirPath, name))
		}
	}
	sort.Strings(sstFiles)

	if len(sstFiles) == 0 {
		return fmt.Errorf("no .sst files found in dir: %s", *dirPath)
	}

	fmt.Printf("Collision check: dir=%s sst_files=%d\n", *dirPath, len(sstFiles))

	seen := make(map[string]seenEntry, 4096)
	found := 0

	for _, p := range sstFiles {
		file, err := fs.OpenRandomAccess(p)
		if err != nil {
			return fmt.Errorf("open sst: %s: %w", p, err)
		}
		reader, err := table.Open(file, table.ReaderOptions{VerifyChecksums: false})
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("open table: %s: %w", p, err)
		}

		iter := reader.NewIterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()

			kCopy := append([]byte(nil), k...)
			keyStr := string(kCopy)
			sum := sha256.Sum256(v)

			if prev, ok := seen[keyStr]; ok {
				if prev.len != len(v) || prev.sha != sum {
					found++

					user := extractUserKey(kCopy)
					var seq uint64
					var typ uint64
					if len(kCopy) >= 8 {
						packed := binary.LittleEndian.Uint64(kCopy[len(kCopy)-8:])
						seq = packed >> 8
						typ = packed & 0xff
					}

					fmt.Printf("COLLISION #%d\n", found)
					fmt.Printf("  user_key=%s\n", formatOutput(user))
					fmt.Printf("  internal_key_hex=%s\n", hex.EncodeToString(kCopy))
					fmt.Printf("  seq=%d type=%d\n", seq, typ)
					fmt.Printf("  first_file=%s\n", filepath.Base(prev.file))
					fmt.Printf("  second_file=%s\n", filepath.Base(p))
					fmt.Printf("  first_value_sha256=%x len=%d\n", prev.sha, prev.len)
					fmt.Printf("  second_value_sha256=%x len=%d\n", sum, len(v))

					if found >= *maxCollisions {
						if err := iter.Error(); err != nil {
							return fmt.Errorf("iterator error: %s: %w", p, err)
						}
						return fmt.Errorf("internal-key collision detected (count=%d)", found)
					}
				}
				continue
			}

			seen[keyStr] = seenEntry{file: p, sha: sum, len: len(v)}
		}

		if err := iter.Error(); err != nil {
			return fmt.Errorf("iterator error: %s: %w", p, err)
		}
	}

	fmt.Println("OK: no internal-key collisions detected")
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
