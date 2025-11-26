// Block format compatibility tests
//
// Reference: RocksDB v10.7.5
//   - table/block_based/block_builder.h (block format)
//   - table/block_based/block.cc (block parsing)
package main

import (
	"fmt"
	"os"

	"github.com/aalhour/rockyardkv/internal/block"
)

// verifyGoReadsBlock reads a C++ generated block with Go.
// Note: This function is not called directly because block format is verified
// implicitly through SST file reading. Kept for potential future standalone testing.
func verifyGoReadsBlock(path string) error { //nolint:unused // reserved for future use
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("fixture not found: %s", path)
	}

	// Read the block data
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read block: %w", err)
	}

	if len(data) < 4 {
		return fmt.Errorf("block too small: %d bytes", len(data))
	}

	// Create a block
	blk, err := block.NewBlock(data)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}

	// Create an iterator and read all entries
	iter := blk.NewIterator()

	entryCount := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		entryCount++

		if *verbose {
			fmt.Printf("    Entry %d: key=%q, value=%q\n", entryCount, key, value)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	if entryCount == 0 {
		return fmt.Errorf("no entries found in block")
	}

	if *verbose {
		fmt.Printf("    Successfully read %d entries\n", entryCount)
	}

	return nil
}
