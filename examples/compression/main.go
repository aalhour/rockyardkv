// Package main demonstrates compression options in RockyardKV.
//
// This example shows how to:
//   - Enable different compression algorithms (LZ4, Snappy, ZSTD)
//   - Compare database sizes with different compression settings
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	baseDir := "/tmp/rockyardkv_compression"

	// Clean up from previous runs
	os.RemoveAll(baseDir)
	os.MkdirAll(baseDir, 0755)

	// Test different compression types
	compressionTypes := []struct {
		name        string
		compression db.CompressionType
	}{
		{"none", db.NoCompression},
		{"snappy", db.SnappyCompression},
		{"lz4", db.LZ4Compression},
		{"zstd", db.ZstdCompression},
	}

	// Generate sample data (repetitive data compresses well)
	sampleValue := strings.Repeat("RockyardKV is a pure Go implementation of RocksDB. ", 20)
	numKeys := 1000

	results := make(map[string]int64)

	for _, ct := range compressionTypes {
		dbPath := filepath.Join(baseDir, ct.name)

		// Configure compression
		opts := db.DefaultOptions()
		opts.CreateIfMissing = true
		opts.Compression = ct.compression

		database, err := db.Open(dbPath, opts)
		if err != nil {
			log.Printf("Failed to open with %s compression: %v", ct.name, err)
			continue
		}

		// Write data
		wo := db.DefaultWriteOptions()
		for i := range numKeys {
			key := fmt.Sprintf("key-%05d", i)
			err = database.Put(wo, []byte(key), []byte(sampleValue))
			if err != nil {
				log.Fatal(err)
			}
		}

		// Force flush to disk
		err = database.Flush(db.DefaultFlushOptions())
		if err != nil {
			log.Fatal(err)
		}

		database.Close()

		// Measure directory size
		size, _ := dirSize(dbPath)
		results[ct.name] = size

		fmt.Printf("%-8s compression: %d KB\n", ct.name, size/1024)
	}

	// Calculate compression ratios
	if baseSize, ok := results["none"]; ok && baseSize > 0 {
		fmt.Println("\nCompression ratios:")
		for name, size := range results {
			if name != "none" {
				ratio := float64(baseSize) / float64(size)
				savings := 100.0 * (1.0 - float64(size)/float64(baseSize))
				fmt.Printf("  %s: %.2fx smaller (%.1f%% space savings)\n", name, ratio, savings)
			}
		}
	}

	fmt.Println("\nDone!")
}

// dirSize calculates the total size of a directory
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
