// Package main demonstrates compaction filters in RockyardKV.
//
// This example shows how to:
//   - Implement a custom compaction filter
//   - Filter out expired or unwanted data during compaction
//   - Transform values during compaction
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

// ExampleCompactionFilter removes keys with a "deleted:" prefix
// and transforms keys with an "uppercase:" prefix.
type ExampleCompactionFilter struct {
	deletedCount     int
	transformedCount int
}

func (f *ExampleCompactionFilter) Name() string {
	return "ExampleCompactionFilter"
}

func (f *ExampleCompactionFilter) Filter(level int, key, existingValue []byte) (db.CompactionFilterDecision, []byte) {
	// Delete keys marked for deletion
	if bytes.HasPrefix(key, []byte("deleted:")) {
		f.deletedCount++
		return db.FilterRemove, nil
	}

	// Transform: uppercase values for keys with "uppercase:" prefix
	if bytes.HasPrefix(key, []byte("uppercase:")) {
		f.transformedCount++
		return db.FilterChange, bytes.ToUpper(existingValue)
	}

	// Keep all other keys unchanged
	return db.FilterKeep, nil
}

func (f *ExampleCompactionFilter) FilterMergeOperand(level int, key, operand []byte) db.CompactionFilterDecision {
	return db.FilterKeep
}

func main() {
	dbPath := "/tmp/rockyardkv_compaction_filter"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Configure with compaction filter
	filter := &ExampleCompactionFilter{}
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.CompactionFilter = filter

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	wo := db.DefaultWriteOptions()
	ro := db.DefaultReadOptions()

	fmt.Println("Database opened with ExampleCompactionFilter")

	// Write various keys
	testData := map[string]string{
		"user:1":         "alice",
		"user:2":         "bob",
		"deleted:old1":   "should be removed",
		"deleted:old2":   "should be removed",
		"uppercase:name": "hello world",
		"uppercase:city": "new york",
		"config:setting": "value",
	}

	for k, v := range testData {
		err = database.Put(wo, []byte(k), []byte(v))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Wrote %d keys\n", len(testData))

	// Show data before compaction
	fmt.Println("\n=== Before Compaction ===")
	iter := database.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	// Trigger compaction
	fmt.Println("\nRunning compaction...")
	err = database.CompactRange(nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Show data after compaction
	fmt.Println("\n=== After Compaction ===")
	iter = database.NewIterator(ro)
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
		count++
	}
	iter.Close()

	fmt.Printf("\nCompaction stats:\n")
	fmt.Printf("  Keys removed: %d\n", filter.deletedCount)
	fmt.Printf("  Keys transformed: %d\n", filter.transformedCount)
	fmt.Printf("  Keys remaining: %d\n", count)

	fmt.Println("\nDone!")
}
