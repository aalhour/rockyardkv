// Package main demonstrates iterator usage in RockyardKV.
//
// This example shows how to:
//   - Create an iterator
//   - Scan all keys forward
//   - Scan keys in reverse
//   - Seek to a specific key
//   - Iterate over a key range
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_iteration"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Open database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Insert sample data
	wo := db.DefaultWriteOptions()
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, k := range keys {
		err = database.Put(wo, []byte(k), []byte("value_"+k))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Inserted %d keys\n\n", len(keys))

	// Forward iteration: scan all keys
	fmt.Println("=== Forward scan (all keys) ===")
	ro := db.DefaultReadOptions()
	iter := database.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s -> %s\n", iter.Key(), iter.Value())
	}
	if err := iter.Error(); err != nil {
		log.Fatal(err)
	}
	iter.Close()

	// Reverse iteration: scan all keys backward
	fmt.Println("\n=== Reverse scan (all keys) ===")
	iter = database.NewIterator(ro)
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		fmt.Printf("  %s -> %s\n", iter.Key(), iter.Value())
	}
	if err := iter.Error(); err != nil {
		log.Fatal(err)
	}
	iter.Close()

	// Seek to a specific key
	fmt.Println("\n=== Seek to 'cherry' and scan forward ===")
	iter = database.NewIterator(ro)
	for iter.Seek([]byte("cherry")); iter.Valid(); iter.Next() {
		fmt.Printf("  %s -> %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	// Range iteration with bounds
	fmt.Println("\n=== Range scan: 'banana' to 'elderberry' ===")
	ro.IterateLowerBound = []byte("banana")
	ro.IterateUpperBound = []byte("elderberry") // exclusive
	iter = database.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s -> %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	fmt.Println("\nDone!")
}
