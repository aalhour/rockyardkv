// Package main demonstrates snapshots in RockyardKV.
//
// This example shows how to:
//   - Create a point-in-time snapshot
//   - Read consistent data from a snapshot while writes continue
//   - Release snapshots when done
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_snapshots"

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

	wo := db.DefaultWriteOptions()
	ro := db.DefaultReadOptions()

	// Write initial data
	err = database.Put(wo, []byte("version"), []byte("1.0"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("config"), []byte("initial"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Initial state: version=1.0, config=initial")

	// Create a snapshot
	snapshot := database.GetSnapshot()
	fmt.Println("\nSnapshot created")

	// Continue writing to the database
	err = database.Put(wo, []byte("version"), []byte("2.0"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("config"), []byte("updated"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("newkey"), []byte("newvalue"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("After writes: version=2.0, config=updated, newkey=newvalue")

	// Read current state
	fmt.Println("\n=== Current state (no snapshot) ===")
	value, _ := database.Get(ro, []byte("version"))
	fmt.Printf("version = %s\n", value)
	value, _ = database.Get(ro, []byte("config"))
	fmt.Printf("config = %s\n", value)
	value, _ = database.Get(ro, []byte("newkey"))
	fmt.Printf("newkey = %s\n", value)

	// Read from snapshot - sees the old state
	fmt.Println("\n=== Snapshot state (point-in-time) ===")
	roSnap := db.DefaultReadOptions()
	roSnap.Snapshot = snapshot

	value, _ = database.Get(roSnap, []byte("version"))
	fmt.Printf("version = %s\n", value)
	value, _ = database.Get(roSnap, []byte("config"))
	fmt.Printf("config = %s\n", value)
	_, err = database.Get(roSnap, []byte("newkey"))
	if err == db.ErrNotFound {
		fmt.Println("newkey = (not found - didn't exist at snapshot time)")
	}

	// Iterate using snapshot
	fmt.Println("\n=== All keys at snapshot time ===")
	iter := database.NewIterator(roSnap)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	// Release the snapshot when done
	database.ReleaseSnapshot(snapshot)
	fmt.Println("\nSnapshot released")

	fmt.Println("Done!")
}
