// Package main demonstrates column families in RockyardKV.
//
// This example shows how to:
//   - Create and open a database with multiple column families
//   - Write and read data in different column families
//   - Perform atomic writes across column families
//   - Drop a column family
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_column_families"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Open the database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Create a new column family for user metadata
	cfOpts := db.DefaultColumnFamilyOptions()
	usersCF, err := database.CreateColumnFamily(cfOpts, "users")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created column family: users")

	// Create another column family for session data
	sessionsCF, err := database.CreateColumnFamily(cfOpts, "sessions")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Created column family: sessions")

	// Get default column family handle
	defaultCF := database.DefaultColumnFamily()

	// Write to different column families
	wo := db.DefaultWriteOptions()

	// Default column family: application config
	err = database.PutCF(wo, defaultCF, []byte("app:version"), []byte("1.0.0"))
	if err != nil {
		log.Fatal(err)
	}

	// Users column family: user data
	err = database.PutCF(wo, usersCF, []byte("user:1"), []byte(`{"name":"alice","email":"alice@example.com"}`))
	if err != nil {
		log.Fatal(err)
	}
	err = database.PutCF(wo, usersCF, []byte("user:2"), []byte(`{"name":"bob","email":"bob@example.com"}`))
	if err != nil {
		log.Fatal(err)
	}

	// Sessions column family: session data
	err = database.PutCF(wo, sessionsCF, []byte("session:abc123"), []byte(`{"user_id":1,"expires":"2024-12-31"}`))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nWrote data to all column families")

	// Read from different column families
	ro := db.DefaultReadOptions()

	fmt.Println("\n=== Default CF ===")
	value, _ := database.GetCF(ro, defaultCF, []byte("app:version"))
	fmt.Printf("app:version = %s\n", value)

	fmt.Println("\n=== Users CF ===")
	iter := database.NewIteratorCF(ro, usersCF)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	fmt.Println("\n=== Sessions CF ===")
	iter = database.NewIteratorCF(ro, sessionsCF)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	// Atomic write across column families
	fmt.Println("\n=== Atomic write across column families ===")
	wb := db.NewWriteBatch()
	wb.PutCF(usersCF.ID(), []byte("user:3"), []byte(`{"name":"charlie"}`))
	wb.PutCF(sessionsCF.ID(), []byte("session:xyz789"), []byte(`{"user_id":3}`))
	wb.PutCF(defaultCF.ID(), []byte("app:last_user"), []byte("3"))

	err = database.Write(wo, wb)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Batch write completed")

	// List all column families
	cfNames := database.ListColumnFamilies()
	fmt.Printf("\nColumn families: %v\n", cfNames)

	// Drop the sessions column family
	fmt.Println("\n=== Dropping sessions column family ===")
	err = database.DropColumnFamily(sessionsCF)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Sessions column family dropped")

	// List again
	cfNames = database.ListColumnFamilies()
	fmt.Printf("Column families after drop: %v\n", cfNames)

	fmt.Println("\nDone!")
}
