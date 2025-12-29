// Package main demonstrates basic RockyardKV operations.
//
// This example shows how to:
//   - Open a database
//   - Write key-value pairs
//   - Read values back
//   - Delete keys
//   - Close the database properly
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_basic"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Configure and open the database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	fmt.Println("Database opened successfully")

	// Write a key-value pair
	wo := db.DefaultWriteOptions()
	err = database.Put(wo, []byte("greeting"), []byte("Hello, RockyardKV!"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Put: greeting -> Hello, RockyardKV!")

	// Read the value back
	ro := db.DefaultReadOptions()
	value, err := database.Get(ro, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: greeting -> %s\n", value)

	// Write more keys
	err = database.Put(wo, []byte("user:1"), []byte("alice"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("user:2"), []byte("bob"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Put: user:1 -> alice, user:2 -> bob")

	// Delete a key
	err = database.Delete(wo, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Delete: greeting")

	// Verify deletion
	_, err = database.Get(ro, []byte("greeting"))
	if err != nil {
		if err == db.ErrNotFound {
			fmt.Println("Get: greeting -> (not found)")
		} else {
			log.Fatal(err)
		}
	}

	// Database closes automatically via defer
	fmt.Println("Done!")
}
