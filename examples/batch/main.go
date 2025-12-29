// Package main demonstrates atomic batch writes in RockyardKV.
//
// This example shows how to:
//   - Create a WriteBatch for atomic operations
//   - Add multiple puts and deletes to a batch
//   - Apply the batch atomically
//   - Clear and reuse a batch
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_batch"

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
	err = database.Put(wo, []byte("balance:alice"), []byte("100"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("balance:bob"), []byte("50"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Initial balances: alice=100, bob=50")

	// Create a batch for atomic transfer
	// This simulates transferring 30 from alice to bob
	wb := db.NewWriteBatch()

	// Debit alice, credit bob
	wb.Put([]byte("balance:alice"), []byte("70"))
	wb.Put([]byte("balance:bob"), []byte("80"))
	wb.Put([]byte("transfer:1"), []byte("alice->bob:30"))

	fmt.Println("\nApplying batch (transfer 30 from alice to bob)...")

	// Apply the batch atomically
	err = database.Write(wo, wb)
	if err != nil {
		log.Fatal(err)
	}

	// Verify the transfer
	alice, _ := database.Get(ro, []byte("balance:alice"))
	bob, _ := database.Get(ro, []byte("balance:bob"))
	transfer, _ := database.Get(ro, []byte("transfer:1"))

	fmt.Printf("After transfer: alice=%s, bob=%s\n", alice, bob)
	fmt.Printf("Transfer record: %s\n", transfer)

	// Demonstrate batch with deletes
	fmt.Println("\nClearing transfer record and adding new accounts...")
	wb.Clear() // Reuse the batch

	wb.Delete([]byte("transfer:1"))
	wb.Put([]byte("balance:charlie"), []byte("200"))
	wb.Put([]byte("balance:dana"), []byte("150"))

	err = database.Write(wo, wb)
	if err != nil {
		log.Fatal(err)
	}

	// Show final state
	fmt.Println("\nFinal state:")
	iter := database.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	fmt.Println("\nDone!")
}
