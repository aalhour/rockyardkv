// Package main demonstrates transactions in RockyardKV.
//
// This example shows how to:
//   - Open a TransactionDB
//   - Begin transactions with pessimistic concurrency control
//   - Use snapshot isolation for repeatable reads
//   - Handle conflicts and rollbacks
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_transactions"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Open a TransactionDB
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	txnOpts := db.DefaultTransactionDBOptions()

	txnDB, err := db.OpenTransactionDB(dbPath, opts, txnOpts)
	if err != nil {
		log.Fatal(err)
	}
	defer txnDB.Close()

	wo := db.DefaultWriteOptions()

	fmt.Println("TransactionDB opened")

	// Example 1: Simple transaction
	fmt.Println("\n=== Simple Transaction ===")

	txn := txnDB.BeginTransaction(db.DefaultPessimisticTransactionOptions(), wo)

	// Read a key (doesn't exist yet)
	_, err = txn.Get([]byte("account:alice"))
	if err == db.ErrNotFound {
		fmt.Println("txn.Get(account:alice) -> not found")
	}

	// Write within transaction
	err = txn.Put([]byte("account:alice"), []byte("1000"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("txn.Put(account:alice, 1000)")

	// Commit the transaction
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("txn.Commit()")

	// Now visible outside transaction
	value, _ := txnDB.Get([]byte("account:alice"))
	fmt.Printf("txnDB.Get(account:alice) -> %s (committed)\n", value)

	// Example 2: Transfer with conflict detection
	fmt.Println("\n=== Transfer with Conflict Detection ===")

	// Set up accounts
	err = txnDB.Put([]byte("account:alice"), []byte("500"))
	if err != nil {
		log.Fatal(err)
	}
	err = txnDB.Put([]byte("account:bob"), []byte("300"))
	if err != nil {
		log.Fatal(err)
	}

	// Begin transfer transaction
	txn = txnDB.BeginTransaction(db.DefaultPessimisticTransactionOptions(), wo)

	// Read alice's balance with lock (GetForUpdate)
	aliceBalance, err := txn.GetForUpdate([]byte("account:alice"), true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("txn.GetForUpdate(account:alice) -> %s\n", aliceBalance)

	// Simulate: debit alice, credit bob
	err = txn.Put([]byte("account:alice"), []byte("400")) // 500 - 100
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Put([]byte("account:bob"), []byte("400")) // 300 + 100
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transfer committed: alice -> bob (100)")

	// Verify final balances
	alice, _ := txnDB.Get([]byte("account:alice"))
	bob, _ := txnDB.Get([]byte("account:bob"))
	fmt.Printf("Final balances: alice=%s, bob=%s\n", alice, bob)

	// Example 3: Rollback
	fmt.Println("\n=== Transaction Rollback ===")

	txn = txnDB.BeginTransaction(db.DefaultPessimisticTransactionOptions(), wo)
	err = txn.Put([]byte("account:alice"), []byte("999"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("txn.Put(account:alice, 999)")

	// Rollback instead of commit
	txn.Rollback()
	fmt.Println("txn.Rollback()")

	// Value should be unchanged
	alice, _ = txnDB.Get([]byte("account:alice"))
	fmt.Printf("txnDB.Get(account:alice) -> %s (unchanged after rollback)\n", alice)

	fmt.Println("\nDone!")
}
