// Package main demonstrates backup and restore in RockyardKV.
//
// This example shows how to:
//   - Create a backup engine
//   - Create incremental backups
//   - List and verify backups
//   - Restore from a backup
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

func main() {
	dbPath := "/tmp/rockyardkv_backup_db"
	backupPath := "/tmp/rockyardkv_backup_store"
	restorePath := "/tmp/rockyardkv_backup_restored"

	// Clean up from previous runs
	os.RemoveAll(dbPath)
	os.RemoveAll(backupPath)
	os.RemoveAll(restorePath)

	// Open the database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}

	wo := db.DefaultWriteOptions()

	// Write initial data
	err = database.Put(wo, []byte("version"), []byte("1.0"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("data:1"), []byte("first record"))
	if err != nil {
		log.Fatal(err)
	}

	// Flush to ensure data is on disk
	err = database.Flush(db.DefaultFlushOptions())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Initial data written: version=1.0, data:1=first record")

	// Create backup engine
	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		log.Fatal(err)
	}
	defer backupEngine.Close()

	// Create first backup
	_, err = backupEngine.CreateNewBackup()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Backup 1 created")

	// Add more data
	err = database.Put(wo, []byte("data:2"), []byte("second record"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Put(wo, []byte("data:3"), []byte("third record"))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Flush(db.DefaultFlushOptions())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("More data written: data:2, data:3")

	// Create second backup
	_, err = backupEngine.CreateNewBackup()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Backup 2 created")

	// List backups
	backups, err := backupEngine.GetBackupInfo()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n=== Backups (%d total) ===\n", len(backups))
	for _, info := range backups {
		fmt.Printf("  Backup %d: files=%d\n", info.ID, info.NumFiles)
	}

	// Close original database
	database.Close()
	fmt.Println("\nOriginal database closed")

	// Restore from backup 1 (only has version and data:1)
	fmt.Println("\n=== Restoring from Backup 1 ===")
	err = backupEngine.RestoreDBFromBackup(1, restorePath)
	if err != nil {
		log.Fatal(err)
	}

	// Open restored database
	restoredDB, err := db.Open(restorePath, opts)
	if err != nil {
		log.Fatal(err)
	}

	// Verify restored data
	ro := db.DefaultReadOptions()
	fmt.Println("Restored data:")
	iter := restoredDB.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
	}
	iter.Close()

	// data:2 and data:3 should not exist (they were added after backup 1)
	_, err = restoredDB.Get(ro, []byte("data:2"))
	if err == db.ErrNotFound {
		fmt.Println("\ndata:2 not found (correct - added after backup 1)")
	}

	restoredDB.Close()

	fmt.Println("\nDone!")
}
