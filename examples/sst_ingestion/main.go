// Package main demonstrates SST file ingestion in RockyardKV.
//
// This example shows how to:
//   - Create SST files externally using SstFileWriter
//   - Ingest SST files into a database for bulk loading
//   - Configure ingestion options
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aalhour/rockyardkv"
)

func main() {
	dbPath := "/tmp/rockyardkv_sst_ingestion"
	sstPath := "/tmp/rockyardkv_sst_files"

	// Clean up from previous runs
	os.RemoveAll(dbPath)
	os.RemoveAll(sstPath)
	os.MkdirAll(sstPath, 0755)

	// Create external SST files
	fmt.Println("=== Creating SST Files ===")

	// SST file 1: users data
	sst1Path := filepath.Join(sstPath, "users.sst")
	err := createUsersSSTFile(sst1Path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created: %s\n", sst1Path)

	// SST file 2: products data
	sst2Path := filepath.Join(sstPath, "products.sst")
	err = createProductsSSTFile(sst2Path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created: %s\n", sst2Path)

	// Open database
	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := rockyardkv.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Write some existing data
	wo := rockyardkv.DefaultWriteOptions()
	err = database.Put(wo, []byte("config:version"), []byte("1.0"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nExisting data: config:version = 1.0")

	// Ingest the SST files
	fmt.Println("\n=== Ingesting SST Files ===")

	ingestOpts := rockyardkv.DefaultIngestExternalFileOptions()
	ingestOpts.MoveFiles = false // Copy files (keep originals)
	ingestOpts.VerifyChecksumsBeforeIngest = true

	err = database.IngestExternalFile([]string{sst1Path, sst2Path}, ingestOpts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("SST files ingested successfully")

	// Verify ingested data
	fmt.Println("\n=== All Data After Ingestion ===")
	ro := rockyardkv.DefaultReadOptions()
	iter := database.NewIterator(ro)
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %s\n", iter.Key(), iter.Value())
		count++
	}
	iter.Close()
	fmt.Printf("\nTotal keys: %d\n", count)

	// Query specific ingested data
	fmt.Println("\n=== Query Ingested Data ===")
	value, err := database.Get(ro, []byte("user:001"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("user:001 = %s\n", value)

	value, err = database.Get(ro, []byte("product:A100"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("product:A100 = %s\n", value)

	fmt.Println("\nDone!")
}

// createUsersSSTFile creates an SST file with user data.
// Keys must be added in sorted order.
func createUsersSSTFile(path string) error {
	writerOpts := rockyardkv.DefaultSstFileWriterOptions()
	writer := rockyardkv.NewSstFileWriter(writerOpts)

	err := writer.Open(path)
	if err != nil {
		return err
	}

	// Add keys in sorted order
	users := []struct {
		key   string
		value string
	}{
		{"user:001", `{"name":"alice","email":"alice@example.com"}`},
		{"user:002", `{"name":"bob","email":"bob@example.com"}`},
		{"user:003", `{"name":"charlie","email":"charlie@example.com"}`},
		{"user:004", `{"name":"diana","email":"diana@example.com"}`},
		{"user:005", `{"name":"eve","email":"eve@example.com"}`},
	}

	for _, u := range users {
		err = writer.Put([]byte(u.key), []byte(u.value))
		if err != nil {
			return err
		}
	}

	_, err = writer.Finish()
	return err
}

// createProductsSSTFile creates an SST file with product data.
// Keys must be added in sorted order.
func createProductsSSTFile(path string) error {
	writerOpts := rockyardkv.DefaultSstFileWriterOptions()
	writer := rockyardkv.NewSstFileWriter(writerOpts)

	err := writer.Open(path)
	if err != nil {
		return err
	}

	// Add keys in sorted order
	products := []struct {
		key   string
		value string
	}{
		{"product:A100", `{"name":"Widget","price":9.99}`},
		{"product:A200", `{"name":"Gadget","price":19.99}`},
		{"product:B100", `{"name":"Gizmo","price":29.99}`},
		{"product:B200", `{"name":"Thingamajig","price":39.99}`},
	}

	for _, p := range products {
		err = writer.Put([]byte(p.key), []byte(p.value))
		if err != nil {
			return err
		}
	}

	_, err = writer.Finish()
	return err
}
