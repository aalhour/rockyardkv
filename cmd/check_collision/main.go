// Minimal collision checker for internal keys across SST files
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

type KeyEntry struct {
	InternalKey []byte
	Value       []byte
	File        string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <db_dir>\n", os.Args[0])
		os.Exit(1)
	}

	dbDir := os.Args[1]

	// Collect all internal keys from all SST files
	keyMap := make(map[string][]KeyEntry) // map[internal_key_hex][]entries

	fs := vfs.Default()
	files, err := fs.ListDir(dbDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to list directory: %v\n", err)
		os.Exit(1)
	}

	for _, filename := range files {
		if filepath.Ext(filename) != ".sst" {
			continue
		}

		filePath := filepath.Join(dbDir, filename)
		file, err := fs.OpenRandomAccess(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open %s: %v\n", filename, err)
			continue
		}

		reader, err := table.Open(file, table.ReaderOptions{VerifyChecksums: false})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open SST %s: %v\n", filename, err)
			file.Close()
			continue
		}

		iter := reader.NewIterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			internalKey := iter.Key()
			value := iter.Value()

			keyHex := hex.EncodeToString(internalKey)

			entry := KeyEntry{
				InternalKey: append([]byte{}, internalKey...),
				Value:       append([]byte{}, value...),
				File:        filename,
			}

			keyMap[keyHex] = append(keyMap[keyHex], entry)
		}

		if err := iter.Error(); err != nil {
			fmt.Fprintf(os.Stderr, "Iterator error in %s: %v\n", filename, err)
		}

		file.Close()
	}

	// Find collisions
	collisions := 0
	var collisionKeys []string

	for keyHex, entries := range keyMap {
		if len(entries) > 1 {
			// Check if values differ
			firstValue := hex.EncodeToString(entries[0].Value)
			hasDifferentValue := false

			for i := 1; i < len(entries); i++ {
				if hex.EncodeToString(entries[i].Value) != firstValue {
					hasDifferentValue = true
					break
				}
			}

			if hasDifferentValue {
				collisions++
				collisionKeys = append(collisionKeys, keyHex)
			}
		}
	}

	if collisions > 0 {
		fmt.Printf("🔥 SMOKING GUN: Found %d internal key collision(s) with different values!\n\n", collisions)

		// Sort for consistent output
		sort.Strings(collisionKeys)

		for i, keyHex := range collisionKeys {
			if i >= 5 {
				fmt.Printf("... and %d more collisions\n", len(collisionKeys)-5)
				break
			}

			entries := keyMap[keyHex]
			fmt.Printf("═══════════════════════════════════════════════════════════\n")
			fmt.Printf("Collision #%d:\n", i+1)
			fmt.Printf("Internal Key (hex): %s\n", keyHex)

			if len(entries[0].InternalKey) >= 8 {
				userKey := entries[0].InternalKey[:len(entries[0].InternalKey)-8]
				seqAndType := entries[0].InternalKey[len(entries[0].InternalKey)-8:]
				fmt.Printf("User Key (hex):     %s\n", hex.EncodeToString(userKey))
				fmt.Printf("Seq+Type (hex):     %s\n", hex.EncodeToString(seqAndType))
			}
			fmt.Printf("\n")

			for j, entry := range entries {
				valueHex := hex.EncodeToString(entry.Value)
				valueStr := ""
				if len(entry.Value) > 0 && entry.Value[0] >= 32 && entry.Value[0] < 127 {
					valueStr = fmt.Sprintf(" (%q)", string(entry.Value))
				}
				fmt.Printf("  [%d] File: %-15s Value: %s%s\n", j+1, entry.File, valueHex, valueStr)
			}
			fmt.Printf("\n")
		}

		os.Exit(1)
	} else {
		fmt.Println("✓ No internal key collisions found")
		os.Exit(0)
	}
}
