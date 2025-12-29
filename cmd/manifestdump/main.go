// MANIFEST dump utility for RockyardKV.
//
// Use `manifestdump` to print a summary of a MANIFEST file.
// This tool decodes VersionEdits from the MANIFEST and prints a per-level live file set.
//
// Run the tool:
//
// ```bash
// ./bin/manifestdump <MANIFEST_FILE>
// ```
//
// Output includes:
// - Total decoded edits.
// - Final live file numbers per level.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/wal"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: manifestdump <manifest-file>")
		os.Exit(1)
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	reader := wal.NewStrictReader(bytes.NewReader(data), nil, 0)
	editCount := 0
	// Track files per level: level -> fileNum -> exists
	liveFiles := make(map[int]map[uint64]bool)
	for i := range 7 {
		liveFiles[i] = make(map[uint64]bool)
	}

	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Printf("Error at edit %d: %v\n", editCount+1, err)
			break
		}

		ve := &manifest.VersionEdit{}
		if err := ve.DecodeFrom(record); err != nil {
			fmt.Printf("Decode error at edit %d: %v\n", editCount+1, err)
			continue
		}

		editCount++
		for _, nf := range ve.NewFiles {
			fileNum := nf.Meta.FD.GetNumber()
			liveFiles[nf.Level][fileNum] = true
		}
		for _, df := range ve.DeletedFiles {
			delete(liveFiles[df.Level], df.FileNumber)
		}
	}

	fmt.Printf("Total edits: %d\n", editCount)
	fmt.Printf("\nFinal live files by level:\n")
	totalLive := 0
	for level := range 7 {
		if len(liveFiles[level]) > 0 {
			fmt.Printf("  Level %d: ", level)
			for fn := range liveFiles[level] {
				fmt.Printf("%d ", fn)
			}
			fmt.Println()
			totalLive += len(liveFiles[level])
		}
	}
	fmt.Printf("Total live: %d\n", totalLive)
}
