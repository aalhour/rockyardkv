// Package main demonstrates merge operators in RockyardKV.
//
// This example shows how to:
//   - Implement a custom merge operator
//   - Use merge for read-modify-write operations
//   - Build efficient counters and appenders
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/aalhour/rockyardkv/db"
)

// CounterMergeOperator implements a simple counter that adds values.
type CounterMergeOperator struct{}

func (m *CounterMergeOperator) Name() string {
	return "CounterMergeOperator"
}

func (m *CounterMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	// Start with existing value or 0
	var counter int64
	if len(existingValue) >= 8 {
		counter = int64(binary.LittleEndian.Uint64(existingValue))
	}

	// Apply each operand (add delta)
	for _, operand := range operands {
		if len(operand) >= 8 {
			delta := int64(binary.LittleEndian.Uint64(operand))
			counter += delta
		}
	}

	// Return new value
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(counter))
	return result, true
}

func (m *CounterMergeOperator) PartialMerge(key, left, right []byte) ([]byte, bool) {
	// Partial merge: combine two deltas
	var leftDelta, rightDelta int64
	if len(left) >= 8 {
		leftDelta = int64(binary.LittleEndian.Uint64(left))
	}
	if len(right) >= 8 {
		rightDelta = int64(binary.LittleEndian.Uint64(right))
	}

	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, uint64(leftDelta+rightDelta))
	return result, true
}

// encodeCounter encodes an int64 as bytes
func encodeCounter(n int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(n))
	return buf
}

// decodeCounter decodes bytes to int64
func decodeCounter(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func main() {
	dbPath := "/tmp/rockyardkv_merge"

	// Clean up from previous runs
	os.RemoveAll(dbPath)

	// Configure with merge operator
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true
	opts.MergeOperator = &CounterMergeOperator{}

	database, err := db.Open(dbPath, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	wo := db.DefaultWriteOptions()
	ro := db.DefaultReadOptions()

	fmt.Println("Database opened with CounterMergeOperator")

	// Initialize a counter
	err = database.Put(wo, []byte("visits:home"), encodeCounter(100))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Initial value: visits:home = 100")

	// Increment using merge (efficient read-modify-write)
	err = database.Merge(wo, []byte("visits:home"), encodeCounter(1))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Merge +1")

	err = database.Merge(wo, []byte("visits:home"), encodeCounter(5))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Merge +5")

	err = database.Merge(wo, []byte("visits:home"), encodeCounter(10))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Merge +10")

	// Read the merged value
	value, err := database.Get(ro, []byte("visits:home"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Final value: visits:home = %d\n", decodeCounter(value))

	// Merge on non-existent key (starts from 0)
	fmt.Println("\nMerge on new key:")
	err = database.Merge(wo, []byte("visits:about"), encodeCounter(42))
	if err != nil {
		log.Fatal(err)
	}

	value, _ = database.Get(ro, []byte("visits:about"))
	fmt.Printf("visits:about = %d (created via merge)\n", decodeCounter(value))

	// Batch merge operations
	fmt.Println("\nBatch merge:")
	err = database.Merge(wo, []byte("visits:home"), encodeCounter(100))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Merge(wo, []byte("visits:about"), encodeCounter(50))
	if err != nil {
		log.Fatal(err)
	}
	err = database.Merge(wo, []byte("visits:contact"), encodeCounter(25))
	if err != nil {
		log.Fatal(err)
	}

	// Show all counters
	fmt.Println("\nFinal counters:")
	iter := database.NewIterator(ro)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("  %s = %d\n", iter.Key(), decodeCounter(iter.Value()))
	}
	iter.Close()

	fmt.Println("\nDone!")
}
