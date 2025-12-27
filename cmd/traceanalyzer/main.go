// traceanalyzer is a tool for analyzing and replaying trace files.
//
// Usage:
//
//	trace_analyzer <command> [options] <trace_file>
//
// Commands:
//
//	stats     Display statistics about the trace file
//	dump      Dump all trace records
//	replay    Replay the trace against a database
//
// Examples:
//
//	trace_analyzer stats trace.log
//	trace_analyzer dump --limit 100 trace.log
//	trace_analyzer replay --db /tmp/replay_db trace.log
//
// Reference: RocksDB v10.7.5
//   - tools/trace_analyzer_tool.h
//   - tools/trace_analyzer_tool.cc
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/trace"
)

var (
	// Global flags
	verbose = flag.Bool("v", false, "Verbose output")

	// Dump flags
	dumpLimit = flag.Int("limit", 0, "Maximum number of records to dump (0 = all)")

	// Replay flags
	replayDB     = flag.String("db", "", "Database path for replay")
	preserveTime = flag.Bool("preserve-timing", false, "Preserve original timing during replay")
	dryRun       = flag.Bool("dry-run", false, "Count operations without applying them (default for replay)")
	createDB     = flag.Bool("create", true, "Create database if it doesn't exist")
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	traceFile := args[1]

	var err error
	switch command {
	case "stats":
		err = cmdStats(traceFile)
	case "dump":
		err = cmdDump(traceFile)
	case "replay":
		err = cmdReplay(traceFile)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`trace_analyzer - RockyardKV trace file analyzer

Usage:
  trace_analyzer <command> [options] <trace_file>

Commands:
  stats     Display statistics about the trace file
  dump      Dump trace records
  replay    Replay the trace against a database

Options:
  -v              Verbose output
  -limit N        Maximum records to dump (dump command)
  -db PATH        Database path for replay (replay command)
  -preserve-timing  Preserve original timing during replay

Examples:
  trace_analyzer stats trace.log
  trace_analyzer dump -limit 100 trace.log
  trace_analyzer replay -db /tmp/replay_db trace.log`)
}

func cmdStats(traceFile string) error {
	file, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	reader, err := trace.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	stats, err := reader.ComputeStats()
	if err != nil {
		return fmt.Errorf("failed to compute stats: %w", err)
	}

	fmt.Println("Trace File Statistics")
	fmt.Println("=====================")
	fmt.Printf("Total Records: %d\n", stats.TotalRecords)
	fmt.Printf("Duration:      %s\n", time.Duration(stats.Duration))
	fmt.Println("\nRecord Types:")

	for recType, count := range stats.RecordCounts {
		fmt.Printf("  %-15s %d\n", recType.String()+":", count)
	}

	if stats.TotalRecords > 0 && stats.Duration > 0 {
		opsPerSec := float64(stats.TotalRecords) / (float64(stats.Duration) / float64(time.Second))
		fmt.Printf("\nOperations/sec: %.2f\n", opsPerSec)
	}

	return nil
}

func cmdDump(traceFile string) error {
	file, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	reader, err := trace.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	count := 0
	limit := *dumpLimit

	err = reader.Iterate(func(record *trace.Record) error {
		if limit > 0 && count >= limit {
			return fmt.Errorf("limit reached")
		}

		// Format timestamp
		ts := record.Timestamp.Format("2006-01-02 15:04:05.000000")

		// Format payload based on type
		var payloadStr string
		switch record.Type {
		case trace.TypeGet:
			payload, err := trace.DecodeGetPayload(record.Payload)
			if err == nil {
				payloadStr = fmt.Sprintf("cf=%d key=%q", payload.ColumnFamilyID, string(payload.Key))
			}
		case trace.TypeWrite:
			payload, err := trace.DecodeWritePayload(record.Payload)
			if err == nil {
				payloadStr = fmt.Sprintf("cf=%d batch_size=%d", payload.ColumnFamilyID, len(payload.Data))
			}
		case trace.TypeIterSeek:
			payload, err := trace.DecodeGetPayload(record.Payload)
			if err == nil {
				payloadStr = fmt.Sprintf("cf=%d key=%q", payload.ColumnFamilyID, string(payload.Key))
			}
		default:
			payloadStr = fmt.Sprintf("(%d bytes)", len(record.Payload))
		}

		fmt.Printf("[%s] %-12s %s\n", ts, record.Type.String(), payloadStr)
		count++
		return nil
	})

	if err != nil && err.Error() != "limit reached" {
		return err
	}

	fmt.Printf("\nDumped %d records\n", count)
	return nil
}

func cmdReplay(traceFile string) error {
	if *replayDB == "" {
		return fmt.Errorf("--db flag is required for replay")
	}

	file, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	reader, err := trace.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	// Create handler based on mode
	var handler trace.ReplayHandler
	var database db.DB

	if *dryRun {
		// Dry run mode: just count operations
		handler = &countingHandler{}
		fmt.Println("Running in dry-run mode (operations counted but not applied)")
	} else {
		// Real replay mode: open database and apply operations
		opts := db.DefaultOptions()
		opts.CreateIfMissing = *createDB

		database, err = db.Open(*replayDB, opts)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		defer database.Close()

		handler = &dbHandler{
			database: database,
			verbose:  *verbose,
		}
		fmt.Printf("Replaying to database: %s\n", *replayDB)
	}

	opts := trace.DefaultReplayerOptions()
	opts.PreserveTiming = *preserveTime

	replayer := trace.NewReplayer(reader, handler, opts)
	stats, err := replayer.Replay()
	if err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	fmt.Println("\nReplay Statistics")
	fmt.Println("=================")
	fmt.Printf("Total Records:   %d\n", stats.TotalRecords)
	fmt.Printf("Successful Ops:  %d\n", stats.SuccessfulOps)
	fmt.Printf("Failed Ops:      %d\n", stats.FailedOps)
	fmt.Printf("Duration:        %s\n", stats.Duration)

	if stats.Duration > 0 {
		opsPerSec := float64(stats.TotalRecords) / stats.Duration.Seconds()
		fmt.Printf("Operations/sec:  %.2f\n", opsPerSec)
	}

	return nil
}

// countingHandler is a simple handler that counts operations without executing them
type countingHandler struct {
	writes      int
	gets        int
	iterSeeks   int
	flushes     int
	compactions int
}

func (h *countingHandler) HandleWrite(cfID uint32, batchData []byte) error {
	h.writes++
	return nil
}

func (h *countingHandler) HandleGet(cfID uint32, key []byte) error {
	h.gets++
	return nil
}

func (h *countingHandler) HandleIterSeek(cfID uint32, key []byte) error {
	h.iterSeeks++
	return nil
}

func (h *countingHandler) HandleFlush() error {
	h.flushes++
	return nil
}

func (h *countingHandler) HandleCompaction() error {
	h.compactions++
	return nil
}

// dbHandler applies trace operations to a real database.
type dbHandler struct {
	database db.DB
	verbose  bool
}

func (h *dbHandler) HandleWrite(cfID uint32, batchData []byte) error {
	// The trace format from stresstest is: [key_len:4][key][value_len:4][value]
	// Parse and apply as a Put operation
	if len(batchData) < 4 {
		return fmt.Errorf("invalid write payload: too short")
	}

	keyLen := binary.LittleEndian.Uint32(batchData[0:4])
	if len(batchData) < int(4+keyLen+4) {
		// This might be a delete (just key, no value)
		if len(batchData) == int(4+keyLen) {
			key := batchData[4 : 4+keyLen]
			if h.verbose {
				fmt.Printf("  DELETE key=%q\n", string(key))
			}
			return h.database.Delete(nil, key)
		}
		return fmt.Errorf("invalid write payload: incomplete")
	}

	key := batchData[4 : 4+keyLen]
	valueStart := 4 + keyLen
	valueLen := binary.LittleEndian.Uint32(batchData[valueStart : valueStart+4])

	if len(batchData) < int(valueStart+4+valueLen) {
		return fmt.Errorf("invalid write payload: value truncated")
	}

	value := batchData[valueStart+4 : valueStart+4+valueLen]

	if h.verbose {
		fmt.Printf("  PUT key=%q value_len=%d\n", string(key), len(value))
	}

	return h.database.Put(nil, key, value)
}

func (h *dbHandler) HandleGet(cfID uint32, key []byte) error {
	// Parse the payload: [key_len:4][key]
	if len(key) < 4 {
		return fmt.Errorf("invalid get payload: too short")
	}

	keyLen := binary.LittleEndian.Uint32(key[0:4])
	if len(key) < int(4+keyLen) {
		return fmt.Errorf("invalid get payload: key truncated")
	}

	actualKey := key[4 : 4+keyLen]

	if h.verbose {
		fmt.Printf("  GET key=%q\n", string(actualKey))
	}

	_, err := h.database.Get(nil, actualKey)
	// We don't care if the key doesn't exist, just if there's an error
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return err
	}
	return nil
}

func (h *dbHandler) HandleIterSeek(cfID uint32, key []byte) error {
	// Parse the payload: [key_len:4][key]
	if len(key) < 4 {
		return fmt.Errorf("invalid seek payload: too short")
	}

	keyLen := binary.LittleEndian.Uint32(key[0:4])
	if len(key) < int(4+keyLen) {
		return fmt.Errorf("invalid seek payload: key truncated")
	}

	actualKey := key[4 : 4+keyLen]

	if h.verbose {
		fmt.Printf("  SEEK key=%q\n", string(actualKey))
	}

	iter := h.database.NewIterator(nil)
	defer iter.Close()
	iter.Seek(actualKey)

	return nil
}

func (h *dbHandler) HandleFlush() error {
	if h.verbose {
		fmt.Println("  FLUSH")
	}
	return h.database.Flush(nil)
}

func (h *dbHandler) HandleCompaction() error {
	if h.verbose {
		fmt.Println("  COMPACT")
	}
	// Trigger a manual compaction on the full range
	return h.database.CompactRange(nil, nil, nil)
}
