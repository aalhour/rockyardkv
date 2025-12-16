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
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aalhour/rockyardkv/internal/trace"
)

var (
	// Global flags
	_ = flag.Bool("v", false, "Verbose output") // Reserved for future use

	// Dump flags
	dumpLimit = flag.Int("limit", 0, "Maximum number of records to dump (0 = all)")

	// Replay flags
	replayDB     = flag.String("db", "", "Database path for replay")
	preserveTime = flag.Bool("preserve-timing", false, "Preserve original timing during replay")
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

	// Create a dummy handler that just counts operations
	handler := &countingHandler{}

	opts := trace.DefaultReplayerOptions()
	opts.PreserveTiming = *preserveTime

	replayer := trace.NewReplayer(reader, handler, opts)
	stats, err := replayer.Replay()
	if err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}

	fmt.Println("Replay Statistics")
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
