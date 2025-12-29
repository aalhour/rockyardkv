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
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aalhour/rockyardkv/db"
	"github.com/aalhour/rockyardkv/internal/batch"
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

	// Note: internal/trace.Replayer intentionally continues on errors and only returns
	// aggregate counts. For harness/debuggability we want to surface concrete handler
	// errors, so we replay in-process here and print the first few failures.
	stats, err := replayWithErrors(reader, handler, *preserveTime, 5 /* maxErrorsToPrint */)
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

	if stats.FailedOps > 0 {
		return fmt.Errorf("replay finished with %d failed operations (see errors above)", stats.FailedOps)
	}
	return nil
}

func replayWithErrors(reader *trace.Reader, handler trace.ReplayHandler, preserveTiming bool, maxErrorsToPrint int) (*trace.ReplayStats, error) {
	stats := &trace.ReplayStats{
		OperationCounts: make(map[trace.RecordType]uint64),
	}

	startTime := time.Now()
	var lastTimestamp time.Time

	errsPrinted := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			stats.Duration = time.Since(startTime)
			return stats, err
		}

		stats.TotalRecords++
		stats.OperationCounts[record.Type]++

		if preserveTiming && !lastTimestamp.IsZero() {
			delay := record.Timestamp.Sub(lastTimestamp)
			if delay > 0 {
				time.Sleep(delay)
			}
		}
		lastTimestamp = record.Timestamp

		if execErr := executeRecord(handler, record); execErr != nil {
			stats.FailedOps++
			if errsPrinted < maxErrorsToPrint {
				fmt.Fprintf(os.Stderr, "Replay op failed: type=%s ts=%s err=%v\n",
					record.Type.String(),
					record.Timestamp.Format(time.RFC3339Nano),
					execErr,
				)
				errsPrinted++
			}
			continue
		}
		stats.SuccessfulOps++
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}

func executeRecord(handler trace.ReplayHandler, record *trace.Record) error {
	switch record.Type {
	case trace.TypeWrite:
		payload, err := trace.DecodeWritePayload(record.Payload)
		if err != nil {
			return err
		}
		return handler.HandleWrite(payload.ColumnFamilyID, payload.Data)
	case trace.TypeGet:
		payload, err := trace.DecodeGetPayload(record.Payload)
		if err != nil {
			return err
		}
		return handler.HandleGet(payload.ColumnFamilyID, payload.Key)
	case trace.TypeIterSeek:
		payload, err := trace.DecodeGetPayload(record.Payload)
		if err != nil {
			return err
		}
		return handler.HandleIterSeek(payload.ColumnFamilyID, payload.Key)
	case trace.TypeFlush:
		return handler.HandleFlush()
	case trace.TypeCompaction:
		return handler.HandleCompaction()
	default:
		return nil
	}
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
	// internal/trace encodes writes as raw RocksDB WriteBatch bytes.
	// This is the same format used by internal/batch and by WAL WriteBatch records.
	//
	// Reference:
	// - internal/trace.WritePayload{Data: <WriteBatch bytes>}
	// - internal/batch.WriteBatch format (Header + Records)
	if cfID != 0 {
		return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
	}

	internalWB, err := batch.NewFromData(batchData)
	if err != nil {
		return fmt.Errorf("invalid write payload (not a WriteBatch): %w", err)
	}

	wb := db.NewWriteBatch()
	if err := internalWB.Iterate(&writeBatchCopier{dst: wb}); err != nil {
		return fmt.Errorf("invalid write batch records: %w", err)
	}

	if h.verbose {
		fmt.Printf("  WRITE batch_ops=%d bytes=%d\n", wb.Count(), len(batchData))
	}

	return h.database.Write(nil, wb)
}

func (h *dbHandler) HandleGet(cfID uint32, key []byte) error {
	// internal/trace encodes get payload as raw key bytes (no length prefix).
	if cfID != 0 {
		return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
	}
	actualKey := key
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
	// internal/trace encodes iter seek payload as raw key bytes (no length prefix).
	if cfID != 0 {
		return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
	}
	actualKey := key
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

// writeBatchCopier copies internal/batch operations into a public db.WriteBatch.
// This keeps replay applying an atomic Write() instead of individual ops.
type writeBatchCopier struct {
	dst *db.WriteBatch
}

func (c *writeBatchCopier) Put(key, value []byte) error {
	c.dst.Put(key, value)
	return nil
}

func (c *writeBatchCopier) Delete(key []byte) error {
	c.dst.Delete(key)
	return nil
}

func (c *writeBatchCopier) SingleDelete(key []byte) error {
	c.dst.SingleDelete(key)
	return nil
}

func (c *writeBatchCopier) Merge(key, value []byte) error {
	c.dst.Merge(key, value)
	return nil
}

func (c *writeBatchCopier) DeleteRange(startKey, endKey []byte) error {
	c.dst.DeleteRange(startKey, endKey)
	return nil
}

func (c *writeBatchCopier) LogData(_ []byte) {
	// No-op for trace replay.
}

func (c *writeBatchCopier) PutCF(cfID uint32, key, value []byte) error {
	// Column families are not currently used by stresstest traces.
	return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
}

func (c *writeBatchCopier) DeleteCF(cfID uint32, key []byte) error {
	return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
}

func (c *writeBatchCopier) SingleDeleteCF(cfID uint32, key []byte) error {
	return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
}

func (c *writeBatchCopier) MergeCF(cfID uint32, key, value []byte) error {
	return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
}

func (c *writeBatchCopier) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	return fmt.Errorf("trace replay does not support column families yet: cf=%d", cfID)
}
