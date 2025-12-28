// Package testutil provides test utilities for stress testing and verification.
//
// This file implements trace-based expected state recovery for crash testing.
// It records operations to a trace file and replays them after crash recovery
// to rebuild the expected state that matches the DB's recovered sequence number.
//
// Reference: RocksDB v10.7.5
//   - db_stress_tool/expected_state.h (SaveAtAndAfter, Restore)
//   - db_stress_tool/expected_state.cc (ExpectedStateTraceRecordHandler)
//
// Design:
//   - Before crash window: snapshot expected state + start recording trace
//   - During operations: append trace records (op type, key, value, seqno)
//   - After crash: load snapshot + replay trace up to recovered seqno
//   - Result: expected state matches exactly what DB recovered
package testutil

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Trace file format constants
const (
	traceMagic      = "RKYTRACE"
	traceVersion    = uint32(1)
	traceHeaderSize = 32 // magic(8) + version(4) + startSeq(8) + numCFs(4) + maxKey(8)
	traceRecordSize = 24 // op(1) + cf(1) + pad(2) + key(8) + value(4) + seqno(8)
)

// TraceOpType identifies the type of operation in a trace record.
type TraceOpType uint8

const (
	// TraceOpPut represents a put operation.
	TraceOpPut TraceOpType = 1
	// TraceOpDelete represents a delete operation.
	TraceOpDelete TraceOpType = 2
)

// TraceRecord represents a single operation in the trace.
type TraceRecord struct {
	Op        TraceOpType
	CF        int
	Key       int64
	ValueBase uint32
	SeqNo     uint64
}

// TraceWriter writes operation trace records to a file.
// It is safe for concurrent use.
type TraceWriter struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	startSeq uint64
	count    uint64
	closed   bool
}

// NewTraceWriter creates a new trace writer.
// The trace file records all operations from startSeq onwards.
func NewTraceWriter(path string, startSeq uint64, numCFs int, maxKey int64) (*TraceWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create trace file: %w", err)
	}

	tw := &TraceWriter{
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		startSeq: startSeq,
	}

	// Write header
	header := make([]byte, traceHeaderSize)
	copy(header[0:8], traceMagic)
	binary.LittleEndian.PutUint32(header[8:12], traceVersion)
	binary.LittleEndian.PutUint64(header[12:20], startSeq)
	binary.LittleEndian.PutUint32(header[20:24], uint32(numCFs))
	binary.LittleEndian.PutUint64(header[24:32], uint64(maxKey))

	if _, err := tw.writer.Write(header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("write trace header: %w", err)
	}

	return tw, nil
}

// Record appends an operation to the trace.
func (tw *TraceWriter) Record(op TraceOpType, cf int, key int64, valueBase uint32, seqno uint64) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.closed {
		return errors.New("trace writer is closed")
	}

	// Encode record
	record := make([]byte, traceRecordSize)
	record[0] = byte(op)
	record[1] = byte(cf)
	// bytes 2-3 are padding
	binary.LittleEndian.PutUint64(record[4:12], uint64(key))
	binary.LittleEndian.PutUint32(record[12:16], valueBase)
	binary.LittleEndian.PutUint64(record[16:24], seqno)

	if _, err := tw.writer.Write(record); err != nil {
		return fmt.Errorf("write trace record: %w", err)
	}

	tw.count++
	return nil
}

// RecordPut is a convenience method for recording a put operation.
func (tw *TraceWriter) RecordPut(cf int, key int64, valueBase uint32, seqno uint64) error {
	return tw.Record(TraceOpPut, cf, key, valueBase, seqno)
}

// RecordDelete is a convenience method for recording a delete operation.
func (tw *TraceWriter) RecordDelete(cf int, key int64, seqno uint64) error {
	return tw.Record(TraceOpDelete, cf, key, 0, seqno)
}

// Flush ensures all buffered data is written to disk.
func (tw *TraceWriter) Flush() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.closed {
		return nil
	}

	if err := tw.writer.Flush(); err != nil {
		return fmt.Errorf("flush trace buffer: %w", err)
	}

	if err := tw.file.Sync(); err != nil {
		return fmt.Errorf("sync trace file: %w", err)
	}

	return nil
}

// Close flushes and closes the trace writer.
func (tw *TraceWriter) Close() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.closed {
		return nil
	}

	tw.closed = true

	if err := tw.writer.Flush(); err != nil {
		_ = tw.file.Close()
		return fmt.Errorf("flush trace buffer: %w", err)
	}

	return tw.file.Close()
}

// Count returns the number of records written.
func (tw *TraceWriter) Count() uint64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.count
}

// StartSeq returns the starting sequence number for this trace.
func (tw *TraceWriter) StartSeq() uint64 {
	return tw.startSeq
}

// TraceReader reads operation trace records from a file.
type TraceReader struct {
	file     *os.File
	reader   *bufio.Reader
	startSeq uint64
	numCFs   int
	maxKey   int64
	recordNo uint64
}

// OpenTraceReader opens a trace file for reading.
func OpenTraceReader(path string) (*TraceReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open trace file: %w", err)
	}

	tr := &TraceReader{
		file:   file,
		reader: bufio.NewReaderSize(file, 64*1024),
	}

	// Read header
	header := make([]byte, traceHeaderSize)
	if _, err := io.ReadFull(tr.reader, header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("read trace header: %w", err)
	}

	// Verify magic
	if string(header[0:8]) != traceMagic {
		_ = file.Close()
		return nil, fmt.Errorf("invalid trace magic: %s", string(header[0:8]))
	}

	// Verify version
	version := binary.LittleEndian.Uint32(header[8:12])
	if version != traceVersion {
		_ = file.Close()
		return nil, fmt.Errorf("unsupported trace version: %d", version)
	}

	tr.startSeq = binary.LittleEndian.Uint64(header[12:20])
	tr.numCFs = int(binary.LittleEndian.Uint32(header[20:24]))
	tr.maxKey = int64(binary.LittleEndian.Uint64(header[24:32]))

	return tr, nil
}

// StartSeq returns the starting sequence number of the trace.
func (tr *TraceReader) StartSeq() uint64 {
	return tr.startSeq
}

// NumCFs returns the number of column families.
func (tr *TraceReader) NumCFs() int {
	return tr.numCFs
}

// MaxKey returns the maximum key value.
func (tr *TraceReader) MaxKey() int64 {
	return tr.maxKey
}

// Next reads the next trace record.
// Returns io.EOF when no more records are available.
func (tr *TraceReader) Next() (TraceRecord, error) {
	record := make([]byte, traceRecordSize)
	if _, err := io.ReadFull(tr.reader, record); err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			return TraceRecord{}, io.EOF
		}
		return TraceRecord{}, fmt.Errorf("read trace record: %w", err)
	}

	tr.recordNo++

	return TraceRecord{
		Op:        TraceOpType(record[0]),
		CF:        int(record[1]),
		Key:       int64(binary.LittleEndian.Uint64(record[4:12])),
		ValueBase: binary.LittleEndian.Uint32(record[12:16]),
		SeqNo:     binary.LittleEndian.Uint64(record[16:24]),
	}, nil
}

// Close closes the trace reader.
func (tr *TraceReader) Close() error {
	return tr.file.Close()
}

// ReplayTrace replays a trace file onto an expected state, applying only
// operations with sequence numbers <= targetSeqno.
// Returns the number of operations applied.
func ReplayTrace(tracePath string, targetSeqno uint64, state *ExpectedStateV2) (int, error) {
	reader, err := OpenTraceReader(tracePath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = reader.Close() }()

	applied := 0
	for {
		rec, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return applied, fmt.Errorf("read trace record %d: %w", applied+1, err)
		}

		// Only apply operations up to the recovered sequence number
		if rec.SeqNo > targetSeqno {
			continue
		}

		// Apply the operation to expected state
		switch rec.Op {
		case TraceOpPut:
			state.SyncPut(rec.CF, rec.Key, rec.ValueBase)
		case TraceOpDelete:
			state.SyncDelete(rec.CF, rec.Key)
		default:
			return applied, fmt.Errorf("unknown trace op type: %d", rec.Op)
		}

		applied++
	}

	return applied, nil
}

// ExpectedStateRecovery orchestrates trace-based expected state recovery.
// It implements the SaveAtAndAfter/Restore pattern from C++ RocksDB.
type ExpectedStateRecovery struct {
	// Base path for recovery files (snapshot and trace)
	basePath string

	// Configuration
	numCFs int
	maxKey int64

	// Active trace writer (nil when not tracing)
	traceWriter *TraceWriter
	traceMu     sync.Mutex
}

// NewExpectedStateRecovery creates a new recovery orchestrator.
// basePath is used as prefix for snapshot and trace files.
func NewExpectedStateRecovery(basePath string, numCFs int, maxKey int64) *ExpectedStateRecovery {
	return &ExpectedStateRecovery{
		basePath: basePath,
		numCFs:   numCFs,
		maxKey:   maxKey,
	}
}

// SnapshotPath returns the path to the snapshot file.
func (r *ExpectedStateRecovery) SnapshotPath() string {
	return r.basePath + ".snapshot"
}

// TracePath returns the path to the trace file.
func (r *ExpectedStateRecovery) TracePath() string {
	return r.basePath + ".trace"
}

// SaveAtAndAfter saves the current expected state as a snapshot and starts
// recording operations to a trace file.
//
// This should be called before entering a "crash window" where we might
// simulate a crash. The seqno parameter is the current sequence number
// which serves as the baseline for the snapshot.
//
// Returns the trace writer for recording subsequent operations.
func (r *ExpectedStateRecovery) SaveAtAndAfter(state *ExpectedStateV2, seqno uint64) (*TraceWriter, error) {
	r.traceMu.Lock()
	defer r.traceMu.Unlock()

	// Close any existing trace writer
	if r.traceWriter != nil {
		_ = r.traceWriter.Close()
		r.traceWriter = nil
	}

	// Save snapshot
	if err := state.SaveToFile(r.SnapshotPath()); err != nil {
		return nil, fmt.Errorf("save snapshot: %w", err)
	}

	// Start trace
	tw, err := NewTraceWriter(r.TracePath(), seqno, r.numCFs, r.maxKey)
	if err != nil {
		return nil, fmt.Errorf("create trace: %w", err)
	}

	r.traceWriter = tw
	return tw, nil
}

// StopTracing stops the active trace and flushes it to disk.
func (r *ExpectedStateRecovery) StopTracing() error {
	r.traceMu.Lock()
	defer r.traceMu.Unlock()

	if r.traceWriter != nil {
		if err := r.traceWriter.Close(); err != nil {
			return err
		}
		r.traceWriter = nil
	}
	return nil
}

// Restore loads the snapshot and replays the trace up to recoveredSeqno.
// This reconstructs the expected state that matches the DB's recovered state.
//
// recoveredSeqno should be obtained from the database after crash recovery
// (e.g., via DB.GetLatestSequenceNumber()).
func (r *ExpectedStateRecovery) Restore(recoveredSeqno uint64) (*ExpectedStateV2, uint64, error) {
	// Load snapshot
	state, err := LoadExpectedStateV2FromFile(r.SnapshotPath())
	if err != nil {
		return nil, 0, fmt.Errorf("load snapshot: %w", err)
	}

	// Replay trace up to recovered seqno
	applied, err := ReplayTrace(r.TracePath(), recoveredSeqno, state)
	if err != nil {
		return nil, 0, fmt.Errorf("replay trace: %w", err)
	}

	return state, uint64(applied), nil
}

// HasRecoveryFiles checks if snapshot and trace files exist.
func (r *ExpectedStateRecovery) HasRecoveryFiles() bool {
	_, err1 := os.Stat(r.SnapshotPath())
	_, err2 := os.Stat(r.TracePath())
	return err1 == nil && err2 == nil
}

// Cleanup removes the snapshot and trace files.
func (r *ExpectedStateRecovery) Cleanup() error {
	r.traceMu.Lock()
	if r.traceWriter != nil {
		_ = r.traceWriter.Close()
		r.traceWriter = nil
	}
	r.traceMu.Unlock()

	var errs []error
	if err := os.Remove(r.SnapshotPath()); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(r.TracePath()); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

// TraceRecordChecksum computes a checksum for a trace record (for debugging).
func TraceRecordChecksum(rec TraceRecord) uint32 {
	data := make([]byte, 24)
	data[0] = byte(rec.Op)
	data[1] = byte(rec.CF)
	binary.LittleEndian.PutUint64(data[4:12], uint64(rec.Key))
	binary.LittleEndian.PutUint32(data[12:16], rec.ValueBase)
	binary.LittleEndian.PutUint64(data[16:24], rec.SeqNo)
	return crc32.ChecksumIEEE(data)
}
