// reader.go implements trace file reading for operation replay.
//
// TraceReader in RocksDB reads traces one operation at a time.
// FileTraceReader is the file-based implementation with Reset support.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/trace_reader_writer.h (TraceReader interface)
//   - utilities/trace/file_trace_reader_writer.h (FileTraceReader)
//   - utilities/trace/file_trace_reader_writer.cc
package trace

import (
	"errors"
	"io"
)

// Reader reads trace records from an input stream.
type Reader struct {
	r      io.Reader
	header *Header
	count  uint64
}

// NewReader creates a new trace reader.
func NewReader(r io.Reader) (*Reader, error) {
	// Read and validate header
	header, err := DecodeHeader(r)
	if err != nil {
		return nil, err
	}

	return &Reader{
		r:      r,
		header: header,
	}, nil
}

// Header returns the trace file header.
func (tr *Reader) Header() *Header {
	return tr.header
}

// Read reads the next trace record.
// Returns io.EOF when there are no more records.
func (tr *Reader) Read() (*Record, error) {
	record, err := DecodeRecord(tr.r)
	if err != nil {
		return nil, err
	}
	tr.count++
	return record, nil
}

// ReadAll reads all remaining trace records.
func (tr *Reader) ReadAll() ([]*Record, error) {
	var records []*Record

	for {
		record, err := tr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}

// Iterate iterates over all trace records, calling fn for each.
func (tr *Reader) Iterate(fn func(*Record) error) error {
	for {
		record, err := tr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(record); err != nil {
			return err
		}
	}
}

// Count returns the number of records read so far.
func (tr *Reader) Count() uint64 {
	return tr.count
}

// TraceStats represents statistics about a trace file.
type TraceStats struct {
	TotalRecords uint64
	Duration     int64 // nanoseconds between first and last record
	RecordCounts map[RecordType]uint64
}

// ComputeStats computes statistics from the trace file.
func (tr *Reader) ComputeStats() (*TraceStats, error) {
	stats := &TraceStats{
		RecordCounts: make(map[RecordType]uint64),
	}

	var firstTS, lastTS int64

	for {
		record, err := tr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		stats.TotalRecords++
		stats.RecordCounts[record.Type]++

		ts := record.Timestamp.UnixNano()
		if firstTS == 0 || ts < firstTS {
			firstTS = ts
		}
		if ts > lastTS {
			lastTS = ts
		}
	}

	if firstTS > 0 && lastTS > 0 {
		stats.Duration = lastTS - firstTS
	}

	return stats, nil
}
