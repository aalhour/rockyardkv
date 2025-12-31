// writer.go implements trace file writing for operation tracing.
//
// TraceWriter in RocksDB exports traces one operation at a time.
// FileTraceWriter is the file-based implementation.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/trace_reader_writer.h (TraceWriter interface)
//   - utilities/trace/file_trace_reader_writer.h (FileTraceWriter)
//   - utilities/trace/file_trace_reader_writer.cc
package trace

import (
	"io"
	"sync"
	"time"
)

// Writer writes trace records to an output stream.
type Writer struct {
	mu           sync.Mutex
	w            io.Writer
	closed       bool
	count        uint64
	bytesWritten int64
	maxBytes     int64 // 0 = unlimited
	truncated    bool
}

// WriterOption configures a Writer.
type WriterOption func(*Writer)

// WithMaxBytes sets the maximum bytes to write before stopping.
// When the limit is reached, subsequent writes are silently dropped.
// Use 0 (default) for unlimited.
func WithMaxBytes(maxBytes int64) WriterOption {
	return func(tw *Writer) {
		tw.maxBytes = maxBytes
	}
}

// NewWriter creates a new trace writer.
func NewWriter(w io.Writer, opts ...WriterOption) (*Writer, error) {
	tw := &Writer{w: w}

	// Apply options
	for _, opt := range opts {
		opt(tw)
	}

	// Write header
	header := &Header{
		Magic:   MagicNumber,
		Version: CurrentVersion,
	}
	if err := header.Encode(w); err != nil {
		return nil, err
	}

	// Account for header size (approximate)
	tw.bytesWritten = 16 // Magic(8) + Version(4) + padding

	return tw, nil
}

// Write writes a trace record with the current timestamp.
func (tw *Writer) Write(recordType RecordType, payload []byte) error {
	return tw.WriteAt(time.Now(), recordType, payload)
}

// WriteAt writes a trace record with a specific timestamp.
// If a max size is configured and exceeded, the write is silently dropped.
func (tw *Writer) WriteAt(timestamp time.Time, recordType RecordType, payload []byte) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.closed {
		return io.ErrClosedPipe
	}

	// Check size limit before writing
	if tw.maxBytes > 0 && tw.bytesWritten >= tw.maxBytes {
		tw.truncated = true
		return nil // Silently drop - limit reached
	}

	record := &Record{
		Timestamp: timestamp,
		Type:      recordType,
		Payload:   payload,
	}

	if err := record.Encode(tw.w); err != nil {
		return err
	}

	// Approximate record size: timestamp(8) + type(1) + length(4) + payload
	tw.bytesWritten += 13 + int64(len(payload))
	tw.count++
	return nil
}

// WriteGet writes a Get trace record.
func (tw *Writer) WriteGet(cfID uint32, key []byte) error {
	payload := &GetPayload{
		ColumnFamilyID: cfID,
		Key:            key,
	}
	return tw.Write(TypeGet, payload.Encode())
}

// WriteWrite writes a Write trace record.
func (tw *Writer) WriteWrite(cfID uint32, batchData []byte) error {
	payload := &WritePayload{
		ColumnFamilyID: cfID,
		Data:           batchData,
	}
	return tw.Write(TypeWrite, payload.Encode())
}

// WriteFlush writes a Flush trace record.
func (tw *Writer) WriteFlush() error {
	return tw.Write(TypeFlush, nil)
}

// WriteCompaction writes a Compaction trace record.
func (tw *Writer) WriteCompaction() error {
	return tw.Write(TypeCompaction, nil)
}

// WriteIterSeek writes an iterator seek trace record.
func (tw *Writer) WriteIterSeek(cfID uint32, key []byte) error {
	payload := &GetPayload{
		ColumnFamilyID: cfID,
		Key:            key,
	}
	return tw.Write(TypeIterSeek, payload.Encode())
}

// Count returns the number of records written.
func (tw *Writer) Count() uint64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.count
}

// BytesWritten returns the approximate number of bytes written.
func (tw *Writer) BytesWritten() int64 {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.bytesWritten
}

// Truncated returns true if the writer stopped accepting records due to size limit.
func (tw *Writer) Truncated() bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.truncated
}

// Close marks the writer as closed.
func (tw *Writer) Close() error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.closed = true
	return nil
}
