// Package trace implements operation tracing and replay for debugging.
//
// Trace files record all database operations with timestamps, allowing
// users to replay workloads, debug issues, and analyze performance.
//
// Trace File Format:
//
//	[Header]
//	[Trace Record 1]
//	[Trace Record 2]
//	...
//
// Header (16 bytes):
//
//	Magic Number (8 bytes): "ROCKSTRC"
//	Version (4 bytes)
//	Reserved (4 bytes)
//
// Trace Record:
//
//	Timestamp (8 bytes, nanoseconds)
//	Type (1 byte)
//	Payload Length (4 bytes, varint)
//	Payload (variable)
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/trace_record.h
//   - trace_replay/trace_replay.cc
package trace

import (
	"encoding/binary"
	"errors"
	"io"
	"time"
)

// Constants for trace file format
const (
	// HeaderSize is the size of the trace file header
	HeaderSize = 16

	// MagicNumber identifies a trace file
	MagicNumber uint64 = 0x524F434B53545243 // "ROCKSTRC"

	// CurrentVersion is the current trace format version.
	// Version 2 adds SequenceNumber to WritePayload for seqno-prefix verification.
	CurrentVersion uint32 = 2

	// Version1 is the legacy format without sequence numbers.
	Version1 uint32 = 1
)

var (
	// ErrInvalidTraceFile indicates the file is not a valid trace file
	ErrInvalidTraceFile = errors.New("trace: invalid trace file")

	// ErrUnsupportedVersion indicates an unsupported trace format version
	ErrUnsupportedVersion = errors.New("trace: unsupported version")
)

// RecordType identifies the type of trace record
type RecordType uint8

const (
	// TypeWrite is a write batch operation
	TypeWrite RecordType = iota + 1
	// TypeGet is a get operation
	TypeGet
	// TypeIterSeek is an iterator seek
	TypeIterSeek
	// TypeIterSeekForPrev is an iterator seek for prev
	TypeIterSeekForPrev
	// TypeFlush is a flush operation
	TypeFlush
	// TypeCompaction is a compaction operation
	TypeCompaction
	// TypeMultiGet is a multi-get operation
	TypeMultiGet
	// TypeNewIterator is creating a new iterator
	TypeNewIterator
)

// String returns the string representation of the record type
func (t RecordType) String() string {
	switch t {
	case TypeWrite:
		return "Write"
	case TypeGet:
		return "Get"
	case TypeIterSeek:
		return "IterSeek"
	case TypeIterSeekForPrev:
		return "IterSeekForPrev"
	case TypeFlush:
		return "Flush"
	case TypeCompaction:
		return "Compaction"
	case TypeMultiGet:
		return "MultiGet"
	case TypeNewIterator:
		return "NewIterator"
	default:
		return "Unknown"
	}
}

// Header represents the trace file header
type Header struct {
	Magic   uint64
	Version uint32
}

// Encode writes the header to the given writer
func (h *Header) Encode(w io.Writer) error {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint64(buf[0:8], h.Magic)
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	// bytes 12-15 reserved
	_, err := w.Write(buf)
	return err
}

// DecodeHeader reads a header from the given reader
func DecodeHeader(r io.Reader) (*Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	h := &Header{
		Magic:   binary.LittleEndian.Uint64(buf[0:8]),
		Version: binary.LittleEndian.Uint32(buf[8:12]),
	}

	if h.Magic != MagicNumber {
		return nil, ErrInvalidTraceFile
	}

	if h.Version > CurrentVersion {
		return nil, ErrUnsupportedVersion
	}

	return h, nil
}

// Record represents a single trace record
type Record struct {
	Timestamp time.Time
	Type      RecordType
	Payload   []byte
}

// Encode writes the record to the given writer
func (r *Record) Encode(w io.Writer) error {
	// Write timestamp (nanoseconds since Unix epoch)
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(r.Timestamp.UnixNano()))
	if _, err := w.Write(tsBuf); err != nil {
		return err
	}

	// Write type
	if _, err := w.Write([]byte{byte(r.Type)}); err != nil {
		return err
	}

	// Write payload length and payload
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(r.Payload)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	if len(r.Payload) > 0 {
		if _, err := w.Write(r.Payload); err != nil {
			return err
		}
	}

	return nil
}

// DecodeRecord reads a record from the given reader
func DecodeRecord(r io.Reader) (*Record, error) {
	// Read timestamp
	tsBuf := make([]byte, 8)
	if _, err := io.ReadFull(r, tsBuf); err != nil {
		return nil, err
	}
	ts := binary.LittleEndian.Uint64(tsBuf)

	// Read type
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, err
	}

	// Read payload length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	payloadLen := binary.LittleEndian.Uint32(lenBuf)

	// Read payload
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Record{
		Timestamp: time.Unix(0, int64(ts)),
		Type:      RecordType(typeBuf[0]),
		Payload:   payload,
	}, nil
}

// WritePayload encodes a Write operation payload.
//
// Version 2 format (current):
//
//	ColumnFamilyID (4 bytes)
//	SequenceNumber (8 bytes) - seqno assigned by DB after Write()
//	Data (variable)          - WriteBatch bytes
//
// Version 1 format (legacy):
//
//	ColumnFamilyID (4 bytes)
//	Data (variable)          - WriteBatch bytes
type WritePayload struct {
	ColumnFamilyID uint32
	SequenceNumber uint64 // Seqno assigned by DB after Write() - for seqno-prefix verification
	Data           []byte // WriteBatch data
}

// Encode encodes the write payload (version 2 format with sequence number).
func (p *WritePayload) Encode() []byte {
	buf := make([]byte, 4+8+len(p.Data))
	binary.LittleEndian.PutUint32(buf[0:4], p.ColumnFamilyID)
	binary.LittleEndian.PutUint64(buf[4:12], p.SequenceNumber)
	copy(buf[12:], p.Data)
	return buf
}

// DecodeWritePayload decodes a write payload.
// Supports both version 1 (no seqno) and version 2 (with seqno) formats.
func DecodeWritePayload(data []byte) (*WritePayload, error) {
	if len(data) < 4 {
		return nil, errors.New("trace: invalid write payload")
	}
	return &WritePayload{
		ColumnFamilyID: binary.LittleEndian.Uint32(data[0:4]),
		// Version 1 format: seqno not present, will be 0
		Data: data[4:],
	}, nil
}

// DecodeWritePayloadV2 decodes a version 2 write payload with sequence number.
func DecodeWritePayloadV2(data []byte) (*WritePayload, error) {
	if len(data) < 12 {
		return nil, errors.New("trace: invalid write payload v2 (too short)")
	}
	return &WritePayload{
		ColumnFamilyID: binary.LittleEndian.Uint32(data[0:4]),
		SequenceNumber: binary.LittleEndian.Uint64(data[4:12]),
		Data:           data[12:],
	}, nil
}

// GetPayload encodes a Get operation payload
type GetPayload struct {
	ColumnFamilyID uint32
	Key            []byte
}

// Encode encodes the get payload
func (p *GetPayload) Encode() []byte {
	buf := make([]byte, 4+len(p.Key))
	binary.LittleEndian.PutUint32(buf[0:4], p.ColumnFamilyID)
	copy(buf[4:], p.Key)
	return buf
}

// DecodeGetPayload decodes a get payload
func DecodeGetPayload(data []byte) (*GetPayload, error) {
	if len(data) < 4 {
		return nil, errors.New("trace: invalid get payload")
	}
	return &GetPayload{
		ColumnFamilyID: binary.LittleEndian.Uint32(data[0:4]),
		Key:            data[4:],
	}, nil
}
