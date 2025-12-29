package trace

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func TestHeaderEncodeDecode(t *testing.T) {
	h := &Header{
		Magic:   MagicNumber,
		Version: CurrentVersion,
	}

	var buf bytes.Buffer
	if err := h.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if buf.Len() != HeaderSize {
		t.Fatalf("Expected header size %d, got %d", HeaderSize, buf.Len())
	}

	decoded, err := DecodeHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Magic != h.Magic {
		t.Errorf("Magic mismatch: got %x, want %x", decoded.Magic, h.Magic)
	}
	if decoded.Version != h.Version {
		t.Errorf("Version mismatch: got %d, want %d", decoded.Version, h.Version)
	}
}

func TestRecordEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "get",
			record: &Record{
				Timestamp: time.Unix(1234567890, 123456789),
				Type:      TypeGet,
				Payload:   []byte("test-key"),
			},
		},
		{
			name: "write",
			record: &Record{
				Timestamp: time.Now(),
				Type:      TypeWrite,
				Payload:   bytes.Repeat([]byte("x"), 1000),
			},
		},
		{
			name: "flush",
			record: &Record{
				Timestamp: time.Now(),
				Type:      TypeFlush,
				Payload:   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tt.record.Encode(&buf); err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := DecodeRecord(&buf)
			if err != nil {
				t.Fatalf("DecodeRecord failed: %v", err)
			}

			if decoded.Timestamp.UnixNano() != tt.record.Timestamp.UnixNano() {
				t.Errorf("Timestamp mismatch")
			}
			if decoded.Type != tt.record.Type {
				t.Errorf("Type mismatch: got %v, want %v", decoded.Type, tt.record.Type)
			}
			if !bytes.Equal(decoded.Payload, tt.record.Payload) {
				t.Errorf("Payload mismatch")
			}
		})
	}
}

func TestWriterReader(t *testing.T) {
	var buf bytes.Buffer

	// Write trace records
	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write various record types
	if err := writer.WriteGet(0, []byte("key1")); err != nil {
		t.Fatalf("WriteGet failed: %v", err)
	}
	if err := writer.WriteWrite(0, []byte("batch-data")); err != nil {
		t.Fatalf("WriteWrite failed: %v", err)
	}
	if err := writer.WriteFlush(); err != nil {
		t.Fatalf("WriteFlush failed: %v", err)
	}
	if err := writer.WriteIterSeek(1, []byte("seek-key")); err != nil {
		t.Fatalf("WriteIterSeek failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if writer.Count() != 4 {
		t.Errorf("Expected 4 records, got %d", writer.Count())
	}

	// Read trace records
	reader, err := NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(records) != 4 {
		t.Fatalf("Expected 4 records, got %d", len(records))
	}

	// Verify record types
	expectedTypes := []RecordType{TypeGet, TypeWrite, TypeFlush, TypeIterSeek}
	for i, rec := range records {
		if rec.Type != expectedTypes[i] {
			t.Errorf("Record %d: expected type %v, got %v", i, expectedTypes[i], rec.Type)
		}
	}
}

func TestPayloads(t *testing.T) {
	// Test GetPayload
	getPayload := &GetPayload{
		ColumnFamilyID: 42,
		Key:            []byte("test-key"),
	}
	encoded := getPayload.Encode()
	decoded, err := DecodeGetPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeGetPayload failed: %v", err)
	}
	if decoded.ColumnFamilyID != getPayload.ColumnFamilyID {
		t.Errorf("CF ID mismatch")
	}
	if !bytes.Equal(decoded.Key, getPayload.Key) {
		t.Errorf("Key mismatch")
	}

	// Test WritePayload (V2 format with sequence number)
	writePayload := &WritePayload{
		ColumnFamilyID: 123,
		SequenceNumber: 999,
		Data:           []byte("batch-data"),
	}
	encoded = writePayload.Encode()
	decodedWrite, err := DecodeWritePayloadV2(encoded)
	if err != nil {
		t.Fatalf("DecodeWritePayloadV2 failed: %v", err)
	}
	if decodedWrite.ColumnFamilyID != writePayload.ColumnFamilyID {
		t.Errorf("CF ID mismatch")
	}
	if decodedWrite.SequenceNumber != writePayload.SequenceNumber {
		t.Errorf("SequenceNumber mismatch: got %d, want %d", decodedWrite.SequenceNumber, writePayload.SequenceNumber)
	}
	if !bytes.Equal(decodedWrite.Data, writePayload.Data) {
		t.Errorf("Data mismatch")
	}

	// Test backward compatibility: V1 decoder on V1 data
	writePayloadV1 := &WritePayload{
		ColumnFamilyID: 456,
		Data:           []byte("v1-data"),
	}
	// Manually encode V1 format (no seqno)
	v1Encoded := make([]byte, 4+len(writePayloadV1.Data))
	binary.LittleEndian.PutUint32(v1Encoded[0:4], writePayloadV1.ColumnFamilyID)
	copy(v1Encoded[4:], writePayloadV1.Data)

	decodedV1, err := DecodeWritePayload(v1Encoded)
	if err != nil {
		t.Fatalf("DecodeWritePayload (V1) failed: %v", err)
	}
	if decodedV1.ColumnFamilyID != writePayloadV1.ColumnFamilyID {
		t.Errorf("V1 CF ID mismatch")
	}
	if decodedV1.SequenceNumber != 0 {
		t.Errorf("V1 SequenceNumber should be 0, got %d", decodedV1.SequenceNumber)
	}
}

func TestIterateAndStats(t *testing.T) {
	var buf bytes.Buffer

	// Write some records
	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	now := time.Now()
	for i := range 10 {
		ts := now.Add(time.Duration(i) * time.Millisecond)
		if err := writer.WriteAt(ts, TypeGet, []byte("key")); err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}
	}
	for range 5 {
		if err := writer.WriteFlush(); err != nil {
			t.Fatalf("WriteFlush failed: %v", err)
		}
	}
	writer.Close()

	// Test Iterate
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	count := 0
	err = reader.Iterate(func(r *Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}
	if count != 15 {
		t.Errorf("Expected 15 records, got %d", count)
	}

	// Test ComputeStats
	reader2, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	stats, err := reader2.ComputeStats()
	if err != nil {
		t.Fatalf("ComputeStats failed: %v", err)
	}

	if stats.TotalRecords != 15 {
		t.Errorf("Expected 15 total records, got %d", stats.TotalRecords)
	}
	if stats.RecordCounts[TypeGet] != 10 {
		t.Errorf("Expected 10 Get records, got %d", stats.RecordCounts[TypeGet])
	}
	if stats.RecordCounts[TypeFlush] != 5 {
		t.Errorf("Expected 5 Flush records, got %d", stats.RecordCounts[TypeFlush])
	}
}

func TestInvalidHeader(t *testing.T) {
	// Invalid magic number
	buf := bytes.NewBuffer(make([]byte, HeaderSize))
	_, err := DecodeHeader(buf)
	if !errors.Is(err, ErrInvalidTraceFile) {
		t.Errorf("Expected ErrInvalidTraceFile, got %v", err)
	}
}

func TestRecordTypeString(t *testing.T) {
	if TypeGet.String() != "Get" {
		t.Errorf("TypeGet.String() = %s, want Get", TypeGet.String())
	}
	if TypeWrite.String() != "Write" {
		t.Errorf("TypeWrite.String() = %s, want Write", TypeWrite.String())
	}
	if RecordType(255).String() != "Unknown" {
		t.Errorf("Unknown type should return Unknown")
	}
}

func TestWriterAfterClose(t *testing.T) {
	var buf bytes.Buffer
	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	writer.Close()

	err = writer.Write(TypeGet, nil)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Expected ErrClosedPipe, got %v", err)
	}
}

func TestReaderHeader(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	writer.WriteGet(0, []byte("key"))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	header := reader.Header()
	if header.Magic != MagicNumber {
		t.Errorf("Magic mismatch: got %x, want %x", header.Magic, MagicNumber)
	}
	if header.Version != CurrentVersion {
		t.Errorf("Version mismatch: got %d, want %d", header.Version, CurrentVersion)
	}
}

func TestReaderCount(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	for range 7 {
		writer.WriteGet(0, []byte("key"))
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	// Count() returns records read so far, starts at 0
	if reader.Count() != 0 {
		t.Errorf("Initial count should be 0, got %d", reader.Count())
	}

	// Read 3 records
	for range 3 {
		_, err := reader.Read()
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
	}
	if reader.Count() != 3 {
		t.Errorf("After 3 reads, count should be 3, got %d", reader.Count())
	}

	// Read remaining records
	_, err = reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if reader.Count() != 7 {
		t.Errorf("After ReadAll, count should be 7, got %d", reader.Count())
	}
}

func TestRecordTypeStringAllTypes(t *testing.T) {
	tests := []struct {
		typ  RecordType
		want string
	}{
		{TypeGet, "Get"},
		{TypeWrite, "Write"},
		{TypeFlush, "Flush"},
		{TypeCompaction, "Compaction"},
		{TypeIterSeek, "IterSeek"},
		{RecordType(99), "Unknown"},
	}
	for _, tt := range tests {
		got := tt.typ.String()
		if got != tt.want {
			t.Errorf("RecordType(%d).String() = %s, want %s", tt.typ, got, tt.want)
		}
	}
}

func TestDecodePayloadErrors(t *testing.T) {
	// WritePayload too short
	_, err := DecodeWritePayload([]byte{1, 2, 3})
	if err == nil {
		t.Error("DecodeWritePayload should fail on short input")
	}

	// GetPayload too short
	_, err = DecodeGetPayload([]byte{1, 2, 3})
	if err == nil {
		t.Error("DecodeGetPayload should fail on short input")
	}

	// Empty payload
	_, err = DecodeWritePayload(nil)
	if err == nil {
		t.Error("DecodeWritePayload should fail on nil input")
	}

	_, err = DecodeGetPayload(nil)
	if err == nil {
		t.Error("DecodeGetPayload should fail on nil input")
	}
}

func TestIterateEarlyExit(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	for range 10 {
		writer.WriteGet(0, []byte("key"))
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	// Early exit after 3 records
	count := 0
	customErr := errors.New("stop iteration")
	err = reader.Iterate(func(r *Record) error {
		count++
		if count >= 3 {
			return customErr
		}
		return nil
	})
	if !errors.Is(err, customErr) {
		t.Errorf("Expected custom error, got %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 iterations, got %d", count)
	}
}
