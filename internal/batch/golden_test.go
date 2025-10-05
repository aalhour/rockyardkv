package batch

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// TestGoldenWriteBatchHeader tests the WriteBatch header format.
// Header is 12 bytes: 8 bytes sequence number + 4 bytes count.
func TestGoldenWriteBatchHeader(t *testing.T) {
	testCases := []struct {
		name     string
		sequence uint64
		count    uint32
	}{
		{
			name:     "zero values",
			sequence: 0,
			count:    0,
		},
		{
			name:     "sequence 1, count 1",
			sequence: 1,
			count:    1,
		},
		{
			name:     "large sequence",
			sequence: 0x0123456789ABCDEF,
			count:    100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wb := New()
			wb.SetSequence(tc.sequence)
			// Add enough entries to match count
			for range uint32(tc.count) {
				wb.Put([]byte("k"), []byte("v"))
			}

			// Extract header from data
			data := wb.Data()
			if len(data) < HeaderSize {
				t.Fatalf("WriteBatch data too short: %d bytes", len(data))
			}

			header := data[:HeaderSize]

			// Verify sequence
			gotSeq := encoding.DecodeFixed64(header[:8])
			if gotSeq != tc.sequence {
				t.Errorf("sequence = 0x%016x, want 0x%016x", gotSeq, tc.sequence)
			}

			// Verify count
			gotCount := encoding.DecodeFixed32(header[8:12])
			if gotCount != tc.count {
				t.Errorf("count = %d, want %d", gotCount, tc.count)
			}
		})
	}
}

// TestGoldenWriteBatchRecordTypes tests that record types match RocksDB values.
func TestGoldenWriteBatchRecordTypes(t *testing.T) {
	// These must match RocksDB's db/dbformat.h ValueType enum
	testCases := []struct {
		name     string
		got      byte
		expected byte
	}{
		{"TypeDeletion", TypeDeletion, 0x00},
		{"TypeValue", TypeValue, 0x01},
		{"TypeMerge", TypeMerge, 0x02},
		{"TypeSingleDeletion", TypeSingleDeletion, 0x07},
		{"TypeColumnFamilyDeletion", TypeColumnFamilyDeletion, 0x04},
		{"TypeColumnFamilyValue", TypeColumnFamilyValue, 0x05},
		{"TypeColumnFamilyMerge", TypeColumnFamilyMerge, 0x06},
		{"TypeColumnFamilySingleDeletion", TypeColumnFamilySingleDeletion, 0x08},
		{"TypeBeginPrepareXID", TypeBeginPrepareXID, 0x09},
		{"TypeEndPrepareXID", TypeEndPrepareXID, 0x0A},
		{"TypeCommitXID", TypeCommitXID, 0x0B},
		{"TypeRollbackXID", TypeRollbackXID, 0x0C},
		{"TypeNoop", TypeNoop, 0x0D},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Errorf("%s = 0x%02x, want 0x%02x", tc.name, tc.got, tc.expected)
			}
		})
	}
}

// TestGoldenWriteBatchPutFormat tests the format of Put records.
func TestGoldenWriteBatchPutFormat(t *testing.T) {
	wb := New()
	wb.SetSequence(100)
	wb.Put([]byte("hello"), []byte("world"))

	data := wb.Data()

	// Skip header (12 bytes)
	record := data[HeaderSize:]

	// First byte should be TypeValue (0x01)
	if record[0] != TypeValue {
		t.Errorf("record type = 0x%02x, want 0x%02x", record[0], TypeValue)
	}

	// Key is length-prefixed
	keyLen, n, err := encoding.DecodeVarint32(record[1:])
	if err != nil {
		t.Fatalf("DecodeVarint32 for key length failed: %v", err)
	}
	if keyLen != 5 {
		t.Errorf("key length = %d, want 5", keyLen)
	}

	key := record[1+n : 1+n+int(keyLen)]
	if !bytes.Equal(key, []byte("hello")) {
		t.Errorf("key = %q, want %q", key, "hello")
	}

	// Value is length-prefixed
	valueOffset := 1 + n + int(keyLen)
	valueLen, n2, err := encoding.DecodeVarint32(record[valueOffset:])
	if err != nil {
		t.Fatalf("DecodeVarint32 for value length failed: %v", err)
	}
	if valueLen != 5 {
		t.Errorf("value length = %d, want 5", valueLen)
	}

	value := record[valueOffset+n2 : valueOffset+n2+int(valueLen)]
	if !bytes.Equal(value, []byte("world")) {
		t.Errorf("value = %q, want %q", value, "world")
	}
}

// TestGoldenWriteBatchDeleteFormat tests the format of Delete records.
func TestGoldenWriteBatchDeleteFormat(t *testing.T) {
	wb := New()
	wb.SetSequence(100)
	wb.Delete([]byte("key"))

	data := wb.Data()
	record := data[HeaderSize:]

	// First byte should be TypeDeletion (0x00)
	if record[0] != TypeDeletion {
		t.Errorf("record type = 0x%02x, want 0x%02x", record[0], TypeDeletion)
	}

	// Key is length-prefixed (no value for deletions)
	keyLen, n, err := encoding.DecodeVarint32(record[1:])
	if err != nil {
		t.Fatalf("DecodeVarint32 for key length failed: %v", err)
	}
	if keyLen != 3 {
		t.Errorf("key length = %d, want 3", keyLen)
	}

	key := record[1+n : 1+n+int(keyLen)]
	if !bytes.Equal(key, []byte("key")) {
		t.Errorf("key = %q, want %q", key, "key")
	}
}

// TestGoldenWriteBatchColumnFamilyFormat tests column family record format.
func TestGoldenWriteBatchColumnFamilyFormat(t *testing.T) {
	wb := New()
	wb.SetSequence(100)
	wb.PutCF(42, []byte("key"), []byte("value"))

	data := wb.Data()
	record := data[HeaderSize:]

	// First byte should be TypeColumnFamilyValue (0x05)
	if record[0] != TypeColumnFamilyValue {
		t.Errorf("record type = 0x%02x, want 0x%02x", record[0], TypeColumnFamilyValue)
	}

	// Column family ID is varint-encoded
	cfID, n, err := encoding.DecodeVarint32(record[1:])
	if err != nil {
		t.Fatalf("DecodeVarint32 for cfID failed: %v", err)
	}
	if cfID != 42 {
		t.Errorf("column family ID = %d, want 42", cfID)
	}

	// Key is length-prefixed
	keyLen, n2, err := encoding.DecodeVarint32(record[1+n:])
	if err != nil {
		t.Fatalf("DecodeVarint32 for key length failed: %v", err)
	}
	if keyLen != 3 {
		t.Errorf("key length = %d, want 3", keyLen)
	}

	key := record[1+n+n2 : 1+n+n2+int(keyLen)]
	if !bytes.Equal(key, []byte("key")) {
		t.Errorf("key = %q, want %q", key, "key")
	}
}

// TestGoldenWriteBatchRoundtrip tests WriteBatch roundtrip encoding/decoding.
func TestGoldenWriteBatchRoundtrip(t *testing.T) {
	wb := New()
	wb.SetSequence(1000)
	wb.Put([]byte("key1"), []byte("val1"))
	wb.Delete([]byte("key2"))
	wb.Put([]byte("key3"), []byte("val3"))

	// Serialize
	data := wb.Data()

	// Deserialize
	wb2, err := NewFromData(data)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}
	if wb2 == nil {
		t.Fatal("NewFromData returned nil")
	}

	if wb2.Sequence() != 1000 {
		t.Errorf("sequence = %d, want 1000", wb2.Sequence())
	}
	if wb2.Count() != 3 {
		t.Errorf("count = %d, want 3", wb2.Count())
	}
}
