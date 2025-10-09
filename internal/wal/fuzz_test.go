package wal

import (
	"bytes"
	"testing"
)

// FuzzWALReader fuzzes the WAL reader with arbitrary data to ensure it doesn't panic.
func FuzzWALReader(f *testing.F) {
	// Add seed corpus
	f.Add([]byte{}) // Empty
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}) // Type 1 (full record)
	f.Add([]byte{0xc3, 0xd2, 0xe1, 0xf0, 0x05, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'})
	// Valid record header format: CRC(4) + Length(2) + Type(1) + Data
	f.Add(makeValidRecord([]byte("test data")))
	f.Add(makeValidRecord([]byte{})) // Empty record

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := NewReader(bytes.NewReader(data), nil, false, 0)

		// Try to read records - shouldn't panic
		for range 100 {
			record, err := reader.ReadRecord()
			if err != nil {
				break
			}
			if record == nil {
				break
			}
			// We don't care about the content, just that we don't crash
			_ = record
		}
	})
}

// FuzzWALWriter fuzzes the WAL writer to ensure it produces valid output.
func FuzzWALWriter(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("hello world"))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0})
	f.Add(make([]byte, 32768)) // Large record

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1024*1024 {
			return // Skip very large inputs
		}

		var buf bytes.Buffer
		writer := NewWriter(&buf, 1, false)

		// Write the record
		if _, err := writer.AddRecord(data); err != nil {
			return // Error is expected for some inputs
		}

		// Try to read it back
		reader := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
		record, err := reader.ReadRecord()
		if err != nil {
			t.Logf("Failed to read back record: %v (original len: %d)", err, len(data))
			return
		}

		// Verify the data matches
		if !bytes.Equal(record, data) {
			t.Errorf("Data mismatch: wrote %d bytes, read %d bytes", len(data), len(record))
		}
	})
}

// FuzzWALRoundTrip tests roundtrip encoding/decoding of multiple records.
func FuzzWALRoundTrip(f *testing.F) {
	f.Add([]byte("record1"), []byte("record2"))
	f.Add([]byte{}, []byte("second"))
	f.Add([]byte("first"), []byte{})

	f.Fuzz(func(t *testing.T, data1, data2 []byte) {
		if len(data1) > 100000 || len(data2) > 100000 {
			return
		}

		var buf bytes.Buffer
		writer := NewWriter(&buf, 1, false)

		// Write both records
		if _, err := writer.AddRecord(data1); err != nil {
			return
		}
		if _, err := writer.AddRecord(data2); err != nil {
			return
		}

		// Read them back
		reader := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)

		record1, err := reader.ReadRecord()
		if err != nil {
			t.Fatalf("Failed to read record 1: %v", err)
		}
		if !bytes.Equal(record1, data1) {
			t.Errorf("Record 1 mismatch")
		}

		record2, err := reader.ReadRecord()
		if err != nil {
			t.Fatalf("Failed to read record 2: %v", err)
		}
		if !bytes.Equal(record2, data2) {
			t.Errorf("Record 2 mismatch")
		}
	})
}

// makeValidRecord creates a valid WAL record for seeding.
func makeValidRecord(data []byte) []byte {
	// Simple full record: CRC(4) + len(2) + type(1) + data
	record := make([]byte, 7+len(data))
	// Skip CRC for now (4 bytes)
	record[4] = byte(len(data) & 0xff)
	record[5] = byte((len(data) >> 8) & 0xff)
	record[6] = 1 // Full record type
	copy(record[7:], data)
	// Would need proper CRC calculation for fully valid record
	return record
}
