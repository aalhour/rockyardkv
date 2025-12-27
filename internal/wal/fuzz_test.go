package wal

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/encoding"
)

// FuzzWALReader fuzzes the WAL reader with arbitrary data to ensure it doesn't panic.
// Includes fragmented record edge cases matching C++ log_test.cc patterns.
func FuzzWALReader(f *testing.F) {
	// Basic seed corpus
	f.Add([]byte{}) // Empty
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}) // Type 1 (full record)
	f.Add([]byte{0xc3, 0xd2, 0xe1, 0xf0, 0x05, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'})
	// Valid record header format: CRC(4) + Length(2) + Type(1) + Data
	f.Add(makeValidRecord(FullType, []byte("test data")))
	f.Add(makeValidRecord(FullType, []byte{})) // Empty record

	// ==========================================================================
	// Fragmented record edge cases (matching C++ log_test.cc patterns)
	// ==========================================================================

	// Case 1: Middle record without First (should report corruption)
	f.Add(makeValidRecord(MiddleType, []byte("orphan middle")))

	// Case 2: Last record without First (should report corruption)
	f.Add(makeValidRecord(LastType, []byte("orphan last")))

	// Case 3: First -> Middle -> EOF (missing Last)
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte("start")),
		makeValidRecord(MiddleType, []byte("middle")),
	))

	// Case 4: First -> Full (unexpected Full while in fragment)
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte("start")),
		makeValidRecord(FullType, []byte("complete")),
	))

	// Case 5: First -> First (nested First records)
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte("first1")),
		makeValidRecord(FirstType, []byte("first2")),
		makeValidRecord(LastType, []byte("end")),
	))

	// Case 6: Zero length fragment sequence
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte{}),
		makeValidRecord(MiddleType, []byte{}),
		makeValidRecord(LastType, []byte{}),
	))

	// Case 7: Multiple Middle records before Last
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte("A")),
		makeValidRecord(MiddleType, []byte("B")),
		makeValidRecord(MiddleType, []byte("C")),
		makeValidRecord(MiddleType, []byte("D")),
		makeValidRecord(LastType, []byte("E")),
	))

	// Case 8: Recyclable variants of edge cases
	f.Add(makeValidRecord(RecyclableMiddleType, []byte("recyclable orphan")))
	f.Add(appendRecords(
		makeValidRecord(RecyclableFirstType, []byte("recyclable start")),
		makeValidRecord(RecyclableFullType, []byte("recyclable interrupt")),
	))

	// Case 9: Mixed legacy and recyclable (should handle gracefully)
	f.Add(appendRecords(
		makeValidRecord(FirstType, []byte("legacy")),
		makeValidRecord(RecyclableLastType, []byte("recyclable")),
	))

	// Case 10: Corrupted header mid-sequence (invalid length)
	corruptedSeq := appendRecords(
		makeValidRecord(FirstType, []byte("start")),
	)
	// Append garbage that looks like a header but has invalid length
	corruptedSeq = append(corruptedSeq, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x03)
	f.Add(corruptedSeq)

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := NewReader(bytes.NewReader(data), nil, false, 0)

		// Try to read records - shouldn't panic or hang
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

// FuzzWALReaderFragmented specifically fuzzes fragmented record sequences.
// This is a targeted fuzzer for First/Middle/Last state machine logic.
func FuzzWALReaderFragmented(f *testing.F) {
	// Seed with valid fragmented sequences
	f.Add([]byte("part1"), []byte("part2"), []byte("part3"))
	f.Add([]byte(""), []byte("middle"), []byte(""))
	f.Add([]byte("a"), []byte("b"), []byte("c"))

	f.Fuzz(func(t *testing.T, first, middle, last []byte) {
		// Limit sizes to avoid OOM
		if len(first) > 10000 || len(middle) > 10000 || len(last) > 10000 {
			return
		}

		// Build a fragmented record sequence
		data := appendRecords(
			makeValidRecord(FirstType, first),
			makeValidRecord(MiddleType, middle),
			makeValidRecord(LastType, last),
		)

		reader := NewReader(bytes.NewReader(data), nil, true, 0)
		record, err := reader.ReadRecord()

		// Should successfully read the assembled record
		if err != nil {
			t.Fatalf("Failed to read fragmented record: %v", err)
		}

		expected := append(append(first, middle...), last...)
		if !bytes.Equal(record, expected) {
			t.Errorf("Content mismatch: got %d bytes, want %d bytes", len(record), len(expected))
		}
	})
}

// FuzzWALReaderMalformedFragments fuzzes with intentionally malformed fragment sequences.
func FuzzWALReaderMalformedFragments(f *testing.F) {
	// Seed with fragment type sequences
	// Encoding: 1=Full, 2=First, 3=Middle, 4=Last
	f.Add([]byte{2, 3, 4})       // Valid: First, Middle, Last
	f.Add([]byte{3, 4})          // Invalid: Middle, Last (missing First)
	f.Add([]byte{2, 1, 4})       // Invalid: First, Full, Last
	f.Add([]byte{2, 2, 3, 4})    // Invalid: First, First, Middle, Last
	f.Add([]byte{4})             // Invalid: Last alone
	f.Add([]byte{3})             // Invalid: Middle alone
	f.Add([]byte{2, 3})          // Invalid: First, Middle (missing Last)
	f.Add([]byte{1, 1, 1})       // Valid: Full, Full, Full
	f.Add([]byte{2, 3, 3, 3, 4}) // Valid: First, Middle*3, Last

	f.Fuzz(func(t *testing.T, typeSeq []byte) {
		if len(typeSeq) == 0 || len(typeSeq) > 20 {
			return
		}

		// Build records with the given type sequence
		var records [][]byte
		for i, typeCode := range typeSeq {
			if typeCode < 1 || typeCode > 4 {
				// Map to valid range
				typeCode = (typeCode % 4) + 1
			}
			rt := RecordType(typeCode)
			records = append(records, makeValidRecord(rt, []byte{byte(i)}))
		}

		data := appendRecords(records...)
		reader := NewReader(bytes.NewReader(data), nil, false, 0)

		// Should not panic or hang
		for range 100 {
			_, err := reader.ReadRecord()
			if err != nil {
				break
			}
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

// makeValidRecord creates a valid WAL record with correct CRC for the given record type.
func makeValidRecord(rt RecordType, data []byte) []byte {
	headerSize := HeaderSize
	if IsRecyclableType(rt) {
		headerSize = RecyclableHeaderSize
	}

	record := make([]byte, headerSize+len(data))

	// Length (2 bytes, little-endian)
	record[4] = byte(len(data) & 0xFF)
	record[5] = byte((len(data) >> 8) & 0xFF)

	// Type (1 byte)
	record[6] = byte(rt)

	// Log number for recyclable types (4 bytes)
	if IsRecyclableType(rt) {
		encoding.EncodeFixed32(record[7:11], 0) // logNumber = 0 for tests
	}

	// Payload
	copy(record[headerSize:], data)

	// Compute and write CRC
	crc := checksum.Value([]byte{byte(rt)})
	if IsRecyclableType(rt) {
		crc = checksum.Extend(crc, record[7:11]) // log number
	}
	crc = checksum.Extend(crc, data)
	crc = checksum.Mask(crc)
	encoding.EncodeFixed32(record[0:4], crc)

	return record
}

// appendRecords concatenates multiple record byte slices.
func appendRecords(records ...[]byte) []byte {
	var result []byte
	for _, r := range records {
		result = append(result, r...)
	}
	return result
}
