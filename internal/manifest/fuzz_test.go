package manifest

import (
	"bytes"
	"testing"

	"github.com/aalhour/rockyardkv/internal/wal"
)

// FuzzVersionEditDecode fuzzes the VersionEdit decoder to ensure it doesn't panic.
func FuzzVersionEditDecode(f *testing.F) {
	// Add seed corpus with valid and edge-case inputs
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{1, 4, 'l', 'c', 'm', 'p'}) // Comparator tag = 1
	f.Add([]byte{2, 1})                     // Log number tag = 2
	f.Add([]byte{3, 10})                    // Next file number tag = 3
	f.Add([]byte{4, 100})                   // Last sequence tag = 4

	f.Fuzz(func(t *testing.T, data []byte) {
		edit := &VersionEdit{}

		// Try to decode - shouldn't panic
		_ = edit.DecodeFrom(data)
		// Error is expected for most random data
	})
}

// FuzzVersionEditRoundTrip tests encoding/decoding roundtrip.
func FuzzVersionEditRoundTrip(f *testing.F) {
	f.Add("comparator", uint64(1), uint64(2), uint64(3))
	f.Add("leveldb.BytewiseComparator", uint64(100), uint64(200), uint64(300))
	f.Add("", uint64(0), uint64(0), uint64(0))

	f.Fuzz(func(t *testing.T, comparator string, logNum, nextFile, lastSeq uint64) {
		// Create edit with fuzzed values
		edit := &VersionEdit{
			HasComparator:     len(comparator) > 0,
			Comparator:        comparator,
			HasLogNumber:      true,
			LogNumber:         logNum,
			HasNextFileNumber: true,
			NextFileNumber:    nextFile,
			HasLastSequence:   true,
			LastSequence:      SequenceNumber(lastSeq),
			HasColumnFamily:   true,
			ColumnFamily:      0,
		}

		// Encode
		encoded := edit.EncodeTo()
		if len(encoded) == 0 && (edit.HasComparator || edit.HasLogNumber) {
			t.Logf("Unexpected empty encoding")
		}

		// Decode
		edit2 := &VersionEdit{}
		if err := edit2.DecodeFrom(encoded); err != nil {
			t.Logf("Decode failed: %v (encoded len: %d)", err, len(encoded))
			return
		}

		// Verify fields match
		if edit2.HasComparator != edit.HasComparator {
			t.Errorf("HasComparator mismatch")
		}
		if edit2.Comparator != edit.Comparator {
			t.Errorf("Comparator mismatch: %q vs %q", edit2.Comparator, edit.Comparator)
		}
		if edit2.LogNumber != edit.LogNumber {
			t.Errorf("LogNumber mismatch: %d vs %d", edit2.LogNumber, edit.LogNumber)
		}
		if edit2.NextFileNumber != edit.NextFileNumber {
			t.Errorf("NextFileNumber mismatch")
		}
		if edit2.LastSequence != edit.LastSequence {
			t.Errorf("LastSequence mismatch")
		}
	})
}

// FuzzVersionEditBuilder tests building version edits.
func FuzzVersionEditBuilder(f *testing.F) {
	f.Add(uint32(0), uint64(1))
	f.Add(uint32(1), uint64(100))

	f.Fuzz(func(t *testing.T, cf uint32, fileNum uint64) {
		edit := &VersionEdit{}
		edit.SetColumnFamily(cf)
		edit.SetLogNumber(fileNum)
		edit.SetNextFileNumber(fileNum + 1)
		edit.SetLastSequence(SequenceNumber(fileNum * 10))

		// Encode
		encoded := edit.EncodeTo()

		// Should be non-empty
		if len(encoded) == 0 {
			t.Error("Empty encoding")
			return
		}

		// Verify we can decode it
		edit2 := &VersionEdit{}
		if err := edit2.DecodeFrom(encoded); err != nil {
			t.Errorf("Decode failed: %v", err)
		}
	})
}

// FuzzManifestWALFormat tests that MANIFEST records can be read as WAL records.
func FuzzManifestWALFormat(f *testing.F) {
	f.Add(uint64(1), uint64(10), uint64(100))

	f.Fuzz(func(t *testing.T, logNum, nextFile, lastSeq uint64) {
		// Create a version edit
		edit := &VersionEdit{
			HasLogNumber:      true,
			LogNumber:         logNum,
			HasNextFileNumber: true,
			NextFileNumber:    nextFile,
			HasLastSequence:   true,
			LastSequence:      SequenceNumber(lastSeq),
		}

		// Encode the edit
		editData := edit.EncodeTo()

		// Write to WAL-format buffer
		var buf bytes.Buffer
		walWriter := wal.NewWriter(&buf, 1, false)
		if _, err := walWriter.AddRecord(editData); err != nil {
			t.Fatalf("WAL write failed: %v", err)
		}

		// Read back from WAL format
		walReader := wal.NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
		record, err := walReader.ReadRecord()
		if err != nil {
			t.Fatalf("WAL read failed: %v", err)
		}

		// Decode the version edit
		edit2 := &VersionEdit{}
		if err := edit2.DecodeFrom(record); err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		// Verify values match
		if edit2.LogNumber != edit.LogNumber {
			t.Errorf("LogNumber mismatch: %d vs %d", edit2.LogNumber, edit.LogNumber)
		}
	})
}
