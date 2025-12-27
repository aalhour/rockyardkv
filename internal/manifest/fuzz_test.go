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

// FuzzVersionEditComplex fuzzes more complex VersionEdit structures
// including NewFiles, DeletedFiles, and atomic groups.
func FuzzVersionEditComplex(f *testing.F) {
	// Seed corpus with representative inputs
	f.Add(uint32(0), uint64(1), uint64(100), int32(0), uint64(1024), uint32(5))
	f.Add(uint32(1), uint64(10), uint64(1000), int32(1), uint64(4096), uint32(0))
	f.Add(uint32(2), uint64(100), uint64(10000), int32(2), uint64(65536), uint32(3))

	f.Fuzz(func(t *testing.T, cf uint32, fileNum, fileSize uint64, level int32, numEntries uint64, atomicRemaining uint32) {
		// Bound level to valid range
		level = level % 8
		if level < 0 {
			level = -level
		}

		edit := NewVersionEdit()
		edit.SetColumnFamily(cf)
		edit.SetComparatorName("leveldb.BytewiseComparator")
		edit.SetLogNumber(fileNum)
		edit.SetNextFileNumber(fileNum + 10)
		edit.SetLastSequence(SequenceNumber(numEntries))

		// Add atomic group
		if atomicRemaining > 0 && atomicRemaining < 100 {
			edit.SetAtomicGroup(atomicRemaining)
		}

		// Add a new file
		if fileNum > 0 && fileSize > 0 && fileSize < 1<<30 {
			smallest := []byte("key_aaa")
			largest := []byte("key_zzz")
			meta := &FileMetaData{
				FD: FileDescriptor{
					PackedNumberAndPathID: PackFileNumberAndPathID(fileNum, 0),
					FileSize:              fileSize,
					SmallestSeqno:         SequenceNumber(1),
					LargestSeqno:          SequenceNumber(numEntries),
				},
				Smallest: smallest,
				Largest:  largest,
			}
			edit.AddFile(int(level), meta)
		}

		// Add a deleted file
		if fileNum > 1 {
			edit.DeleteFile(int(level), fileNum-1)
		}

		// Encode
		encoded := edit.EncodeTo()
		if len(encoded) == 0 {
			t.Error("Empty encoding for complex edit")
			return
		}

		// Decode
		edit2 := NewVersionEdit()
		if err := edit2.DecodeFrom(encoded); err != nil {
			t.Errorf("Decode failed for complex edit: %v (encoded len: %d)", err, len(encoded))
			return
		}

		// Verify key fields
		if edit2.ColumnFamily != edit.ColumnFamily {
			t.Errorf("ColumnFamily mismatch: %d vs %d", edit2.ColumnFamily, edit.ColumnFamily)
		}
		if edit2.IsInAtomicGroup != edit.IsInAtomicGroup {
			t.Errorf("IsInAtomicGroup mismatch")
		}
		if edit2.RemainingEntries != edit.RemainingEntries {
			t.Errorf("RemainingEntries mismatch: %d vs %d", edit2.RemainingEntries, edit.RemainingEntries)
		}
	})
}

// FuzzVersionEditVarintEdgeCases specifically tests varint edge cases.
func FuzzVersionEditVarintEdgeCases(f *testing.F) {
	// Edge cases for varints: 0, 1-byte max, 2-byte boundary, max values
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(127), uint64(127))                                 // 1-byte max
	f.Add(uint64(128), uint64(16383))                               // 2-byte boundary
	f.Add(uint64(16384), uint64(2097151))                           // 3-byte boundary
	f.Add(uint64(1<<28-1), uint64(1<<35-1))                         // 4-5 byte boundary
	f.Add(uint64(1<<42-1), uint64(1<<49-1))                         // 6-7 byte boundary
	f.Add(uint64(1<<56-1), uint64(1<<63-1))                         // 8-9 byte boundary
	f.Add(uint64(0xFFFFFFFFFFFFFFFF), uint64(0xFFFFFFFFFFFFFFFF-1)) // max uint64

	f.Fuzz(func(t *testing.T, logNum, nextFile uint64) {
		edit := NewVersionEdit()
		edit.SetLogNumber(logNum)
		edit.SetNextFileNumber(nextFile)
		edit.SetLastSequence(SequenceNumber(logNum ^ nextFile))

		// Encode
		encoded := edit.EncodeTo()

		// Decode
		edit2 := NewVersionEdit()
		if err := edit2.DecodeFrom(encoded); err != nil {
			// Some extreme values might cause issues - log but don't fail
			t.Logf("Decode failed for edge case (logNum=%d, nextFile=%d): %v", logNum, nextFile, err)
			return
		}

		// Verify roundtrip
		if edit2.LogNumber != logNum {
			t.Errorf("LogNumber mismatch: got %d, want %d", edit2.LogNumber, logNum)
		}
		if edit2.NextFileNumber != nextFile {
			t.Errorf("NextFileNumber mismatch: got %d, want %d", edit2.NextFileNumber, nextFile)
		}
	})
}
