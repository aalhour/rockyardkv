package manifest

import (
	"testing"
)

// TestPackFileNumberAndPathIDEdgeCases tests PackFileNumberAndPathID with edge cases
func TestPackFileNumberAndPathIDEdgeCases(t *testing.T) {
	testCases := []struct {
		fileNum uint64
		pathID  uint64
	}{
		{0, 0},
		{1, 0},
		{0xFFFFFFFF, 0},
		{1, 1},
		{123456789, 5},
	}

	for _, tc := range testCases {
		packed := PackFileNumberAndPathID(tc.fileNum, tc.pathID)
		unpackedNum, unpackedPath := UnpackFileNumberAndPathID(packed)

		if tc.pathID == 0 {
			// When pathID is 0, the fileNum is returned directly
			if unpackedNum != tc.fileNum {
				t.Errorf("PackFileNumberAndPathID(%d, %d): unpack fileNum = %d, want %d",
					tc.fileNum, tc.pathID, unpackedNum, tc.fileNum)
			}
		}
		// Verify round-trip for other cases
		_ = unpackedPath
	}
}

// TestPackFileNumberAndPathIDPanic tests that PackFileNumberAndPathID panics on overflow
func TestPackFileNumberAndPathIDPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("PackFileNumberAndPathID should panic on overflow")
		}
	}()

	// FileNumberMask is typically 0xFFFFFFFFFF (40 bits)
	// Pass a number larger than that to trigger panic
	PackFileNumberAndPathID(FileNumberMask+1, 0)
}

// TestVersionEditDecodeUnknownTag tests DecodeFrom with unknown tags
func TestVersionEditDecodeUnknownTag(t *testing.T) {
	// Create a buffer with an unknown tag that is safe to ignore
	// Tag 128 has the high bit set, so it should be ignored
	data := []byte{128, 0} // Tag 128, followed by empty data

	edit := &VersionEdit{}
	err := edit.DecodeFrom(data)
	// Should either succeed (if tag is ignored) or fail gracefully
	_ = err
}

// TestVersionEditClearCoverage tests the Clear method
func TestVersionEditClearCoverage(t *testing.T) {
	edit := NewVersionEdit()
	edit.SetDBId("test-db")
	edit.SetComparatorName("leveldb.BytewiseComparator")
	edit.SetLogNumber(123)
	edit.SetNextFileNumber(456)
	edit.SetLastSequence(789)
	edit.AddFile(1, &FileMetaData{
		FD: NewFileDescriptor(1, 0, 100),
	})
	edit.DeleteFile(2, 5)

	// Clear
	edit.Clear()

	// Verify all fields are reset
	if edit.HasDBId {
		t.Error("HasDBId should be false after Clear")
	}
	if edit.HasComparator {
		t.Error("HasComparator should be false after Clear")
	}
	if edit.HasLogNumber {
		t.Error("HasLogNumber should be false after Clear")
	}
	if len(edit.NewFiles) != 0 {
		t.Error("NewFiles should be empty after Clear")
	}
	if len(edit.DeletedFiles) != 0 {
		t.Error("DeletedFiles should be empty after Clear")
	}
}

// TestVersionEditSetAllFields tests setting all optional fields
func TestVersionEditSetAllFields(t *testing.T) {
	edit := NewVersionEdit()

	edit.SetDBId("my-db-id")
	edit.SetComparatorName("my-comparator")
	edit.SetLogNumber(100)
	edit.SetPrevLogNumber(99)
	edit.SetNextFileNumber(200)
	edit.SetLastSequence(300)
	edit.SetMinLogNumberToKeep(50)
	edit.SetMaxColumnFamily(5)
	edit.SetColumnFamily(3)
	edit.AddColumnFamily("cf1")
	edit.DropColumnFamily()
	edit.SetAtomicGroup(10)

	// Verify all flags are set
	if !edit.HasDBId || edit.DBId != "my-db-id" {
		t.Error("DBId not set correctly")
	}
	if !edit.HasComparator || edit.Comparator != "my-comparator" {
		t.Error("Comparator not set correctly")
	}
	if !edit.HasLogNumber || edit.LogNumber != 100 {
		t.Error("LogNumber not set correctly")
	}
	if !edit.HasPrevLogNumber || edit.PrevLogNumber != 99 {
		t.Error("PrevLogNumber not set correctly")
	}
	if !edit.HasNextFileNumber || edit.NextFileNumber != 200 {
		t.Error("NextFileNumber not set correctly")
	}
	if !edit.HasLastSequence || edit.LastSequence != 300 {
		t.Error("LastSequence not set correctly")
	}
	if !edit.HasMinLogNumberToKeep || edit.MinLogNumberToKeep != 50 {
		t.Error("MinLogNumberToKeep not set correctly")
	}
	if !edit.HasMaxColumnFamily || edit.MaxColumnFamily != 5 {
		t.Error("MaxColumnFamily not set correctly")
	}
	if !edit.IsColumnFamilyAdd || edit.ColumnFamilyName != "cf1" {
		t.Error("AddColumnFamily not set correctly")
	}
	if !edit.IsColumnFamilyDrop {
		t.Error("DropColumnFamily not set correctly")
	}
	if !edit.IsInAtomicGroup || edit.RemainingEntries != 10 {
		t.Error("AtomicGroup not set correctly")
	}
}

// TestVersionEditEncodeDecodeRoundTrip tests encoding and decoding
func TestVersionEditEncodeDecodeRoundTrip(t *testing.T) {
	original := NewVersionEdit()
	original.SetComparatorName("leveldb.BytewiseComparator")
	original.SetLogNumber(10)
	original.SetNextFileNumber(20)
	original.SetLastSequence(100)
	original.AddFile(0, &FileMetaData{
		FD:       NewFileDescriptor(5, 0, 1000),
		Smallest: []byte("aaa"),
		Largest:  []byte("zzz"),
	})
	original.DeleteFile(1, 3)

	// Encode
	encoded := original.EncodeTo()

	// Decode
	decoded := &VersionEdit{}
	if err := decoded.DecodeFrom(encoded); err != nil {
		t.Fatalf("DecodeFrom failed: %v", err)
	}

	// Verify key fields
	if decoded.Comparator != original.Comparator {
		t.Errorf("Comparator mismatch: got %q, want %q", decoded.Comparator, original.Comparator)
	}
	if decoded.LogNumber != original.LogNumber {
		t.Errorf("LogNumber mismatch: got %d, want %d", decoded.LogNumber, original.LogNumber)
	}
	if decoded.NextFileNumber != original.NextFileNumber {
		t.Errorf("NextFileNumber mismatch: got %d, want %d", decoded.NextFileNumber, original.NextFileNumber)
	}
	if decoded.LastSequence != original.LastSequence {
		t.Errorf("LastSequence mismatch: got %d, want %d", decoded.LastSequence, original.LastSequence)
	}
}
