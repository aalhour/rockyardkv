package manifest

import (
	"bytes"
	"errors"
	"testing"
)

// -----------------------------------------------------------------------------
// Tag tests
// -----------------------------------------------------------------------------

func TestTagIsSafeToIgnore(t *testing.T) {
	safeToIgnore := []Tag{
		TagDBID,
		TagWalAddition,
		TagWalDeletion,
		TagFullHistoryTSLow,
	}
	for _, tag := range safeToIgnore {
		if !tag.IsSafeToIgnore() {
			t.Errorf("Tag %d should be safe to ignore", tag)
		}
	}

	notSafeToIgnore := []Tag{
		TagComparator,
		TagLogNumber,
		TagNextFileNumber,
		TagLastSequence,
		TagDeletedFile,
		TagNewFile4,
		TagColumnFamily,
	}
	for _, tag := range notSafeToIgnore {
		if tag.IsSafeToIgnore() {
			t.Errorf("Tag %d should NOT be safe to ignore", tag)
		}
	}
}

func TestNewFileCustomTagIsSafeToIgnore(t *testing.T) {
	safeToIgnore := []NewFileCustomTag{
		NewFileTagTerminate,
		NewFileTagNeedCompaction,
		NewFileTagOldestBlobFileNumber,
		NewFileTagOldestAncestorTime,
		NewFileTagFileCreationTime,
		NewFileTagEpochNumber,
	}
	for _, tag := range safeToIgnore {
		if !tag.IsSafeToIgnore() {
			t.Errorf("NewFileCustomTag %d should be safe to ignore", tag)
		}
	}

	notSafeToIgnore := []NewFileCustomTag{
		NewFileTagPathID,
	}
	for _, tag := range notSafeToIgnore {
		if tag.IsSafeToIgnore() {
			t.Errorf("NewFileCustomTag %d should NOT be safe to ignore", tag)
		}
	}
}

// -----------------------------------------------------------------------------
// FileDescriptor tests
// -----------------------------------------------------------------------------

func TestFileDescriptor(t *testing.T) {
	fd := NewFileDescriptor(12345, 3, 67890)

	if fd.GetNumber() != 12345 {
		t.Errorf("GetNumber() = %d, want 12345", fd.GetNumber())
	}
	if fd.GetPathID() != 3 {
		t.Errorf("GetPathID() = %d, want 3", fd.GetPathID())
	}
	if fd.FileSize != 67890 {
		t.Errorf("FileSize = %d, want 67890", fd.FileSize)
	}
	if fd.SmallestSeqno != MaxSequenceNumber {
		t.Errorf("SmallestSeqno = %d, want MaxSequenceNumber", fd.SmallestSeqno)
	}
	if fd.LargestSeqno != 0 {
		t.Errorf("LargestSeqno = %d, want 0", fd.LargestSeqno)
	}
}

func TestPackUnpackFileNumberAndPathID(t *testing.T) {
	// Note: pathID can only be 0-3 (2 bits) since FileNumberMask uses 62 bits
	tests := []struct {
		number uint64
		pathID uint64
	}{
		{0, 0},
		{1, 0},
		{12345, 0},
		{12345, 1},
		{12345, 2},
		{12345, 3}, // Max valid pathID
		{FileNumberMask, 0},
		{FileNumberMask, 3},
	}

	for _, tt := range tests {
		packed := PackFileNumberAndPathID(tt.number, tt.pathID)
		gotNumber, gotPathID := UnpackFileNumberAndPathID(packed)

		if gotNumber != tt.number {
			t.Errorf("UnpackFileNumberAndPathID(%x): number = %d, want %d", packed, gotNumber, tt.number)
		}
		if uint64(gotPathID) != tt.pathID {
			t.Errorf("UnpackFileNumberAndPathID(%x): pathID = %d, want %d", packed, gotPathID, tt.pathID)
		}
	}
}

// -----------------------------------------------------------------------------
// VersionEdit encoding/decoding tests
// -----------------------------------------------------------------------------

func TestVersionEditEmpty(t *testing.T) {
	ve := NewVersionEdit()
	encoded := ve.EncodeTo()

	// Empty version edit should encode to empty slice
	if len(encoded) != 0 {
		t.Errorf("Empty VersionEdit encoded to %d bytes, want 0", len(encoded))
	}

	// Decode should work
	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}
}

func TestVersionEditDBId(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetDBId("test-db-id-12345")

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasDBId {
		t.Error("HasDBId should be true")
	}
	if ve2.DBId != "test-db-id-12345" {
		t.Errorf("DBId = %q, want %q", ve2.DBId, "test-db-id-12345")
	}
}

func TestVersionEditComparator(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetComparatorName("leveldb.BytewiseComparator")

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasComparator {
		t.Error("HasComparator should be true")
	}
	if ve2.Comparator != "leveldb.BytewiseComparator" {
		t.Errorf("Comparator = %q, want %q", ve2.Comparator, "leveldb.BytewiseComparator")
	}
}

func TestVersionEditLogNumbers(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetLogNumber(100)
	ve.SetPrevLogNumber(99)
	ve.SetMinLogNumberToKeep(50)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasLogNumber || ve2.LogNumber != 100 {
		t.Errorf("LogNumber: has=%v, val=%d", ve2.HasLogNumber, ve2.LogNumber)
	}
	if !ve2.HasPrevLogNumber || ve2.PrevLogNumber != 99 {
		t.Errorf("PrevLogNumber: has=%v, val=%d", ve2.HasPrevLogNumber, ve2.PrevLogNumber)
	}
	if !ve2.HasMinLogNumberToKeep || ve2.MinLogNumberToKeep != 50 {
		t.Errorf("MinLogNumberToKeep: has=%v, val=%d", ve2.HasMinLogNumberToKeep, ve2.MinLogNumberToKeep)
	}
}

func TestVersionEditNextFileAndSequence(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetNextFileNumber(1000)
	ve.SetLastSequence(999)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasNextFileNumber || ve2.NextFileNumber != 1000 {
		t.Errorf("NextFileNumber: has=%v, val=%d", ve2.HasNextFileNumber, ve2.NextFileNumber)
	}
	if !ve2.HasLastSequence || ve2.LastSequence != 999 {
		t.Errorf("LastSequence: has=%v, val=%d", ve2.HasLastSequence, ve2.LastSequence)
	}
}

func TestVersionEditDeletedFiles(t *testing.T) {
	ve := NewVersionEdit()
	ve.DeleteFile(0, 10)
	ve.DeleteFile(1, 20)
	ve.DeleteFile(2, 30)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.DeletedFiles) != 3 {
		t.Fatalf("DeletedFiles count = %d, want 3", len(ve2.DeletedFiles))
	}

	expected := []DeletedFileEntry{
		{Level: 0, FileNumber: 10},
		{Level: 1, FileNumber: 20},
		{Level: 2, FileNumber: 30},
	}
	for i, df := range ve2.DeletedFiles {
		if df != expected[i] {
			t.Errorf("DeletedFiles[%d] = %+v, want %+v", i, df, expected[i])
		}
	}
}

func TestVersionEditNewFile(t *testing.T) {
	ve := NewVersionEdit()

	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(100, 0, 5000)
	meta.FD.SmallestSeqno = 10
	meta.FD.LargestSeqno = 50
	meta.Smallest = []byte("aaa")
	meta.Largest = []byte("zzz")
	meta.OldestAncestorTime = 123456789
	meta.FileCreationTime = 987654321
	meta.EpochNumber = 5
	meta.Temperature = TemperatureWarm
	meta.MarkedForCompaction = true

	ve.AddFile(2, meta)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("NewFiles count = %d, want 1", len(ve2.NewFiles))
	}

	nf := ve2.NewFiles[0]
	if nf.Level != 2 {
		t.Errorf("Level = %d, want 2", nf.Level)
	}

	m := nf.Meta
	if m.FD.GetNumber() != 100 {
		t.Errorf("FileNumber = %d, want 100", m.FD.GetNumber())
	}
	if m.FD.FileSize != 5000 {
		t.Errorf("FileSize = %d, want 5000", m.FD.FileSize)
	}
	if m.FD.SmallestSeqno != 10 {
		t.Errorf("SmallestSeqno = %d, want 10", m.FD.SmallestSeqno)
	}
	if m.FD.LargestSeqno != 50 {
		t.Errorf("LargestSeqno = %d, want 50", m.FD.LargestSeqno)
	}
	if !bytes.Equal(m.Smallest, []byte("aaa")) {
		t.Errorf("Smallest = %q, want %q", m.Smallest, "aaa")
	}
	if !bytes.Equal(m.Largest, []byte("zzz")) {
		t.Errorf("Largest = %q, want %q", m.Largest, "zzz")
	}
	if m.OldestAncestorTime != 123456789 {
		t.Errorf("OldestAncestorTime = %d, want 123456789", m.OldestAncestorTime)
	}
	if m.FileCreationTime != 987654321 {
		t.Errorf("FileCreationTime = %d, want 987654321", m.FileCreationTime)
	}
	if m.EpochNumber != 5 {
		t.Errorf("EpochNumber = %d, want 5", m.EpochNumber)
	}
	if m.Temperature != TemperatureWarm {
		t.Errorf("Temperature = %d, want %d", m.Temperature, TemperatureWarm)
	}
	if !m.MarkedForCompaction {
		t.Error("MarkedForCompaction should be true")
	}
}

func TestVersionEditColumnFamily(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetColumnFamily(5)
	ve.SetMaxColumnFamily(10)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasColumnFamily || ve2.ColumnFamily != 5 {
		t.Errorf("ColumnFamily: has=%v, val=%d", ve2.HasColumnFamily, ve2.ColumnFamily)
	}
	if !ve2.HasMaxColumnFamily || ve2.MaxColumnFamily != 10 {
		t.Errorf("MaxColumnFamily: has=%v, val=%d", ve2.HasMaxColumnFamily, ve2.MaxColumnFamily)
	}
}

func TestVersionEditColumnFamilyAdd(t *testing.T) {
	ve := NewVersionEdit()
	ve.AddColumnFamily("my_cf")

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.IsColumnFamilyAdd {
		t.Error("IsColumnFamilyAdd should be true")
	}
	if ve2.ColumnFamilyName != "my_cf" {
		t.Errorf("ColumnFamilyName = %q, want %q", ve2.ColumnFamilyName, "my_cf")
	}
}

func TestVersionEditColumnFamilyDrop(t *testing.T) {
	ve := NewVersionEdit()
	ve.DropColumnFamily()

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.IsColumnFamilyDrop {
		t.Error("IsColumnFamilyDrop should be true")
	}
}

func TestVersionEditAtomicGroup(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetAtomicGroup(3)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.IsInAtomicGroup {
		t.Error("IsInAtomicGroup should be true")
	}
	if ve2.RemainingEntries != 3 {
		t.Errorf("RemainingEntries = %d, want 3", ve2.RemainingEntries)
	}
}

func TestVersionEditCompactCursor(t *testing.T) {
	ve := NewVersionEdit()
	ve.CompactCursors = append(ve.CompactCursors, struct {
		Level int
		Key   []byte
	}{Level: 1, Key: []byte("cursor_key")})

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.CompactCursors) != 1 {
		t.Fatalf("CompactCursors count = %d, want 1", len(ve2.CompactCursors))
	}

	cc := ve2.CompactCursors[0]
	if cc.Level != 1 {
		t.Errorf("Level = %d, want 1", cc.Level)
	}
	if !bytes.Equal(cc.Key, []byte("cursor_key")) {
		t.Errorf("Key = %q, want %q", cc.Key, "cursor_key")
	}
}

func TestVersionEditFullHistoryTSLow(t *testing.T) {
	ve := NewVersionEdit()
	ve.FullHistoryTSLow = []byte{0x12, 0x34, 0x56, 0x78}
	ve.HasFullHistoryTSLow = true

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasFullHistoryTSLow {
		t.Error("HasFullHistoryTSLow should be true")
	}
	if !bytes.Equal(ve2.FullHistoryTSLow, []byte{0x12, 0x34, 0x56, 0x78}) {
		t.Errorf("FullHistoryTSLow = %x, want 12345678", ve2.FullHistoryTSLow)
	}
}

func TestVersionEditComplex(t *testing.T) {
	// Test a complex version edit with multiple fields
	ve := NewVersionEdit()
	ve.SetDBId("complex-db")
	ve.SetComparatorName("bytewise")
	ve.SetLogNumber(100)
	ve.SetNextFileNumber(200)
	ve.SetLastSequence(50)
	ve.SetColumnFamily(2)

	ve.DeleteFile(0, 10)
	ve.DeleteFile(1, 20)

	meta1 := NewFileMetaData()
	meta1.FD = NewFileDescriptor(30, 0, 1000)
	meta1.FD.SmallestSeqno = 1
	meta1.FD.LargestSeqno = 10
	meta1.Smallest = []byte("a")
	meta1.Largest = []byte("m")
	meta1.EpochNumber = 1
	ve.AddFile(0, meta1)

	meta2 := NewFileMetaData()
	meta2.FD = NewFileDescriptor(31, 0, 2000)
	meta2.FD.SmallestSeqno = 11
	meta2.FD.LargestSeqno = 20
	meta2.Smallest = []byte("n")
	meta2.Largest = []byte("z")
	meta2.EpochNumber = 2
	ve.AddFile(1, meta2)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	// Verify all fields
	if ve2.DBId != "complex-db" {
		t.Errorf("DBId = %q", ve2.DBId)
	}
	if ve2.Comparator != "bytewise" {
		t.Errorf("Comparator = %q", ve2.Comparator)
	}
	if ve2.LogNumber != 100 {
		t.Errorf("LogNumber = %d", ve2.LogNumber)
	}
	if ve2.NextFileNumber != 200 {
		t.Errorf("NextFileNumber = %d", ve2.NextFileNumber)
	}
	if ve2.LastSequence != 50 {
		t.Errorf("LastSequence = %d", ve2.LastSequence)
	}
	if ve2.ColumnFamily != 2 {
		t.Errorf("ColumnFamily = %d", ve2.ColumnFamily)
	}
	if len(ve2.DeletedFiles) != 2 {
		t.Errorf("DeletedFiles count = %d", len(ve2.DeletedFiles))
	}
	if len(ve2.NewFiles) != 2 {
		t.Errorf("NewFiles count = %d", len(ve2.NewFiles))
	}
}

func TestVersionEditClear(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetDBId("test")
	ve.SetLogNumber(100)
	ve.DeleteFile(0, 10)

	ve.Clear()

	if ve.HasDBId || ve.HasLogNumber || len(ve.DeletedFiles) != 0 {
		t.Error("Clear() did not reset all fields")
	}
}

// -----------------------------------------------------------------------------
// Edge case tests
// -----------------------------------------------------------------------------

func TestVersionEditNewFileWithPathId(t *testing.T) {
	ve := NewVersionEdit()

	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(100, 3, 1000) // path ID = 3 (max valid)
	meta.FD.SmallestSeqno = 1
	meta.FD.LargestSeqno = 10
	meta.Smallest = []byte("a")
	meta.Largest = []byte("z")
	meta.EpochNumber = 1

	ve.AddFile(0, meta)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("NewFiles count = %d, want 1", len(ve2.NewFiles))
	}

	m := ve2.NewFiles[0].Meta
	if m.FD.GetPathID() != 3 {
		t.Errorf("PathId = %d, want 3", m.FD.GetPathID())
	}
}

func TestVersionEditNewFileWithChecksum(t *testing.T) {
	ve := NewVersionEdit()

	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(100, 0, 1000)
	meta.FD.SmallestSeqno = 1
	meta.FD.LargestSeqno = 10
	meta.Smallest = []byte("a")
	meta.Largest = []byte("z")
	meta.EpochNumber = 1
	meta.FileChecksum = "abc123"
	meta.FileChecksumFuncName = "crc32c"

	ve.AddFile(0, meta)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("NewFiles count = %d, want 1", len(ve2.NewFiles))
	}

	m := ve2.NewFiles[0].Meta
	if m.FileChecksum != "abc123" {
		t.Errorf("FileChecksum = %q, want %q", m.FileChecksum, "abc123")
	}
	if m.FileChecksumFuncName != "crc32c" {
		t.Errorf("FileChecksumFuncName = %q, want %q", m.FileChecksumFuncName, "crc32c")
	}
}

func TestVersionEditDecodeError(t *testing.T) {
	// Test with truncated input
	ve := NewVersionEdit()
	err := ve.DecodeFrom([]byte{0x01}) // Just a tag, no value
	if !errors.Is(err, ErrUnexpectedEndOfInput) {
		t.Errorf("Expected ErrUnexpectedEndOfInput, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Additional tests for test parity
// -----------------------------------------------------------------------------

func TestVersionEditEncodeDecodeConsistency(t *testing.T) {
	// Encode and decode multiple times - should be idempotent
	ve := NewVersionEdit()
	ve.SetDBId("test-db")
	ve.SetLogNumber(100)

	encoded1 := ve.EncodeTo()

	ve2 := NewVersionEdit()
	ve2.DecodeFrom(encoded1)

	encoded2 := ve2.EncodeTo()

	if !bytes.Equal(encoded1, encoded2) {
		t.Error("Double encode-decode is not idempotent")
	}
}

func TestVersionEditMultipleFiles(t *testing.T) {
	ve := NewVersionEdit()

	// Add many files to different levels
	for level := range 7 {
		for i := range 10 {
			meta := NewFileMetaData()
			meta.FD = NewFileDescriptor(uint64(level*100+i), 0, uint64(1000+i))
			meta.FD.SmallestSeqno = SequenceNumber(i)
			meta.FD.LargestSeqno = SequenceNumber(i + 10)
			meta.Smallest = []byte{byte('a' + i)}
			meta.Largest = []byte{byte('z' - i)}
			meta.EpochNumber = uint64(level + 1)
			ve.AddFile(level, meta)
		}
	}

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 70 {
		t.Errorf("NewFiles count = %d, want 70", len(ve2.NewFiles))
	}
}

func TestVersionEditDeletedFilesVarious(t *testing.T) {
	ve := NewVersionEdit()

	// Delete files from various levels
	for level := range 7 {
		for i := range 5 {
			ve.DeleteFile(level, uint64(level*100+i))
		}
	}

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.DeletedFiles) != 35 {
		t.Errorf("DeletedFiles count = %d, want 35", len(ve2.DeletedFiles))
	}
}

func TestVersionEditEmptyStrings(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetDBId("")
	ve.SetComparatorName("")
	ve.AddColumnFamily("")

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if !ve2.HasDBId || ve2.DBId != "" {
		t.Errorf("DBId: has=%v, val=%q", ve2.HasDBId, ve2.DBId)
	}
	if !ve2.HasComparator || ve2.Comparator != "" {
		t.Errorf("Comparator: has=%v, val=%q", ve2.HasComparator, ve2.Comparator)
	}
}

func TestVersionEditLargeSequenceNumbers(t *testing.T) {
	ve := NewVersionEdit()
	ve.SetLastSequence(MaxSequenceNumber)
	ve.SetLogNumber(uint64(MaxSequenceNumber) - 1)
	ve.SetNextFileNumber(uint64(MaxSequenceNumber) - 2)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if ve2.LastSequence != MaxSequenceNumber {
		t.Errorf("LastSequence = %d, want %d", ve2.LastSequence, MaxSequenceNumber)
	}
}

func TestVersionEditNewFileMinimalMetadata(t *testing.T) {
	ve := NewVersionEdit()

	// Minimal file metadata
	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(1, 0, 100)
	meta.FD.SmallestSeqno = 0
	meta.FD.LargestSeqno = 0
	meta.Smallest = []byte{}
	meta.Largest = []byte{}

	ve.AddFile(0, meta)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("NewFiles count = %d, want 1", len(ve2.NewFiles))
	}

	m := ve2.NewFiles[0].Meta
	if m.FD.GetNumber() != 1 {
		t.Errorf("FileNumber = %d, want 1", m.FD.GetNumber())
	}
}

func TestVersionEditNewFileAllCustomTags(t *testing.T) {
	ve := NewVersionEdit()

	meta := NewFileMetaData()
	meta.FD = NewFileDescriptor(100, 1, 5000)
	meta.FD.SmallestSeqno = 10
	meta.FD.LargestSeqno = 50
	meta.Smallest = []byte("aaa")
	meta.Largest = []byte("zzz")
	meta.OldestAncestorTime = 123456789
	meta.FileCreationTime = 987654321
	meta.EpochNumber = 5
	meta.Temperature = TemperatureHot
	meta.MarkedForCompaction = true
	meta.FileChecksum = "checksum123"
	meta.FileChecksumFuncName = "xxhash64"
	meta.OldestBlobFileNumber = 42
	meta.CompensatedRangeDeletionSize = 1000
	meta.TailSize = 500

	ve.AddFile(2, meta)

	encoded := ve.EncodeTo()

	ve2 := NewVersionEdit()
	err := ve2.DecodeFrom(encoded)
	if err != nil {
		t.Fatalf("DecodeFrom error: %v", err)
	}

	if len(ve2.NewFiles) != 1 {
		t.Fatalf("NewFiles count = %d, want 1", len(ve2.NewFiles))
	}

	m := ve2.NewFiles[0].Meta
	if m.OldestBlobFileNumber != 42 {
		t.Errorf("OldestBlobFileNumber = %d, want 42", m.OldestBlobFileNumber)
	}
	if m.CompensatedRangeDeletionSize != 1000 {
		t.Errorf("CompensatedRangeDeletionSize = %d", m.CompensatedRangeDeletionSize)
	}
	if m.TailSize != 500 {
		t.Errorf("TailSize = %d", m.TailSize)
	}
}

func TestVersionEditTagConstants(t *testing.T) {
	// Verify tag values match RocksDB exactly (critical for format compatibility)
	tests := []struct {
		tag  Tag
		want uint32
	}{
		{TagComparator, 1},
		{TagLogNumber, 2},
		{TagNextFileNumber, 3},
		{TagLastSequence, 4},
		{TagCompactCursor, 5},
		{TagDeletedFile, 6},
		{TagNewFile, 7},
		{TagPrevLogNumber, 9},
		{TagMinLogNumberToKeep, 10},
		{TagNewFile2, 100},
		{TagNewFile3, 102},
		{TagNewFile4, 103},
		{TagColumnFamily, 200},
		{TagColumnFamilyAdd, 201},
		{TagColumnFamilyDrop, 202},
		{TagMaxColumnFamily, 203},
		{TagInAtomicGroup, 300},
	}

	for _, tt := range tests {
		if uint32(tt.tag) != tt.want {
			t.Errorf("Tag constant %d has value %d, want %d", tt.tag, uint32(tt.tag), tt.want)
		}
	}
}

func TestNewFileCustomTagConstants(t *testing.T) {
	// Verify custom tag values match RocksDB exactly
	tests := []struct {
		tag  NewFileCustomTag
		want uint32
	}{
		{NewFileTagTerminate, 1},
		{NewFileTagNeedCompaction, 2},
		{NewFileTagMinLogNumberToKeepHack, 3},
		{NewFileTagOldestBlobFileNumber, 4},
		{NewFileTagOldestAncestorTime, 5},
		{NewFileTagFileCreationTime, 6},
		{NewFileTagFileChecksum, 7},
		{NewFileTagFileChecksumFuncName, 8},
		{NewFileTagTemperature, 9},
		{NewFileTagMinTimestamp, 10},
		{NewFileTagMaxTimestamp, 11},
		{NewFileTagUniqueID, 12},
		{NewFileTagEpochNumber, 13},
		{NewFileTagCompensatedRangeDeletionSize, 14},
		{NewFileTagTailSize, 15},
		{NewFileTagUserDefinedTimestampsPersisted, 16},
	}

	for _, tt := range tests {
		if uint32(tt.tag) != tt.want {
			t.Errorf("NewFileCustomTag constant %d has value %d, want %d", tt.tag, uint32(tt.tag), tt.want)
		}
	}
}

func TestTemperatureConstants(t *testing.T) {
	// Verify temperature values match RocksDB
	tests := []struct {
		temp Temperature
		want uint8
	}{
		{TemperatureUnknown, 0},
		{TemperatureHot, 1},
		{TemperatureWarm, 2},
		{TemperatureCold, 3},
	}

	for _, tt := range tests {
		if uint8(tt.temp) != tt.want {
			t.Errorf("Temperature constant %d has value %d, want %d", tt.temp, uint8(tt.temp), tt.want)
		}
	}
}

// Fuzz test for VersionEdit
func FuzzVersionEditRoundtrip(f *testing.F) {
	// Add some seed data
	ve := NewVersionEdit()
	ve.SetLogNumber(100)
	f.Add(ve.EncodeTo())

	f.Fuzz(func(t *testing.T, data []byte) {
		ve := NewVersionEdit()
		err := ve.DecodeFrom(data)
		if err != nil {
			return // Invalid input is ok
		}

		// Re-encode should not panic
		encoded := ve.EncodeTo()

		// Re-decode should succeed
		ve2 := NewVersionEdit()
		err = ve2.DecodeFrom(encoded)
		if err != nil {
			t.Errorf("Re-decode failed: %v", err)
		}
	})
}
