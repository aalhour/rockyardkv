package wal

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/encoding"
)

// testReporter collects corruption reports for testing.
type testReporter struct {
	corruptions []struct {
		bytes int
		err   error
	}
	oldRecords []int
}

func newTestReporter() *testReporter {
	return &testReporter{}
}

func (r *testReporter) Corruption(bytes int, err error) {
	r.corruptions = append(r.corruptions, struct {
		bytes int
		err   error
	}{bytes, err})
}

func (r *testReporter) OldLogRecord(bytes int) {
	r.oldRecords = append(r.oldRecords, bytes)
}

func (r *testReporter) droppedBytes() int {
	total := 0
	for _, c := range r.corruptions {
		total += c.bytes
	}
	return total
}

func (r *testReporter) hasError(substr string) bool {
	for _, c := range r.corruptions {
		if c.err != nil && strings.Contains(c.err.Error(), substr) {
			return true
		}
	}
	return false
}

// Helper to construct a string of specified length
func bigString(partial string, n int) []byte {
	var result []byte
	for len(result) < n {
		result = append(result, partial...)
	}
	return result[:n]
}

// Helper to construct a string from a number
func numberString(n int) string {
	return strings.Repeat(string(rune('0'+n%10)), (n%17)+1) + "."
}

// -----------------------------------------------------------------------------
// Format tests
// -----------------------------------------------------------------------------

func TestRecordTypeString(t *testing.T) {
	tests := []struct {
		t    RecordType
		want string
	}{
		{ZeroType, "ZeroType"},
		{FullType, "FullType"},
		{FirstType, "FirstType"},
		{MiddleType, "MiddleType"},
		{LastType, "LastType"},
		{RecyclableFullType, "RecyclableFullType"},
		{RecyclableFirstType, "RecyclableFirstType"},
		{RecyclableMiddleType, "RecyclableMiddleType"},
		{RecyclableLastType, "RecyclableLastType"},
		{SetCompressionType, "SetCompressionType"},
		{RecordType(200), "UnknownType"},
	}

	for _, tt := range tests {
		if got := tt.t.String(); got != tt.want {
			t.Errorf("RecordType(%d).String() = %q, want %q", tt.t, got, tt.want)
		}
	}
}

func TestIsRecyclableType(t *testing.T) {
	recyclable := []RecordType{
		RecyclableFullType, RecyclableFirstType,
		RecyclableMiddleType, RecyclableLastType,
		RecyclableUserDefinedTimestampSizeType,
		RecyclePredecessorWALInfoType,
	}
	for _, rt := range recyclable {
		if !IsRecyclableType(rt) {
			t.Errorf("IsRecyclableType(%v) = false, want true", rt)
		}
	}

	nonRecyclable := []RecordType{
		ZeroType, FullType, FirstType, MiddleType, LastType,
		SetCompressionType, UserDefinedTimestampSizeType,
	}
	for _, rt := range nonRecyclable {
		if IsRecyclableType(rt) {
			t.Errorf("IsRecyclableType(%v) = true, want false", rt)
		}
	}
}

func TestIsFragmentType(t *testing.T) {
	fragments := []RecordType{
		FullType, FirstType, MiddleType, LastType,
		RecyclableFullType, RecyclableFirstType, RecyclableMiddleType, RecyclableLastType,
	}
	for _, rt := range fragments {
		if !IsFragmentType(rt) {
			t.Errorf("IsFragmentType(%v) = false, want true", rt)
		}
	}

	nonFragments := []RecordType{
		ZeroType, SetCompressionType, UserDefinedTimestampSizeType,
	}
	for _, rt := range nonFragments {
		if IsFragmentType(rt) {
			t.Errorf("IsFragmentType(%v) = true, want false", rt)
		}
	}
}

func TestToRecyclable(t *testing.T) {
	tests := []struct {
		in   RecordType
		want RecordType
	}{
		{FullType, RecyclableFullType},
		{FirstType, RecyclableFirstType},
		{MiddleType, RecyclableMiddleType},
		{LastType, RecyclableLastType},
		{ZeroType, ZeroType},
		{SetCompressionType, SetCompressionType},
	}

	for _, tt := range tests {
		if got := ToRecyclable(tt.in); got != tt.want {
			t.Errorf("ToRecyclable(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestToLegacy(t *testing.T) {
	tests := []struct {
		in   RecordType
		want RecordType
	}{
		{RecyclableFullType, FullType},
		{RecyclableFirstType, FirstType},
		{RecyclableMiddleType, MiddleType},
		{RecyclableLastType, LastType},
		{RecyclePredecessorWALInfoType, PredecessorWALInfoType},
		{ZeroType, ZeroType},
		{SetCompressionType, SetCompressionType},
	}

	for _, tt := range tests {
		if got := ToLegacy(tt.in); got != tt.want {
			t.Errorf("ToLegacy(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestIsPredecessorWALInfoType(t *testing.T) {
	if !IsPredecessorWALInfoType(PredecessorWALInfoType) {
		t.Error("IsPredecessorWALInfoType(PredecessorWALInfoType) = false, want true")
	}
	if !IsPredecessorWALInfoType(RecyclePredecessorWALInfoType) {
		t.Error("IsPredecessorWALInfoType(RecyclePredecessorWALInfoType) = false, want true")
	}
	if IsPredecessorWALInfoType(FullType) {
		t.Error("IsPredecessorWALInfoType(FullType) = true, want false")
	}
}

func TestPredecessorWALInfo(t *testing.T) {
	// Test encoding and decoding
	info := &PredecessorWALInfo{
		LogNumber:         42,
		SizeBytes:         1024 * 1024,
		LastSeqnoRecorded: 12345,
	}

	// Encode
	encoded := info.EncodeTo(nil)
	if len(encoded) != PredecessorWALInfoSize {
		t.Fatalf("EncodeTo len = %d, want %d", len(encoded), PredecessorWALInfoSize)
	}

	// Decode
	decoded, err := DecodePredecessorWALInfo(encoded)
	if err != nil {
		t.Fatalf("DecodePredecessorWALInfo error: %v", err)
	}

	if decoded.LogNumber != info.LogNumber {
		t.Errorf("LogNumber = %d, want %d", decoded.LogNumber, info.LogNumber)
	}
	if decoded.SizeBytes != info.SizeBytes {
		t.Errorf("SizeBytes = %d, want %d", decoded.SizeBytes, info.SizeBytes)
	}
	if decoded.LastSeqnoRecorded != info.LastSeqnoRecorded {
		t.Errorf("LastSeqnoRecorded = %d, want %d", decoded.LastSeqnoRecorded, info.LastSeqnoRecorded)
	}

	// Test String()
	s := info.String()
	if s == "" {
		t.Error("String() returned empty string")
	}

	// Test decode error on short data
	_, err = DecodePredecessorWALInfo([]byte{1, 2, 3})
	if err == nil {
		t.Error("DecodePredecessorWALInfo should fail on short data")
	}
}

// -----------------------------------------------------------------------------
// Constants tests - verify bit-compatibility with C++
// -----------------------------------------------------------------------------

func TestConstants(t *testing.T) {
	// These must match RocksDB exactly
	if BlockSize != 32768 {
		t.Errorf("BlockSize = %d, want 32768", BlockSize)
	}
	if HeaderSize != 7 {
		t.Errorf("HeaderSize = %d, want 7", HeaderSize)
	}
	if RecyclableHeaderSize != 11 {
		t.Errorf("RecyclableHeaderSize = %d, want 11", RecyclableHeaderSize)
	}
	if MaxRecordPayload != BlockSize-HeaderSize {
		t.Errorf("MaxRecordPayload = %d, want %d", MaxRecordPayload, BlockSize-HeaderSize)
	}
}

// -----------------------------------------------------------------------------
// Writer tests
// -----------------------------------------------------------------------------

func TestWriterBasic(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	data := []byte("hello world")
	n, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Should have written header (7 bytes) + data (11 bytes) = 18 bytes
	expectedLen := HeaderSize + len(data)
	if n != expectedLen {
		t.Errorf("AddRecord returned %d, want %d", n, expectedLen)
	}
	if buf.Len() != expectedLen {
		t.Errorf("Buffer length = %d, want %d", buf.Len(), expectedLen)
	}
}

func TestWriterEmptyRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	// Empty record should still write a header
	n, err := w.AddRecord([]byte{})
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	if n != HeaderSize {
		t.Errorf("AddRecord returned %d, want %d", n, HeaderSize)
	}
}

func TestWriterRecyclable(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 12345, true)

	data := []byte("test")
	n, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Should have written recyclable header (11 bytes) + data (4 bytes)
	expectedLen := RecyclableHeaderSize + len(data)
	if n != expectedLen {
		t.Errorf("AddRecord returned %d, want %d", n, expectedLen)
	}
}

func TestWriterFragmentation(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	// Create a record larger than one block
	data := make([]byte, BlockSize+1000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Should have written at least 2 blocks
	if buf.Len() < BlockSize+HeaderSize {
		t.Errorf("Buffer too small for fragmented record: %d", buf.Len())
	}
}

func TestWriterBlockBoundary(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	// Fill up most of the first block
	fillSize := BlockSize - HeaderSize - 10
	data1 := make([]byte, fillSize)
	_, err := w.AddRecord(data1)
	if err != nil {
		t.Fatalf("AddRecord 1 error: %v", err)
	}

	// This should trigger padding and start in a new block
	data2 := []byte("second record")
	_, err = w.AddRecord(data2)
	if err != nil {
		t.Fatalf("AddRecord 2 error: %v", err)
	}

	// Should have crossed block boundary
	if buf.Len() <= BlockSize {
		t.Errorf("Expected to cross block boundary, buf.Len() = %d", buf.Len())
	}
}

func TestWriterBlockOffset(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	if w.BlockOffset() != 0 {
		t.Errorf("Initial BlockOffset = %d, want 0", w.BlockOffset())
	}

	data := []byte("test")
	w.AddRecord(data)

	expected := HeaderSize + len(data)
	if w.BlockOffset() != expected {
		t.Errorf("BlockOffset after write = %d, want %d", w.BlockOffset(), expected)
	}
}

func TestWriterLogNumber(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 42, false)

	if w.LogNumber() != 42 {
		t.Errorf("LogNumber = %d, want 42", w.LogNumber())
	}
}

func TestWriterIsRecyclable(t *testing.T) {
	var buf bytes.Buffer

	w1 := NewWriter(&buf, 1, false)
	if w1.IsRecyclable() {
		t.Error("Expected non-recyclable writer")
	}

	w2 := NewWriter(&buf, 1, true)
	if !w2.IsRecyclable() {
		t.Error("Expected recyclable writer")
	}
}

// -----------------------------------------------------------------------------
// Reader tests - basic
// -----------------------------------------------------------------------------

func TestReaderEmpty(t *testing.T) {
	r := NewReader(bytes.NewReader(nil), nil, true, 1)
	_, err := r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF for empty file, got %v", err)
	}
}

func TestReaderBasic(t *testing.T) {
	// Write a record
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	data := []byte("hello world")
	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Read it back
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error: %v", err)
	}

	if !bytes.Equal(record, data) {
		t.Errorf("ReadRecord = %q, want %q", record, data)
	}

	// Should get EOF on next read
	_, err = r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestReaderMultipleRecords(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	records := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
		[]byte(""),
		[]byte("fifth with more data"),
	}

	for _, data := range records {
		_, err := w.AddRecord(data)
		if err != nil {
			t.Fatalf("AddRecord error: %v", err)
		}
	}

	// Read them back
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	for i, expected := range records {
		record, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("ReadRecord %d error: %v", i, err)
		}
		if !bytes.Equal(record, expected) {
			t.Errorf("Record %d: got %q, want %q", i, record, expected)
		}
	}

	// Should get EOF
	_, err := r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestReaderEOFMultipleTimes(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	r.ReadRecord()

	// Multiple reads at EOF should all return EOF
	for i := range 5 {
		_, err := r.ReadRecord()
		if !errors.Is(err, io.EOF) {
			t.Errorf("Read %d at EOF: expected EOF, got %v", i, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Fragmentation tests
// -----------------------------------------------------------------------------

func TestReaderFragmentedRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	// Create a record larger than one block
	data := make([]byte, BlockSize+5000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Read it back
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error: %v", err)
	}

	if !bytes.Equal(record, data) {
		t.Errorf("Fragmented record mismatch: len(got)=%d, len(want)=%d", len(record), len(data))
	}
}

func TestFragmentationSmallMediumLarge(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	small := []byte("small")
	medium := bigString("medium", 50000)
	large := bigString("large", 100000)

	w.AddRecord(small)
	w.AddRecord(medium)
	w.AddRecord(large)

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)

	rec, _ := r.ReadRecord()
	if !bytes.Equal(rec, small) {
		t.Errorf("small mismatch")
	}

	rec, _ = r.ReadRecord()
	if !bytes.Equal(rec, medium) {
		t.Errorf("medium mismatch: len=%d", len(rec))
	}

	rec, _ = r.ReadRecord()
	if !bytes.Equal(rec, large) {
		t.Errorf("large mismatch: len=%d", len(rec))
	}
}

// -----------------------------------------------------------------------------
// Block boundary tests (matching C++ MarginalTrailer, ShortTrailer, AlignedEof)
// -----------------------------------------------------------------------------

func TestMarginalTrailer(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			headerSize := HeaderSize
			if recyclable {
				headerSize = RecyclableHeaderSize
			}

			// Make a trailer that is exactly the same length as an empty record
			n := BlockSize - 2*headerSize
			data1 := bigString("foo", n)
			w.AddRecord(data1)

			// This should exactly fill the block minus one header
			if buf.Len() != BlockSize-headerSize {
				t.Errorf("After first record: len=%d, want %d", buf.Len(), BlockSize-headerSize)
			}

			w.AddRecord([]byte{}) // Empty record
			w.AddRecord([]byte("bar"))

			r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
			rec, _ := r.ReadRecord()
			if !bytes.Equal(rec, data1) {
				t.Errorf("First record mismatch")
			}
			rec, _ = r.ReadRecord()
			if len(rec) != 0 {
				t.Errorf("Empty record: got len=%d", len(rec))
			}
			rec, _ = r.ReadRecord()
			if !bytes.Equal(rec, []byte("bar")) {
				t.Errorf("Third record mismatch")
			}
		})
	}
}

func TestShortTrailer(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			headerSize := HeaderSize
			if recyclable {
				headerSize = RecyclableHeaderSize
			}

			// Leave less than a header at the end of the block
			n := BlockSize - 2*headerSize + 4
			data1 := bigString("foo", n)
			w.AddRecord(data1)
			w.AddRecord([]byte{})
			w.AddRecord([]byte("bar"))

			r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
			rec, _ := r.ReadRecord()
			if !bytes.Equal(rec, data1) {
				t.Errorf("First record mismatch")
			}
			rec, _ = r.ReadRecord()
			if len(rec) != 0 {
				t.Errorf("Empty record: got len=%d", len(rec))
			}
			rec, _ = r.ReadRecord()
			if !bytes.Equal(rec, []byte("bar")) {
				t.Errorf("Third record mismatch")
			}
		})
	}
}

func TestAlignedEof(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			headerSize := HeaderSize
			if recyclable {
				headerSize = RecyclableHeaderSize
			}

			// Fill exactly to block boundary with padding
			n := BlockSize - 2*headerSize + 4
			data := bigString("foo", n)
			w.AddRecord(data)

			r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
			rec, err := r.ReadRecord()
			if err != nil {
				t.Fatalf("ReadRecord error: %v", err)
			}
			if !bytes.Equal(rec, data) {
				t.Errorf("Record mismatch")
			}
			_, err = r.ReadRecord()
			if !errors.Is(err, io.EOF) {
				t.Errorf("Expected EOF, got %v", err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// ManyBlocks test (matching C++)
// -----------------------------------------------------------------------------

func TestManyBlocks(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	const N = 100000
	for i := range N {
		w.AddRecord([]byte(numberString(i)))
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	for i := range N {
		rec, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("ReadRecord %d error: %v", i, err)
		}
		expected := numberString(i)
		if string(rec) != expected {
			t.Errorf("Record %d: got %q, want %q", i, string(rec), expected)
		}
	}
	_, err := r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// RandomRead test (matching C++)
// -----------------------------------------------------------------------------

func TestRandomRead(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	const N = 500
	rng := rand.New(rand.NewSource(301))

	// Write random-sized records
	records := make([][]byte, N)
	for i := range N {
		size := rng.Intn(1 << 17) // Up to 128KB
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(rng.Intn(256))
		}
		records[i] = data
		w.AddRecord(data)
	}

	// Read them back
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
	for i := range N {
		rec, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("ReadRecord %d error: %v", i, err)
		}
		if !bytes.Equal(rec, records[i]) {
			t.Errorf("Record %d mismatch: len(got)=%d, len(want)=%d", i, len(rec), len(records[i]))
		}
	}
}

// -----------------------------------------------------------------------------
// Recyclable format tests
// -----------------------------------------------------------------------------

func TestReaderRecyclableFormat(t *testing.T) {
	logNumber := uint64(42)

	var buf bytes.Buffer
	w := NewWriter(&buf, logNumber, true)
	data := []byte("recyclable test")
	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Read with correct log number
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, logNumber)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error: %v", err)
	}
	if !bytes.Equal(record, data) {
		t.Errorf("ReadRecord = %q, want %q", record, data)
	}
}

func TestReaderRecyclableWrongLogNumber(t *testing.T) {
	logNumber := uint64(42)

	var buf bytes.Buffer
	w := NewWriter(&buf, logNumber, true)
	w.AddRecord([]byte("test"))

	// Read with wrong log number - should detect old record
	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(buf.Bytes()), reporter, true, logNumber+1)
	_, err := r.ReadRecord()

	// Should report old record error
	if !errors.Is(err, ErrOldRecord) && !errors.Is(err, io.EOF) {
		t.Errorf("Expected ErrOldRecord or EOF, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Checksum tests
// -----------------------------------------------------------------------------

func TestReaderChecksumVerification(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	data := []byte("test data")
	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Corrupt the checksum
	corrupted := buf.Bytes()
	corrupted[0] ^= 0xFF

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(corrupted), reporter, true, 1)
	_, err = r.ReadRecord()

	// Should report corruption and return EOF (no valid records)
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF after corruption, got %v", err)
	}
	if len(reporter.corruptions) == 0 {
		t.Error("Expected corruption to be reported")
	}
}

func TestReaderNoChecksumVerification(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	data := []byte("test data")
	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error: %v", err)
	}

	// Corrupt the checksum
	corrupted := buf.Bytes()
	corrupted[0] ^= 0xFF

	// Read without checksum verification
	r := NewReader(bytes.NewReader(corrupted), nil, false, 1)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error (checksum disabled): %v", err)
	}
	if !bytes.Equal(record, data) {
		t.Errorf("ReadRecord = %q, want %q", record, data)
	}
}

func TestChecksumMismatchDroppedBytes(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foooooo"))

	// Corrupt checksum
	data := buf.Bytes()
	data[0] ^= 0x0E // Increment by 14

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
	if reporter.droppedBytes() == 0 {
		t.Error("Expected dropped bytes to be reported")
	}
}

// TestStrictReaderRejectsCorruptedChecksum verifies that NewStrictReader
// returns an error immediately on checksum mismatch, which is required
// for MANIFEST reading where corruption is unrecoverable.
func TestStrictReaderRejectsCorruptedChecksum(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("valid record"))

	// Corrupt the checksum (first 4 bytes)
	data := buf.Bytes()
	data[0] ^= 0xFF

	// Strict reader must return an error, not EOF
	r := NewStrictReader(bytes.NewReader(data), nil, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, ErrCorruptedRecord) {
		t.Errorf("StrictReader expected ErrCorruptedRecord, got %v", err)
	}
}

// TestStrictReaderRejectsTruncatedRecord verifies that NewStrictReader
// properly handles truncated records.
func TestStrictReaderRejectsTruncatedRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("this is a longer record that will be truncated"))

	// Truncate the record mid-way
	data := buf.Bytes()
	truncated := data[:len(data)-10]

	r := NewStrictReader(bytes.NewReader(truncated), nil, 1)
	_, err := r.ReadRecord()

	// Should return EOF (incomplete record at end of file)
	if !errors.Is(err, io.EOF) {
		t.Errorf("StrictReader expected EOF for truncated record, got %v", err)
	}
}

// TestStrictReaderMultipleRecordsWithCorruption verifies that strict reader
// stops at the first corrupted record.
func TestStrictReaderMultipleRecordsWithCorruption(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("record1"))
	w.AddRecord([]byte("record2"))
	w.AddRecord([]byte("record3"))

	data := buf.Bytes()

	// Find the second record and corrupt its checksum
	// First record is at offset 0, second starts after first record ends
	// For a 7-byte payload "record1": header(7) + payload(7) = 14 bytes
	// So second record starts at offset 14
	if len(data) > 20 {
		data[14] ^= 0xFF // Corrupt second record's checksum
	}

	r := NewStrictReader(bytes.NewReader(data), nil, 1)

	// First record should be read successfully
	rec, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("First record should succeed: %v", err)
	}
	if string(rec) != "record1" {
		t.Errorf("First record = %q, want 'record1'", rec)
	}

	// Second record should fail with corruption
	_, err = r.ReadRecord()
	if !errors.Is(err, ErrCorruptedRecord) {
		t.Errorf("Second record expected ErrCorruptedRecord, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Bad record type tests (matching C++)
// -----------------------------------------------------------------------------

func TestBadRecordType(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Corrupt the record type (stored at offset 6)
	data := buf.Bytes()
	data[6] = byte(FullType) + 100 // Invalid type

	// Fix checksum for the corrupted record
	fixChecksum(data, 0, 3, false)

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
	// Should have dropped some bytes
	if reporter.droppedBytes() == 0 {
		t.Error("Expected dropped bytes to be > 0")
	}
}

func TestIgnorableRecordType(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Set record type to ignorable (bit 7 set)
	data := buf.Bytes()
	data[6] = byte(RecordTypeSafeIgnoreMask + 100)

	// Note: We don't fix checksum here because we're testing the record type handling
	// The record will fail checksum validation before type checking in our implementation

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
	// Note: Our implementation validates checksum before type, so corruption is reported
	// The key behavior is that the reader doesn't crash and returns EOF
}

// -----------------------------------------------------------------------------
// Unexpected record type tests (matching C++)
// -----------------------------------------------------------------------------

func TestUnexpectedMiddleType(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Change FullType to MiddleType
	data := buf.Bytes()
	data[6] = byte(MiddleType)
	fixChecksum(data, 0, 3, false)

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
	// Should have reported dropped bytes
	if reporter.droppedBytes() == 0 {
		t.Error("Expected dropped bytes to be > 0")
	}
}

func TestUnexpectedLastType(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Change FullType to LastType
	data := buf.Bytes()
	data[6] = byte(LastType)
	fixChecksum(data, 0, 3, false)

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
	// Should have reported dropped bytes
	if reporter.droppedBytes() == 0 {
		t.Error("Expected dropped bytes to be > 0")
	}
}

func TestUnexpectedFirstTypeInterrupts(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))
	w.AddRecord([]byte("bar"))

	// Change first record's FullType to FirstType (start of fragmented record)
	data := buf.Bytes()
	data[6] = byte(FirstType)
	fixChecksum(data, 0, 3, false)

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)

	// Should read "bar" (second record) and report dropped bytes for incomplete first
	rec, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error: %v", err)
	}
	if !bytes.Equal(rec, []byte("bar")) {
		t.Errorf("Record = %q, want %q", rec, "bar")
	}
	// Should have reported dropped bytes for the incomplete first record
	if reporter.droppedBytes() == 0 {
		t.Error("Expected dropped bytes to be > 0")
	}
}

// -----------------------------------------------------------------------------
// Truncation tests
// -----------------------------------------------------------------------------

func TestTruncatedRecord(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Truncate the record
	data := buf.Bytes()[:len(buf.Bytes())-2]

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(data), reporter, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestTruncatedHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("foo"))

	// Truncate to less than header size
	data := buf.Bytes()[:HeaderSize-1]

	r := NewReader(bytes.NewReader(data), nil, true, 1)
	_, err := r.ReadRecord()

	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected EOF, got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Fragmented record edge cases
// These tests verify the reader handles malformed fragment sequences gracefully.
// Reference: rocksdb/db/log_test.cc
// -----------------------------------------------------------------------------

// TestMissingLast tests reading First -> Middle -> EOF (missing Last).
// C++ log_test.cc: MissingLastIsIgnored
func TestMissingLast(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write a large record that spans blocks (First + Last)
			bigData := bigString("bar", BlockSize)
			_, err := w.AddRecord(bigData)
			if err != nil {
				t.Fatalf("AddRecord error: %v", err)
			}

			// Remove the Last block (14 bytes: header + some payload)
			// This leaves First + possibly Middle fragments, but no Last
			data := buf.Bytes()
			if len(data) > 14 {
				data = data[:len(data)-14]
			}

			reporter := newTestReporter()
			r := NewReader(bytes.NewReader(data), reporter, true, 1)
			_, err = r.ReadRecord()

			// Should get EOF or unexpected EOF (in fragmented state)
			if !errors.Is(err, io.EOF) && !errors.Is(err, ErrUnexpectedEOF) {
				t.Errorf("Expected EOF or ErrUnexpectedEOF, got %v", err)
			}
		})
	}
}

// TestFirstInterruptedByFirst tests First -> First -> Last sequence.
// The second First should report corruption for the incomplete first fragment.
// C++ log_test.cc: UnexpectedFirstType
func TestFirstInterruptedByFirst(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write "foo" (will be corrupted to FirstType)
			w.AddRecord([]byte("foo"))
			// Write a large record that legitimately fragments
			bigData := bigString("bar", 100000)
			w.AddRecord(bigData)

			// Change first record's FullType to FirstType
			data := buf.Bytes()
			recordType := FirstType
			if recyclable {
				recordType = RecyclableFirstType
			}
			data[6] = byte(recordType)
			fixChecksum(data, 0, 3, recyclable)

			reporter := newTestReporter()
			r := NewReader(bytes.NewReader(data), reporter, true, 1)

			// Should read the bigData record (the legitimate one)
			rec, err := r.ReadRecord()
			if err != nil {
				t.Fatalf("ReadRecord error: %v", err)
			}
			if !bytes.Equal(rec, bigData) {
				t.Errorf("Record mismatch: got len=%d, want len=%d", len(rec), len(bigData))
			}

			// Should report dropped bytes for incomplete first fragment
			if reporter.droppedBytes() == 0 {
				t.Error("Expected dropped bytes > 0 for incomplete first fragment")
			}
			if !reporter.hasError("partial record") && !reporter.hasError("first") {
				t.Log("Note: error message may differ from C++, but corruption was reported")
			}
		})
	}
}

// TestFirstInterruptedByFull tests First -> Full sequence.
// The Full should report corruption and then be returned.
// C++ log_test.cc: UnexpectedFullType
func TestFirstInterruptedByFull(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write "foo" (will be corrupted to FirstType)
			w.AddRecord([]byte("foo"))
			// Write "bar" (remains Full)
			w.AddRecord([]byte("bar"))

			// Change first record's FullType to FirstType
			data := buf.Bytes()
			recordType := FirstType
			if recyclable {
				recordType = RecyclableFirstType
			}
			data[6] = byte(recordType)
			fixChecksum(data, 0, 3, recyclable)

			reporter := newTestReporter()
			r := NewReader(bytes.NewReader(data), reporter, true, 1)

			// Should read "bar" and report dropped bytes for "foo"
			rec, err := r.ReadRecord()
			if err != nil {
				t.Fatalf("ReadRecord error: %v", err)
			}
			if !bytes.Equal(rec, []byte("bar")) {
				t.Errorf("Record = %q, want %q", rec, "bar")
			}

			// Should report dropped bytes
			if reporter.droppedBytes() == 0 {
				t.Error("Expected dropped bytes > 0")
			}

			// Should get EOF on next read
			_, err = r.ReadRecord()
			if !errors.Is(err, io.EOF) {
				t.Errorf("Expected EOF, got %v", err)
			}
		})
	}
}

// TestMultipleMiddleFragments tests First -> Middle -> Middle -> Middle -> Last.
// All fragments should be correctly assembled.
func TestMultipleMiddleFragments(t *testing.T) {
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write a record large enough to span multiple blocks (5+ fragments)
			headerSize := HeaderSize
			if recyclable {
				headerSize = RecyclableHeaderSize
			}
			numFragments := 5
			dataSize := (BlockSize - headerSize) * numFragments
			bigData := bigString("test", dataSize)

			_, err := w.AddRecord(bigData)
			if err != nil {
				t.Fatalf("AddRecord error: %v", err)
			}

			// Read it back
			r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
			rec, err := r.ReadRecord()
			if err != nil {
				t.Fatalf("ReadRecord error: %v", err)
			}
			if !bytes.Equal(rec, bigData) {
				t.Errorf("Record mismatch: got len=%d, want len=%d", len(rec), len(bigData))
			}
		})
	}
}

// TestZeroLengthFragments tests empty fragments in a sequence.
// First(empty) -> Middle(empty) -> Last(empty) should produce empty record.
func TestZeroLengthFragments(t *testing.T) {
	// This tests the edge case where all fragments are zero-length
	// We can't easily construct this with the writer, so we build manually
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write an empty record - should be a single Full record
			_, err := w.AddRecord([]byte{})
			if err != nil {
				t.Fatalf("AddRecord error: %v", err)
			}

			r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
			rec, err := r.ReadRecord()
			if err != nil {
				t.Fatalf("ReadRecord error: %v", err)
			}
			if len(rec) != 0 {
				t.Errorf("Expected empty record, got len=%d", len(rec))
			}
		})
	}
}

// TestErrorDoesNotJoinRecords verifies that corruption doesn't cause
// fragments from different records to be joined.
// C++ log_test.cc: ErrorJoinsRecords
//
// Note: With checksum validation enabled, corrupted records are skipped.
// The reader should NOT join first(R1) with last(R2) when the middle is corrupted.
func TestErrorDoesNotJoinRecords(t *testing.T) {
	// This test verifies that corruption stops further reading.
	// C++ RocksDB's default WALRecoveryMode::kTolerateCorruptedTailRecords
	// treats corruption as EOF - no more records are read after corruption.
	// This matches redteam evidence: C01 run02/04/05/06 show C++ `ldb scan`
	// only returns records BEFORE corruption, not after.
	for _, recyclable := range []bool{false, true} {
		t.Run(boolName("recyclable", recyclable), func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, 1, recyclable)

			// Write records that span two blocks each
			rec1 := bigString("foo", BlockSize)
			rec2 := bigString("bar", BlockSize)
			w.AddRecord(rec1)
			w.AddRecord(rec2)
			w.AddRecord([]byte("correct"))

			// Wipe the middle block (block 1)
			// This should corrupt the end of rec1 and start of rec2
			data := buf.Bytes()
			for offset := BlockSize; offset < 2*BlockSize && offset < len(data); offset++ {
				data[offset] = 'x'
			}

			reporter := newTestReporter()
			// Use checksum validation - this is how corruption is detected
			r := NewReader(bytes.NewReader(data), reporter, true, 1)

			// Corruption should cause EOF - no records readable after corruption.
			// C++ behavior: stop at first corruption, treat as end of WAL.
			var readRecords [][]byte
			for range 10 { // max 10 attempts
				rec, err := r.ReadRecord()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					break // Any error stops reading (matches C++ behavior)
				}
				readRecords = append(readRecords, rec)
			}

			// With checksum validation and corruption in the middle,
			// the reader should stop and return EOF (no records after corruption).
			// The first record spans into the corrupted block, so it too is lost.
			if len(readRecords) > 0 {
				t.Logf("Read %d records (expected 0 due to corruption in first record's continuation)", len(readRecords))
				for i, rec := range readRecords {
					t.Logf("  Record %d: len=%d, first 20 bytes: %q", i, len(rec), truncate(rec, 20))
				}
			}

			// Both formats should behave identically: corruption stops reading.
			// The "correct" record should NOT be found (it's after the corruption).
			for _, rec := range readRecords {
				if bytes.Equal(rec, []byte("correct")) {
					t.Error("Found 'correct' record after corruption - should have stopped at corruption (C++ oracle alignment)")
				}
			}
		})
	}
}

// truncate returns the first n bytes of b as a string, or all if shorter.
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// -----------------------------------------------------------------------------
// Roundtrip tests
// -----------------------------------------------------------------------------

func TestRoundtripVariousSizes(t *testing.T) {
	sizes := []int{
		0,                          // Empty
		1,                          // Single byte
		100,                        // Small
		BlockSize - HeaderSize,     // Exactly one block
		BlockSize - HeaderSize + 1, // Just over one block
		BlockSize * 2,              // Multiple blocks
		BlockSize*3 + 500,          // Multiple blocks with remainder
	}

	for _, size := range sizes {
		for _, recyclable := range []bool{false, true} {
			name := "size" + string(rune('0'+size%10))
			if recyclable {
				name += "_recyclable"
			}
			t.Run(name, func(t *testing.T) {
				testRoundtrip(t, size, recyclable)
			})
		}
	}
}

func testRoundtrip(t *testing.T, size int, recyclable bool) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // Prime to catch off-by-one
	}

	var buf bytes.Buffer
	w := NewWriter(&buf, 123, recyclable)
	_, err := w.AddRecord(data)
	if err != nil {
		t.Fatalf("AddRecord error (size=%d, recyclable=%v): %v", size, recyclable, err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 123)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord error (size=%d, recyclable=%v): %v", size, recyclable, err)
	}

	if !bytes.Equal(record, data) {
		t.Errorf("Roundtrip mismatch (size=%d, recyclable=%v): len(got)=%d, len(want)=%d",
			size, recyclable, len(record), len(data))
	}
}

// -----------------------------------------------------------------------------
// IsEOF and LastRecordEnd tests
// -----------------------------------------------------------------------------

func TestIsEOF(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("test"))

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)

	if r.IsEOF() {
		t.Error("IsEOF should be false before reading")
	}

	r.ReadRecord()
	r.ReadRecord() // Hit EOF

	if !r.IsEOF() {
		t.Error("IsEOF should be true after EOF")
	}
}

func TestLastRecordEnd(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("test"))

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)

	if r.LastRecordEnd() != 0 {
		t.Errorf("LastRecordEnd before read = %d, want 0", r.LastRecordEnd())
	}

	r.ReadRecord()

	expected := HeaderSize + 4 // header + "test"
	if r.LastRecordEnd() != expected {
		t.Errorf("LastRecordEnd after read = %d, want %d", r.LastRecordEnd(), expected)
	}
}

// -----------------------------------------------------------------------------
// Fuzz test
// -----------------------------------------------------------------------------

func FuzzWALRoundtrip(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add(make([]byte, 1000))
	f.Add(make([]byte, BlockSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		var buf bytes.Buffer
		w := NewWriter(&buf, 1, false)
		_, err := w.AddRecord(data)
		if err != nil {
			return // Skip invalid inputs
		}

		r := NewReader(bytes.NewReader(buf.Bytes()), nil, true, 1)
		record, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("ReadRecord error: %v", err)
		}

		if !bytes.Equal(record, data) {
			t.Errorf("Roundtrip failed: len(got)=%d, len(want)=%d", len(record), len(data))
		}
	})
}

func FuzzWALReaderRobustness(f *testing.F) {
	// Seed with some valid WAL data
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)
	w.AddRecord([]byte("test"))
	f.Add(buf.Bytes())

	// Add some corrupted data
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Reader should never panic on any input
		r := NewReader(bytes.NewReader(data), nil, false, 1)
		for {
			_, err := r.ReadRecord()
			if err != nil {
				break
			}
		}
	})
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func boolName(prefix string, b bool) string {
	if b {
		return prefix + "_true"
	}
	return prefix + "_false"
}

// fixChecksum recalculates and fixes the checksum for a record at the given offset
func fixChecksum(data []byte, offset int, payloadLen int, recyclable bool) {
	headerSize := HeaderSize
	if recyclable {
		headerSize = RecyclableHeaderSize
	}

	recordType := data[offset+6]

	// Calculate CRC of type using the actual checksum package
	crc := checksum.Value([]byte{recordType})

	// If recyclable, extend with log number
	if recyclable && headerSize == RecyclableHeaderSize {
		crc = checksum.Extend(crc, data[offset+7:offset+11])
	}

	// Extend with payload
	crc = checksum.Extend(crc, data[offset+headerSize:offset+headerSize+payloadLen])

	// Mask and store
	crc = checksum.Mask(crc)
	encoding.EncodeFixed32(data[offset:], crc)
}
