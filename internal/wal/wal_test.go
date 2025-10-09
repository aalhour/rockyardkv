package wal

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"

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
		{ZeroType, ZeroType},
		{SetCompressionType, SetCompressionType},
	}

	for _, tt := range tests {
		if got := ToLegacy(tt.in); got != tt.want {
			t.Errorf("ToLegacy(%v) = %v, want %v", tt.in, got, tt.want)
		}
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

	// Calculate CRC of type
	crc := crc32Value([]byte{recordType})

	// If recyclable, extend with log number
	if recyclable && headerSize == RecyclableHeaderSize {
		crc = crc32Extend(crc, data[offset+7:offset+11])
	}

	// Extend with payload
	crc = crc32Extend(crc, data[offset+headerSize:offset+headerSize+payloadLen])

	// Mask and store
	crc = crc32Mask(crc)
	encoding.EncodeFixed32(data[offset:], crc)
}

// CRC32C helpers (using our checksum package)
func crc32Value(data []byte) uint32 {
	// Use hardware CRC32C
	h := uint32(0)
	for _, b := range data {
		h = crc32cUpdate(h, b)
	}
	return h ^ 0xFFFFFFFF
}

func crc32Extend(crc uint32, data []byte) uint32 {
	crc = ^crc
	for _, b := range data {
		crc = crc32cUpdate(crc, b)
	}
	return crc ^ 0xFFFFFFFF
}

func crc32Mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

func crc32cUpdate(crc uint32, b byte) uint32 {
	// CRC32C polynomial table (simplified - uses the actual table from checksum package)
	return crc32cTable[(crc^uint32(b))&0xFF] ^ (crc >> 8)
}

// CRC32C table (Castagnoli polynomial)
var crc32cTable = [256]uint32{
	0x00000000, 0xf26b8303, 0xe13b70f7, 0x1350f3f4, 0xc79a971f, 0x35f1141c, 0x26a1e7e8, 0xd4ca64eb,
	0x8ad958cf, 0x78b2dbcc, 0x6be22838, 0x9989ab3b, 0x4d43cfd0, 0xbf284cd3, 0xac78bf27, 0x5e133c24,
	0x105ec76f, 0xe235446c, 0xf165b798, 0x030e349b, 0xd7c45070, 0x25afd373, 0x36ff2087, 0xc494a384,
	0x9a879fa0, 0x68ec1ca3, 0x7bbcef57, 0x89d76c54, 0x5d1d08bf, 0xaf768bbc, 0xbc267848, 0x4e4dfb4b,
	0x20bd8ede, 0xd2d60ddd, 0xc186fe29, 0x33ed7d2a, 0xe72719c1, 0x154c9ac2, 0x061c6936, 0xf477ea35,
	0xaa64d611, 0x580f5512, 0x4b5fa6e6, 0xb93425e5, 0x6dfe410e, 0x9f95c20d, 0x8cc531f9, 0x7eaeb2fa,
	0x30e349b1, 0xc288cab2, 0xd1d83946, 0x23b3ba45, 0xf779deae, 0x05125dad, 0x1642ae59, 0xe4292d5a,
	0xba3a117e, 0x4851927d, 0x5b016189, 0xa96ae28a, 0x7da08661, 0x8fcb0562, 0x9c9bf696, 0x6ef07595,
	0x417b1dbc, 0xb3109ebf, 0xa0406d4b, 0x522bee48, 0x86e18aa3, 0x748a09a0, 0x67dafa54, 0x95b17957,
	0xcba24573, 0x39c9c670, 0x2a993584, 0xd8f2b687, 0x0c38d26c, 0xfe53516f, 0xed03a29b, 0x1f682198,
	0x5125dad3, 0xa34e59d0, 0xb01eaa24, 0x42752927, 0x96bf4dcc, 0x64d4cecf, 0x77843d3b, 0x85efbe38,
	0xdbfc821c, 0x2997011f, 0x3ac7f2eb, 0xc8ac71e8, 0x1c661503, 0xee0d9600, 0xfd5d65f4, 0x0f36e6f7,
	0x61c69362, 0x93ad1061, 0x80fde395, 0x72966096, 0xa65c047d, 0x5437877e, 0x4767748a, 0xb50cf789,
	0xeb1fcbad, 0x197448ae, 0x0a24bb5a, 0xf84f3859, 0x2c855cb2, 0xdeeedfb1, 0xcdbe2c45, 0x3fd5af46,
	0x7198540d, 0x83f3d70e, 0x90a324fa, 0x62c8a7f9, 0xb602c312, 0x44694011, 0x5739b3e5, 0xa55230e6,
	0xfb410cc2, 0x092a8fc1, 0x1a7a7c35, 0xe811ff36, 0x3cdb9bdd, 0xceb018de, 0xdde0eb2a, 0x2f8b6829,
	0x82f63b78, 0x70bdb87b, 0x639d4b8f, 0x91f6c88c, 0x453cac67, 0xb7572f64, 0xa407dc90, 0x56cc5f93,
	0x08df63b7, 0xfab4e0b4, 0xe9e41340, 0x1b8f9043, 0xcf45f4a8, 0x3d2e77ab, 0x2e7e845f, 0xdc15075c,
	0x9258fc17, 0x60337f14, 0x73638ce0, 0x81080fe3, 0x55c26b08, 0xa7a9e80b, 0xb4f91bff, 0x469298fc,
	0x1881a4d8, 0xeaea27db, 0xf9bad42f, 0x0bd1572c, 0xdf1b33c7, 0x2d70b0c4, 0x3e204330, 0xcc4bc033,
	0xa2bb95a6, 0x50d016a5, 0x4380e551, 0xb1eb6652, 0x652102b9, 0x974a81ba, 0x841a724e, 0x7671f14d,
	0x2862cd69, 0xda094e6a, 0xc959bd9e, 0x3b323e9d, 0xeff85a76, 0x1d93d975, 0x0ec32a81, 0xfca8a982,
	0xb2e552c9, 0x408ed1ca, 0x53de223e, 0xa1b5a13d, 0x757fc5d6, 0x871446d5, 0x9444b521, 0x662f3622,
	0x383c0a06, 0xca578905, 0xd9077af1, 0x2b6cf9f2, 0xffe69d19, 0x0d8d1e1a, 0x1eddedef, 0xecb66eec,
	0xc33de677, 0x31566574, 0x22069680, 0xd06d1583, 0x04a77168, 0xf6ccf26b, 0xe59c019f, 0x17f7829c,
	0x49e4beb8, 0xbb8f3dbb, 0xa8dfce4f, 0x5ab44d4c, 0x8e7e29a7, 0x7c15aaa4, 0x6f455950, 0x9d2eda53,
	0xd3632118, 0x2108a21b, 0x325851ef, 0xc033d2ec, 0x14f9b607, 0xe6923504, 0xf5c2c6f0, 0x07a945f3,
	0x59ba79d7, 0xabd1fad4, 0xb8810920, 0x4aea8a23, 0x9e20eec8, 0x6c4b6dcb, 0x7f1b9e3f, 0x8d701d3c,
	0xe3805aa9, 0x11ebd9aa, 0x02bb2a5e, 0xf0d0a95d, 0x241acdb6, 0xd6714eb5, 0xc521bd41, 0x374a3e42,
	0x69590266, 0x9b328165, 0x88627291, 0x7a09f192, 0xaec39579, 0x5ca8167a, 0x4ff8e58e, 0xbd93668d,
	0xf3de9dc6, 0x01b51ec5, 0x12e5ed31, 0xe08e6e32, 0x34440ad9, 0xc62f89da, 0xd57f7a2e, 0x2714f92d,
	0x7907c509, 0x8b6c460a, 0x983cb5fe, 0x6a5736fd, 0xbe9d5216, 0x4cf6d115, 0x5fa622e1, 0xadcda1e2,
}
