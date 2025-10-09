package wal

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestRecordTypeStringCoverage tests the String method for all RecordType values
func TestRecordTypeStringCoverage(t *testing.T) {
	testCases := []struct {
		rt   RecordType
		want string
	}{
		{ZeroType, "ZeroType"},
		{FullType, "FullType"},
		{FirstType, "FirstType"},
		{MiddleType, "MiddleType"},
		{LastType, "LastType"},
		{RecordType(255), "UnknownType"},
	}

	for _, tc := range testCases {
		got := tc.rt.String()
		if got != tc.want {
			t.Errorf("RecordType(%d).String() = %q, want %q", tc.rt, got, tc.want)
		}
	}
}

// syncableBuffer is a bytes.Buffer that implements Sync()
type syncableBuffer struct {
	bytes.Buffer
	syncCalled bool
	syncErr    error
}

func (s *syncableBuffer) Sync() error {
	s.syncCalled = true
	return s.syncErr
}

// TestWriterSync tests the Sync method
func TestWriterSync(t *testing.T) {
	t.Run("NoSync", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWriter(&buf, 1, false)

		// Add a record
		_, err := w.AddRecord([]byte("test record"))
		if err != nil {
			t.Fatalf("AddRecord failed: %v", err)
		}

		// Sync should work (buf doesn't have Sync, so it's a no-op)
		err = w.Sync()
		if err != nil {
			t.Errorf("Sync failed: %v", err)
		}
	})

	t.Run("WithSync", func(t *testing.T) {
		buf := &syncableBuffer{}
		w := NewWriter(buf, 1, false)

		// Add a record
		_, err := w.AddRecord([]byte("test record"))
		if err != nil {
			t.Fatalf("AddRecord failed: %v", err)
		}

		// Sync should call the underlying Sync method
		err = w.Sync()
		if err != nil {
			t.Errorf("Sync failed: %v", err)
		}
		if !buf.syncCalled {
			t.Error("Sync was not called on the underlying writer")
		}
	})
}

// TestReaderReadRecordFragmented tests reading fragmented records
func TestReaderReadRecordFragmented(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	// Write a record that's larger than block size to force fragmentation
	largeRecord := make([]byte, BlockSize+1000)
	for i := range largeRecord {
		largeRecord[i] = byte(i % 256)
	}

	_, err := w.AddRecord(largeRecord)
	if err != nil {
		t.Fatalf("AddRecord failed: %v", err)
	}

	// Read it back
	r := NewReader(bytes.NewReader(buf.Bytes()), nil, false, 1)
	record, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}

	if !bytes.Equal(record, largeRecord) {
		t.Error("record content mismatch")
	}
}

// TestReaderReadRecordMultiple tests reading multiple records
func TestReaderReadRecordMultiple(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	for _, rec := range records {
		_, err := w.AddRecord(rec)
		if err != nil {
			t.Fatalf("AddRecord failed: %v", err)
		}
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, false, 1)

	for i, expected := range records {
		record, err := r.ReadRecord()
		if err != nil {
			t.Fatalf("ReadRecord %d failed: %v", i, err)
		}
		if !bytes.Equal(record, expected) {
			t.Errorf("record %d mismatch: got %q, want %q", i, record, expected)
		}
	}

	// Should get EOF
	_, err := r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

// TestReaderLastRecordEnd tests the LastRecordEnd method
func TestReaderLastRecordEnd(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	_, err := w.AddRecord([]byte("test"))
	if err != nil {
		t.Fatalf("AddRecord failed: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, false, 1)
	_, err = r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}

	end := r.LastRecordEnd()
	if end == 0 {
		t.Error("LastRecordEnd should be > 0 after reading a record")
	}
}

// TestReaderIsEOF tests the IsEOF method
func TestReaderIsEOF(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	_, err := w.AddRecord([]byte("single record"))
	if err != nil {
		t.Fatalf("AddRecord failed: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), nil, false, 1)

	if r.IsEOF() {
		t.Error("IsEOF should be false before reading")
	}

	_, err = r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}

	// Read again to hit EOF
	_, err = r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}

	if !r.IsEOF() {
		t.Error("IsEOF should be true after hitting EOF")
	}
}

// TestReaderEmptyInput tests reading from empty input
func TestReaderEmptyInput(t *testing.T) {
	r := NewReader(bytes.NewReader(nil), nil, false, 1)
	_, err := r.ReadRecord()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF for empty input, got %v", err)
	}
}
