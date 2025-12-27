package wal

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestReader_CorruptionStopsFurtherRecords is a contract test aligned to the
// RocksDB v10.7.5 oracle behavior observed via `ldb dump_wal`.
//
// Contract:
//   - If a record is checksum-corrupted, the reader must not return any later
//     records from the same WAL stream (safe-prefix behavior).
//
// Reference: rocksdb/db/log_reader.cc WALRecoveryMode::kTolerateCorruptedTailRecords
func TestReader_CorruptionStopsFurtherRecords(t *testing.T) {
	// Build a WAL stream with 3 small records.
	var buf bytes.Buffer
	w := NewWriter(&buf, 1, false)

	rec1 := []byte("record-1")
	rec2 := []byte("record-2")
	rec3 := []byte("record-3")

	if _, err := w.AddRecord(rec1); err != nil {
		t.Fatalf("AddRecord rec1: %v", err)
	}
	if _, err := w.AddRecord(rec2); err != nil {
		t.Fatalf("AddRecord rec2: %v", err)
	}
	if _, err := w.AddRecord(rec3); err != nil {
		t.Fatalf("AddRecord rec3: %v", err)
	}

	// Corrupt record #2 by flipping a byte inside its payload.
	//
	// Physical record layout (legacy format):
	//   CRC(4) + len(2) + type(1) + payload
	//
	// All records here are FullType and small, so they are contiguous.
	// Record #2 starts at: (HeaderSize + len(rec1)) = 7 + len(rec1).
	raw := buf.Bytes()
	rec2Start := HeaderSize + len(rec1) // start of record #2 header
	payloadOff := rec2Start + HeaderSize
	if payloadOff >= len(raw) {
		t.Fatalf("unexpected layout: payloadOff=%d len(raw)=%d", payloadOff, len(raw))
	}
	raw[payloadOff] ^= 0x01

	reporter := newTestReporter()
	r := NewReader(bytes.NewReader(raw), reporter, true /* verifyChecksum */, 0 /* logNumber */)

	// First record must be readable.
	got1, err := r.ReadRecord()
	if err != nil {
		t.Fatalf("ReadRecord rec1: %v", err)
	}
	if !bytes.Equal(got1, rec1) {
		t.Fatalf("rec1 mismatch: got=%q want=%q", got1, rec1)
	}

	// After a checksum mismatch, the oracle stops and does not apply later records.
	// We require the reader to stop returning further records.
	//
	// Implementation detail: acceptable outcomes are io.EOF or ErrCorruptedRecord
	// (or another explicit error), but NOT returning rec3.
	for range 5 {
		rec, err := r.ReadRecord()
		if err == nil {
			if bytes.Equal(rec, rec3) {
				t.Fatalf("contract violated: reader returned rec3 after corruption")
			}
			// Any successful record after corruption is a contract violation.
			t.Fatalf("contract violated: reader returned record %q after corruption", rec)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, ErrCorruptedRecord) {
			break
		}
		// Other errors: continue trying to read more records
	}

	// Ensure corruption was actually detected/reported.
	if len(reporter.corruptions) == 0 {
		t.Fatalf("expected corruption to be reported")
	}
	if !reporter.hasError("bad checksum") && !reporter.hasError("corrupted record") {
		// Reporter error strings may vary, but should be present.
		t.Fatalf("expected corruption report mentioning checksum; got=%v", reporter.corruptions[0].err)
	}
}
