package db

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/encoding"
)

func appendPutRecord(data []byte, key, value []byte) []byte {
	data = append(data, batch.TypeValue)
	data = encoding.AppendLengthPrefixedSlice(data, key)
	data = encoding.AppendLengthPrefixedSlice(data, value)
	return data
}

// Regression test for internal-key collisions caused by sequence reuse when a
// WriteBatch header count under-reports the number of sequence-consuming records.
//
// Old behavior:
// - DBImpl.Write() reserves sequence numbers based on wb.Count() (header field)
// - If wb.Count() is smaller than the number of applied records, db.seq is under-advanced
// - A later write can reuse an already-assigned sequence number, violating internal-key uniqueness.
func TestWriteAdvancesSequenceByAppliedOps_NotHeaderCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "seq_advance_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	dbPath := filepath.Join(dir, "db")
	database, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	impl := database.(*DBImpl)

	// Build a raw write batch with two Put records, but a header count of 1.
	// This models the observed failure mode: wb.Count() < applied ops.
	raw := make([]byte, batch.HeaderSize)
	binary.LittleEndian.PutUint64(raw[0:8], 0)  // sequence (will be overwritten by DBImpl.Write)
	binary.LittleEndian.PutUint32(raw[8:12], 1) // UNDER-COUNT on purpose
	raw = appendPutRecord(raw, []byte("k1"), []byte("v1"))
	raw = appendPutRecord(raw, []byte("k2"), []byte("v2"))

	wb, err := batch.NewFromData(raw)
	if err != nil {
		t.Fatalf("NewFromData failed: %v", err)
	}

	if err := impl.Write(nil, wb); err != nil {
		t.Fatalf("Write(undercount batch) failed: %v", err)
	}

	// Two applied puts should consume seqnos {1,2} so db.seq must be 2.
	if got, want := impl.GetLatestSequenceNumber(), uint64(2); got != want {
		t.Fatalf("GetLatestSequenceNumber=%d, want %d", got, want)
	}

	// Next write should use seqno 3 (not reuse seqno 2).
	if err := database.Put(nil, []byte("k3"), []byte("v3")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if got, want := impl.GetLatestSequenceNumber(), uint64(3); got != want {
		t.Fatalf("GetLatestSequenceNumber=%d after Put, want %d", got, want)
	}
}
