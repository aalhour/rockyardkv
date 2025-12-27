package trace

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// mockHandler records all operations for verification.
type mockHandler struct {
	writes    []writeOp
	gets      []getOp
	seeks     []getOp
	flushes   int
	compacts  int
	failNext  bool
	failError error
}

type writeOp struct {
	cfID uint32
	data []byte
}

type getOp struct {
	cfID uint32
	key  []byte
}

func (h *mockHandler) HandleWrite(cfID uint32, batchData []byte) error {
	if h.failNext {
		h.failNext = false
		return h.failError
	}
	h.writes = append(h.writes, writeOp{cfID, append([]byte{}, batchData...)})
	return nil
}

func (h *mockHandler) HandleGet(cfID uint32, key []byte) error {
	if h.failNext {
		h.failNext = false
		return h.failError
	}
	h.gets = append(h.gets, getOp{cfID, append([]byte{}, key...)})
	return nil
}

func (h *mockHandler) HandleIterSeek(cfID uint32, key []byte) error {
	if h.failNext {
		h.failNext = false
		return h.failError
	}
	h.seeks = append(h.seeks, getOp{cfID, append([]byte{}, key...)})
	return nil
}

func (h *mockHandler) HandleFlush() error {
	if h.failNext {
		h.failNext = false
		return h.failError
	}
	h.flushes++
	return nil
}

func (h *mockHandler) HandleCompaction() error {
	if h.failNext {
		h.failNext = false
		return h.failError
	}
	h.compacts++
	return nil
}

// TestReplayer_WritesApplyInOrder verifies that writes are replayed in order.
func TestReplayer_WritesApplyInOrder(t *testing.T) {
	var buf bytes.Buffer

	// Write trace records
	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	expectedWrites := []writeOp{
		{0, []byte("batch1")},
		{0, []byte("batch2")},
		{1, []byte("batch3-cf1")},
	}

	for _, w := range expectedWrites {
		if err := writer.WriteWrite(w.cfID, w.data); err != nil {
			t.Fatalf("WriteWrite failed: %v", err)
		}
	}
	writer.Close()

	// Replay
	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Verify writes were applied in order
	if len(handler.writes) != len(expectedWrites) {
		t.Fatalf("Expected %d writes, got %d", len(expectedWrites), len(handler.writes))
	}

	for i, expected := range expectedWrites {
		got := handler.writes[i]
		if got.cfID != expected.cfID {
			t.Errorf("Write %d: cfID mismatch: got %d, want %d", i, got.cfID, expected.cfID)
		}
		if !bytes.Equal(got.data, expected.data) {
			t.Errorf("Write %d: data mismatch: got %s, want %s", i, got.data, expected.data)
		}
	}

	// Verify stats
	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.SuccessfulOps != 3 {
		t.Errorf("SuccessfulOps: got %d, want 3", stats.SuccessfulOps)
	}
	if stats.OperationCounts[TypeWrite] != 3 {
		t.Errorf("Write count: got %d, want 3", stats.OperationCounts[TypeWrite])
	}
}

// TestReplayer_GetHandlerCalled verifies that get operations are handled.
func TestReplayer_GetHandlerCalled(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	expectedGets := []getOp{
		{0, []byte("key1")},
		{0, []byte("key2")},
		{2, []byte("key3-cf2")},
	}

	for _, g := range expectedGets {
		if err := writer.WriteGet(g.cfID, g.key); err != nil {
			t.Fatalf("WriteGet failed: %v", err)
		}
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	_, err = replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(handler.gets) != len(expectedGets) {
		t.Fatalf("Expected %d gets, got %d", len(expectedGets), len(handler.gets))
	}

	for i, expected := range expectedGets {
		got := handler.gets[i]
		if got.cfID != expected.cfID {
			t.Errorf("Get %d: cfID mismatch", i)
		}
		if !bytes.Equal(got.key, expected.key) {
			t.Errorf("Get %d: key mismatch", i)
		}
	}
}

// TestReplayer_FlushAndCompactionHandled verifies flush and compaction handling.
func TestReplayer_FlushAndCompactionHandled(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteFlush(); err != nil {
		t.Fatalf("WriteFlush failed: %v", err)
	}
	if err := writer.WriteCompaction(); err != nil {
		t.Fatalf("WriteCompaction failed: %v", err)
	}
	if err := writer.WriteFlush(); err != nil {
		t.Fatalf("WriteFlush failed: %v", err)
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if handler.flushes != 2 {
		t.Errorf("Expected 2 flushes, got %d", handler.flushes)
	}
	if handler.compacts != 1 {
		t.Errorf("Expected 1 compaction, got %d", handler.compacts)
	}
	if stats.OperationCounts[TypeFlush] != 2 {
		t.Errorf("Flush count mismatch in stats")
	}
	if stats.OperationCounts[TypeCompaction] != 1 {
		t.Errorf("Compaction count mismatch in stats")
	}
}

// TestReplayer_CountsMatchRecords verifies stats match actual records.
func TestReplayer_CountsMatchRecords(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write a mix of operations
	for range 5 {
		writer.WriteGet(0, []byte("key"))
	}
	for range 3 {
		writer.WriteWrite(0, []byte("data"))
	}
	for range 2 {
		writer.WriteFlush()
	}
	writer.WriteIterSeek(0, []byte("seek"))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if stats.TotalRecords != 11 {
		t.Errorf("TotalRecords: got %d, want 11", stats.TotalRecords)
	}
	if stats.SuccessfulOps != 11 {
		t.Errorf("SuccessfulOps: got %d, want 11", stats.SuccessfulOps)
	}
	if stats.FailedOps != 0 {
		t.Errorf("FailedOps: got %d, want 0", stats.FailedOps)
	}

	// Verify handler received all operations
	if len(handler.gets) != 5 {
		t.Errorf("Gets: got %d, want 5", len(handler.gets))
	}
	if len(handler.writes) != 3 {
		t.Errorf("Writes: got %d, want 3", len(handler.writes))
	}
	if handler.flushes != 2 {
		t.Errorf("Flushes: got %d, want 2", handler.flushes)
	}
	if len(handler.seeks) != 1 {
		t.Errorf("Seeks: got %d, want 1", len(handler.seeks))
	}
}

// TestReplayer_FailedOpsCountedCorrectly verifies failed operations are counted.
func TestReplayer_FailedOpsCountedCorrectly(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	writer.WriteGet(0, []byte("key1"))
	writer.WriteGet(0, []byte("key2")) // This one will fail
	writer.WriteGet(0, []byte("key3"))
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	// Make the second operation fail
	var opCount atomic.Int32
	originalHandler := handler
	wrappedHandler := &failingHandler{
		mockHandler: originalHandler,
		failOnOp:    2,
		opCount:     &opCount,
	}

	replayer := NewReplayer(reader, wrappedHandler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.SuccessfulOps != 2 {
		t.Errorf("SuccessfulOps: got %d, want 2", stats.SuccessfulOps)
	}
	if stats.FailedOps != 1 {
		t.Errorf("FailedOps: got %d, want 1", stats.FailedOps)
	}
}

// failingHandler wraps mockHandler and fails on a specific operation.
type failingHandler struct {
	*mockHandler
	failOnOp int32
	opCount  *atomic.Int32
}

func (h *failingHandler) HandleGet(cfID uint32, key []byte) error {
	n := h.opCount.Add(1)
	if n == h.failOnOp {
		return errors.New("simulated failure")
	}
	return h.mockHandler.HandleGet(cfID, key)
}

// TestReplayer_PreserveTiming verifies timing preservation adds delays.
func TestReplayer_PreserveTiming(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write records with 100ms spacing
	now := time.Now()
	spacing := 100 * time.Millisecond
	for i := range 3 {
		ts := now.Add(time.Duration(i) * spacing)
		if err := writer.WriteAt(ts, TypeGet, (&GetPayload{Key: []byte("key")}).Encode()); err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}
	}
	writer.Close()

	// Replay WITHOUT timing preservation
	reader1, _ := NewReader(bytes.NewReader(buf.Bytes()))
	handler1 := &mockHandler{}
	replayer1 := NewReplayer(reader1, handler1, ReplayerOptions{PreserveTiming: false})

	startFast := time.Now()
	replayer1.Replay()
	fastDuration := time.Since(startFast)

	// Replay WITH timing preservation
	reader2, _ := NewReader(bytes.NewReader(buf.Bytes()))
	handler2 := &mockHandler{}
	replayer2 := NewReplayer(reader2, handler2, ReplayerOptions{PreserveTiming: true})

	startSlow := time.Now()
	replayer2.Replay()
	slowDuration := time.Since(startSlow)

	// With timing preservation, replay should take at least 200ms (2 * 100ms gaps)
	// Without it, should be nearly instant
	if fastDuration > 50*time.Millisecond {
		t.Errorf("Fast replay took too long: %v", fastDuration)
	}
	if slowDuration < 150*time.Millisecond {
		t.Errorf("Slow replay was too fast: %v (expected at least 150ms)", slowDuration)
	}
}

// TestReplayer_EmptyTrace verifies empty trace handling.
func TestReplayer_EmptyTrace(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if stats.TotalRecords != 0 {
		t.Errorf("TotalRecords: got %d, want 0", stats.TotalRecords)
	}
	if stats.SuccessfulOps != 0 {
		t.Errorf("SuccessfulOps: got %d, want 0", stats.SuccessfulOps)
	}
}

// TestReplayer_IterSeekHandled verifies iterator seek operations are handled.
func TestReplayer_IterSeekHandled(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteIterSeek(0, []byte("key1")); err != nil {
		t.Fatalf("WriteIterSeek failed: %v", err)
	}
	if err := writer.WriteIterSeek(1, []byte("key2")); err != nil {
		t.Fatalf("WriteIterSeek failed: %v", err)
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	_, err = replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(handler.seeks) != 2 {
		t.Errorf("Expected 2 seeks, got %d", len(handler.seeks))
	}
	if handler.seeks[0].cfID != 0 || !bytes.Equal(handler.seeks[0].key, []byte("key1")) {
		t.Errorf("First seek mismatch")
	}
	if handler.seeks[1].cfID != 1 || !bytes.Equal(handler.seeks[1].key, []byte("key2")) {
		t.Errorf("Second seek mismatch")
	}
}

// TestReplayer_UnknownRecordTypeSkipped verifies unknown types are skipped.
func TestReplayer_UnknownRecordTypeSkipped(t *testing.T) {
	var buf bytes.Buffer

	// Write header
	header := &Header{Magic: MagicNumber, Version: CurrentVersion}
	if err := header.Encode(&buf); err != nil {
		t.Fatalf("Encode header failed: %v", err)
	}

	// Write a valid record
	record1 := &Record{Timestamp: time.Now(), Type: TypeGet, Payload: (&GetPayload{Key: []byte("key")}).Encode()}
	if err := record1.Encode(&buf); err != nil {
		t.Fatalf("Encode record failed: %v", err)
	}

	// Write a record with unknown type (255)
	unknownRecord := &Record{Timestamp: time.Now(), Type: RecordType(255), Payload: []byte("unknown")}
	if err := unknownRecord.Encode(&buf); err != nil {
		t.Fatalf("Encode unknown record failed: %v", err)
	}

	// Write another valid record
	record2 := &Record{Timestamp: time.Now(), Type: TypeFlush, Payload: nil}
	if err := record2.Encode(&buf); err != nil {
		t.Fatalf("Encode record failed: %v", err)
	}

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// All 3 records should be counted, unknown type is "successful" (skipped without error)
	if stats.TotalRecords != 3 {
		t.Errorf("TotalRecords: got %d, want 3", stats.TotalRecords)
	}
	if stats.SuccessfulOps != 3 {
		t.Errorf("SuccessfulOps: got %d, want 3", stats.SuccessfulOps)
	}

	// But only 2 operations should have been handled
	if len(handler.gets) != 1 {
		t.Errorf("Gets: got %d, want 1", len(handler.gets))
	}
	if handler.flushes != 1 {
		t.Errorf("Flushes: got %d, want 1", handler.flushes)
	}
}

// TestReplayer_ReplayDuration verifies duration is recorded.
func TestReplayer_ReplayDuration(t *testing.T) {
	var buf bytes.Buffer

	writer, err := NewWriter(&buf)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	for range 100 {
		writer.WriteGet(0, []byte("key"))
	}
	writer.Close()

	reader, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	handler := &mockHandler{}
	replayer := NewReplayer(reader, handler, DefaultReplayerOptions())

	stats, err := replayer.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if stats.Duration <= 0 {
		t.Errorf("Duration should be positive, got %v", stats.Duration)
	}
}
