package table

import (
	"bytes"
	"fmt"
	"testing"
)

func TestTableIteratorPrev(t *testing.T) {
	// Build a table with multiple entries
	opts := DefaultBuilderOptions()
	opts.BlockSize = 100 // Small blocks to test multi-block iteration

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	entries := []struct {
		key   string
		value string
	}{
		{"aaa", "value1"},
		{"bbb", "value2"},
		{"ccc", "value3"},
		{"ddd", "value4"},
		{"eee", "value5"},
	}

	for _, e := range entries {
		builder.Add([]byte(e.key), []byte(e.value))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Seek to last and iterate backwards
	iter.SeekToLast()
	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if string(iter.Key()) != "eee" {
		t.Errorf("expected key 'eee', got %q", iter.Key())
	}

	// Go backwards
	for i := len(entries) - 2; i >= 0; i-- {
		iter.Prev()
		if !iter.Valid() {
			t.Fatalf("Prev should be valid at index %d", i)
		}
		if string(iter.Key()) != entries[i].key {
			t.Errorf("at index %d: expected key %q, got %q", i, entries[i].key, iter.Key())
		}
	}

	// One more Prev should make it invalid
	iter.Prev()
	if iter.Valid() {
		t.Error("Prev past first entry should be invalid")
	}
}

func TestTableIteratorPrevMultiBlock(t *testing.T) {
	// Build a table with many entries that span multiple blocks
	opts := DefaultBuilderOptions()
	opts.BlockSize = 50 // Very small blocks

	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	numEntries := 20
	for i := range numEntries {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		builder.Add([]byte(key), []byte(value))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, err := Open(memFile, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()

	iter := reader.NewIterator()

	// Iterate forward, collecting keys
	var forwardKeys []string
	iter.SeekToFirst()
	for iter.Valid() {
		forwardKeys = append(forwardKeys, string(iter.Key()))
		iter.Next()
	}

	if len(forwardKeys) != numEntries {
		t.Fatalf("forward iteration got %d entries, expected %d", len(forwardKeys), numEntries)
	}

	// Iterate backward from end
	var backwardKeys []string
	iter.SeekToLast()
	for iter.Valid() {
		backwardKeys = append(backwardKeys, string(iter.Key()))
		iter.Prev()
	}

	if len(backwardKeys) != numEntries {
		t.Fatalf("backward iteration got %d entries, expected %d", len(backwardKeys), numEntries)
	}

	// Backward keys should be reverse of forward keys
	for i := range numEntries {
		if forwardKeys[i] != backwardKeys[numEntries-1-i] {
			t.Errorf("mismatch at %d: forward=%q, backward=%q",
				i, forwardKeys[i], backwardKeys[numEntries-1-i])
		}
	}
}

func TestTableIteratorBidirectional(t *testing.T) {
	opts := DefaultBuilderOptions()
	buf := &bytes.Buffer{}
	builder := NewTableBuilder(buf, opts)

	for i := range 10 {
		key := fmt.Sprintf("key%03d", i)
		builder.Add([]byte(key), []byte("value"))
	}
	builder.Finish()

	memFile := NewMemFile(buf.Bytes())
	reader, _ := Open(memFile, ReaderOptions{})
	defer reader.Close()

	iter := reader.NewIterator()

	// Forward 3 steps
	iter.SeekToFirst()
	iter.Next()
	iter.Next()
	iter.Next()

	if !iter.Valid() || string(iter.Key()) != "key003" {
		t.Errorf("after 3 next: expected key003, got %q (valid=%v)", iter.Key(), iter.Valid())
	}

	// Back 2 steps
	iter.Prev()
	iter.Prev()

	if !iter.Valid() || string(iter.Key()) != "key001" {
		t.Errorf("after 2 prev: expected key001, got %q (valid=%v)", iter.Key(), iter.Valid())
	}

	// Forward 1 step
	iter.Next()

	if !iter.Valid() || string(iter.Key()) != "key002" {
		t.Errorf("after 1 next: expected key002, got %q (valid=%v)", iter.Key(), iter.Valid())
	}
}
