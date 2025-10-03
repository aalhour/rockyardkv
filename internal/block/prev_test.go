package block

import (
	"fmt"
	"testing"
)

func TestBlockIteratorPrev(t *testing.T) {
	// Build a block with multiple entries
	builder := NewBuilder(4)

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

	data := builder.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	iter := block.NewIterator()

	// First verify forward iteration works
	t.Log("Forward iteration:")
	iter.SeekToFirst()
	for iter.Valid() {
		t.Logf("  key=%q", iter.Key())
		iter.Next()
	}

	// Seek to last and iterate backwards
	iter.SeekToLast()
	t.Logf("After SeekToLast: key=%q valid=%v", iter.Key(), iter.Valid())

	if !iter.Valid() {
		t.Fatal("SeekToLast should be valid")
	}
	if string(iter.Key()) != "eee" {
		t.Errorf("expected key 'eee', got %q", iter.Key())
	}

	// Go backwards
	for i := len(entries) - 2; i >= 0; i-- {
		t.Logf("Calling Prev, expecting index %d (key=%q)", i, entries[i].key)
		iter.Prev()
		t.Logf("After Prev: valid=%v key=%q", iter.Valid(), iter.Key())
		if !iter.Valid() {
			t.Fatalf("Prev should be valid at index %d", i)
		}
		if string(iter.Key()) != entries[i].key {
			t.Errorf("at index %d: expected key %q, got %q", i, entries[i].key, iter.Key())
		}
		if string(iter.Value()) != entries[i].value {
			t.Errorf("at index %d: expected value %q, got %q", i, entries[i].value, iter.Value())
		}
	}

	// One more Prev should make it invalid
	iter.Prev()
	if iter.Valid() {
		t.Error("Prev past first entry should be invalid")
	}
}

func TestBlockIteratorPrevFromMiddle(t *testing.T) {
	// Build a block with entries
	builder := NewBuilder(2)

	for i := range 10 {
		builder.Add(fmt.Appendf(nil, "key%03d", i), fmt.Appendf(nil, "value%03d", i))
	}

	data := builder.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	iter := block.NewIterator()

	// Seek to middle
	iter.Seek([]byte("key005"))
	if !iter.Valid() {
		t.Fatal("Seek should be valid")
	}
	if string(iter.Key()) != "key005" {
		t.Errorf("expected key 'key005', got %q", iter.Key())
	}

	// Go back one
	iter.Prev()
	if !iter.Valid() {
		t.Fatal("Prev should be valid")
	}
	if string(iter.Key()) != "key004" {
		t.Errorf("expected key 'key004', got %q", iter.Key())
	}
}

func TestBlockIteratorForwardThenBackward(t *testing.T) {
	builder := NewBuilder(4)

	for i := range 5 {
		builder.Add(fmt.Appendf(nil, "key%d", i), fmt.Appendf(nil, "val%d", i))
	}

	data := builder.Finish()
	block, err := NewBlock(data)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	iter := block.NewIterator()

	// Forward iteration
	iter.SeekToFirst()
	for i := range 3 {
		if !iter.Valid() {
			t.Fatalf("should be valid at forward step %d", i)
		}
		iter.Next()
	}

	// Now at key3, go back
	if string(iter.Key()) != "key3" {
		t.Errorf("expected key3, got %q", iter.Key())
	}

	iter.Prev()
	if string(iter.Key()) != "key2" {
		t.Errorf("expected key2 after Prev, got %q", iter.Key())
	}

	iter.Next()
	if string(iter.Key()) != "key3" {
		t.Errorf("expected key3 after Next, got %q", iter.Key())
	}
}
