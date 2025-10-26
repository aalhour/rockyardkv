package iterator

import (
	"bytes"
	"errors"
	"testing"
)

// mockIterator is a simple iterator over a slice of key-value pairs.
type mockIterator struct {
	entries []kvEntry
	pos     int
	err     error
}

type kvEntry struct {
	key   []byte
	value []byte
}

func newMockIterator(entries []kvEntry) *mockIterator {
	return &mockIterator{
		entries: entries,
		pos:     -1,
	}
}

func (m *mockIterator) Valid() bool {
	return m.pos >= 0 && m.pos < len(m.entries)
}

func (m *mockIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}
	return m.entries[m.pos].key
}

func (m *mockIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}
	return m.entries[m.pos].value
}

func (m *mockIterator) SeekToFirst() {
	if len(m.entries) > 0 {
		m.pos = 0
	} else {
		m.pos = -1
	}
}

func (m *mockIterator) SeekToLast() {
	if len(m.entries) > 0 {
		m.pos = len(m.entries) - 1
	} else {
		m.pos = -1
	}
}

func (m *mockIterator) Seek(target []byte) {
	for i, e := range m.entries {
		if bytes.Compare(e.key, target) >= 0 {
			m.pos = i
			return
		}
	}
	m.pos = -1
}

func (m *mockIterator) Next() {
	if m.Valid() {
		m.pos++
		if m.pos >= len(m.entries) {
			m.pos = -1
		}
	}
}

func (m *mockIterator) Prev() {
	if m.Valid() {
		m.pos--
		if m.pos < 0 {
			m.pos = -1
		}
	}
}

func (m *mockIterator) Error() error {
	return m.err
}

// bytewiseCompare is a simple bytewise comparator for testing.
func bytewiseCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestMergingIteratorEmpty(t *testing.T) {
	mi := NewMergingIterator(nil, bytewiseCompare)
	mi.SeekToFirst()
	if mi.Valid() {
		t.Error("Empty merging iterator should be invalid")
	}
}

func TestMergingIteratorSingleChild(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	expected := []string{"a", "b", "c"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Next()
	}

	if mi.Valid() {
		t.Error("Should be invalid after last entry")
	}
}

func TestMergingIteratorTwoChildren(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
		{[]byte("e"), []byte("5")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
		{[]byte("d"), []byte("4")},
		{[]byte("f"), []byte("6")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e", "f"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Next()
	}

	if mi.Valid() {
		t.Error("Should be invalid after last entry")
	}
}

func TestMergingIteratorOverlapping(t *testing.T) {
	// Children with overlapping key ranges
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("v1")},
		{[]byte("b"), []byte("v1")},
		{[]byte("c"), []byte("v1")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("v2")},
		{[]byte("b"), []byte("v2")},
		{[]byte("c"), []byte("v2")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToFirst()

	// Should see all 6 entries (no deduplication)
	count := 0
	for mi.Valid() {
		count++
		mi.Next()
	}

	if count != 6 {
		t.Errorf("Expected 6 entries, got %d", count)
	}
}

func TestMergingIteratorThreeChildren(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("d"), []byte("4")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
		{[]byte("e"), []byte("5")},
	})
	child3 := newMockIterator([]kvEntry{
		{[]byte("c"), []byte("3")},
		{[]byte("f"), []byte("6")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2, child3}, bytewiseCompare)
	mi.SeekToFirst()

	expected := []string{"a", "b", "c", "d", "e", "f"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Next()
	}
}

func TestMergingIteratorSeek(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
		{[]byte("e"), []byte("5")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
		{[]byte("d"), []byte("4")},
		{[]byte("f"), []byte("6")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)

	// Seek to existing key
	mi.Seek([]byte("c"))
	if !mi.Valid() || string(mi.Key()) != "c" {
		t.Errorf("Seek(c) = %s, want c", mi.Key())
	}

	// Seek to non-existing key (should find next)
	mi.Seek([]byte("cc"))
	if !mi.Valid() || string(mi.Key()) != "d" {
		t.Errorf("Seek(cc) = %s, want d", mi.Key())
	}

	// Seek beyond last
	mi.Seek([]byte("z"))
	if mi.Valid() {
		t.Error("Seek beyond last should be invalid")
	}

	// Seek before first
	mi.Seek([]byte(""))
	if !mi.Valid() || string(mi.Key()) != "a" {
		t.Errorf("Seek('') = %s, want a", mi.Key())
	}
}

func TestMergingIteratorSeekToLast(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
		{[]byte("d"), []byte("4")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToLast()

	if !mi.Valid() || string(mi.Key()) != "d" {
		t.Errorf("SeekToLast = %s, want d", mi.Key())
	}
}

func TestMergingIteratorEmptyChild(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
	})
	child2 := newMockIterator([]kvEntry{}) // empty
	child3 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2, child3}, bytewiseCompare)
	mi.SeekToFirst()

	expected := []string{"a", "b", "c"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Next()
	}
}

func TestMergingIteratorManyChildren(t *testing.T) {
	// Create 10 children, each with 10 entries
	children := make([]Iterator, 10)
	totalEntries := 0
	for i := range 10 {
		entries := make([]kvEntry, 10)
		for j := range 10 {
			// Keys are like "00", "01", ..., "99"
			key := []byte{byte('0' + i), byte('0' + j)}
			entries[j] = kvEntry{key: key, value: []byte{byte(i*10 + j)}}
			totalEntries++
		}
		children[i] = newMockIterator(entries)
	}

	mi := NewMergingIterator(children, bytewiseCompare)
	mi.SeekToFirst()

	count := 0
	var prevKey []byte
	for mi.Valid() {
		if prevKey != nil && bytes.Compare(prevKey, mi.Key()) > 0 {
			t.Errorf("Keys not in order: %s > %s", prevKey, mi.Key())
		}
		prevKey = append([]byte{}, mi.Key()...)
		count++
		mi.Next()
	}

	if count != totalEntries {
		t.Errorf("Iterated %d entries, want %d", count, totalEntries)
	}
}

func TestMergingIteratorReseek(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)

	// First iteration
	mi.SeekToFirst()
	mi.Next()
	if string(mi.Key()) != "b" {
		t.Errorf("After Next, key = %s, want b", mi.Key())
	}

	// Re-seek
	mi.SeekToFirst()
	if string(mi.Key()) != "a" {
		t.Errorf("After re-SeekToFirst, key = %s, want a", mi.Key())
	}
}

func TestMergingIteratorDuplicateKeys(t *testing.T) {
	// Same key from multiple children with different values
	child1 := newMockIterator([]kvEntry{
		{[]byte("key"), []byte("value1")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("key"), []byte("value2")},
	})
	child3 := newMockIterator([]kvEntry{
		{[]byte("key"), []byte("value3")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2, child3}, bytewiseCompare)
	mi.SeekToFirst()

	// Should yield all three entries (merging iterator doesn't dedupe)
	count := 0
	for mi.Valid() {
		if string(mi.Key()) != "key" {
			t.Errorf("Expected key 'key', got %s", mi.Key())
		}
		count++
		mi.Next()
	}

	if count != 3 {
		t.Errorf("Expected 3 entries, got %d", count)
	}
}

// =============================================================================
// Additional Iterator Tests
// =============================================================================

func TestMergingIteratorPrev(t *testing.T) {
	child1 := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
		{[]byte("e"), []byte("5")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("b"), []byte("2")},
		{[]byte("d"), []byte("4")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToLast()

	if !mi.Valid() || string(mi.Key()) != "e" {
		t.Errorf("SeekToLast = %s, want e", mi.Key())
	}

	// Go backwards through e, d, c, b, a
	expected := []string{"e", "d", "c", "b", "a"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Prev()
	}
}

func TestMergingIteratorPrevFromMiddle(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
		{[]byte("d"), []byte("4")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.Seek([]byte("c"))

	if string(mi.Key()) != "c" {
		t.Fatalf("Seek(c) = %s, want c", mi.Key())
	}

	mi.Prev()
	if !mi.Valid() || string(mi.Key()) != "b" {
		t.Errorf("After Prev from c = %s, want b", mi.Key())
	}
}

func TestMergingIteratorNextPrevCycle(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	// a -> b
	if string(mi.Key()) != "a" {
		t.Fatalf("Start = %s, want a", mi.Key())
	}
	mi.Next()
	if string(mi.Key()) != "b" {
		t.Fatalf("After Next = %s, want b", mi.Key())
	}

	// b -> a
	mi.Prev()
	if string(mi.Key()) != "a" {
		t.Fatalf("After Prev = %s, want a", mi.Key())
	}

	// a -> b -> c
	mi.Next()
	mi.Next()
	if string(mi.Key()) != "c" {
		t.Fatalf("After 2x Next = %s, want c", mi.Key())
	}
}

func TestMergingIteratorAllEmptyChildren(t *testing.T) {
	children := []Iterator{
		newMockIterator([]kvEntry{}),
		newMockIterator([]kvEntry{}),
		newMockIterator([]kvEntry{}),
	}

	mi := NewMergingIterator(children, bytewiseCompare)

	mi.SeekToFirst()
	if mi.Valid() {
		t.Error("All empty children should be invalid after SeekToFirst")
	}

	mi.SeekToLast()
	if mi.Valid() {
		t.Error("All empty children should be invalid after SeekToLast")
	}

	mi.Seek([]byte("any"))
	if mi.Valid() {
		t.Error("All empty children should be invalid after Seek")
	}
}

func TestMergingIteratorKeyValueAfterInvalid(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("only"), []byte("one")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()
	mi.Next() // Now invalid

	if mi.Valid() {
		t.Error("Should be invalid after exhausting entries")
	}
	if mi.Key() != nil {
		t.Errorf("Key() when invalid should be nil, got %s", mi.Key())
	}
	if mi.Value() != nil {
		t.Errorf("Value() when invalid should be nil, got %s", mi.Value())
	}
}

func TestMergingIteratorNextOnInvalid(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()
	mi.Next() // Now invalid
	mi.Next() // Should be a no-op, not panic

	if mi.Valid() {
		t.Error("Should still be invalid")
	}
}

func TestMergingIteratorPrevOnInvalid(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	// Don't seek, so it's initially invalid
	mi.Prev() // Should be a no-op, not panic

	if mi.Valid() {
		t.Error("Should still be invalid")
	}
}

func TestMergingIteratorSeekExact(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("aa"), []byte("1")},
		{[]byte("bb"), []byte("2")},
		{[]byte("cc"), []byte("3")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)

	tests := []struct {
		target string
		want   string
	}{
		{"aa", "aa"},
		{"bb", "bb"},
		{"cc", "cc"},
	}

	for _, tt := range tests {
		mi.Seek([]byte(tt.target))
		if !mi.Valid() {
			t.Errorf("Seek(%s) should be valid", tt.target)
			continue
		}
		if string(mi.Key()) != tt.want {
			t.Errorf("Seek(%s) = %s, want %s", tt.target, mi.Key(), tt.want)
		}
	}
}

func TestMergingIteratorSeekBetween(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("c"), []byte("3")},
		{[]byte("e"), []byte("5")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)

	tests := []struct {
		target string
		want   string
	}{
		{"b", "c"},
		{"d", "e"},
		{"aa", "c"},
	}

	for _, tt := range tests {
		mi.Seek([]byte(tt.target))
		if !mi.Valid() {
			t.Errorf("Seek(%s) should be valid", tt.target)
			continue
		}
		if string(mi.Key()) != tt.want {
			t.Errorf("Seek(%s) = %s, want %s", tt.target, mi.Key(), tt.want)
		}
	}
}

func TestMergingIteratorValueCopying(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("value1")},
		{[]byte("b"), []byte("value2")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	// Get value and key
	key1 := mi.Key()
	val1 := mi.Value()

	mi.Next()

	key2 := mi.Key()
	val2 := mi.Value()

	// Check that values are correct
	if string(key1) != "a" || string(val1) != "value1" {
		t.Errorf("First entry = %s:%s, want a:value1", key1, val1)
	}
	if string(key2) != "b" || string(val2) != "value2" {
		t.Errorf("Second entry = %s:%s, want b:value2", key2, val2)
	}
}

func TestMergingIteratorWithNilComparator(t *testing.T) {
	// Should use default internal key comparator
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
	})

	mi := NewMergingIterator([]Iterator{child}, nil)
	mi.SeekToFirst()

	if !mi.Valid() {
		t.Error("Should be valid with nil comparator")
	}
}

func TestMergingIteratorLargeValues(t *testing.T) {
	// Create entries with large values
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	child := newMockIterator([]kvEntry{
		{[]byte("key1"), largeValue},
		{[]byte("key2"), largeValue},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	if !mi.Valid() {
		t.Fatal("Should be valid")
	}
	if len(mi.Value()) != len(largeValue) {
		t.Errorf("Value length = %d, want %d", len(mi.Value()), len(largeValue))
	}
}

func TestMergingIteratorBinaryKeys(t *testing.T) {
	// Keys with binary data
	child1 := newMockIterator([]kvEntry{
		{[]byte{0x00, 0x00}, []byte("zero")},
		{[]byte{0x00, 0xFF}, []byte("mixed")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte{0x00, 0x80}, []byte("mid")},
		{[]byte{0xFF, 0xFF}, []byte("max")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToFirst()

	// Should iterate in binary order
	expected := [][]byte{
		{0x00, 0x00},
		{0x00, 0x80},
		{0x00, 0xFF},
		{0xFF, 0xFF},
	}

	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if !bytes.Equal(mi.Key(), exp) {
			t.Errorf("Key %d = %v, want %v", i, mi.Key(), exp)
		}
		mi.Next()
	}
}

func TestMergingIteratorInterleaved(t *testing.T) {
	// Keys that interleave between children
	child1 := newMockIterator([]kvEntry{
		{[]byte("a1"), []byte("c1")},
		{[]byte("a3"), []byte("c1")},
		{[]byte("a5"), []byte("c1")},
	})
	child2 := newMockIterator([]kvEntry{
		{[]byte("a2"), []byte("c2")},
		{[]byte("a4"), []byte("c2")},
		{[]byte("a6"), []byte("c2")},
	})

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.SeekToFirst()

	expected := []string{"a1", "a2", "a3", "a4", "a5", "a6"}
	for i, exp := range expected {
		if !mi.Valid() {
			t.Fatalf("Expected valid at position %d", i)
		}
		if string(mi.Key()) != exp {
			t.Errorf("Key %d = %s, want %s", i, mi.Key(), exp)
		}
		mi.Next()
	}
}

// errorIterator is an iterator that returns an error.
type errorIterator struct {
	err error
}

func (e *errorIterator) Valid() bool        { return false }
func (e *errorIterator) Key() []byte        { return nil }
func (e *errorIterator) Value() []byte      { return nil }
func (e *errorIterator) SeekToFirst()       {}
func (e *errorIterator) SeekToLast()        {}
func (e *errorIterator) Seek(target []byte) {}
func (e *errorIterator) Next()              {}
func (e *errorIterator) Prev()              {}
func (e *errorIterator) Error() error       { return e.err }

func TestMergingIteratorError(t *testing.T) {
	testErr := bytes.ErrTooLarge // Just using any error
	child := &errorIterator{err: testErr}

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	if !errors.Is(mi.Error(), testErr) {
		t.Errorf("Error() = %v, want %v", mi.Error(), testErr)
	}
	if mi.Valid() {
		t.Error("Should be invalid on error")
	}
}

func TestMergingIteratorErrorDuringSeek(t *testing.T) {
	testErr := bytes.ErrTooLarge
	child1 := newMockIterator([]kvEntry{{[]byte("a"), []byte("1")}})
	child2 := &errorIterator{err: testErr}

	mi := NewMergingIterator([]Iterator{child1, child2}, bytewiseCompare)
	mi.Seek([]byte("a"))

	if !errors.Is(mi.Error(), testErr) {
		t.Errorf("Error() = %v, want %v", mi.Error(), testErr)
	}
}

func TestMergingIteratorSingleEntry(t *testing.T) {
	child := newMockIterator([]kvEntry{
		{[]byte("only"), []byte("one")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	if !mi.Valid() {
		t.Fatal("Should be valid")
	}
	if string(mi.Key()) != "only" {
		t.Errorf("Key = %s, want only", mi.Key())
	}
	if string(mi.Value()) != "one" {
		t.Errorf("Value = %s, want one", mi.Value())
	}

	mi.Next()
	if mi.Valid() {
		t.Error("Should be invalid after only entry")
	}
}

func TestMergingIteratorSeekToLastEmpty(t *testing.T) {
	mi := NewMergingIterator(nil, bytewiseCompare)
	mi.SeekToLast()

	if mi.Valid() {
		t.Error("SeekToLast on empty should be invalid")
	}
}

func TestMergingIteratorStability(t *testing.T) {
	// Test that the iterator is stable across multiple operations
	child := newMockIterator([]kvEntry{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)

	// Multiple SeekToFirst should be idempotent
	for i := range 5 {
		mi.SeekToFirst()
		if !mi.Valid() || string(mi.Key()) != "a" {
			t.Errorf("Iteration %d: SeekToFirst = %s, want a", i, mi.Key())
		}
	}

	// Multiple Seek to same key should be idempotent
	for i := range 5 {
		mi.Seek([]byte("b"))
		if !mi.Valid() || string(mi.Key()) != "b" {
			t.Errorf("Iteration %d: Seek(b) = %s, want b", i, mi.Key())
		}
	}
}

func TestMergingIteratorEmptyKeys(t *testing.T) {
	// Some implementations might have issues with empty keys
	child := newMockIterator([]kvEntry{
		{[]byte{}, []byte("empty")},
		{[]byte("a"), []byte("1")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	if !mi.Valid() {
		t.Fatal("Should be valid")
	}
	if len(mi.Key()) != 0 {
		t.Errorf("First key should be empty, got %v", mi.Key())
	}
	if string(mi.Value()) != "empty" {
		t.Errorf("Value = %s, want empty", mi.Value())
	}
}

func TestMergingIteratorLongKeys(t *testing.T) {
	// Very long keys
	longKey := bytes.Repeat([]byte("x"), 10000)

	child := newMockIterator([]kvEntry{
		{longKey, []byte("long")},
	})

	mi := NewMergingIterator([]Iterator{child}, bytewiseCompare)
	mi.SeekToFirst()

	if !mi.Valid() {
		t.Fatal("Should be valid")
	}
	if !bytes.Equal(mi.Key(), longKey) {
		t.Error("Key mismatch for long key")
	}
}
