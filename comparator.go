package rockyardkv

// comparator.go implements key comparison.
//
// Comparator defines the total ordering over keys in the database.
// The default is bytewise comparison. Custom comparators enable
// application-specific key ordering.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/comparator.h


import "bytes"

// Comparator defines a total ordering over keys.
type Comparator interface {
	// Compare returns a value < 0 if a < b, 0 if a == b, > 0 if a > b.
	Compare(a, b []byte) int

	// Name returns the name of the comparator.
	Name() string

	// FindShortestSeparator finds a key k such that a <= k < b.
	// This is used to shorten keys in index blocks.
	// If no such key exists, a should be returned unchanged.
	FindShortestSeparator(a, b []byte) []byte

	// FindShortSuccessor finds a short key that is >= a.
	// This is used to shorten keys at the end of an index block.
	FindShortSuccessor(a []byte) []byte
}

// BytewiseComparator is the default comparator that compares keys lexicographically.
type BytewiseComparator struct{}

// Compare compares two keys lexicographically.
func (c BytewiseComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Name returns the comparator name.
func (c BytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

// FindShortestSeparator finds a key between a and b.
func (c BytewiseComparator) FindShortestSeparator(a, b []byte) []byte {
	// Find the common prefix
	minLen := min(len(b), len(a))

	diffIndex := 0
	for diffIndex < minLen && a[diffIndex] == b[diffIndex] {
		diffIndex++
	}

	if diffIndex >= minLen {
		// One is a prefix of another
		return a
	}

	// Try to increment the byte at diffIndex
	diffByte := a[diffIndex]
	if diffByte < 0xFF && diffByte+1 < b[diffIndex] {
		result := make([]byte, diffIndex+1)
		copy(result, a[:diffIndex+1])
		result[diffIndex]++
		return result
	}

	return a
}

// FindShortSuccessor finds a short key >= a.
func (c BytewiseComparator) FindShortSuccessor(a []byte) []byte {
	// Find first byte that can be incremented
	for i := range a {
		if a[i] != 0xFF {
			result := make([]byte, i+1)
			copy(result, a[:i+1])
			result[i]++
			return result
		}
	}
	// All bytes are 0xFF
	return a
}

// DefaultComparator returns the default bytewise comparator.
func DefaultComparator() Comparator {
	return BytewiseComparator{}
}
