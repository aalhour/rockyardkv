// comparator_contract_test.go implements tests for comparator contract.
package rockyardkv

import (
	"path/filepath"
	"sort"
	"testing"
)

// =============================================================================
// Comparator API Contract Tests
//
// These tests verify that the Comparator interface maintains its semantic
// contract. They document expected behavior and prevent regressions.
//
// Reference: RocksDB v10.7.5 include/rocksdb/comparator.h
// =============================================================================

// TestComparator_Contract_CompareOrdering verifies that Compare() defines
// a strict total ordering.
//
// Contract: Compare(a, b) < 0 iff a < b, == 0 iff a == b, > 0 iff a > b.
func TestComparator_Contract_CompareOrdering(t *testing.T) {
	cmp := BytewiseComparator{}

	tests := []struct {
		a, b     string
		expected int // -1, 0, or 1
	}{
		{"aaa", "bbb", -1},
		{"bbb", "aaa", 1},
		{"aaa", "aaa", 0},
		{"", "", 0},
		{"a", "", 1},
		{"", "a", -1},
		{"abc", "abd", -1},
		{"abc", "ab", 1},
		{"ab", "abc", -1},
	}

	for _, tc := range tests {
		result := cmp.Compare([]byte(tc.a), []byte(tc.b))
		normalized := normalizeCompare(result)

		if normalized != tc.expected {
			t.Errorf("Compare(%q, %q) = %d, want %d", tc.a, tc.b, normalized, tc.expected)
		}
	}
}

// normalizeCompare converts any negative to -1, any positive to 1
func normalizeCompare(n int) int {
	if n < 0 {
		return -1
	}
	if n > 0 {
		return 1
	}
	return 0
}

// TestComparator_Contract_Transitivity verifies that Compare() is transitive.
//
// Contract: If a < b and b < c, then a < c.
func TestComparator_Contract_Transitivity(t *testing.T) {
	cmp := BytewiseComparator{}

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			for k := j + 1; k < len(keys); k++ {
				a, b, c := []byte(keys[i]), []byte(keys[j]), []byte(keys[k])

				ab := cmp.Compare(a, b)
				bc := cmp.Compare(b, c)
				ac := cmp.Compare(a, c)

				// Contract: If a < b and b < c, then a < c
				if ab < 0 && bc < 0 && ac >= 0 {
					t.Errorf("Transitivity violated: %q < %q < %q but Compare(%q, %q) = %d",
						keys[i], keys[j], keys[k], keys[i], keys[k], ac)
				}
			}
		}
	}
}

// TestComparator_Contract_Antisymmetry verifies that Compare() is antisymmetric.
//
// Contract: Compare(a, b) == -Compare(b, a).
func TestComparator_Contract_Antisymmetry(t *testing.T) {
	cmp := BytewiseComparator{}

	keys := []string{"aaa", "bbb", "ccc", "", "abc", "abd"}

	for i := range keys {
		for j := range keys {
			a, b := []byte(keys[i]), []byte(keys[j])
			ab := cmp.Compare(a, b)
			ba := cmp.Compare(b, a)

			// Contract: Compare(a,b) == -Compare(b,a)
			if normalizeCompare(ab) != -normalizeCompare(ba) {
				t.Errorf("Antisymmetry violated: Compare(%q, %q) = %d, Compare(%q, %q) = %d",
					keys[i], keys[j], ab, keys[j], keys[i], ba)
			}
		}
	}
}

// TestComparator_Contract_NameIsConsistent verifies that Name() returns
// a consistent value.
//
// Contract: Name() returns the same non-empty string on every call.
func TestComparator_Contract_NameIsConsistent(t *testing.T) {
	cmp := BytewiseComparator{}

	name1 := cmp.Name()
	name2 := cmp.Name()

	// Contract: Name should be non-empty
	if name1 == "" {
		t.Error("Name() returned empty string")
	}

	// Contract: Name should be consistent
	if name1 != name2 {
		t.Errorf("Name() not consistent: %q != %q", name1, name2)
	}
}

// TestComparator_Contract_FindShortestSeparatorBounds verifies that
// FindShortestSeparator returns a key between a and b.
//
// Contract: a <= FindShortestSeparator(a, b) < b.
func TestComparator_Contract_FindShortestSeparatorBounds(t *testing.T) {
	cmp := BytewiseComparator{}

	tests := []struct {
		a, b string
	}{
		{"aaa", "bbb"},
		{"abc", "abd"},
		{"abc", "abcd"},
		{"abc", "xyz"},
		{"prefix_123", "prefix_456"},
	}

	for _, tc := range tests {
		a, b := []byte(tc.a), []byte(tc.b)
		sep := cmp.FindShortestSeparator(a, b)

		// Contract: a <= sep
		if cmp.Compare(a, sep) > 0 {
			t.Errorf("FindShortestSeparator(%q, %q) = %q, but a > sep", tc.a, tc.b, sep)
		}

		// Contract: sep < b (or sep == a when no shorter separator exists)
		if cmp.Compare(sep, b) >= 0 && cmp.Compare(sep, a) != 0 {
			t.Errorf("FindShortestSeparator(%q, %q) = %q, but sep >= b", tc.a, tc.b, sep)
		}
	}
}

// TestComparator_Contract_FindShortSuccessorBounds verifies that
// FindShortSuccessor returns a key >= a.
//
// Contract: FindShortSuccessor(a) >= a.
func TestComparator_Contract_FindShortSuccessorBounds(t *testing.T) {
	cmp := BytewiseComparator{}

	keys := []string{"aaa", "abc", "xyz", "\x00\x00", "\xff\xff\xff"}

	for _, k := range keys {
		a := []byte(k)
		succ := cmp.FindShortSuccessor(a)

		// Contract: succ >= a
		if cmp.Compare(succ, a) < 0 {
			t.Errorf("FindShortSuccessor(%q) = %q, but succ < a", k, succ)
		}
	}
}

// TestComparator_Contract_IntegrationWithDB verifies that a custom
// comparator affects iteration order.
//
// Contract: Iterator order matches comparator ordering.
func TestComparator_Contract_IntegrationWithDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	// Use reverse comparator
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = &ReverseComparator{}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert keys
	keys := []string{"aaa", "bbb", "ccc", "ddd"}
	for _, k := range keys {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Collect keys via forward iteration
	iter := db.NewIterator(nil)
	defer iter.Close()

	var collected []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		collected = append(collected, string(iter.Key()))
	}

	// Contract: Keys should be in reverse order
	expected := make([]string, len(keys))
	copy(expected, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(expected)))

	if len(collected) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(collected))
	}

	for i, k := range collected {
		if k != expected[i] {
			t.Errorf("Key at position %d: got %q, want %q", i, k, expected[i])
		}
	}
}

// TestComparator_Contract_SeekRespectsComparator verifies that Seek()
// uses the comparator for positioning.
//
// Contract: Seek(target) positions at first key >= target per comparator.
func TestComparator_Contract_SeekRespectsComparator(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")

	// Use reverse comparator
	opts := DefaultOptions()
	opts.CreateIfMissing = true
	opts.Comparator = &ReverseComparator{}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Insert keys
	for _, k := range []string{"aaa", "bbb", "ccc", "ddd"} {
		if err := db.Put(nil, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := db.NewIterator(nil)
	defer iter.Close()

	// With reverse comparator, "bbb" > "ccc" > "ddd"
	// So Seek("ccc") should land on "ccc" or the next larger key per reverse order
	iter.Seek([]byte("ccc"))

	if !iter.Valid() {
		t.Fatal("Iterator not valid after Seek")
	}

	// Contract: With reverse comparator, Seek("ccc") should position at "ccc"
	// (since "ccc" is the first key >= "ccc" in reverse order)
	if string(iter.Key()) != "ccc" {
		t.Errorf("Seek('ccc') with reverse comparator: got %q", iter.Key())
	}
}

// =============================================================================
// Custom Comparators for Testing
// =============================================================================

// ReverseComparator compares keys in reverse lexicographical order.
type ReverseComparator struct{}

func (c *ReverseComparator) Compare(a, b []byte) int {
	// Reverse the comparison
	return -BytewiseComparator{}.Compare(a, b)
}

func (c *ReverseComparator) Name() string {
	return "ReverseComparator"
}

func (c *ReverseComparator) FindShortestSeparator(a, b []byte) []byte {
	return a // Simple implementation
}

func (c *ReverseComparator) FindShortSuccessor(a []byte) []byte {
	return a // Simple implementation
}
