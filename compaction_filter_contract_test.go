package rockyardkv

// compaction_filter_contract_test.go implements tests for compaction filter contract.


import (
	"testing"
)

// =============================================================================
// CompactionFilter API Contract Tests
//
// These tests verify that the CompactionFilter interface maintains its
// semantic contract. They document expected behavior and prevent regressions.
//
// Reference: RocksDB v10.7.5 include/rocksdb/compaction_filter.h
// =============================================================================

// TestCompactionFilter_Contract_FilterKeepReturnsEntry verifies that
// FilterKeep decision preserves the entry.
//
// Contract: Filter returning FilterKeep preserves the key-value pair.
func TestCompactionFilter_Contract_FilterKeepReturnsEntry(t *testing.T) {
	filter := &BaseCompactionFilter{}

	decision, _ := filter.Filter(0, []byte("key"), []byte("value"))

	// Contract: BaseCompactionFilter always returns FilterKeep
	if decision != FilterKeep {
		t.Errorf("Expected FilterKeep, got %v", decision)
	}
}

// TestCompactionFilter_Contract_FilterRemoveDeletesEntry verifies that
// FilterRemove decision removes the entry.
//
// Contract: Filter returning FilterRemove removes the key-value pair.
func TestCompactionFilter_Contract_FilterRemoveDeletesEntry(t *testing.T) {
	filter := &RemoveByPrefixFilter{Prefix: []byte("temp_")}

	tests := []struct {
		key      string
		expected CompactionFilterDecision
	}{
		{"temp_123", FilterRemove},
		{"temp_abc", FilterRemove},
		{"data_123", FilterKeep},
		{"other", FilterKeep},
	}

	for _, tc := range tests {
		decision, _ := filter.Filter(0, []byte(tc.key), []byte("value"))

		if decision != tc.expected {
			t.Errorf("Filter(%q) = %v, want %v", tc.key, decision, tc.expected)
		}
	}
}

// TestCompactionFilter_Contract_NameReturnsConsistentValue verifies that
// Name() returns a consistent value.
//
// Contract: Name() returns the same non-empty string on every call.
func TestCompactionFilter_Contract_NameReturnsConsistentValue(t *testing.T) {
	filters := []CompactionFilter{
		&BaseCompactionFilter{},
		&RemoveByPrefixFilter{Prefix: []byte("test")},
		&RemoveByRangeFilter{StartKey: []byte("a"), EndKey: []byte("z")},
	}

	for _, filter := range filters {
		name1 := filter.Name()
		name2 := filter.Name()

		// Contract: Name should be non-empty
		if name1 == "" {
			t.Errorf("Name() returned empty string for %T", filter)
		}

		// Contract: Name should be consistent
		if name1 != name2 {
			t.Errorf("Name() not consistent for %T: %q != %q", filter, name1, name2)
		}
	}
}

// TestCompactionFilter_Contract_RemoveByRangeFilter verifies that
// RemoveByRangeFilter correctly filters keys in range.
//
// Contract: Keys in [StartKey, EndKey) are removed, others are kept.
func TestCompactionFilter_Contract_RemoveByRangeFilter(t *testing.T) {
	filter := &RemoveByRangeFilter{
		StartKey: []byte("c"),
		EndKey:   []byte("f"),
	}

	tests := []struct {
		key      string
		expected CompactionFilterDecision
	}{
		{"a", FilterKeep},   // Before range
		{"b", FilterKeep},   // Before range
		{"c", FilterRemove}, // At start (inclusive)
		{"d", FilterRemove}, // In range
		{"e", FilterRemove}, // In range
		{"f", FilterKeep},   // At end (exclusive)
		{"g", FilterKeep},   // After range
		{"z", FilterKeep},   // After range
	}

	for _, tc := range tests {
		decision, _ := filter.Filter(0, []byte(tc.key), []byte("value"))

		if decision != tc.expected {
			t.Errorf("Filter(%q) = %v, want %v", tc.key, decision, tc.expected)
		}
	}
}

// TestCompactionFilter_Contract_FilterMergeOperandDefault verifies that
// the default FilterMergeOperand returns FilterKeep.
//
// Contract: Default FilterMergeOperand implementation returns FilterKeep.
func TestCompactionFilter_Contract_FilterMergeOperandDefault(t *testing.T) {
	filter := &BaseCompactionFilter{}

	decision := filter.FilterMergeOperand(0, []byte("key"), []byte("operand"))

	// Contract: Default should keep merge operands
	if decision != FilterKeep {
		t.Errorf("Expected FilterKeep for merge operand, got %v", decision)
	}
}

// TestCompactionFilter_Contract_LevelParameter verifies that the level
// parameter is correctly passed to Filter.
//
// Contract: Filter receives the correct compaction level.
func TestCompactionFilter_Contract_LevelParameter(t *testing.T) {
	var capturedLevel int
	filter := &levelCapturingFilter{
		captureLevel: &capturedLevel,
	}

	for level := range 5 {
		filter.Filter(level, []byte("key"), []byte("value"))

		if capturedLevel != level {
			t.Errorf("Filter received level %d, expected %d", capturedLevel, level)
		}
	}
}

// TestCompactionFilter_Contract_PrefixMatchingEdgeCases verifies edge cases
// for prefix matching.
//
// Contract: Prefix matching is exact and handles edge cases correctly.
func TestCompactionFilter_Contract_PrefixMatchingEdgeCases(t *testing.T) {
	filter := &RemoveByPrefixFilter{Prefix: []byte("abc")}

	tests := []struct {
		key      string
		expected CompactionFilterDecision
	}{
		{"abc", FilterRemove},    // Exact match
		{"abcd", FilterRemove},   // Prefix match
		{"abc123", FilterRemove}, // Prefix match
		{"ab", FilterKeep},       // Too short
		{"abd", FilterKeep},      // Different prefix
		{"xyzabc", FilterKeep},   // Prefix not at start
		{"", FilterKeep},         // Empty key
	}

	for _, tc := range tests {
		decision, _ := filter.Filter(0, []byte(tc.key), []byte("value"))

		if decision != tc.expected {
			t.Errorf("Filter(%q) = %v, want %v", tc.key, decision, tc.expected)
		}
	}
}

// TestCompactionFilter_Contract_EmptyPrefix verifies behavior with empty prefix.
//
// Contract: Empty prefix matches all keys.
func TestCompactionFilter_Contract_EmptyPrefix(t *testing.T) {
	filter := &RemoveByPrefixFilter{Prefix: []byte{}}

	tests := []struct {
		key      string
		expected CompactionFilterDecision
	}{
		{"any", FilterRemove},
		{"key", FilterRemove},
		{"", FilterRemove},
	}

	for _, tc := range tests {
		decision, _ := filter.Filter(0, []byte(tc.key), []byte("value"))

		if decision != tc.expected {
			t.Errorf("Filter(%q) with empty prefix = %v, want %v", tc.key, decision, tc.expected)
		}
	}
}

// TestCompactionFilter_Contract_OpenRanges verifies behavior with open-ended ranges.
//
// Contract: nil StartKey means from beginning, nil EndKey means to end.
func TestCompactionFilter_Contract_OpenRanges(t *testing.T) {
	// No start bound
	filterNoStart := &RemoveByRangeFilter{
		StartKey: nil,
		EndKey:   []byte("m"),
	}

	// No end bound
	filterNoEnd := &RemoveByRangeFilter{
		StartKey: []byte("m"),
		EndKey:   nil,
	}

	tests := []struct {
		filter   *RemoveByRangeFilter
		key      string
		expected CompactionFilterDecision
	}{
		{filterNoStart, "a", FilterRemove},
		{filterNoStart, "l", FilterRemove},
		{filterNoStart, "m", FilterKeep},
		{filterNoStart, "z", FilterKeep},
		{filterNoEnd, "a", FilterKeep},
		{filterNoEnd, "l", FilterKeep},
		{filterNoEnd, "m", FilterRemove},
		{filterNoEnd, "z", FilterRemove},
	}

	for _, tc := range tests {
		decision, _ := tc.filter.Filter(0, []byte(tc.key), []byte("value"))

		if decision != tc.expected {
			t.Errorf("Filter(%q) = %v, want %v", tc.key, decision, tc.expected)
		}
	}
}

// =============================================================================
// Custom Filters for Testing
// =============================================================================

// levelCapturingFilter captures the level passed to Filter.
type levelCapturingFilter struct {
	BaseCompactionFilter
	captureLevel *int
}

func (f *levelCapturingFilter) Name() string {
	return "levelCapturingFilter"
}

func (f *levelCapturingFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	*f.captureLevel = level
	return FilterKeep, nil
}
