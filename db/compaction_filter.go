// Package db provides the main database interface and implementation.
// This file implements Compaction Filter support.
//
// Compaction filters allow users to filter out or modify key-value pairs
// during compaction. This is useful for:
// - TTL (time-to-live) - removing expired keys
// - Garbage collection - removing unused data
// - Data transformation - modifying values during compaction
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/compaction_filter.h
//   - db/compaction/compaction_iterator.cc
package db

import "github.com/aalhour/rockyardkv/internal/dbformat"

// CompactionFilterDecision represents the decision made by a compaction filter.
type CompactionFilterDecision int

const (
	// FilterKeep keeps the key-value pair unchanged.
	FilterKeep CompactionFilterDecision = iota

	// FilterRemove removes the key-value pair from the database.
	FilterRemove

	// FilterChange changes the value of the key-value pair.
	// The new value should be set via the compaction filter context.
	FilterChange
)

// CompactionFilter is the interface for custom compaction filters.
// During compaction, the Filter method is called for each key-value pair,
// allowing the user to decide whether to keep, remove, or modify the entry.
type CompactionFilter interface {
	// Name returns the name of the compaction filter.
	// This is used for logging and identification.
	Name() string

	// Filter is called for each key-value pair during compaction.
	// Returns the decision (Keep, Remove, or Change) and optionally a new value.
	//
	// Parameters:
	//   - level: The compaction output level
	//   - key: The user key (not internal key)
	//   - oldValue: The current value
	//
	// Returns:
	//   - decision: Whether to keep, remove, or change the entry
	//   - newValue: If decision is FilterChange, this is the new value
	Filter(level int, key, oldValue []byte) (decision CompactionFilterDecision, newValue []byte)

	// FilterMergeOperand is called for merge operands.
	// This allows filtering of individual merge operands.
	// Default implementation returns FilterKeep.
	FilterMergeOperand(level int, key, operand []byte) CompactionFilterDecision
}

// CompactionFilterFactory creates compaction filters.
// A new filter is created for each compaction, allowing filters to
// maintain state during a single compaction.
type CompactionFilterFactory interface {
	// Name returns the name of the factory.
	Name() string

	// CreateCompactionFilter creates a new compaction filter for a compaction.
	// The context provides information about the current compaction.
	CreateCompactionFilter(context CompactionFilterContext) CompactionFilter
}

// CompactionFilterContext provides context about the current compaction.
type CompactionFilterContext struct {
	// IsFull is true if this is a full compaction (all levels).
	IsFull bool

	// IsManual is true if this is a manually triggered compaction.
	IsManual bool

	// ColumnFamilyID is the ID of the column family being compacted.
	ColumnFamilyID uint32
}

// BaseCompactionFilter provides a base implementation of CompactionFilter
// that can be embedded in custom filters.
type BaseCompactionFilter struct{}

// Name returns "BaseCompactionFilter".
func (b *BaseCompactionFilter) Name() string {
	return "BaseCompactionFilter"
}

// Filter default implementation keeps all entries.
func (b *BaseCompactionFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	return FilterKeep, nil
}

// FilterMergeOperand default implementation keeps all operands.
func (b *BaseCompactionFilter) FilterMergeOperand(level int, key, operand []byte) CompactionFilterDecision {
	return FilterKeep
}

// RemoveByPrefixFilter removes keys with a specific prefix.
type RemoveByPrefixFilter struct {
	BaseCompactionFilter
	Prefix []byte
}

// Name returns the filter name.
func (f *RemoveByPrefixFilter) Name() string {
	return "RemoveByPrefixFilter"
}

// Filter removes keys that have the specified prefix.
func (f *RemoveByPrefixFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	if len(key) >= len(f.Prefix) && hasPrefix(key, f.Prefix) {
		return FilterRemove, nil
	}
	return FilterKeep, nil
}

// hasPrefix checks if a byte slice starts with the given prefix.
func hasPrefix(data, prefix []byte) bool {
	if len(data) < len(prefix) {
		return false
	}
	for i := range prefix {
		if data[i] != prefix[i] {
			return false
		}
	}
	return true
}

// RemoveByRangeFilter removes keys within a specific range.
type RemoveByRangeFilter struct {
	BaseCompactionFilter
	StartKey []byte // Inclusive
	EndKey   []byte // Exclusive
}

// Name returns the filter name.
func (f *RemoveByRangeFilter) Name() string {
	return "RemoveByRangeFilter"
}

// Filter removes keys within the specified range.
func (f *RemoveByRangeFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	if f.inRange(key) {
		return FilterRemove, nil
	}
	return FilterKeep, nil
}

func (f *RemoveByRangeFilter) inRange(key []byte) bool {
	// Check start bound
	if f.StartKey != nil && bytesCompare(key, f.StartKey) < 0 {
		return false
	}
	// Check end bound
	if f.EndKey != nil && bytesCompare(key, f.EndKey) >= 0 {
		return false
	}
	return true
}

// bytesCompare compares two byte slices lexicographically.
func bytesCompare(a, b []byte) int {
	return dbformat.BytewiseCompare(a, b)
}
