package rockyardkv

// merge_operator.go implements merge operator.
//
// MergeOperator allows users to define custom merge semantics for
// atomic read-modify-write operations like counters and append-only lists.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/merge_operator.h


// MergeOperator is the interface for user-defined merge operations.
//
// A MergeOperator specifies the semantics of a merge operation, which only
// the client knows. It could be numeric addition, list append, string
// concatenation, or any custom operation.
//
// RocksDB calls the merge operator during:
// - Get operations (to compute the final value)
// - Compaction (to combine merge operands)
// - Iteration (to compute values on the fly)
//
// There are two types of merge operators:
// 1. AssociativeMergeOperator - for simple operations like addition
// 2. MergeOperator - for complex operations requiring full control
type MergeOperator interface {
	// Name returns a unique identifier for this merge operator.
	// Used to check compatibility when opening an existing database.
	Name() string

	// FullMerge performs a merge operation.
	//
	// Parameters:
	// - key: The key associated with this merge operation
	// - existingValue: The existing value (nil if key doesn't exist)
	// - operands: List of merge operands to apply, oldest first
	//
	// Returns:
	// - newValue: The result of the merge
	// - ok: Whether the merge succeeded
	//
	// If ok is false, the merge is considered failed and treated as an error.
	FullMerge(key []byte, existingValue []byte, operands [][]byte) (newValue []byte, ok bool)

	// PartialMerge merges two operands into a single operand.
	// This is an optimization that allows combining operands before FullMerge.
	//
	// Parameters:
	// - key: The key associated with this merge operation
	// - leftOperand: The first operand
	// - rightOperand: The second operand
	//
	// Returns:
	// - newOperand: The combined operand
	// - ok: Whether the partial merge succeeded
	//
	// If ok is false, the operands cannot be combined and both must be kept.
	// PartialMerge is optional - returning (nil, false) is always valid.
	PartialMerge(key []byte, leftOperand, rightOperand []byte) (newOperand []byte, ok bool)
}

// AssociativeMergeOperator is a simplified interface for associative operations.
// Use this when merging is associative: Merge(Merge(a, b), c) == Merge(a, Merge(b, c))
// Examples: numeric addition, string concatenation, set union
type AssociativeMergeOperator interface {
	// Name returns a unique identifier for this merge operator.
	Name() string

	// Merge merges a new value with an existing value.
	// If existingValue is nil, treat it as the identity element for the operation.
	Merge(key []byte, existingValue, value []byte) ([]byte, bool)
}

// =============================================================================
// Built-in Merge Operators
// =============================================================================

// UInt64AddOperator is a merge operator that treats values as uint64 and adds them.
type UInt64AddOperator struct{}

// Name returns the name of this merge operator.
func (o *UInt64AddOperator) Name() string {
	return "UInt64AddOperator"
}

// FullMerge adds all operands to the existing value.
func (o *UInt64AddOperator) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var result uint64

	// Parse existing value
	if existingValue != nil {
		if len(existingValue) != 8 {
			return nil, false
		}
		result = decodeUint64(existingValue)
	}

	// Add all operands
	for _, op := range operands {
		if len(op) != 8 {
			return nil, false
		}
		result += decodeUint64(op)
	}

	return encodeUint64(result), true
}

// PartialMerge adds two operands together.
func (o *UInt64AddOperator) PartialMerge(key []byte, left, right []byte) ([]byte, bool) {
	if len(left) != 8 || len(right) != 8 {
		return nil, false
	}
	result := decodeUint64(left) + decodeUint64(right)
	return encodeUint64(result), true
}

// StringAppendOperator is a merge operator that concatenates strings with a delimiter.
type StringAppendOperator struct {
	Delimiter string
}

// Name returns the name of this merge operator.
func (o *StringAppendOperator) Name() string {
	return "StringAppendOperator"
}

// FullMerge concatenates all operands with the delimiter.
func (o *StringAppendOperator) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var result []byte

	if existingValue != nil {
		result = make([]byte, len(existingValue))
		copy(result, existingValue)
	}

	for _, op := range operands {
		if len(result) > 0 && len(op) > 0 {
			result = append(result, []byte(o.Delimiter)...)
		}
		result = append(result, op...)
	}

	return result, true
}

// PartialMerge concatenates two operands with the delimiter.
func (o *StringAppendOperator) PartialMerge(key []byte, left, right []byte) ([]byte, bool) {
	if len(left) == 0 {
		return right, true
	}
	if len(right) == 0 {
		return left, true
	}

	result := make([]byte, 0, len(left)+len(o.Delimiter)+len(right))
	result = append(result, left...)
	result = append(result, []byte(o.Delimiter)...)
	result = append(result, right...)

	return result, true
}

// MaxOperator is a merge operator that keeps the maximum value.
type MaxOperator struct{}

// Name returns the name of this merge operator.
func (o *MaxOperator) Name() string {
	return "MaxOperator"
}

// FullMerge returns the maximum of all values.
func (o *MaxOperator) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var maxVal []byte

	if existingValue != nil {
		maxVal = make([]byte, len(existingValue))
		copy(maxVal, existingValue)
	}

	for _, op := range operands {
		if maxVal == nil || compareBytes(op, maxVal) > 0 {
			maxVal = make([]byte, len(op))
			copy(maxVal, op)
		}
	}

	return maxVal, true
}

// PartialMerge returns the maximum of two operands.
func (o *MaxOperator) PartialMerge(key []byte, left, right []byte) ([]byte, bool) {
	if compareBytes(left, right) >= 0 {
		result := make([]byte, len(left))
		copy(result, left)
		return result, true
	}
	result := make([]byte, len(right))
	copy(result, right)
	return result, true
}

// =============================================================================
// Helper Adapter
// =============================================================================

// AssociativeMergeOperatorAdapter wraps an AssociativeMergeOperator to implement MergeOperator.
type AssociativeMergeOperatorAdapter struct {
	Op AssociativeMergeOperator
}

// Name returns the name of the underlying operator.
func (a *AssociativeMergeOperatorAdapter) Name() string {
	return a.Op.Name()
}

// FullMerge implements MergeOperator by calling Merge repeatedly.
func (a *AssociativeMergeOperatorAdapter) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	result := existingValue

	for _, op := range operands {
		var ok bool
		result, ok = a.Op.Merge(key, result, op)
		if !ok {
			return nil, false
		}
	}

	return result, true
}

// PartialMerge implements MergeOperator using Merge.
func (a *AssociativeMergeOperatorAdapter) PartialMerge(key []byte, left, right []byte) ([]byte, bool) {
	return a.Op.Merge(key, left, right)
}

// =============================================================================
// Helper functions
// =============================================================================

func decodeUint64(b []byte) uint64 {
	_ = b[7] // bounds check
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
	return b
}

func compareBytes(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
