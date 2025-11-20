package db

import (
	"bytes"
	"testing"
)

// =============================================================================
// UInt64AddOperator Tests
// =============================================================================

func TestUInt64AddOperator_Name(t *testing.T) {
	op := &UInt64AddOperator{}
	if op.Name() != "UInt64AddOperator" {
		t.Errorf("Name() = %q, want %q", op.Name(), "UInt64AddOperator")
	}
}

func TestUInt64AddOperator_FullMerge(t *testing.T) {
	op := &UInt64AddOperator{}

	testCases := []struct {
		name          string
		existingValue []byte
		operands      [][]byte
		wantResult    uint64
		wantOk        bool
	}{
		{
			name:          "nil_existing_single_operand",
			existingValue: nil,
			operands:      [][]byte{encodeUint64(42)},
			wantResult:    42,
			wantOk:        true,
		},
		{
			name:          "existing_plus_operands",
			existingValue: encodeUint64(10),
			operands:      [][]byte{encodeUint64(5), encodeUint64(3)},
			wantResult:    18,
			wantOk:        true,
		},
		{
			name:          "existing_no_operands",
			existingValue: encodeUint64(100),
			operands:      [][]byte{},
			wantResult:    100,
			wantOk:        true,
		},
		{
			name:          "nil_existing_multiple_operands",
			existingValue: nil,
			operands:      [][]byte{encodeUint64(1), encodeUint64(2), encodeUint64(3)},
			wantResult:    6,
			wantOk:        true,
		},
		{
			name:          "invalid_existing_value",
			existingValue: []byte{1, 2, 3}, // Wrong length
			operands:      [][]byte{encodeUint64(1)},
			wantOk:        false,
		},
		{
			name:          "invalid_operand",
			existingValue: encodeUint64(10),
			operands:      [][]byte{{1, 2}}, // Wrong length
			wantOk:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.FullMerge([]byte("key"), tc.existingValue, tc.operands)

			if ok != tc.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOk)
			}
			if !ok {
				return
			}

			got := decodeUint64(result)
			if got != tc.wantResult {
				t.Errorf("result = %d, want %d", got, tc.wantResult)
			}
		})
	}
}

func TestUInt64AddOperator_PartialMerge(t *testing.T) {
	op := &UInt64AddOperator{}

	testCases := []struct {
		name       string
		left       []byte
		right      []byte
		wantResult uint64
		wantOk     bool
	}{
		{
			name:       "simple_add",
			left:       encodeUint64(5),
			right:      encodeUint64(3),
			wantResult: 8,
			wantOk:     true,
		},
		{
			name:       "add_zero",
			left:       encodeUint64(100),
			right:      encodeUint64(0),
			wantResult: 100,
			wantOk:     true,
		},
		{
			name:   "invalid_left",
			left:   []byte{1},
			right:  encodeUint64(5),
			wantOk: false,
		},
		{
			name:   "invalid_right",
			left:   encodeUint64(5),
			right:  []byte{1, 2, 3},
			wantOk: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.PartialMerge([]byte("key"), tc.left, tc.right)

			if ok != tc.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOk)
			}
			if !ok {
				return
			}

			got := decodeUint64(result)
			if got != tc.wantResult {
				t.Errorf("result = %d, want %d", got, tc.wantResult)
			}
		})
	}
}

// =============================================================================
// StringAppendOperator Tests
// =============================================================================

func TestStringAppendOperator_Name(t *testing.T) {
	op := &StringAppendOperator{Delimiter: ","}
	if op.Name() != "StringAppendOperator" {
		t.Errorf("Name() = %q, want %q", op.Name(), "StringAppendOperator")
	}
}

func TestStringAppendOperator_FullMerge(t *testing.T) {
	op := &StringAppendOperator{Delimiter: ","}

	testCases := []struct {
		name          string
		existingValue []byte
		operands      [][]byte
		wantResult    string
	}{
		{
			name:          "nil_existing_single_operand",
			existingValue: nil,
			operands:      [][]byte{[]byte("hello")},
			wantResult:    "hello",
		},
		{
			name:          "existing_plus_operands",
			existingValue: []byte("a"),
			operands:      [][]byte{[]byte("b"), []byte("c")},
			wantResult:    "a,b,c",
		},
		{
			name:          "existing_no_operands",
			existingValue: []byte("existing"),
			operands:      [][]byte{},
			wantResult:    "existing",
		},
		{
			name:          "nil_existing_multiple_operands",
			existingValue: nil,
			operands:      [][]byte{[]byte("x"), []byte("y"), []byte("z")},
			wantResult:    "x,y,z",
		},
		{
			name:          "empty_operands",
			existingValue: []byte("base"),
			operands:      [][]byte{[]byte("")},
			wantResult:    "base",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.FullMerge([]byte("key"), tc.existingValue, tc.operands)

			if !ok {
				t.Fatalf("ok = false, want true")
			}

			if string(result) != tc.wantResult {
				t.Errorf("result = %q, want %q", result, tc.wantResult)
			}
		})
	}
}

func TestStringAppendOperator_PartialMerge(t *testing.T) {
	op := &StringAppendOperator{Delimiter: "-"}

	testCases := []struct {
		name       string
		left       []byte
		right      []byte
		wantResult string
	}{
		{
			name:       "simple_concat",
			left:       []byte("hello"),
			right:      []byte("world"),
			wantResult: "hello-world",
		},
		{
			name:       "empty_left",
			left:       []byte(""),
			right:      []byte("only"),
			wantResult: "only",
		},
		{
			name:       "empty_right",
			left:       []byte("only"),
			right:      []byte(""),
			wantResult: "only",
		},
		{
			name:       "both_empty",
			left:       []byte(""),
			right:      []byte(""),
			wantResult: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.PartialMerge([]byte("key"), tc.left, tc.right)

			if !ok {
				t.Fatalf("ok = false, want true")
			}

			if string(result) != tc.wantResult {
				t.Errorf("result = %q, want %q", result, tc.wantResult)
			}
		})
	}
}

// =============================================================================
// MaxOperator Tests
// =============================================================================

func TestMaxOperator_Name(t *testing.T) {
	op := &MaxOperator{}
	if op.Name() != "MaxOperator" {
		t.Errorf("Name() = %q, want %q", op.Name(), "MaxOperator")
	}
}

func TestMaxOperator_FullMerge(t *testing.T) {
	op := &MaxOperator{}

	testCases := []struct {
		name          string
		existingValue []byte
		operands      [][]byte
		wantResult    []byte
	}{
		{
			name:          "nil_existing_single_operand",
			existingValue: nil,
			operands:      [][]byte{[]byte("abc")},
			wantResult:    []byte("abc"),
		},
		{
			name:          "existing_is_max",
			existingValue: []byte("zzz"),
			operands:      [][]byte{[]byte("aaa"), []byte("bbb")},
			wantResult:    []byte("zzz"),
		},
		{
			name:          "operand_is_max",
			existingValue: []byte("aaa"),
			operands:      [][]byte{[]byte("zzz"), []byte("bbb")},
			wantResult:    []byte("zzz"),
		},
		{
			name:          "numeric_values",
			existingValue: []byte{1, 2, 3},
			operands:      [][]byte{{4, 5, 6}, {0, 0, 0}},
			wantResult:    []byte{4, 5, 6},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.FullMerge([]byte("key"), tc.existingValue, tc.operands)

			if !ok {
				t.Fatalf("ok = false, want true")
			}

			if !bytes.Equal(result, tc.wantResult) {
				t.Errorf("result = %v, want %v", result, tc.wantResult)
			}
		})
	}
}

func TestMaxOperator_PartialMerge(t *testing.T) {
	op := &MaxOperator{}

	testCases := []struct {
		name       string
		left       []byte
		right      []byte
		wantResult []byte
	}{
		{
			name:       "left_is_max",
			left:       []byte("zzz"),
			right:      []byte("aaa"),
			wantResult: []byte("zzz"),
		},
		{
			name:       "right_is_max",
			left:       []byte("aaa"),
			right:      []byte("zzz"),
			wantResult: []byte("zzz"),
		},
		{
			name:       "equal_values",
			left:       []byte("same"),
			right:      []byte("same"),
			wantResult: []byte("same"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := op.PartialMerge([]byte("key"), tc.left, tc.right)

			if !ok {
				t.Fatalf("ok = false, want true")
			}

			if !bytes.Equal(result, tc.wantResult) {
				t.Errorf("result = %v, want %v", result, tc.wantResult)
			}
		})
	}
}

// =============================================================================
// AssociativeMergeOperatorAdapter Tests
// =============================================================================

type testAssociativeOp struct{}

func (o *testAssociativeOp) Name() string { return "testAssociativeOp" }
func (o *testAssociativeOp) Merge(key []byte, existing, value []byte) ([]byte, bool) {
	if existing == nil {
		return value, true
	}
	// Simple concatenation
	result := make([]byte, len(existing)+len(value))
	copy(result, existing)
	copy(result[len(existing):], value)
	return result, true
}

func TestAssociativeMergeOperatorAdapter(t *testing.T) {
	adapter := &AssociativeMergeOperatorAdapter{Op: &testAssociativeOp{}}

	if adapter.Name() != "testAssociativeOp" {
		t.Errorf("Name() = %q, want %q", adapter.Name(), "testAssociativeOp")
	}

	// Test FullMerge
	result, ok := adapter.FullMerge([]byte("key"), []byte("a"), [][]byte{[]byte("b"), []byte("c")})
	if !ok {
		t.Fatal("FullMerge failed")
	}
	if string(result) != "abc" {
		t.Errorf("FullMerge result = %q, want %q", result, "abc")
	}

	// Test PartialMerge
	result, ok = adapter.PartialMerge([]byte("key"), []byte("x"), []byte("y"))
	if !ok {
		t.Fatal("PartialMerge failed")
	}
	if string(result) != "xy" {
		t.Errorf("PartialMerge result = %q, want %q", result, "xy")
	}
}

// =============================================================================
// Helper function tests
// =============================================================================

func TestEncodeDecodeUint64(t *testing.T) {
	testCases := []uint64{0, 1, 255, 256, 65535, 1<<32 - 1, 1<<63 - 1, 1 << 63}

	for _, v := range testCases {
		encoded := encodeUint64(v)
		decoded := decodeUint64(encoded)
		if decoded != v {
			t.Errorf("roundtrip(%d) = %d", v, decoded)
		}
	}
}

func TestCompareBytes(t *testing.T) {
	testCases := []struct {
		a, b []byte
		want int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte("ab"), []byte("a"), 1},
		{[]byte("a"), []byte("ab"), -1},
		{[]byte{}, []byte{}, 0},
		{[]byte{0}, []byte{1}, -1},
	}

	for _, tc := range testCases {
		got := compareBytes(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("compareBytes(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
