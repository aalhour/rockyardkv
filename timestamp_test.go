package rockyardkv

// timestamp_test.go implements tests for timestamp.

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeDecodeU64Ts(t *testing.T) {
	tests := []uint64{
		0,
		1,
		42,
		1000000,
		0xFFFFFFFF,
		0xFFFFFFFFFFFFFFFF,
	}

	for _, ts := range tests {
		encoded := EncodeU64Ts(ts)
		if len(encoded) != TimestampSize {
			t.Errorf("EncodeU64Ts(%d): expected %d bytes, got %d", ts, TimestampSize, len(encoded))
		}

		decoded, err := DecodeU64Ts(encoded)
		if err != nil {
			t.Errorf("DecodeU64Ts(%d): unexpected error: %v", ts, err)
		}
		if decoded != ts {
			t.Errorf("DecodeU64Ts(EncodeU64Ts(%d)): expected %d, got %d", ts, ts, decoded)
		}
	}
}

func TestDecodeU64TsInvalidSize(t *testing.T) {
	invalid := [][]byte{
		nil,
		{},
		{1, 2, 3},
		{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}

	for _, ts := range invalid {
		_, err := DecodeU64Ts(ts)
		if !errors.Is(err, ErrInvalidTimestamp) {
			t.Errorf("DecodeU64Ts(%v): expected ErrInvalidTimestamp, got %v", ts, err)
		}
	}
}

func TestMaxMinU64Ts(t *testing.T) {
	maxTS := MaxU64Ts()
	minTS := MinU64Ts()

	if len(maxTS) != TimestampSize {
		t.Errorf("MaxU64Ts: expected %d bytes, got %d", TimestampSize, len(maxTS))
	}
	if len(minTS) != TimestampSize {
		t.Errorf("MinU64Ts: expected %d bytes, got %d", TimestampSize, len(minTS))
	}

	// With inverted encoding, max timestamp (largest logical value) encodes to
	// smallest bytes, and min timestamp (smallest logical value) encodes to
	// largest bytes. This is by design so that bytewise comparison produces
	// descending order (larger timestamps come first in SST files).
	if bytes.Compare(maxTS, minTS) >= 0 {
		t.Errorf("MaxU64Ts (encoded) should be < MinU64Ts (encoded) due to inverted encoding")
	}

	// Verify decoded values
	maxDecoded, _ := DecodeU64Ts(maxTS)
	minDecoded, _ := DecodeU64Ts(minTS)

	if maxDecoded != 0xFFFFFFFFFFFFFFFF {
		t.Errorf("MaxU64Ts: expected decoded value 0xFFFFFFFFFFFFFFFF, got 0x%X", maxDecoded)
	}
	if minDecoded != 0 {
		t.Errorf("MinU64Ts: expected decoded value 0, got %d", minDecoded)
	}
}

func TestAppendStripTimestamp(t *testing.T) {
	key := []byte("user_key")
	ts := EncodeU64Ts(12345)

	keyWithTS := AppendTimestampToKey(key, ts)
	if len(keyWithTS) != len(key)+TimestampSize {
		t.Errorf("AppendTimestampToKey: expected %d bytes, got %d", len(key)+TimestampSize, len(keyWithTS))
	}

	userKey, timestamp := StripTimestampFromKey(keyWithTS, TimestampSize)
	if !bytes.Equal(userKey, key) {
		t.Errorf("StripTimestampFromKey: expected user key %q, got %q", key, userKey)
	}
	if !bytes.Equal(timestamp, ts) {
		t.Errorf("StripTimestampFromKey: expected timestamp %v, got %v", ts, timestamp)
	}
}

func TestBytewiseComparatorWithU64Ts(t *testing.T) {
	cmp := BytewiseComparatorWithU64Ts{}

	// Test name
	if cmp.Name() != "leveldb.BytewiseComparator.u64ts" {
		t.Errorf("Name: expected 'leveldb.BytewiseComparator.u64ts', got %q", cmp.Name())
	}

	// Test timestamp size
	if cmp.TimestampSize() != TimestampSize {
		t.Errorf("TimestampSize: expected %d, got %d", TimestampSize, cmp.TimestampSize())
	}

	// Create test keys with timestamps
	key1 := AppendTimestampToKey([]byte("a"), EncodeU64Ts(100))
	key2 := AppendTimestampToKey([]byte("a"), EncodeU64Ts(200))
	key3 := AppendTimestampToKey([]byte("b"), EncodeU64Ts(100))

	// Same user key, different timestamps: larger timestamp should come first
	if cmp.Compare(key2, key1) >= 0 {
		t.Errorf("Compare(a@200, a@100): expected < 0 (larger timestamp first)")
	}
	if cmp.Compare(key1, key2) <= 0 {
		t.Errorf("Compare(a@100, a@200): expected > 0 (larger timestamp first)")
	}

	// Different user keys
	if cmp.Compare(key1, key3) >= 0 {
		t.Errorf("Compare(a@100, b@100): expected < 0 (a < b)")
	}
	if cmp.Compare(key3, key1) <= 0 {
		t.Errorf("Compare(b@100, a@100): expected > 0 (b > a)")
	}

	// Same key and timestamp
	key1Copy := append([]byte{}, key1...)
	if cmp.Compare(key1, key1Copy) != 0 {
		t.Errorf("Compare(a@100, a@100): expected 0")
	}
}

func TestComparatorWithoutTimestamp(t *testing.T) {
	cmp := BytewiseComparatorWithU64Ts{}

	key1 := AppendTimestampToKey([]byte("abc"), EncodeU64Ts(100))
	key2 := AppendTimestampToKey([]byte("abc"), EncodeU64Ts(200))
	key3 := AppendTimestampToKey([]byte("def"), EncodeU64Ts(100))

	// Same user key, different timestamps
	if cmp.CompareWithoutTimestamp(key1, key2, true, true) != 0 {
		t.Errorf("CompareWithoutTimestamp(abc@100, abc@200): expected 0")
	}

	// Different user keys
	if cmp.CompareWithoutTimestamp(key1, key3, true, true) >= 0 {
		t.Errorf("CompareWithoutTimestamp(abc, def): expected < 0")
	}

	// Compare key with timestamp to key without timestamp
	keyNoTS := []byte("abc")
	if cmp.CompareWithoutTimestamp(key1, keyNoTS, true, false) != 0 {
		t.Errorf("CompareWithoutTimestamp(abc@100, abc): expected 0")
	}
}

func TestCompareTimestamp(t *testing.T) {
	cmp := BytewiseComparatorWithU64Ts{}

	ts1 := EncodeU64Ts(100)
	ts2 := EncodeU64Ts(200)
	ts3 := EncodeU64Ts(100)

	if cmp.CompareTimestamp(ts1, ts2) >= 0 {
		t.Errorf("CompareTimestamp(100, 200): expected < 0")
	}
	if cmp.CompareTimestamp(ts2, ts1) <= 0 {
		t.Errorf("CompareTimestamp(200, 100): expected > 0")
	}
	if cmp.CompareTimestamp(ts1, ts3) != 0 {
		t.Errorf("CompareTimestamp(100, 100): expected 0")
	}
}

func TestReverseBytewiseComparatorWithU64Ts(t *testing.T) {
	cmp := ReverseBytewiseComparatorWithU64Ts{}

	// Test name
	if cmp.Name() != "rocksdb.ReverseBytewiseComparator.u64ts" {
		t.Errorf("Name: expected 'rocksdb.ReverseBytewiseComparator.u64ts', got %q", cmp.Name())
	}

	// Test timestamp size
	if cmp.TimestampSize() != TimestampSize {
		t.Errorf("TimestampSize: expected %d, got %d", TimestampSize, cmp.TimestampSize())
	}

	// Create test keys with timestamps
	key1 := AppendTimestampToKey([]byte("a"), EncodeU64Ts(100))
	key2 := AppendTimestampToKey([]byte("a"), EncodeU64Ts(200))
	key3 := AppendTimestampToKey([]byte("b"), EncodeU64Ts(100))

	// Same user key, different timestamps: larger timestamp should come first
	if cmp.Compare(key2, key1) >= 0 {
		t.Errorf("Compare(a@200, a@100): expected < 0 (larger timestamp first)")
	}

	// Different user keys: reverse order (b < a)
	if cmp.Compare(key3, key1) >= 0 {
		t.Errorf("Compare(b@100, a@100): expected < 0 (reverse: b < a)")
	}
	if cmp.Compare(key1, key3) <= 0 {
		t.Errorf("Compare(a@100, b@100): expected > 0 (reverse: a > b)")
	}
}

func TestFindShortestSeparatorWithTimestamp(t *testing.T) {
	cmp := BytewiseComparatorWithU64Ts{}

	key1 := AppendTimestampToKey([]byte("abc"), EncodeU64Ts(100))
	key2 := AppendTimestampToKey([]byte("xyz"), EncodeU64Ts(100))

	sep := cmp.FindShortestSeparator(key1, key2)

	// The separator should be >= key1 and < key2
	if cmp.Compare(sep, key1) < 0 {
		t.Errorf("FindShortestSeparator: separator should be >= key1")
	}
	if cmp.Compare(sep, key2) >= 0 {
		t.Errorf("FindShortestSeparator: separator should be < key2")
	}
}

func TestFindShortSuccessorWithTimestamp(t *testing.T) {
	cmp := BytewiseComparatorWithU64Ts{}

	key := AppendTimestampToKey([]byte("abc"), EncodeU64Ts(100))

	succ := cmp.FindShortSuccessor(key)

	// The successor should be >= key
	if cmp.Compare(succ, key) < 0 {
		t.Errorf("FindShortSuccessor: successor should be >= key")
	}
}
