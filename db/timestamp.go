// timestamp.go implements user-defined timestamps (UDT) support.
//
// User-defined timestamps allow applications to associate a timestamp with
// each key-value pair. The timestamp is part of the key and is used for
// ordering within the same user key (larger/newer timestamps come first).
//
// Key Format with Timestamps:
//
//	[user_key][timestamp]
//
// Where timestamp is typically an 8-byte big-endian uint64.
//
// For internal keys (with sequence number), the format becomes:
//
//	[user_key][timestamp][sequence_number (7 bytes)][type (1 byte)]
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/comparator.h (BytewiseComparatorWithU64Ts)
//   - util/comparator.cc (timestamp comparator implementation)
//   - db/dbformat.h (key format with timestamps)
package db

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// TimestampSize is the size of a uint64 timestamp in bytes.
const TimestampSize = 8

var (
	// ErrInvalidTimestamp is returned when a timestamp is malformed.
	ErrInvalidTimestamp = errors.New("db: invalid timestamp")

	// maxU64Ts is the maximum uint64 timestamp (all 1s).
	maxU64Ts = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	// minU64Ts is the minimum uint64 timestamp (all 0s).
	minU64Ts = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

// EncodeU64Ts encodes a uint64 timestamp into a byte slice.
// The encoding uses big-endian with bitwise inversion so that byte-wise
// comparison produces DESCENDING order (larger timestamps come first).
// This is required because SST files use bytewise comparison for seeking.
//
// Example:
//   - ts=0 encodes as 0xFFFFFFFFFFFFFFFF (bytewise largest)
//   - ts=100 encodes as ^100 (larger than ^200)
//   - ts=MAX encodes as 0x0000000000000000 (bytewise smallest)
//
// This ensures that when iterating, entries with larger (newer) timestamps
// appear before entries with smaller (older) timestamps for the same user key.
func EncodeU64Ts(ts uint64) []byte {
	buf := make([]byte, TimestampSize)
	// Invert bits so larger timestamps produce smaller byte values
	binary.BigEndian.PutUint64(buf, ^ts)
	return buf
}

// DecodeU64Ts decodes a uint64 timestamp from a byte slice.
// Reverses the encoding done by EncodeU64Ts.
func DecodeU64Ts(ts []byte) (uint64, error) {
	if len(ts) != TimestampSize {
		return 0, ErrInvalidTimestamp
	}
	// Invert bits back to get original timestamp
	return ^binary.BigEndian.Uint64(ts), nil
}

// MaxU64Ts returns the encoded maximum uint64 timestamp.
// With inverted encoding, max timestamp (0xFFFF...) encodes as 0x0000... (bytewise smallest).
func MaxU64Ts() []byte {
	// Return encoded form: ^MAX = 0
	return minU64Ts // 0x0000... is the encoded form of MAX
}

// MinU64Ts returns the encoded minimum uint64 timestamp.
// With inverted encoding, min timestamp (0) encodes as 0xFFFF... (bytewise largest).
func MinU64Ts() []byte {
	// Return encoded form: ^0 = MAX
	return maxU64Ts // 0xFFFF... is the encoded form of MIN
}

// AppendTimestampToKey appends a timestamp to a user key.
func AppendTimestampToKey(key []byte, ts []byte) []byte {
	result := make([]byte, len(key)+len(ts))
	copy(result, key)
	copy(result[len(key):], ts)
	return result
}

// StripTimestampFromKey removes the timestamp from a key.
// Returns the user key portion and the timestamp.
func StripTimestampFromKey(keyWithTS []byte, tsSize int) (userKey, timestamp []byte) {
	if len(keyWithTS) < tsSize {
		return keyWithTS, nil
	}
	return keyWithTS[:len(keyWithTS)-tsSize], keyWithTS[len(keyWithTS)-tsSize:]
}

// TimestampedComparator is a comparator that supports user-defined timestamps.
// It extends the base Comparator interface with timestamp-aware methods.
type TimestampedComparator interface {
	Comparator

	// TimestampSize returns the size of the timestamp in bytes.
	// Returns 0 if timestamps are not supported.
	TimestampSize() int

	// CompareTimestamp compares two timestamps.
	// Returns < 0 if ts1 < ts2, 0 if equal, > 0 if ts1 > ts2.
	CompareTimestamp(ts1, ts2 []byte) int

	// CompareWithoutTimestamp compares two keys ignoring their timestamps.
	CompareWithoutTimestamp(a, b []byte, aHasTS, bHasTS bool) int

	// GetMaxTimestamp returns the maximum timestamp value.
	GetMaxTimestamp() []byte

	// GetMinTimestamp returns the minimum timestamp value.
	GetMinTimestamp() []byte
}

// BytewiseComparatorWithU64Ts is a comparator that supports uint64 timestamps.
// It orders keys bytewise, and for the same user key, larger (newer) timestamps
// come first.
//
// The timestamp encoding uses bitwise inversion (see EncodeU64Ts) so that
// bytewise comparison automatically produces descending timestamp order.
// This allows the comparator to use simple bytewise comparison on the
// entire key (user_key + encoded_timestamp).
//
// Reference: RocksDB v10.7.5
//   - util/comparator.cc (BytewiseComparatorWithU64TsImpl)
type BytewiseComparatorWithU64Ts struct{}

// Compile-time check that BytewiseComparatorWithU64Ts implements TimestampedComparator.
var _ TimestampedComparator = BytewiseComparatorWithU64Ts{}

// Compare compares two keys with timestamps.
// Since timestamps are encoded with bitwise inversion (see EncodeU64Ts),
// bytewise comparison automatically produces the correct ordering:
// - For different user keys: sorted bytewise
// - For same user key: larger timestamps come first (due to inverted encoding)
func (c BytewiseComparatorWithU64Ts) Compare(a, b []byte) int {
	// With inverted timestamp encoding, bytewise comparison is sufficient
	return bytes.Compare(a, b)
}

// Name returns the comparator name.
func (c BytewiseComparatorWithU64Ts) Name() string {
	return "leveldb.BytewiseComparator.u64ts"
}

// FindShortestSeparator finds a key between a and b.
func (c BytewiseComparatorWithU64Ts) FindShortestSeparator(a, b []byte) []byte {
	// For timestamp-aware keys, we use the user key portion for separator finding
	aUserKey, aTS := StripTimestampFromKey(a, TimestampSize)
	bUserKey, _ := StripTimestampFromKey(b, TimestampSize)

	// Find separator for user keys
	bc := BytewiseComparator{}
	sep := bc.FindShortestSeparator(aUserKey, bUserKey)

	if bytes.Equal(sep, aUserKey) {
		// Couldn't shorten, return original
		return a
	}

	// Append original timestamp to the shortened user key
	return AppendTimestampToKey(sep, aTS)
}

// FindShortSuccessor finds a short key >= a.
func (c BytewiseComparatorWithU64Ts) FindShortSuccessor(a []byte) []byte {
	aUserKey, aTS := StripTimestampFromKey(a, TimestampSize)

	bc := BytewiseComparator{}
	succ := bc.FindShortSuccessor(aUserKey)

	if bytes.Equal(succ, aUserKey) {
		return a
	}

	return AppendTimestampToKey(succ, aTS)
}

// TimestampSize returns the size of the timestamp (8 bytes for uint64).
func (c BytewiseComparatorWithU64Ts) TimestampSize() int {
	return TimestampSize
}

// CompareTimestamp compares two timestamps (encoded format).
// Returns < 0 if ts1 < ts2, 0 if equal, > 0 if ts1 > ts2.
// Note: Timestamps are encoded with bitwise inversion, so we need to
// decode them first to get the correct comparison result.
func (c BytewiseComparatorWithU64Ts) CompareTimestamp(ts1, ts2 []byte) int {
	// Decode the inverted timestamps
	decoded1, _ := DecodeU64Ts(ts1)
	decoded2, _ := DecodeU64Ts(ts2)

	if decoded1 < decoded2 {
		return -1
	} else if decoded1 > decoded2 {
		return 1
	}
	return 0
}

// CompareWithoutTimestamp compares two keys ignoring their timestamps.
func (c BytewiseComparatorWithU64Ts) CompareWithoutTimestamp(a, b []byte, aHasTS, bHasTS bool) int {
	aUserKey := a
	if aHasTS && len(a) >= TimestampSize {
		aUserKey = a[:len(a)-TimestampSize]
	}

	bUserKey := b
	if bHasTS && len(b) >= TimestampSize {
		bUserKey = b[:len(b)-TimestampSize]
	}

	return bytes.Compare(aUserKey, bUserKey)
}

// GetMaxTimestamp returns the maximum uint64 timestamp.
func (c BytewiseComparatorWithU64Ts) GetMaxTimestamp() []byte {
	return MaxU64Ts()
}

// GetMinTimestamp returns the minimum uint64 timestamp.
func (c BytewiseComparatorWithU64Ts) GetMinTimestamp() []byte {
	return MinU64Ts()
}

// ReverseBytewiseComparatorWithU64Ts is the reverse ordering version.
// User keys are compared in reverse bytewise order, but timestamps still
// use descending order (larger timestamps come first for same user key).
//
// Reference: RocksDB v10.7.5
//   - util/comparator.cc (ReverseBytewiseComparatorWithU64TsImpl)
type ReverseBytewiseComparatorWithU64Ts struct{}

var _ TimestampedComparator = ReverseBytewiseComparatorWithU64Ts{}

// Compare compares two keys with timestamps in reverse order.
// User keys are compared in reverse bytewise order.
// With inverted timestamp encoding, timestamps are automatically in descending order.
func (c ReverseBytewiseComparatorWithU64Ts) Compare(a, b []byte) int {
	// Extract user keys and timestamps
	aUserKey, aTS := StripTimestampFromKey(a, TimestampSize)
	bUserKey, bTS := StripTimestampFromKey(b, TimestampSize)

	// Compare user keys in reverse order
	cmp := bytes.Compare(bUserKey, aUserKey)
	if cmp != 0 {
		return cmp
	}

	// For the same user key, compare timestamps (already in descending due to inverted encoding)
	return bytes.Compare(aTS, bTS)
}

// Name returns the comparator name.
func (c ReverseBytewiseComparatorWithU64Ts) Name() string {
	return "rocksdb.ReverseBytewiseComparator.u64ts"
}

// FindShortestSeparator is not typically used for reverse comparator.
func (c ReverseBytewiseComparatorWithU64Ts) FindShortestSeparator(a, b []byte) []byte {
	return a
}

// FindShortSuccessor is not typically used for reverse comparator.
func (c ReverseBytewiseComparatorWithU64Ts) FindShortSuccessor(a []byte) []byte {
	return a
}

// TimestampSize returns the size of the timestamp.
func (c ReverseBytewiseComparatorWithU64Ts) TimestampSize() int {
	return TimestampSize
}

// CompareTimestamp compares two timestamps (encoded format).
// Returns < 0 if ts1 < ts2, 0 if equal, > 0 if ts1 > ts2.
func (c ReverseBytewiseComparatorWithU64Ts) CompareTimestamp(ts1, ts2 []byte) int {
	decoded1, _ := DecodeU64Ts(ts1)
	decoded2, _ := DecodeU64Ts(ts2)

	if decoded1 < decoded2 {
		return -1
	} else if decoded1 > decoded2 {
		return 1
	}
	return 0
}

// CompareWithoutTimestamp compares two keys ignoring timestamps in reverse order.
func (c ReverseBytewiseComparatorWithU64Ts) CompareWithoutTimestamp(a, b []byte, aHasTS, bHasTS bool) int {
	aUserKey := a
	if aHasTS && len(a) >= TimestampSize {
		aUserKey = a[:len(a)-TimestampSize]
	}

	bUserKey := b
	if bHasTS && len(b) >= TimestampSize {
		bUserKey = b[:len(b)-TimestampSize]
	}

	return bytes.Compare(bUserKey, aUserKey)
}

// GetMaxTimestamp returns the maximum uint64 timestamp.
func (c ReverseBytewiseComparatorWithU64Ts) GetMaxTimestamp() []byte {
	return MaxU64Ts()
}

// GetMinTimestamp returns the minimum uint64 timestamp.
func (c ReverseBytewiseComparatorWithU64Ts) GetMinTimestamp() []byte {
	return MinU64Ts()
}
