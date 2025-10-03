package block

import (
	"encoding/binary"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// Block represents a parsed block containing key-value pairs.
// The format is:
//
//	entries: key-value pairs with prefix compression
//	restarts: uint32[num_restarts] - offsets of restart points
//	num_restarts: uint32 (or packed footer with index type)
//
// Each entry has the format:
//
//	shared_bytes: varint32 (shared prefix with previous key)
//	unshared_bytes: varint32 (unshared key suffix length)
//	value_length: varint32
//	key_delta: char[unshared_bytes]
//	value: char[value_length]
type Block struct {
	// data is the raw block data
	data []byte

	// restarts is the offset of the restarts array within data
	restarts int

	// numRestarts is the number of restart points
	numRestarts int

	// Global sequence number override (kDisableGlobalSequenceNumber means disabled)
	globalSeqno uint64
}

// kDisableGlobalSequenceNumber indicates no global sequence number override.
const kDisableGlobalSequenceNumber = ^uint64(0)

// DataBlockIndexType represents the type of index within a data block.
type DataBlockIndexType uint8

const (
	// DataBlockBinarySearch uses binary search on restart points.
	DataBlockBinarySearch DataBlockIndexType = 0
	// DataBlockBinaryAndHash uses hash index with binary search fallback.
	DataBlockBinaryAndHash DataBlockIndexType = 1
)

// kDataBlockIndexTypeBitShift is the bit position for the index type flag.
const kDataBlockIndexTypeBitShift = 31

// kNumRestartsMask masks out the index type bit.
const kNumRestartsMask = (1 << kDataBlockIndexTypeBitShift) - 1 // 0x7FFFFFFF

// PackIndexTypeAndNumRestarts packs index type and num_restarts into a single uint32.
// Format: num_restarts | (index_type << 31)
// Reference: table/block_based/data_block_footer.cc
func PackIndexTypeAndNumRestarts(indexType DataBlockIndexType, numRestarts uint32) uint32 {
	footer := numRestarts
	if indexType == DataBlockBinaryAndHash {
		footer |= 1 << kDataBlockIndexTypeBitShift
	}
	return footer
}

// UnpackIndexTypeAndNumRestarts unpacks index type and num_restarts from a packed uint32.
// Reference: table/block_based/data_block_footer.cc
func UnpackIndexTypeAndNumRestarts(footer uint32) (DataBlockIndexType, uint32) {
	var indexType DataBlockIndexType
	if footer&(1<<kDataBlockIndexTypeBitShift) != 0 {
		indexType = DataBlockBinaryAndHash
	} else {
		indexType = DataBlockBinarySearch
	}
	numRestarts := footer & kNumRestartsMask
	return indexType, numRestarts
}

// NewBlock creates a new Block from raw data.
// The data slice is not copied; caller must ensure it remains valid.
func NewBlock(data []byte) (*Block, error) {
	if len(data) < 4 {
		return nil, ErrBadBlock
	}

	// Read the footer (last 4 bytes)
	footerOffset := len(data) - 4
	footer := binary.LittleEndian.Uint32(data[footerOffset:])

	_, numRestarts := UnpackIndexTypeAndNumRestarts(footer)

	// Validate restarts
	if numRestarts == 0 {
		return nil, ErrBadBlock
	}

	// Calculate restarts array offset
	// restarts array is: uint32[numRestarts] followed by footer (uint32)
	restartsSize := int(numRestarts+1) * 4 // +1 for the footer
	if restartsSize > len(data) {
		return nil, ErrBadBlock
	}
	restartsOffset := len(data) - restartsSize

	return &Block{
		data:        data,
		restarts:    restartsOffset,
		numRestarts: int(numRestarts),
		globalSeqno: kDisableGlobalSequenceNumber,
	}, nil
}

// Size returns the size of the block data.
func (b *Block) Size() int {
	return len(b.data)
}

// Data returns the raw block data.
func (b *Block) Data() []byte {
	return b.data
}

// NumRestarts returns the number of restart points.
func (b *Block) NumRestarts() int {
	return b.numRestarts
}

// GetRestartPoint returns the offset of the i-th restart point.
func (b *Block) GetRestartPoint(i int) int {
	if i < 0 || i >= b.numRestarts {
		return -1
	}
	offset := b.restarts + i*4
	return int(binary.LittleEndian.Uint32(b.data[offset:]))
}

// DataEnd returns the end offset of the data section (start of restarts array).
func (b *Block) DataEnd() int {
	return b.restarts
}

// SetGlobalSeqno sets a global sequence number that overrides all entry sequence numbers.
func (b *Block) SetGlobalSeqno(seqno uint64) {
	b.globalSeqno = seqno
}

// GlobalSeqno returns the global sequence number, or kDisableGlobalSequenceNumber if disabled.
func (b *Block) GlobalSeqno() uint64 {
	return b.globalSeqno
}

// Entry represents a decoded key-value entry from a block.
type Entry struct {
	Key   []byte
	Value []byte
}

// Iterator iterates over the entries in a block.
type Iterator struct {
	block       *Block
	data        []byte // points to block.data
	restartsEnd int    // end of data section
	current     int    // current entry start offset in data
	nextOffset  int    // offset of next entry (after current key+value)
	key         []byte // current key (fully assembled)
	value       []byte // current value (slice into data)
	valid       bool   // whether iterator is at a valid entry
	err         error
}

// NewIterator creates a new block iterator.
func (b *Block) NewIterator() *Iterator {
	return &Iterator{
		block:       b,
		data:        b.data,
		restartsEnd: b.restarts,
		current:     0,
		nextOffset:  0,
		valid:       false,
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid && it.err == nil
}

// Key returns the current key. Only valid if Valid() returns true.
func (it *Iterator) Key() []byte {
	return it.key
}

// Value returns the current value. Only valid if Valid() returns true.
func (it *Iterator) Value() []byte {
	return it.value
}

// Error returns any error encountered during iteration.
func (it *Iterator) Error() error {
	return it.err
}

// SeekToFirst positions the iterator at the first entry.
func (it *Iterator) SeekToFirst() {
	// Start at the very beginning (offset 0), not at the first restart point.
	// There may be entries before the first restart point.
	it.key = it.key[:0]
	it.value = nil
	it.valid = false
	it.current = 0
	it.nextOffset = 0
	it.Next()
}

// SeekToLast positions the iterator at the last entry.
func (it *Iterator) SeekToLast() {
	it.seekToRestartPoint(it.block.numRestarts - 1)

	// Find the last entry by iterating
	var lastKey []byte
	var lastValue []byte
	var lastCurrent int
	var lastNextOffset int
	var lastValid bool

	for {
		it.Next()
		if !it.Valid() {
			break
		}
		// Save current entry
		lastKey = append(lastKey[:0], it.key...)
		lastValue = it.value
		lastCurrent = it.current
		lastNextOffset = it.nextOffset
		lastValid = true
	}

	// Restore the last valid entry
	if lastValid {
		it.key = lastKey
		it.value = lastValue
		it.current = lastCurrent
		it.nextOffset = lastNextOffset
		it.valid = true
	}
}

// Next moves to the next entry.
func (it *Iterator) Next() {
	if it.err != nil {
		it.valid = false
		return
	}

	if it.nextOffset >= it.restartsEnd {
		it.valid = false
		return
	}

	it.current = it.nextOffset
	it.parseCurrentEntry()
}

// Prev moves to the previous entry.
// REQUIRES: Valid()
func (it *Iterator) Prev() {
	if it.err != nil {
		it.valid = false
		return
	}

	// We need to find the entry before current.
	// Strategy:
	// 1. Find the restart point at or before current
	// 2. If we're exactly at a restart point, use the previous restart point
	// 3. Scan forward from that restart point until we reach current
	// 4. The entry just before that is our target

	original := it.current

	// Find the restart point for current position
	restartIndex := it.findRestartPointBefore(original)

	// Check if we're exactly at this restart point
	// If so, we need to use the previous restart point to find entries before us
	restartOffset := it.block.GetRestartPoint(restartIndex)
	if restartOffset == original && restartIndex > 0 {
		restartIndex--
	}

	// Seek to that restart point
	it.seekToRestartPoint(restartIndex)

	// Scan forward until we reach the original position
	var prevKey []byte
	var prevValue []byte
	var prevCurrent int
	var prevNextOffset int
	found := false

	for {
		it.Next()
		if !it.Valid() || it.current >= original {
			break
		}
		// Save this entry as the previous
		prevKey = append(prevKey[:0], it.key...)
		prevValue = it.value
		prevCurrent = it.current
		prevNextOffset = it.nextOffset
		found = true
	}

	if found {
		it.key = prevKey
		it.value = prevValue
		it.current = prevCurrent
		it.nextOffset = prevNextOffset
		it.valid = true
	} else {
		// No previous entry exists (we were at the first entry)
		it.valid = false
	}
}

// findRestartPointBefore finds the largest restart index with offset <= target.
func (it *Iterator) findRestartPointBefore(target int) int {
	left := 0
	right := it.block.numRestarts - 1

	for left < right {
		mid := (left + right + 1) / 2
		offset := it.block.GetRestartPoint(mid)
		if offset <= target {
			left = mid
		} else {
			right = mid - 1
		}
	}
	return left
}

// seekToRestartPoint positions the iterator at the given restart point.
func (it *Iterator) seekToRestartPoint(index int) {
	it.key = it.key[:0]
	it.value = nil
	it.valid = false
	offset := max(it.block.GetRestartPoint(index), 0)
	it.current = offset
	it.nextOffset = offset
}

// parseCurrentEntry parses the entry at it.current.
func (it *Iterator) parseCurrentEntry() {
	if it.current >= it.restartsEnd {
		it.valid = false
		return
	}

	// Parse entry header
	data := it.data[it.current:]
	offset := 0

	// shared_bytes
	shared, n1, err := encoding.DecodeVarint32(data)
	if err != nil {
		it.err = ErrBadBlock
		it.valid = false
		return
	}
	offset += n1
	data = data[n1:]

	// unshared_bytes
	unshared, n2, err := encoding.DecodeVarint32(data)
	if err != nil {
		it.err = ErrBadBlock
		it.valid = false
		return
	}
	offset += n2
	data = data[n2:]

	// value_length
	valueLen, n3, err := encoding.DecodeVarint32(data)
	if err != nil {
		it.err = ErrBadBlock
		it.valid = false
		return
	}
	offset += n3
	data = data[n3:]

	// Validate
	if int(shared) > len(it.key) || len(data) < int(unshared)+int(valueLen) {
		it.err = ErrBadBlock
		it.valid = false
		return
	}

	// Build key: keep shared prefix, append unshared suffix
	it.key = append(it.key[:shared], data[:unshared]...)
	offset += int(unshared)
	data = data[unshared:]

	// Set value (slice into original data)
	it.value = data[:valueLen]
	offset += int(valueLen)

	// Update next offset
	it.nextOffset = it.current + offset
	it.valid = true
}

// Seek positions the iterator at the first key >= target.
// Uses binary search on restart points, then linear scan.
func (it *Iterator) Seek(target []byte) {
	// Binary search for the restart point
	left := 0
	right := it.block.numRestarts - 1

	// Find the rightmost restart point with key <= target
	for left < right {
		mid := (left + right + 1) / 2
		it.seekToRestartPoint(mid)
		it.Next()

		if !it.Valid() || it.compareKey(target) > 0 {
			// key at mid > target (or invalid), search left
			right = mid - 1
		} else {
			// key at mid <= target, search right
			left = mid
		}
	}

	// Linear scan from restart point
	it.seekToRestartPoint(left)
	for {
		it.Next()
		if !it.Valid() {
			return
		}
		if it.compareKey(target) >= 0 {
			return
		}
	}
}

// compareKey compares the current key with target using internal key semantics.
// Returns <0 if key < target, 0 if equal, >0 if key > target.
//
// Internal key comparison:
// 1. Compare user keys bytewise (ascending)
// 2. Compare trailers (seq << 8 | type) as 64-bit integer (descending)
//
// This means higher sequence numbers come first (earlier in sorted order).
func (it *Iterator) compareKey(target []byte) int {
	return CompareInternalKeys(it.key, target)
}

// CompareInternalKeys compares two internal keys.
// Internal key format: user_key + 8-byte trailer where trailer = (seq << 8) | type
//
// Comparison order:
// 1. User key (ascending bytewise)
// 2. Trailer (descending) - higher sequence numbers come first
func CompareInternalKeys(a, b []byte) int {
	const trailerSize = 8

	// Extract user keys
	var userKeyA, userKeyB []byte
	var trailerA, trailerB uint64

	if len(a) >= trailerSize {
		userKeyA = a[:len(a)-trailerSize]
		trailerA = decodeTrailer(a[len(a)-trailerSize:])
	} else {
		userKeyA = a
		trailerA = 0
	}

	if len(b) >= trailerSize {
		userKeyB = b[:len(b)-trailerSize]
		trailerB = decodeTrailer(b[len(b)-trailerSize:])
	} else {
		userKeyB = b
		trailerB = 0
	}

	// Compare user keys bytewise (ascending)
	cmp := bytesCompare(userKeyA, userKeyB)
	if cmp != 0 {
		return cmp
	}

	// User keys are equal, compare trailers (descending)
	// Higher trailer (higher seq) should come FIRST (return -1 if a > b)
	if trailerA > trailerB {
		return -1
	}
	if trailerA < trailerB {
		return 1
	}
	return 0
}

// decodeTrailer decodes an 8-byte little-endian trailer.
func decodeTrailer(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// bytesCompare compares two byte slices lexicographically.
func bytesCompare(a, b []byte) int {
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
