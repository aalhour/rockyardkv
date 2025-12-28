package memtable

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/rangedel"
)

// MemTable is an in-memory data structure that holds writes before they are
// flushed to SST files. It uses a SkipList for ordered storage.
//
// Entry format stored in the SkipList:
//
//	internal_key_size : varint32 (length of internal_key)
//	internal_key      : internal_key_size bytes (user_key + 8 bytes for seq+type)
//	value_size        : varint32 (length of value)
//	value             : value_size bytes
//
// Reference: RocksDB v10.7.5 db/memtable.cc
type MemTable struct {
	skiplist *SkipList
	compare  Comparator

	// Range tombstones stored separately from point data.
	// In RocksDB, range tombstones are stored in a separate data structure
	// and written to a separate meta block in SST files.
	rangeTombstones *rangedel.TombstoneList

	// Memory usage tracking
	memoryUsage int64

	// Sequence number range
	firstSeqno    dbformat.SequenceNumber
	earliestSeqno dbformat.SequenceNumber

	// Reference counting
	refs int32

	// nextLogNumber indicates which WAL files can be deleted after this memtable
	// is flushed. WAL files with number < nextLogNumber can be safely deleted.
	// This is set when the memtable becomes immutable, to the log number of
	// the NEW log file that will receive subsequent writes.
	// Reference: RocksDB v10.7.5 db/memtable.h mem_next_walfile_number_
	nextLogNumber uint64

	// Mutex for write synchronization
	mu sync.Mutex
}

// NewMemTable creates a new MemTable.
func NewMemTable(cmp Comparator) *MemTable {
	if cmp == nil {
		cmp = BytewiseComparator
	}

	// Use internal key comparator that compares by user key first,
	// then by sequence number (descending), then by type
	internalCmp := func(a, b []byte) int {
		return compareMemTableEntries(a, b, cmp)
	}

	return &MemTable{
		skiplist:        NewSkipList(internalCmp),
		compare:         cmp,
		rangeTombstones: rangedel.NewTombstoneList(),
		refs:            1,
		firstSeqno:      0,
		earliestSeqno:   ^dbformat.SequenceNumber(0),
	}
}

// extractInternalKey extracts the internal key from a memtable entry.
// Entry format: [keyLen:varint][internalKey][valueLen:varint][value]
// Returns the internal key or nil if invalid.
func extractInternalKey(entry []byte) []byte {
	if len(entry) < 2 {
		return nil
	}
	keyLen, n := decodeVarint32(entry)
	if n <= 0 || int(keyLen) > len(entry)-n {
		return nil
	}
	return entry[n : n+int(keyLen)]
}

// compareMemTableEntries compares two memtable entries.
// Entry format: [keyLen:varint][internalKey][valueLen:varint][value]
// Internal key format: user_key + 8-byte trailer (seq << 8 | type)
// Order: user_key ascending, seq descending, type descending
func compareMemTableEntries(a, b []byte, userCmp Comparator) int {
	// Extract internal keys from entries
	aInternalKey := extractInternalKey(a)
	bInternalKey := extractInternalKey(b)

	if aInternalKey == nil || bInternalKey == nil {
		// Fallback to byte comparison
		return userCmp(a, b)
	}

	// Extract user keys (all but last 8 bytes of internal key)
	if len(aInternalKey) < 8 || len(bInternalKey) < 8 {
		return userCmp(aInternalKey, bInternalKey)
	}

	aUserKey := aInternalKey[:len(aInternalKey)-8]
	bUserKey := bInternalKey[:len(bInternalKey)-8]

	// Compare user keys first
	cmp := userCmp(aUserKey, bUserKey)
	if cmp != 0 {
		return cmp
	}

	// User keys are equal - compare by sequence number (descending)
	// Trailer format: (seq << 8) | type
	aTrailer := binary.LittleEndian.Uint64(aInternalKey[len(aInternalKey)-8:])
	bTrailer := binary.LittleEndian.Uint64(bInternalKey[len(bInternalKey)-8:])

	// Higher sequence numbers (and types) should come first
	// Since trailer = (seq << 8) | type, comparing trailers directly works
	if aTrailer > bTrailer {
		return -1
	} else if aTrailer < bTrailer {
		return 1
	}
	return 0
}

// Ref increments the reference count.
func (mt *MemTable) Ref() {
	atomic.AddInt32(&mt.refs, 1)
}

// Unref decrements the reference count and returns true if no more references.
func (mt *MemTable) Unref() bool {
	return atomic.AddInt32(&mt.refs, -1) == 0
}

// Add inserts a key-value pair into the memtable.
// Type can be kTypeValue (Put) or kTypeDeletion (Delete).
func (mt *MemTable) Add(seq dbformat.SequenceNumber, typ dbformat.ValueType, key, value []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Build internal key: user_key + 8-byte trailer
	internalKeyLen := len(key) + 8
	trailer := dbformat.PackSequenceAndType(seq, typ)

	// Build the entry for the skiplist
	// Format: internal_key (user_key + trailer)
	// We store just the internal key in the skiplist
	// Values are stored separately or encoded together

	// For simplicity, we encode key and value together:
	// [internal_key_len:varint32][internal_key][value_len:varint32][value]
	entry := make([]byte, 0, internalKeyLen+len(value)+10)

	// Append internal key length as varint
	entry = appendVarint32(entry, uint32(internalKeyLen))

	// Append internal key
	entry = append(entry, key...)
	entry = append(entry, 0, 0, 0, 0, 0, 0, 0, 0) // placeholder for trailer
	binary.LittleEndian.PutUint64(entry[len(entry)-8:], trailer)

	// Append value length as varint
	entry = appendVarint32(entry, uint32(len(value)))

	// Append value
	entry = append(entry, value...)

	mt.skiplist.Insert(entry)

	// Update memory usage
	atomic.AddInt64(&mt.memoryUsage, int64(len(entry)+64)) // 64 for skiplist node overhead

	// Update sequence number tracking
	if seq < mt.earliestSeqno {
		mt.earliestSeqno = seq
	}
	if seq > mt.firstSeqno {
		mt.firstSeqno = seq
	}
}

// AddRangeTombstone adds a range deletion [startKey, endKey) at the given sequence number.
// Keys in this range with sequence numbers less than seq will be considered deleted.
func (mt *MemTable) AddRangeTombstone(seq dbformat.SequenceNumber, startKey, endKey []byte) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.rangeTombstones.AddRange(startKey, endKey, seq)

	// Estimate memory usage for the range tombstone
	memUsage := int64(len(startKey) + len(endKey) + 16) // keys + seq + overhead
	atomic.AddInt64(&mt.memoryUsage, memUsage)

	// Update sequence number tracking
	if seq < mt.earliestSeqno {
		mt.earliestSeqno = seq
	}
	if seq > mt.firstSeqno {
		mt.firstSeqno = seq
	}
}

// GetRangeTombstones returns the range tombstones in this memtable.
func (mt *MemTable) GetRangeTombstones() *rangedel.TombstoneList {
	return mt.rangeTombstones
}

// GetFragmentedRangeTombstones returns the range tombstones as a fragmented list
// for efficient lookup.
func (mt *MemTable) GetFragmentedRangeTombstones() *rangedel.FragmentedRangeTombstoneList {
	if mt.rangeTombstones.IsEmpty() {
		return rangedel.NewFragmentedRangeTombstoneList()
	}

	f := rangedel.NewFragmenter()
	for _, t := range mt.rangeTombstones.All() {
		f.AddTombstone(t)
	}
	return f.Finish()
}

// HasRangeTombstones returns true if the memtable has any range tombstones.
func (mt *MemTable) HasRangeTombstones() bool {
	return !mt.rangeTombstones.IsEmpty()
}

// RangeTombstoneCount returns the number of range tombstones.
func (mt *MemTable) RangeTombstoneCount() int {
	return mt.rangeTombstones.Len()
}

// Get looks up a key in the memtable.
// Returns the value and whether the key was found.
// If the key was deleted, returns nil value with found=true and a deletion status.
func (mt *MemTable) Get(key []byte, seq dbformat.SequenceNumber) (value []byte, found bool, deleted bool) {
	// Build a lookup key: user_key + max sequence number
	// This will position us at the first entry for this user key
	lookupKey := make([]byte, len(key)+8)
	copy(lookupKey, key)
	binary.LittleEndian.PutUint64(lookupKey[len(key):], dbformat.PackSequenceAndType(seq, dbformat.ValueTypeForSeek))

	iter := mt.skiplist.NewIterator()
	iter.Seek(buildLookupEntry(lookupKey))

	// Find the highest sequence number among range tombstones covering this key
	var rangeDelSeq dbformat.SequenceNumber
	if !mt.rangeTombstones.IsEmpty() {
		rangeDelSeq = mt.getMaxRangeTombstoneSeq(key, seq)
	}

	if !iter.Valid() {
		// No point data, but check if a range tombstone covers this key
		if rangeDelSeq > 0 {
			return nil, true, true
		}
		return nil, false, false
	}

	// Parse the entry
	entryKey, entryValue, entrySeq, entryType, ok := parseEntry(iter.Key())
	if !ok {
		// No valid point data, check range tombstone
		if rangeDelSeq > 0 {
			return nil, true, true
		}
		return nil, false, false
	}

	// Check if user keys match
	if mt.compare(key, entryKey) != 0 {
		// No matching point data, check range tombstone
		if rangeDelSeq > 0 {
			return nil, true, true
		}
		return nil, false, false
	}

	// Check if sequence number is visible
	if entrySeq > seq {
		// Point data not visible, check range tombstone
		if rangeDelSeq > 0 {
			return nil, true, true
		}
		return nil, false, false
	}

	// Check if a range tombstone with higher seq supersedes the point data
	if rangeDelSeq > entrySeq {
		return nil, true, true
	}

	// Check value type
	switch entryType {
	case dbformat.TypeValue:
		return entryValue, true, false
	case dbformat.TypeDeletion, dbformat.TypeSingleDeletion:
		return nil, true, true
	case dbformat.TypeMerge:
		// Return the merge operand - caller will handle merge resolution
		return entryValue, true, false
	default:
		return nil, false, false
	}
}

// GetWithMerge is like Get but also returns whether the entry is a merge operand.
// Returns: value, found, deleted, isMerge
func (mt *MemTable) GetWithMerge(key []byte, seq dbformat.SequenceNumber) (value []byte, found bool, deleted bool, isMerge bool) {
	// Build a lookup key: user_key + max sequence number
	lookupKey := make([]byte, len(key)+8)
	copy(lookupKey, key)
	binary.LittleEndian.PutUint64(lookupKey[len(key):], dbformat.PackSequenceAndType(seq, dbformat.ValueTypeForSeek))

	iter := mt.skiplist.NewIterator()
	iter.Seek(buildLookupEntry(lookupKey))

	// Find the highest sequence number among range tombstones covering this key
	var rangeDelSeq dbformat.SequenceNumber
	if !mt.rangeTombstones.IsEmpty() {
		rangeDelSeq = mt.getMaxRangeTombstoneSeq(key, seq)
	}

	if !iter.Valid() {
		if rangeDelSeq > 0 {
			return nil, true, true, false
		}
		return nil, false, false, false
	}

	// Parse the entry
	entryKey, entryValue, entrySeq, entryType, ok := parseEntry(iter.Key())
	if !ok {
		if rangeDelSeq > 0 {
			return nil, true, true, false
		}
		return nil, false, false, false
	}

	// Check if user keys match
	if mt.compare(key, entryKey) != 0 {
		if rangeDelSeq > 0 {
			return nil, true, true, false
		}
		return nil, false, false, false
	}

	// Check if sequence number is visible
	if entrySeq > seq {
		if rangeDelSeq > 0 {
			return nil, true, true, false
		}
		return nil, false, false, false
	}

	// Check if a range tombstone with higher seq supersedes the point data
	if rangeDelSeq > entrySeq {
		return nil, true, true, false
	}

	// Check value type
	switch entryType {
	case dbformat.TypeValue:
		return entryValue, true, false, false
	case dbformat.TypeDeletion, dbformat.TypeSingleDeletion:
		return nil, true, true, false
	case dbformat.TypeMerge:
		return entryValue, true, false, true
	default:
		return nil, false, false, false
	}
}

// CollectMergeOperands collects all merge operands for a key until a base value or deletion is found.
// Returns: baseValue (nil if not found or deleted), mergeOperands (newest first), foundBase, deleted
func (mt *MemTable) CollectMergeOperands(key []byte, seq dbformat.SequenceNumber) (baseValue []byte, mergeOperands [][]byte, foundBase bool, deleted bool) {
	// Build a lookup key: user_key + max sequence number
	lookupKey := make([]byte, len(key)+8)
	copy(lookupKey, key)
	binary.LittleEndian.PutUint64(lookupKey[len(key):], dbformat.PackSequenceAndType(seq, dbformat.ValueTypeForSeek))

	iter := mt.skiplist.NewIterator()
	iter.Seek(buildLookupEntry(lookupKey))

	// Find the highest sequence number among range tombstones covering this key
	var rangeDelSeq dbformat.SequenceNumber
	if !mt.rangeTombstones.IsEmpty() {
		rangeDelSeq = mt.getMaxRangeTombstoneSeq(key, seq)
	}

	// Iterate through all entries for this key
	for iter.Valid() {
		entryKey, entryValue, entrySeq, entryType, ok := parseEntry(iter.Key())
		if !ok {
			break
		}

		// Check if user keys still match
		if mt.compare(key, entryKey) != 0 {
			break
		}

		// Check if sequence number is visible
		if entrySeq > seq {
			iter.Next()
			continue
		}

		// Check if a range tombstone with higher seq supersedes this entry
		if rangeDelSeq > entrySeq {
			return nil, mergeOperands, false, true
		}

		// Process based on value type
		switch entryType {
		case dbformat.TypeValue:
			// Found base value
			return entryValue, mergeOperands, true, false
		case dbformat.TypeDeletion, dbformat.TypeSingleDeletion:
			// Key was deleted
			return nil, mergeOperands, false, true
		case dbformat.TypeMerge:
			// Collect merge operand
			mergeOperands = append(mergeOperands, entryValue)
		}

		iter.Next()
	}

	// If we only checked range tombstone at the end
	if rangeDelSeq > 0 && len(mergeOperands) == 0 {
		return nil, nil, false, true
	}

	return nil, mergeOperands, false, false
}

// getMaxRangeTombstoneSeq returns the maximum sequence number among range
// tombstones that cover the given key and are visible at the given sequence.
func (mt *MemTable) getMaxRangeTombstoneSeq(key []byte, visibleSeq dbformat.SequenceNumber) dbformat.SequenceNumber {
	var maxSeq dbformat.SequenceNumber
	for _, t := range mt.rangeTombstones.All() {
		// Check if tombstone is visible
		if t.SequenceNum > visibleSeq {
			continue
		}
		// Check if tombstone covers the key
		if t.Contains(key) && t.SequenceNum > maxSeq {
			maxSeq = t.SequenceNum
		}
	}
	return maxSeq
}

// buildLookupEntry builds an entry suitable for seeking.
func buildLookupEntry(internalKey []byte) []byte {
	entry := make([]byte, 0, len(internalKey)+5)
	entry = appendVarint32(entry, uint32(len(internalKey)))
	entry = append(entry, internalKey...)
	return entry
}

// parseEntry parses a memtable entry and returns its components.
func parseEntry(entry []byte) (key, value []byte, seq dbformat.SequenceNumber, typ dbformat.ValueType, ok bool) {
	if len(entry) < 2 {
		return nil, nil, 0, 0, false
	}

	// Parse internal key length
	keyLen, n := decodeVarint32(entry)
	if n <= 0 || int(keyLen) > len(entry)-n {
		return nil, nil, 0, 0, false
	}
	entry = entry[n:]

	if keyLen < 8 {
		return nil, nil, 0, 0, false
	}

	// Extract internal key
	internalKey := entry[:keyLen]
	entry = entry[keyLen:]

	// Parse user key and trailer
	key = internalKey[:keyLen-8]
	trailer := binary.LittleEndian.Uint64(internalKey[keyLen-8:])
	seq, typ = dbformat.UnpackSequenceAndType(trailer)

	// Parse value length
	if len(entry) < 1 {
		return key, nil, seq, typ, true // No value (deletion)
	}

	valueLen, n := decodeVarint32(entry)
	if n <= 0 {
		return nil, nil, 0, 0, false
	}
	entry = entry[n:]

	if int(valueLen) > len(entry) {
		return nil, nil, 0, 0, false
	}

	value = entry[:valueLen]
	return key, value, seq, typ, true
}

// ApproximateMemoryUsage returns the approximate memory usage in bytes.
func (mt *MemTable) ApproximateMemoryUsage() int64 {
	return atomic.LoadInt64(&mt.memoryUsage)
}

// NextLogNumber returns the log number that can be deleted after this memtable
// is flushed. WAL files with number < NextLogNumber() can be safely deleted.
// Returns 0 if not set.
func (mt *MemTable) NextLogNumber() uint64 {
	return atomic.LoadUint64(&mt.nextLogNumber)
}

// SetNextLogNumber sets the log number for deletion after flush.
// This should be called when the memtable becomes immutable.
func (mt *MemTable) SetNextLogNumber(num uint64) {
	atomic.StoreUint64(&mt.nextLogNumber, num)
}

// Count returns the number of entries in the memtable.
func (mt *MemTable) Count() int64 {
	return mt.skiplist.Count()
}

// Empty returns true if the memtable has no entries.
func (mt *MemTable) Empty() bool {
	return mt.Count() == 0
}

// NewIterator returns an iterator over the memtable.
func (mt *MemTable) NewIterator() *MemTableIterator {
	return &MemTableIterator{
		iter:    mt.skiplist.NewIterator(),
		compare: mt.compare,
	}
}

// MemTableIterator iterates over memtable entries.
type MemTableIterator struct {
	iter    *Iterator
	compare Comparator

	// Cached parsed values
	userKey []byte
	value   []byte
	seq     dbformat.SequenceNumber
	typ     dbformat.ValueType
	valid   bool
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *MemTableIterator) Valid() bool {
	return it.valid && it.iter.Valid()
}

// SeekToFirst positions the iterator at the first entry.
func (it *MemTableIterator) SeekToFirst() {
	it.iter.SeekToFirst()
	it.parseCurrentEntry()
}

// SeekToLast positions the iterator at the last entry.
func (it *MemTableIterator) SeekToLast() {
	it.iter.SeekToLast()
	it.parseCurrentEntry()
}

// Seek positions the iterator at the first entry with key >= target.
func (it *MemTableIterator) Seek(target []byte) {
	it.iter.Seek(buildLookupEntry(target))
	it.parseCurrentEntry()
}

// Next advances to the next entry.
func (it *MemTableIterator) Next() {
	it.iter.Next()
	it.parseCurrentEntry()
}

// Prev moves to the previous entry.
func (it *MemTableIterator) Prev() {
	it.iter.Prev()
	it.parseCurrentEntry()
}

// UserKey returns the user key (without internal key suffix).
func (it *MemTableIterator) UserKey() []byte {
	return it.userKey
}

// Key returns the full internal key (userKey + sequence + type).
func (it *MemTableIterator) Key() []byte {
	// Reconstruct the internal key from parsed components
	key := make([]byte, len(it.userKey)+8)
	copy(key, it.userKey)
	trailer := (uint64(it.seq) << 8) | uint64(it.typ)
	key[len(it.userKey)] = byte(trailer)
	key[len(it.userKey)+1] = byte(trailer >> 8)
	key[len(it.userKey)+2] = byte(trailer >> 16)
	key[len(it.userKey)+3] = byte(trailer >> 24)
	key[len(it.userKey)+4] = byte(trailer >> 32)
	key[len(it.userKey)+5] = byte(trailer >> 40)
	key[len(it.userKey)+6] = byte(trailer >> 48)
	key[len(it.userKey)+7] = byte(trailer >> 56)
	return key
}

// Value returns the value.
func (it *MemTableIterator) Value() []byte {
	return it.value
}

// Error returns any error that occurred during iteration.
func (it *MemTableIterator) Error() error {
	return nil // MemTable iteration doesn't have errors
}

// Sequence returns the sequence number.
func (it *MemTableIterator) Sequence() dbformat.SequenceNumber {
	return it.seq
}

// Type returns the value type.
func (it *MemTableIterator) Type() dbformat.ValueType {
	return it.typ
}

// parseCurrentEntry parses the current entry from the underlying skiplist iterator.
func (it *MemTableIterator) parseCurrentEntry() {
	if !it.iter.Valid() {
		it.valid = false
		it.userKey = nil
		it.value = nil
		return
	}

	var ok bool
	it.userKey, it.value, it.seq, it.typ, ok = parseEntry(it.iter.Key())
	it.valid = ok
}

// Helper functions

func appendVarint32(buf []byte, v uint32) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

func decodeVarint32(data []byte) (uint32, int) {
	var v uint32
	for i := 0; i < 5 && i < len(data); i++ {
		b := data[i]
		v |= uint32(b&0x7F) << (7 * i)
		if b < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0 // Invalid varint
}
