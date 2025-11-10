// iterator.go implements the database iterator.
//
// DBIterator provides a way to iterate over all keys in the database,
// merging data from memtables and SST files at each level.
//
// Reference: RocksDB v10.7.5
//   - db/db_iter.h
//   - db/db_iter.cc
package db

import (
	"bytes"
	"errors"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/rangedel"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/version"
)

// ErrIteratorInvalid indicates an operation was attempted on an invalid iterator.
var ErrIteratorInvalid = errors.New("db: iterator is not valid")

// Iterator provides a way to iterate over keys in the database.
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid entry.
	Valid() bool

	// SeekToFirst positions the iterator at the first key.
	SeekToFirst()

	// SeekToLast positions the iterator at the last key.
	SeekToLast()

	// Seek positions the iterator at the first key >= target.
	Seek(target []byte)

	// SeekForPrev positions the iterator at the last key <= target.
	SeekForPrev(target []byte)

	// Next moves the iterator to the next key.
	Next()

	// Prev moves the iterator to the previous key.
	Prev()

	// Key returns the key at the current position.
	// REQUIRES: Valid()
	Key() []byte

	// Value returns the value at the current position.
	// REQUIRES: Valid()
	Value() []byte

	// Error returns any error that has occurred.
	Error() error

	// Close releases resources associated with the iterator.
	Close() error
}

// errorIterator is an iterator that always returns an error.
type errorIterator struct {
	err error
}

func (it *errorIterator) Valid() bool               { return false }
func (it *errorIterator) SeekToFirst()              {}
func (it *errorIterator) SeekToLast()               {}
func (it *errorIterator) Seek(target []byte)        {}
func (it *errorIterator) SeekForPrev(target []byte) {}
func (it *errorIterator) Next()                     {}
func (it *errorIterator) Prev()                     {}
func (it *errorIterator) Key() []byte               { return nil }
func (it *errorIterator) Value() []byte             { return nil }
func (it *errorIterator) Error() error              { return it.err }
func (it *errorIterator) Close() error              { return nil }

// dbIterator is the internal iterator implementation for the database.
// It merges memtable and SST file iterators, deduplicates keys, and skips deletions.
type dbIterator struct {
	db       *DBImpl
	cfd      *columnFamilyData // Column family (nil = use default via db.mem)
	snapshot *Snapshot
	err      error
	valid    bool

	// Internal iterators
	memIter  *memtable.MemTableIterator
	immIter  *memtable.MemTableIterator // Immutable memtable iterator
	sstIters []*sstIterWrapper          // SST file iterators

	// Version reference (to keep SST files alive)
	version *version.Version

	// Range deletion aggregator for checking if keys are covered by tombstones
	rangeDelAgg *rangedel.RangeDelAggregator

	// Merged iterator state
	iterators   []internalIterator
	currentIter int // Index of current best iterator

	// savedKey is the current user key we're positioned at
	savedKey []byte
	// savedValue is the current value
	savedValue []byte

	// direction indicates whether we're moving forward or backward
	direction int // 1 = forward, -1 = backward, 0 = not moving

	// Prefix seek support
	prefixExtractor   PrefixExtractor
	iterateUpperBound []byte
	iterateLowerBound []byte
	prefixSameAsStart bool
	totalOrderSeek    bool
	seekPrefix        []byte // Prefix from the initial Seek call

	// Comparator for key comparison (nil means use bytewise)
	comparator Comparator
}

// compareKeys compares two user keys using the configured comparator.
// Returns < 0 if a < b, 0 if a == b, > 0 if a > b.
func (it *dbIterator) compareKeys(a, b []byte) int {
	if it.comparator != nil {
		return it.comparator.Compare(a, b)
	}
	return bytes.Compare(a, b)
}

// keysEqual checks if two user keys are equal using the configured comparator.
func (it *dbIterator) keysEqual(a, b []byte) bool {
	if it.comparator != nil {
		return it.comparator.Compare(a, b) == 0
	}
	return bytes.Equal(a, b)
}

const (
	dirForward  = 1
	dirBackward = -1
)

// internalIterator wraps different iterator types with a common interface.
type internalIterator interface {
	Valid() bool
	Key() []byte   // Returns internal key
	Value() []byte // Returns value
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Next()
	Prev()
	UserKey() []byte
	SeqNum() uint64
	Type() dbformat.ValueType
	Error() error
}

// memtableIterWrapper wraps a memtable iterator.
type memtableIterWrapper struct {
	iter *memtable.MemTableIterator
}

func (w *memtableIterWrapper) Valid() bool              { return w.iter.Valid() }
func (w *memtableIterWrapper) Key() []byte              { return w.iter.Key() }
func (w *memtableIterWrapper) Value() []byte            { return w.iter.Value() }
func (w *memtableIterWrapper) SeekToFirst()             { w.iter.SeekToFirst() }
func (w *memtableIterWrapper) SeekToLast()              { w.iter.SeekToLast() }
func (w *memtableIterWrapper) Seek(target []byte)       { w.iter.Seek(target) }
func (w *memtableIterWrapper) Next()                    { w.iter.Next() }
func (w *memtableIterWrapper) Prev()                    { w.iter.Prev() }
func (w *memtableIterWrapper) UserKey() []byte          { return w.iter.UserKey() }
func (w *memtableIterWrapper) SeqNum() uint64           { return uint64(w.iter.Sequence()) }
func (w *memtableIterWrapper) Type() dbformat.ValueType { return w.iter.Type() }
func (w *memtableIterWrapper) Error() error             { return w.iter.Error() }

// sstIterWrapper wraps an SST table iterator.
type sstIterWrapper struct {
	iter     *table.TableIterator
	fileNum  uint64
	reader   *table.Reader
	released bool
}

func (w *sstIterWrapper) Valid() bool        { return w.iter != nil && w.iter.Valid() }
func (w *sstIterWrapper) Key() []byte        { return w.iter.Key() }
func (w *sstIterWrapper) Value() []byte      { return w.iter.Value() }
func (w *sstIterWrapper) SeekToFirst()       { w.iter.SeekToFirst() }
func (w *sstIterWrapper) SeekToLast()        { w.iter.SeekToLast() }
func (w *sstIterWrapper) Seek(target []byte) { w.iter.Seek(target) }
func (w *sstIterWrapper) Next()              { w.iter.Next() }
func (w *sstIterWrapper) Prev()              { w.iter.Prev() }
func (w *sstIterWrapper) Error() error       { return w.iter.Error() }

func (w *sstIterWrapper) UserKey() []byte {
	key := w.iter.Key()
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

func (w *sstIterWrapper) SeqNum() uint64 {
	key := w.iter.Key()
	if len(key) < 8 {
		return 0
	}
	tag := decodeFixed64(key[len(key)-8:])
	return tag >> 8
}

func (w *sstIterWrapper) Type() dbformat.ValueType {
	key := w.iter.Key()
	if len(key) < 8 {
		return dbformat.TypeValue
	}
	return dbformat.ValueType(key[len(key)-8])
}

func decodeFixed64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

// newDBIterator creates a new database iterator for the default column family.
// Reserved - currently NewIterator uses newDBIteratorCF directly.
func newDBIterator(db *DBImpl, snapshot *Snapshot) *dbIterator { //nolint:unused // reserved for future use
	return newDBIteratorCF(db, nil, snapshot)
}

// newDBIteratorCF creates a new database iterator for a specific column family.
func newDBIteratorCF(db *DBImpl, cfd *columnFamilyData, snapshot *Snapshot) *dbIterator {
	// Determine snapshot sequence number for range deletion visibility
	var snapshotSeq dbformat.SequenceNumber
	if snapshot != nil {
		snapshotSeq = dbformat.SequenceNumber(snapshot.Sequence())
	} else {
		snapshotSeq = dbformat.MaxSequenceNumber
	}

	iter := &dbIterator{
		db:          db,
		cfd:         cfd,
		snapshot:    snapshot,
		rangeDelAgg: rangedel.NewRangeDelAggregator(snapshotSeq),
		comparator:  db.comparator,
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// Get memtable iterators
	var mem, imm *memtable.MemTable
	if cfd == nil || cfd.id == DefaultColumnFamilyID {
		mem = db.mem
		imm = db.imm
	} else {
		cfd.memMu.RLock()
		mem = cfd.mem
		if len(cfd.imm) > 0 {
			imm = cfd.imm[0]
		}
		cfd.memMu.RUnlock()
	}

	if mem != nil {
		mem.Ref()
		iter.memIter = mem.NewIterator()
		iter.iterators = append(iter.iterators, &memtableIterWrapper{iter: iter.memIter})

		// Add range tombstones from memtable to aggregator (level -1)
		if mem.HasRangeTombstones() {
			fragmented := mem.GetFragmentedRangeTombstones()
			iter.rangeDelAgg.AddTombstones(-1, fragmented)
		}
	}
	if imm != nil {
		imm.Ref()
		iter.immIter = imm.NewIterator()
		iter.iterators = append(iter.iterators, &memtableIterWrapper{iter: iter.immIter})

		// Add range tombstones from immutable memtable to aggregator (level -1)
		if imm.HasRangeTombstones() {
			fragmented := imm.GetFragmentedRangeTombstones()
			iter.rangeDelAgg.AddTombstones(-1, fragmented)
		}
	}

	// Get SST iterators from the current version
	v := db.versions.Current()
	if v != nil {
		v.Ref()
		iter.version = v

		// Add iterators for all SST files
		for level := range v.NumLevels() {
			files := v.Files(level)
			for _, f := range files {
				sstIter := iter.createSSTIterator(f)
				if sstIter != nil {
					iter.sstIters = append(iter.sstIters, sstIter)
					iter.iterators = append(iter.iterators, sstIter)

					// Add range tombstones from this SST file to aggregator
					if sstIter.reader != nil {
						tombstoneList, err := sstIter.reader.GetRangeTombstoneList()
						if err == nil && !tombstoneList.IsEmpty() {
							iter.rangeDelAgg.AddTombstoneList(level, tombstoneList)
						}
					}
				}
			}
		}
	}

	return iter
}

// createSSTIterator creates an iterator for an SST file.
func (it *dbIterator) createSSTIterator(f *manifest.FileMetaData) *sstIterWrapper {
	fileNum := f.FD.GetNumber()
	path := it.db.sstFilePath(fileNum)

	reader, err := it.db.tableCache.Get(fileNum, path)
	if err != nil {
		it.err = err
		return nil
	}

	return &sstIterWrapper{
		iter:    reader.NewIterator(),
		fileNum: fileNum,
		reader:  reader,
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *dbIterator) Valid() bool {
	return it.valid && it.err == nil
}

// SeekToFirst positions the iterator at the first key.
func (it *dbIterator) SeekToFirst() {
	it.direction = dirForward
	it.err = nil
	it.seekPrefix = nil // Clear prefix on SeekToFirst

	// If we have a lower bound, seek to it instead
	if len(it.iterateLowerBound) > 0 {
		it.Seek(it.iterateLowerBound)
		return
	}

	// Seek all iterators to first
	for _, iter := range it.iterators {
		iter.SeekToFirst()
	}

	it.findNextValidEntry()
}

// SeekToLast positions the iterator at the last key.
func (it *dbIterator) SeekToLast() {
	it.direction = dirBackward
	it.err = nil

	// Seek all iterators to last
	for _, iter := range it.iterators {
		iter.SeekToLast()
	}

	// If we have an upper bound, we need to position before it
	// This is done in findPrevValidEntry which checks the lower bound,
	// but we also need to handle upper bound for SeekToLast
	if len(it.iterateUpperBound) > 0 {
		// Move each iterator backward until it's before the upper bound
		for _, iter := range it.iterators {
			for iter.Valid() && it.compareKeys(iter.UserKey(), it.iterateUpperBound) >= 0 {
				iter.Prev()
			}
		}
	}

	it.findPrevValidEntry()
}

// Seek positions the iterator at the first key >= target.
func (it *dbIterator) Seek(target []byte) {
	it.direction = dirForward
	it.err = nil

	// Check lower bound
	if len(it.iterateLowerBound) > 0 && bytes.Compare(target, it.iterateLowerBound) < 0 {
		target = it.iterateLowerBound
	}

	// Capture the prefix for prefix_same_as_start optimization
	if it.prefixSameAsStart && it.prefixExtractor != nil && it.prefixExtractor.InDomain(target) {
		prefix := it.prefixExtractor.Transform(target)
		it.seekPrefix = make([]byte, len(prefix))
		copy(it.seekPrefix, prefix)
	} else {
		it.seekPrefix = nil
	}

	// Create an internal key for seeking (target + max sequence number)
	seekKey := makeInternalKey(target, uint64(dbformat.MaxSequenceNumber), dbformat.ValueTypeForSeek)

	// Seek all iterators
	for _, iter := range it.iterators {
		iter.Seek(seekKey)
	}

	it.findNextValidEntry()
}

// SeekForPrev positions the iterator at the last key <= target.
func (it *dbIterator) SeekForPrev(target []byte) {
	it.direction = dirBackward
	// First seek to target
	it.Seek(target)
	if !it.Valid() {
		it.SeekToLast()
	} else if bytes.Compare(it.Key(), target) > 0 {
		it.Prev()
	}
}

// Next moves the iterator to the next key.
func (it *dbIterator) Next() {
	if !it.valid {
		return
	}

	prevDirection := it.direction
	it.direction = dirForward

	// If we were going backward, we need to reseek all iterators forward
	// Reference: RocksDB DBIter::ReverseToForward()
	if prevDirection == dirBackward {
		it.resyncIteratorsForward()
		return
	}

	// Skip past all entries with the same user key across all iterators
	for _, iter := range it.iterators {
		for iter.Valid() && it.keysEqual(iter.UserKey(), it.savedKey) {
			iter.Next()
		}
	}

	it.findNextValidEntry()
}

// Prev moves the iterator to the previous key.
func (it *dbIterator) Prev() {
	if !it.valid {
		return
	}

	prevDirection := it.direction
	it.direction = dirBackward

	// If we were going forward, we need to reseek all iterators backward
	// Reference: RocksDB DBIter::ReverseToBackward()
	if prevDirection == dirForward {
		it.resyncIteratorsBackward()
		return
	}

	// Move each iterator backward past current key
	for _, iter := range it.iterators {
		for iter.Valid() && it.keysEqual(iter.UserKey(), it.savedKey) {
			iter.Prev()
		}
	}

	it.findPrevValidEntry()
}

// resyncIteratorsForward repositions all iterators for forward iteration
// after a direction change from backward to forward.
// This ensures all iterators are positioned at keys > savedKey.
func (it *dbIterator) resyncIteratorsForward() {
	// Create a seek key that will position us just after the current key
	// We use the current key + min sequence number to ensure we land after it
	seekKey := makeInternalKey(it.savedKey, 0, dbformat.TypeValue)

	for _, iter := range it.iterators {
		iter.Seek(seekKey)
		// After seeking, we might land exactly on savedKey with seq 0
		// So advance past any entries with savedKey
		for iter.Valid() && it.keysEqual(iter.UserKey(), it.savedKey) {
			iter.Next()
		}
	}

	it.findNextValidEntry()
}

// resyncIteratorsBackward repositions all iterators for backward iteration
// after a direction change from forward to backward.
// This ensures all iterators are positioned at keys < savedKey.
// Reference: RocksDB DBIter::ReverseToBackward()
func (it *dbIterator) resyncIteratorsBackward() {
	// Create a seek key that will position us at or just after the current key
	seekKey := makeInternalKey(it.savedKey, uint64(dbformat.MaxSequenceNumber), dbformat.ValueTypeForSeek)

	for _, iter := range it.iterators {
		iter.Seek(seekKey)

		// After Seek, we're at the first key >= savedKey (or invalid if no such key)
		// We need to position at the last key < savedKey

		if iter.Valid() {
			// If we landed on a key > savedKey, we need to Prev() once
			// If we landed on savedKey, we need to Prev() past all versions of savedKey
			if it.compareKeys(iter.UserKey(), it.savedKey) > 0 {
				// Landed after savedKey, Prev once to get before it
				iter.Prev()
			} else {
				// Landed on savedKey (or earlier due to seek semantics)
				// Skip past all versions of savedKey
				for iter.Valid() && it.keysEqual(iter.UserKey(), it.savedKey) {
					iter.Prev()
				}
			}
		} else {
			// Seek went past all keys in this iterator
			// SeekToLast to position at the last key
			iter.SeekToLast()
			// Then skip past savedKey if we happen to land on it
			for iter.Valid() && it.keysEqual(iter.UserKey(), it.savedKey) {
				iter.Prev()
			}
		}
	}

	it.findPrevValidEntry()
}

// findNextValidEntry finds the smallest key across all iterators
// and skips older versions and deletions.
func (it *dbIterator) findNextValidEntry() {
outerLoop:
	for {
		// Find the iterator with the smallest key
		minIdx := -1
		var minKey []byte
		var minSeq uint64

		for i, iter := range it.iterators {
			if !iter.Valid() {
				continue
			}
			if err := iter.Error(); err != nil {
				it.err = err
				it.valid = false
				return
			}

			userKey := iter.UserKey()
			seq := iter.SeqNum()

			// Check snapshot visibility
			if it.snapshot != nil && seq > it.snapshot.Sequence() {
				// This entry is not visible to the snapshot
				// Advance this iterator and restart the whole scan
				iter.Next()
				continue outerLoop
			}

			if minIdx == -1 {
				minIdx = i
				minKey = userKey
				minSeq = seq
			} else {
				cmp := it.compareKeys(userKey, minKey)
				if cmp < 0 {
					// Smaller key found
					minIdx = i
					minKey = userKey
					minSeq = seq
				} else if cmp == 0 && seq > minSeq {
					// Same key, but higher sequence number (newer)
					minIdx = i
					minSeq = seq
				}
			}
		}

		if minIdx == -1 {
			// No more entries
			it.valid = false
			return
		}

		// Check if this is a point deletion
		valueType := it.iterators[minIdx].Type()
		if valueType == dbformat.TypeDeletion || valueType == dbformat.TypeSingleDeletion {
			// Make a copy of minKey before skipping, since the underlying buffer may be reused
			keyToSkip := make([]byte, len(minKey))
			copy(keyToSkip, minKey)

			// Skip this key in all iterators
			for _, iter := range it.iterators {
				for iter.Valid() && it.keysEqual(iter.UserKey(), keyToSkip) {
					iter.Next()
				}
			}
			continue
		}

		// Check if this key is covered by a range tombstone
		if it.rangeDelAgg != nil && it.rangeDelAgg.ShouldDelete(minKey, dbformat.SequenceNumber(minSeq)) {
			// Make a copy of minKey before skipping
			keyToSkip := make([]byte, len(minKey))
			copy(keyToSkip, minKey)

			// Skip this key in all iterators
			for _, iter := range it.iterators {
				for iter.Valid() && it.keysEqual(iter.UserKey(), keyToSkip) {
					iter.Next()
				}
			}
			continue
		}

		// Check upper bound
		if len(it.iterateUpperBound) > 0 && it.compareKeys(minKey, it.iterateUpperBound) >= 0 {
			it.valid = false
			return
		}

		// Check prefix constraint for prefix seek
		if it.prefixSameAsStart && len(it.seekPrefix) > 0 && it.prefixExtractor != nil {
			if it.prefixExtractor.InDomain(minKey) {
				keyPrefix := it.prefixExtractor.Transform(minKey)
				if !bytes.Equal(keyPrefix, it.seekPrefix) {
					// We've left the prefix, stop iteration
					it.valid = false
					return
				}
			}
		}

		// Found a valid entry
		it.savedKey = make([]byte, len(minKey))
		copy(it.savedKey, minKey)
		it.savedValue = make([]byte, len(it.iterators[minIdx].Value()))
		copy(it.savedValue, it.iterators[minIdx].Value())
		it.currentIter = minIdx
		it.valid = true
		return
	}
}

// findPrevValidEntry finds the largest key across all iterators
// and skips older versions and deletions.
func (it *dbIterator) findPrevValidEntry() {
outerLoop:
	for {
		// Find the iterator with the largest key
		maxIdx := -1
		var maxKey []byte
		var maxSeq uint64

		for i, iter := range it.iterators {
			if !iter.Valid() {
				continue
			}
			if err := iter.Error(); err != nil {
				it.err = err
				it.valid = false
				return
			}

			userKey := iter.UserKey()
			seq := iter.SeqNum()

			// Check snapshot visibility
			if it.snapshot != nil && seq > it.snapshot.Sequence() {
				// Advance this iterator backward and restart the whole scan
				iter.Prev()
				continue outerLoop
			}

			if maxIdx == -1 {
				maxIdx = i
				maxKey = userKey
				maxSeq = seq
			} else {
				cmp := it.compareKeys(userKey, maxKey)
				if cmp > 0 {
					// Larger key found
					maxIdx = i
					maxKey = userKey
					maxSeq = seq
				} else if cmp == 0 && seq > maxSeq {
					// Same key, but higher sequence number (newer)
					maxIdx = i
					maxSeq = seq
				}
			}
		}

		if maxIdx == -1 {
			// No more entries
			it.valid = false
			return
		}

		// Check if this is a point deletion
		valueType := it.iterators[maxIdx].Type()
		if valueType == dbformat.TypeDeletion || valueType == dbformat.TypeSingleDeletion {
			// Make a copy of maxKey before skipping, since the underlying buffer may be reused
			keyToSkip := make([]byte, len(maxKey))
			copy(keyToSkip, maxKey)

			// Skip this key in all iterators
			for _, iter := range it.iterators {
				for iter.Valid() && it.keysEqual(iter.UserKey(), keyToSkip) {
					iter.Prev()
				}
			}
			continue
		}

		// Check if this key is covered by a range tombstone
		if it.rangeDelAgg != nil && it.rangeDelAgg.ShouldDelete(maxKey, dbformat.SequenceNumber(maxSeq)) {
			// Make a copy of maxKey before skipping
			keyToSkip := make([]byte, len(maxKey))
			copy(keyToSkip, maxKey)

			// Skip this key in all iterators
			for _, iter := range it.iterators {
				for iter.Valid() && it.keysEqual(iter.UserKey(), keyToSkip) {
					iter.Prev()
				}
			}
			continue
		}

		// Check lower bound
		if len(it.iterateLowerBound) > 0 && it.compareKeys(maxKey, it.iterateLowerBound) < 0 {
			it.valid = false
			return
		}

		// Check prefix constraint for prefix seek (also applies to reverse)
		if it.prefixSameAsStart && len(it.seekPrefix) > 0 && it.prefixExtractor != nil {
			if it.prefixExtractor.InDomain(maxKey) {
				keyPrefix := it.prefixExtractor.Transform(maxKey)
				if !bytes.Equal(keyPrefix, it.seekPrefix) {
					// We've left the prefix, stop iteration
					it.valid = false
					return
				}
			}
		}

		// Found a valid entry
		it.savedKey = make([]byte, len(maxKey))
		copy(it.savedKey, maxKey)
		it.savedValue = make([]byte, len(it.iterators[maxIdx].Value()))
		copy(it.savedValue, it.iterators[maxIdx].Value())
		it.currentIter = maxIdx
		it.valid = true
		return
	}
}

// Key returns the key at the current position.
func (it *dbIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.savedKey
}

// Value returns the value at the current position.
func (it *dbIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.savedValue
}

// Error returns any error that has occurred.
func (it *dbIterator) Error() error {
	return it.err
}

// Close releases resources associated with the iterator.
func (it *dbIterator) Close() error {
	// Release SST file references
	for _, sstIter := range it.sstIters {
		if !sstIter.released {
			it.db.tableCache.Release(sstIter.fileNum)
			sstIter.released = true
		}
	}

	// Release version reference
	if it.version != nil {
		it.version.Unref()
		it.version = nil
	}

	it.memIter = nil
	it.immIter = nil
	it.sstIters = nil
	it.iterators = nil

	return nil
}
