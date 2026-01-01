// timestamped_db.go implements a timestamped database wrapper.
//
// TimestampedDB wraps a regular database and provides timestamp-aware
// APIs for Put, Get, Delete, and iteration. It uses a comparator that
// supports user-defined timestamps (e.g., BytewiseComparatorWithU64Ts).
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/db.h (Get/Put with timestamp)
//   - db/db_impl/db_impl.cc (timestamp-aware implementations)
package rockyardkv

import (
	"errors"
)

var (
	// ErrTimestampRequired is returned when a timestamp is required but not provided.
	ErrTimestampRequired = errors.New("db: timestamp required for timestamped database")

	// ErrTimestampNotSupported is returned when timestamps are not supported.
	ErrTimestampNotSupported = errors.New("db: timestamps not supported by comparator")

	// ErrInvalidTimestampSize is returned when the timestamp size is incorrect.
	ErrInvalidTimestampSize = errors.New("db: invalid timestamp size")
)

// TimestampedDB wraps a DB and provides timestamp-aware operations.
type TimestampedDB struct {
	db         DB
	comparator TimestampedComparator
	tsSize     int
}

// OpenTimestampedDB opens a database with timestamp support.
// The options must have a TimestampedComparator set.
func OpenTimestampedDB(path string, opts *Options) (*TimestampedDB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Ensure we have a timestamped comparator
	if opts.Comparator == nil {
		opts.Comparator = BytewiseComparatorWithU64Ts{}
	}

	tsCmp, ok := opts.Comparator.(TimestampedComparator)
	if !ok {
		return nil, ErrTimestampNotSupported
	}

	if tsCmp.TimestampSize() == 0 {
		return nil, ErrTimestampNotSupported
	}

	db, err := Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &TimestampedDB{
		db:         db,
		comparator: tsCmp,
		tsSize:     tsCmp.TimestampSize(),
	}, nil
}

// WrapWithTimestamp wraps an existing DB with timestamp support.
func WrapWithTimestamp(db DB, comparator TimestampedComparator) (*TimestampedDB, error) {
	if comparator == nil {
		return nil, ErrTimestampNotSupported
	}
	if comparator.TimestampSize() == 0 {
		return nil, ErrTimestampNotSupported
	}
	return &TimestampedDB{
		db:         db,
		comparator: comparator,
		tsSize:     comparator.TimestampSize(),
	}, nil
}

// Close closes the database.
func (t *TimestampedDB) Close() error {
	return t.db.Close()
}

// PutWithTimestamp stores a key-value pair with the given timestamp.
func (t *TimestampedDB) PutWithTimestamp(opts *WriteOptions, key, value, timestamp []byte) error {
	if len(timestamp) != t.tsSize {
		return ErrInvalidTimestampSize
	}

	// Append timestamp to key
	keyWithTS := AppendTimestampToKey(key, timestamp)
	return t.db.Put(opts, keyWithTS, value)
}

// Put stores a key-value pair using the maximum timestamp.
// This is a convenience method for applications that want to write
// with the "current" timestamp.
func (t *TimestampedDB) Put(opts *WriteOptions, key, value []byte) error {
	return t.PutWithTimestamp(opts, key, value, t.comparator.GetMaxTimestamp())
}

// GetWithTimestamp retrieves the value for a key at the given timestamp.
// Returns the value, the timestamp of the found record, and any error.
func (t *TimestampedDB) GetWithTimestamp(opts *ReadOptions, key, timestamp []byte) (value, foundTS []byte, err error) {
	if len(timestamp) != t.tsSize {
		return nil, nil, ErrInvalidTimestampSize
	}

	// Create an iterator to find the key
	iter := t.db.NewIterator(opts)
	defer func() { _ = iter.Close() }()

	// Seek to the key with the specified timestamp
	// Since larger timestamps come first, we seek to key+timestamp
	// and check if it matches our user key
	keyWithTS := AppendTimestampToKey(key, timestamp)
	iter.Seek(keyWithTS)

	if !iter.Valid() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		return nil, nil, ErrNotFound
	}

	// Check if the found key matches our user key
	foundKey := iter.Key()
	foundUserKey, foundTimestamp := StripTimestampFromKey(foundKey, t.tsSize)

	// Compare user keys
	if t.comparator.CompareWithoutTimestamp(key, foundUserKey, false, false) != 0 {
		return nil, nil, ErrNotFound
	}

	// Check if the found timestamp is <= requested timestamp
	// (larger timestamps come first, so if foundTS > timestamp, we overshot)
	if t.comparator.CompareTimestamp(foundTimestamp, timestamp) > 0 {
		return nil, nil, ErrNotFound
	}

	return iter.Value(), foundTimestamp, nil
}

// Get retrieves the value for a key at the maximum timestamp.
func (t *TimestampedDB) Get(opts *ReadOptions, key []byte) ([]byte, error) {
	value, _, err := t.GetWithTimestamp(opts, key, t.comparator.GetMaxTimestamp())
	return value, err
}

// DeleteWithTimestamp deletes a key at the given timestamp.
func (t *TimestampedDB) DeleteWithTimestamp(opts *WriteOptions, key, timestamp []byte) error {
	if len(timestamp) != t.tsSize {
		return ErrInvalidTimestampSize
	}

	keyWithTS := AppendTimestampToKey(key, timestamp)
	return t.db.Delete(opts, keyWithTS)
}

// Delete deletes a key using the maximum timestamp.
func (t *TimestampedDB) Delete(opts *WriteOptions, key []byte) error {
	return t.DeleteWithTimestamp(opts, key, t.comparator.GetMaxTimestamp())
}

// Flush flushes the memtable to disk.
func (t *TimestampedDB) Flush(opts *FlushOptions) error {
	return t.db.Flush(opts)
}

// NewIterator creates an iterator over the database.
// The iterator returns keys with timestamps appended.
func (t *TimestampedDB) NewIterator(opts *ReadOptions) Iterator {
	return t.db.NewIterator(opts)
}

// NewTimestampedIterator creates a timestamp-aware iterator.
// It filters keys based on the timestamp in ReadOptions.
func (t *TimestampedDB) NewTimestampedIterator(opts *ReadOptions) *TimestampedIterator {
	if opts == nil {
		opts = DefaultReadOptions()
	}

	iter := t.db.NewIterator(opts)

	readTS := opts.Timestamp
	if readTS == nil {
		readTS = t.comparator.GetMaxTimestamp()
	}

	return &TimestampedIterator{
		iter:       iter,
		comparator: t.comparator,
		tsSize:     t.tsSize,
		readTS:     readTS,
	}
}

// TimestampSize returns the size of timestamps in bytes.
func (t *TimestampedDB) TimestampSize() int {
	return t.tsSize
}

// DB returns the underlying database.
func (t *TimestampedDB) DB() DB {
	return t.db
}

// TimestampedIterator wraps an iterator and filters by timestamp.
type TimestampedIterator struct {
	iter       Iterator
	comparator TimestampedComparator
	tsSize     int
	readTS     []byte
}

// skipToValidKey advances the iterator to the next valid key
// (one with timestamp <= readTS).
func (ti *TimestampedIterator) skipToValidKey() {
	for ti.iter.Valid() {
		_, ts := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
		// Skip keys with timestamps > readTS (newer than what we want to read)
		if ti.comparator.CompareTimestamp(ts, ti.readTS) <= 0 {
			break
		}
		ti.iter.Next()
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (ti *TimestampedIterator) Valid() bool {
	return ti.iter.Valid()
}

// SeekToFirst positions the iterator at the first valid key.
func (ti *TimestampedIterator) SeekToFirst() {
	ti.iter.SeekToFirst()
	ti.skipToValidKey()
}

// SeekToLast positions the iterator at the last valid key.
func (ti *TimestampedIterator) SeekToLast() {
	ti.iter.SeekToLast()
	// For SeekToLast, we need to go backwards to find a valid key
	for ti.iter.Valid() {
		_, ts := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
		if ti.comparator.CompareTimestamp(ts, ti.readTS) <= 0 {
			break
		}
		ti.iter.Prev()
	}
}

// Seek positions the iterator at the first key >= target.
// The target should be a user key without timestamp.
func (ti *TimestampedIterator) Seek(target []byte) {
	// For seeks with timestamps, we want to find the first entry
	// with user_key >= target and timestamp <= readTS
	// Since larger timestamps come first, we seek to target + readTS
	targetWithTS := AppendTimestampToKey(target, ti.readTS)
	ti.iter.Seek(targetWithTS)
	ti.skipToValidKey()
}

// SeekForPrev positions the iterator at the last key <= target.
func (ti *TimestampedIterator) SeekForPrev(target []byte) {
	targetWithTS := AppendTimestampToKey(target, ti.readTS)
	ti.iter.SeekForPrev(targetWithTS)
	// For SeekForPrev, we need to check backwards
	for ti.iter.Valid() {
		_, ts := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
		if ti.comparator.CompareTimestamp(ts, ti.readTS) <= 0 {
			break
		}
		ti.iter.Prev()
	}
}

// Next advances the iterator to the next valid entry.
func (ti *TimestampedIterator) Next() {
	ti.iter.Next()
	ti.skipToValidKey()
}

// Prev moves the iterator to the previous valid entry.
func (ti *TimestampedIterator) Prev() {
	ti.iter.Prev()
	// For Prev, we need to skip entries with timestamps > readTS
	for ti.iter.Valid() {
		_, ts := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
		if ti.comparator.CompareTimestamp(ts, ti.readTS) <= 0 {
			break
		}
		ti.iter.Prev()
	}
}

// Key returns the current key (with timestamp).
func (ti *TimestampedIterator) Key() []byte {
	return ti.iter.Key()
}

// UserKey returns the current user key (without timestamp).
func (ti *TimestampedIterator) UserKey() []byte {
	key, _ := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
	return key
}

// Timestamp returns the timestamp of the current key.
func (ti *TimestampedIterator) Timestamp() []byte {
	_, ts := StripTimestampFromKey(ti.iter.Key(), ti.tsSize)
	return ts
}

// Value returns the current value.
func (ti *TimestampedIterator) Value() []byte {
	return ti.iter.Value()
}

// Error returns any error encountered by the iterator.
func (ti *TimestampedIterator) Error() error {
	return ti.iter.Error()
}

// Close closes the iterator.
func (ti *TimestampedIterator) Close() error {
	return ti.iter.Close()
}
