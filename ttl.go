// ttl.go implements TTL (Time-To-Live) support for automatic key expiration.
//
// TTL works by:
// 1. Storing a timestamp with each value (appended to the value)
// 2. Using a compaction filter to remove expired entries during compaction
// 3. Filtering expired entries during reads
//
// Reference: RocksDB v10.7.5
//   - utilities/ttl/db_ttl_impl.h
//   - utilities/ttl/db_ttl_impl.cc
package rockyardkv

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	// TTLTimestampSize is the size of the timestamp suffix in bytes.
	TTLTimestampSize = 8 // int64 Unix timestamp
)

var (
	// ErrKeyExpired is returned when a key has expired.
	ErrKeyExpired = errors.New("db: key has expired")
)

// TTLCompactionFilter removes expired keys during compaction.
type TTLCompactionFilter struct {
	BaseCompactionFilter
	TTL time.Duration
}

// NewTTLCompactionFilter creates a new TTL compaction filter.
func NewTTLCompactionFilter(ttl time.Duration) *TTLCompactionFilter {
	return &TTLCompactionFilter{TTL: ttl}
}

// Name returns the filter name.
func (f *TTLCompactionFilter) Name() string {
	return "TTLCompactionFilter"
}

// Filter removes expired entries during compaction.
func (f *TTLCompactionFilter) Filter(level int, key, oldValue []byte) (CompactionFilterDecision, []byte) {
	if len(oldValue) < TTLTimestampSize {
		// Value doesn't have a timestamp, keep it
		return FilterKeep, nil
	}

	// Extract timestamp from the end of the value
	timestamp := extractTTLTimestamp(oldValue)

	// Check if expired
	if isExpired(timestamp, f.TTL) {
		return FilterRemove, nil
	}

	return FilterKeep, nil
}

// TTLCompactionFilterFactory creates TTL compaction filters.
type TTLCompactionFilterFactory struct {
	TTL time.Duration
}

// Name returns the factory name.
func (f *TTLCompactionFilterFactory) Name() string {
	return "TTLCompactionFilterFactory"
}

// CreateCompactionFilter creates a new TTL compaction filter.
func (f *TTLCompactionFilterFactory) CreateCompactionFilter(context CompactionFilterContext) CompactionFilter {
	return NewTTLCompactionFilter(f.TTL)
}

// TTLDB wraps a database with TTL support.
// Values are automatically timestamped on write and expired entries are
// filtered on read and removed during compaction.
type TTLDB struct {
	db  DB
	ttl time.Duration
}

// OpenWithTTL opens a database with TTL support.
// The TTL duration specifies how long entries remain valid.
func OpenWithTTL(path string, opts *Options, ttl time.Duration) (*TTLDB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Set up TTL compaction filter
	opts.CompactionFilter = NewTTLCompactionFilter(ttl)

	// Open the database
	database, err := Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &TTLDB{
		db:  database,
		ttl: ttl,
	}, nil
}

// Put stores a key-value pair with TTL timestamp.
func (t *TTLDB) Put(opts *WriteOptions, key, value []byte) error {
	return t.PutWithExpiry(opts, key, value, time.Now())
}

// PutWithExpiry stores a key-value pair with a specific creation time.
func (t *TTLDB) PutWithExpiry(opts *WriteOptions, key, value []byte, creationTime time.Time) error {
	// Append timestamp to value
	ttlValue := appendTTLTimestamp(value, creationTime)
	return t.db.Put(opts, key, ttlValue)
}

// Get retrieves a value, returning ErrNotFound if expired.
func (t *TTLDB) Get(opts *ReadOptions, key []byte) ([]byte, error) {
	value, err := t.db.Get(opts, key)
	if err != nil {
		return nil, err
	}

	if len(value) < TTLTimestampSize {
		// Value doesn't have timestamp, return as-is
		return value, nil
	}

	// Check if expired
	timestamp := extractTTLTimestamp(value)
	if isExpired(timestamp, t.ttl) {
		return nil, ErrNotFound
	}

	// Return value without timestamp
	return stripTTLTimestamp(value), nil
}

// Delete removes a key.
func (t *TTLDB) Delete(opts *WriteOptions, key []byte) error {
	return t.db.Delete(opts, key)
}

// Close closes the database.
func (t *TTLDB) Close() error {
	return t.db.Close()
}

// Flush flushes the memtable.
func (t *TTLDB) Flush(opts *FlushOptions) error {
	return t.db.Flush(opts)
}

// GetSnapshot returns a snapshot.
func (t *TTLDB) GetSnapshot() *Snapshot {
	return t.db.GetSnapshot()
}

// ReleaseSnapshot releases a snapshot.
func (t *TTLDB) ReleaseSnapshot(snapshot *Snapshot) {
	t.db.ReleaseSnapshot(snapshot)
}

// NewIterator returns a TTL-aware iterator.
func (t *TTLDB) NewIterator(opts *ReadOptions) Iterator {
	return &ttlIterator{
		iter: t.db.NewIterator(opts),
		ttl:  t.ttl,
	}
}

// Underlying returns the underlying database.
func (t *TTLDB) Underlying() DB {
	return t.db
}

// SetTTL updates the TTL duration.
func (t *TTLDB) SetTTL(ttl time.Duration) {
	t.ttl = ttl
}

// ttlIterator wraps an iterator to skip expired entries.
type ttlIterator struct {
	iter Iterator
	ttl  time.Duration
}

func (i *ttlIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *ttlIterator) SeekToFirst() {
	i.iter.SeekToFirst()
	i.skipExpired()
}

func (i *ttlIterator) SeekToLast() {
	i.iter.SeekToLast()
	i.skipExpiredBackward()
}

func (i *ttlIterator) Seek(target []byte) {
	i.iter.Seek(target)
	i.skipExpired()
}

func (i *ttlIterator) SeekForPrev(target []byte) {
	i.iter.SeekForPrev(target)
	i.skipExpiredBackward()
}

func (i *ttlIterator) Next() {
	i.iter.Next()
	i.skipExpired()
}

func (i *ttlIterator) Prev() {
	i.iter.Prev()
	i.skipExpiredBackward()
}

func (i *ttlIterator) Key() []byte {
	return i.iter.Key()
}

func (i *ttlIterator) Value() []byte {
	value := i.iter.Value()
	return stripTTLTimestamp(value)
}

func (i *ttlIterator) Error() error {
	return i.iter.Error()
}

func (i *ttlIterator) Close() error {
	return i.iter.Close()
}

// skipExpired advances past expired entries.
func (i *ttlIterator) skipExpired() {
	for i.iter.Valid() {
		value := i.iter.Value()
		if len(value) >= TTLTimestampSize {
			timestamp := extractTTLTimestamp(value)
			if isExpired(timestamp, i.ttl) {
				i.iter.Next()
				continue
			}
		}
		break
	}
}

// skipExpiredBackward moves backward past expired entries.
func (i *ttlIterator) skipExpiredBackward() {
	for i.iter.Valid() {
		value := i.iter.Value()
		if len(value) >= TTLTimestampSize {
			timestamp := extractTTLTimestamp(value)
			if isExpired(timestamp, i.ttl) {
				i.iter.Prev()
				continue
			}
		}
		break
	}
}

// Helper functions for TTL timestamp handling

// appendTTLTimestamp appends a timestamp to a value.
func appendTTLTimestamp(value []byte, creationTime time.Time) []byte {
	result := make([]byte, len(value)+TTLTimestampSize)
	copy(result, value)
	binary.LittleEndian.PutUint64(result[len(value):], uint64(creationTime.Unix()))
	return result
}

// extractTTLTimestamp extracts the timestamp from the end of a value.
func extractTTLTimestamp(value []byte) int64 {
	if len(value) < TTLTimestampSize {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(value[len(value)-TTLTimestampSize:]))
}

// stripTTLTimestamp removes the timestamp from a value.
func stripTTLTimestamp(value []byte) []byte {
	if len(value) < TTLTimestampSize {
		return value
	}
	return value[:len(value)-TTLTimestampSize]
}

// isExpired checks if a timestamp has expired given the TTL.
func isExpired(timestamp int64, ttl time.Duration) bool {
	if ttl <= 0 {
		return false // No TTL = never expires
	}
	expiryTime := time.Unix(timestamp, 0).Add(ttl)
	return time.Now().After(expiryTime)
}
