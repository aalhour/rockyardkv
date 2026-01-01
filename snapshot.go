package rockyardkv

// snapshot.go implements snapshot management.
//
// Snapshots provide consistent point-in-time views of the database.
// All reads from a snapshot see the database state at creation time.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/snapshot.h
//   - db/snapshot_impl.h


import (
	"sync/atomic"
	"time"
)

// Snapshot provides a consistent read view of the database.
// The contents of a snapshot are guaranteed to be consistent.
type Snapshot struct {
	db        *DBImpl
	sequence  uint64
	refs      atomic.Int32
	createdAt int64 // Unix timestamp when snapshot was created

	// Linked list for snapshot management
	prev *Snapshot
	next *Snapshot
}

// newSnapshot creates a new snapshot at the given sequence number.
func newSnapshot(db *DBImpl, seq uint64) *Snapshot {
	s := &Snapshot{
		db:        db,
		sequence:  seq,
		createdAt: time.Now().Unix(),
	}
	s.refs.Store(1)
	return s
}

// Sequence returns the sequence number at which this snapshot was taken.
func (s *Snapshot) Sequence() uint64 {
	return s.sequence
}

// Release releases the snapshot.
// After calling Release, the snapshot should not be used.
func (s *Snapshot) Release() {
	if s.refs.Add(-1) == 0 {
		// Notify the DB to clean up
		if s.db != nil {
			s.db.releaseSnapshot(s)
		}
	}
}
