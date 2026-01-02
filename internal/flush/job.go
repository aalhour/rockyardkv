// Package flush implements the flush operation that writes memtable to SST files.
//
// This package is internal and not part of the public API.
//
// Reference: RocksDB v10.7.5
//   - db/flush_job.h
//   - db/flush_job.cc
package flush

import (
	"errors"
	"fmt"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/vfs"
)

// ErrNoOutput is returned when a flush produces no output (empty memtable).
var ErrNoOutput = errors.New("flush: no output")

// DB is the interface that flush jobs require from the database.
type DB interface {
	// NextFileNumber allocates and returns the next file number.
	NextFileNumber() uint64

	// SSTFilePath returns the full path for an SST file with the given number.
	SSTFilePath(fileNum uint64) string

	// FS returns the virtual file system.
	FS() vfs.FS

	// DBPath returns the database directory path.
	DBPath() string

	// ComparatorName returns the name of the comparator.
	ComparatorName() string
}

// Job flushes a memtable to an SST file.
type Job struct {
	db DB

	// The memtable being flushed
	mem *memtable.MemTable

	// Output file number
	fileNum uint64
}

// NewJob creates a new flush job for the given memtable.
func NewJob(db DB, mem *memtable.MemTable) *Job {
	return &Job{
		db:  db,
		mem: mem,
	}
}

// Run executes the flush job.
// Returns the metadata of the created SST file, or an error.
func (fj *Job) Run() (*manifest.FileMetaData, error) {
	// Whitebox [synctest]: barrier at flush start
	_ = testutil.SP(testutil.SPFlushStart)

	// Whitebox [crashtest]: crash before flush begins — tests memtable durability
	testutil.MaybeKill(testutil.KPFlushStart0)

	// Allocate a file number for the new SST file
	fj.fileNum = fj.db.NextFileNumber()

	// Create the SST file
	sstPath := fj.db.SSTFilePath(fj.fileNum)

	// Whitebox [synctest]: barrier before SST write
	_ = testutil.SP(testutil.SPFlushWriteSST)

	// Whitebox [crashtest]: crash before SST write — tests incomplete SST cleanup
	testutil.MaybeKill(testutil.KPFlushWriteSST0)

	file, err := fj.db.FS().Create(sstPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SST file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Create table builder
	opts := table.DefaultBuilderOptions()
	opts.ComparatorName = fj.db.ComparatorName()
	builder := table.NewTableBuilder(file, opts)

	// Iterate over the memtable and add all entries
	iter := fj.mem.NewIterator()
	var firstKey, lastKey []byte
	var smallestSeq, largestSeq uint64

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// The key from memtable iterator is an internal key
		if err := builder.Add(key, value); err != nil {
			builder.Abandon()
			return nil, fmt.Errorf("failed to add entry to SST: %w", err)
		}

		// Track first and last keys
		if firstKey == nil {
			firstKey = append([]byte{}, key...)
			smallestSeq = extractSeqNum(key)
		}
		lastKey = append(lastKey[:0], key...)
		seq := extractSeqNum(key)
		if seq < smallestSeq {
			smallestSeq = seq
		}
		if seq > largestSeq {
			largestSeq = seq
		}
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		builder.Abandon()
		return nil, fmt.Errorf("memtable iteration error: %w", err)
	}

	// Add range tombstones from the memtable to the SST file.
	// Range tombstones are stored in a separate meta-block.
	// Reference: RocksDB flushes range tombstones in flush_job.cc
	hasRangeTombstones := false
	if fj.mem.HasRangeTombstones() {
		tombstones := fj.mem.GetRangeTombstones()
		if tombstones != nil && !tombstones.IsEmpty() {
			if err := builder.AddRangeTombstones(tombstones); err != nil {
				builder.Abandon()
				return nil, fmt.Errorf("failed to add range tombstones to SST: %w", err)
			}
			hasRangeTombstones = true
		}
	}

	// If no entries and no range tombstones were written, abandon the file
	if builder.NumEntries() == 0 && !hasRangeTombstones {
		builder.Abandon()
		// Remove the empty file
		_ = fj.db.FS().Remove(sstPath) // Best-effort cleanup
		return nil, ErrNoOutput
	}

	// Finish the SST file
	if err := builder.Finish(); err != nil {
		return nil, fmt.Errorf("failed to finish SST file: %w", err)
	}
	fileSize := builder.FileSize()

	// Whitebox [synctest]: barrier before SST sync
	_ = testutil.SP(testutil.SPFlushSyncSST)

	// Whitebox [crashtest]: crash before SST file sync — tests partial SST durability
	testutil.MaybeKill(testutil.KPFileSync0)

	// Sync the file
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync SST file: %w", err)
	}

	// Whitebox [crashtest]: crash after SST file sync — SST should be fully durable
	testutil.MaybeKill(testutil.KPFileSync1)

	// Sync directory to make SST file entry durable.
	// This is required before updating MANIFEST to reference this SST.
	// Without this, a crash could leave MANIFEST referencing a non-existent SST
	// (the file content is synced but the directory entry is not).
	if err := fj.db.FS().SyncDir(fj.db.DBPath()); err != nil {
		return nil, fmt.Errorf("failed to sync directory after SST write: %w", err)
	}

	// Whitebox [synctest]: barrier at flush complete
	_ = testutil.SP(testutil.SPFlushComplete)

	// Create file metadata
	meta := manifest.NewFileMetaData()
	meta.FD = manifest.NewFileDescriptor(fj.fileNum, 0, fileSize)
	meta.FD.SmallestSeqno = manifest.SequenceNumber(smallestSeq)
	meta.FD.LargestSeqno = manifest.SequenceNumber(largestSeq)
	meta.Smallest = firstKey
	meta.Largest = lastKey

	return meta, nil
}

// extractSeqNum extracts the sequence number from an internal key.
// Internal key format: user_key + 8 bytes (seq << 8 | type) in little-endian
func extractSeqNum(internalKey []byte) uint64 {
	if len(internalKey) < 8 {
		return 0
	}
	// Last 8 bytes contain (seq << 8 | type) in little-endian
	tag := uint64(internalKey[len(internalKey)-8]) |
		uint64(internalKey[len(internalKey)-7])<<8 |
		uint64(internalKey[len(internalKey)-6])<<16 |
		uint64(internalKey[len(internalKey)-5])<<24 |
		uint64(internalKey[len(internalKey)-4])<<32 |
		uint64(internalKey[len(internalKey)-3])<<40 |
		uint64(internalKey[len(internalKey)-2])<<48 |
		uint64(internalKey[len(internalKey)-1])<<56

	return tag >> 8 // Remove the type bits
}
