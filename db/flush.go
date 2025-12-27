// Package db provides the main database interface and implementation.
// This file implements the flush operation that writes memtable to SST files.
//
// Reference: RocksDB v10.7.5
//   - db/flush_job.h
//   - db/flush_job.cc

package db

import (
	"fmt"
	"path/filepath"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// FlushJob flushes a memtable to an SST file.
type FlushJob struct {
	db *DBImpl

	// The memtable being flushed
	mem *memtable.MemTable

	// Output file number
	fileNum uint64
}

// newFlushJob creates a new flush job for the given memtable.
func newFlushJob(db *DBImpl, mem *memtable.MemTable) *FlushJob {
	return &FlushJob{
		db:  db,
		mem: mem,
	}
}

// Run executes the flush job.
// Returns the metadata of the created SST file, or an error.
func (fj *FlushJob) Run() (*manifest.FileMetaData, error) {
	_ = testutil.SP(testutil.SPFlushStart)

	// Allocate a file number for the new SST file
	fj.fileNum = fj.db.versions.NextFileNumber()

	// Create the SST file
	sstPath := fj.db.sstFilePath(fj.fileNum)
	_ = testutil.SP(testutil.SPFlushWriteSST)
	file, err := fj.db.fs.Create(sstPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SST file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Create table builder
	opts := table.DefaultBuilderOptions()
	opts.ComparatorName = fj.db.comparator.Name()
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
		_ = fj.db.fs.Remove(sstPath) // Best-effort cleanup
		return nil, nil
	}

	// Finish the SST file
	if err := builder.Finish(); err != nil {
		return nil, fmt.Errorf("failed to finish SST file: %w", err)
	}
	fileSize := builder.FileSize()

	// Sync the file
	_ = testutil.SP(testutil.SPFlushSyncSST)
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync SST file: %w", err)
	}

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
// Internal key format: user_key + 8 bytes (seq << 8 | type)
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

// sstFilePath returns the path to an SST file.
func (db *DBImpl) sstFilePath(number uint64) string {
	return filepath.Join(db.name, sstFileName(number))
}

// sstFileName returns the filename for an SST file.
func sstFileName(number uint64) string {
	return fmt.Sprintf("%06d.sst", number)
}

// doFlush performs the actual flush of the immutable memtable.
// This is called from the background flush goroutine or synchronously.
func (db *DBImpl) doFlush() error {
	_ = testutil.SP(testutil.SPDoFlushStart)

	db.mu.Lock()
	if db.imm == nil {
		db.mu.Unlock()
		return nil // Nothing to flush
	}
	imm := db.imm
	db.mu.Unlock()

	// Create and run the flush job
	job := newFlushJob(db, imm)
	meta, err := job.Run()
	if err != nil {
		return err
	}

	// If the memtable was empty, just clear the immutable memtable
	if meta == nil {
		db.mu.Lock()
		db.imm = nil
		// Signal any waiters that immutable memtable is now available
		if db.immCond != nil {
			db.immCond.Broadcast()
		}
		db.mu.Unlock()
		return nil
	}

	db.mu.Lock()
	// Update the version with the new file
	edit := &manifest.VersionEdit{
		HasLogNumber:    true,
		LogNumber:       db.logFileNumber,
		HasLastSequence: true,
		LastSequence:    manifest.SequenceNumber(db.seq),
	}
	edit.NewFiles = append(edit.NewFiles, manifest.NewFileEntry{
		Level: 0, // Flush always goes to L0
		Meta:  meta,
	})

	// Apply the version edit
	if err := db.versions.LogAndApply(edit); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("failed to log version edit: %w", err)
	}

	// Clear the immutable memtable
	db.imm = nil

	// Signal any waiters that immutable memtable is now available
	if db.immCond != nil {
		db.immCond.Broadcast()
	}

	// Recalculate write stall condition after flush
	db.recalculateWriteStall()

	db.mu.Unlock()

	return nil
}

// backgroundFlush runs in a goroutine to handle flush requests.
//
//nolint:unused // Reserved for future use when background flush scheduling is implemented
func (db *DBImpl) backgroundFlush() {
	for {
		select {
		case <-db.shutdownCh:
			return
		default:
			// Check if there's an immutable memtable to flush
			db.mu.RLock()
			hasImm := db.imm != nil
			db.mu.RUnlock()

			if hasImm {
				if err := db.doFlush(); err != nil {
					// Log error but continue
					// TODO: Proper error handling/reporting
				}
			}

			// Sleep briefly to avoid spinning
			// TODO: Use proper signaling instead of polling
		}
	}
}
