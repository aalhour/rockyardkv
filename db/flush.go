// Package db provides the main database interface and implementation.
// This file implements the flush operation that writes memtable to SST files.
//
// Reference: RocksDB v10.7.5
//   - db/flush_job.h
//   - db/flush_job.cc
//
// # Whitebox Testing Hooks
//
// This file contains sync points (requires -tags synctest) and kill points
// (requires -tags crashtest) for whitebox testing. In production builds,
// these compile to no-ops with zero overhead. See docs/testing.md for usage.
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
	// Whitebox [synctest]: barrier at flush start
	_ = testutil.SP(testutil.SPFlushStart)

	// Whitebox [crashtest]: crash before flush begins — tests memtable durability
	testutil.MaybeKill(testutil.KPFlushStart0)

	// Allocate a file number for the new SST file
	fj.fileNum = fj.db.versions.NextFileNumber()

	// Create the SST file
	sstPath := fj.db.sstFilePath(fj.fileNum)

	// Whitebox [synctest]: barrier before SST write
	_ = testutil.SP(testutil.SPFlushWriteSST)

	// Whitebox [crashtest]: crash before SST write — tests incomplete SST cleanup
	testutil.MaybeKill(testutil.KPFlushWriteSST0)

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
	if err := fj.db.fs.SyncDir(fj.db.name); err != nil {
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
	// Whitebox [synctest]: barrier at doFlush start
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
	// Update the version with the new file.
	//
	// IMPORTANT: We do NOT advance LogNumber here. In our current architecture,
	// we don't rotate WALs when switching memtables - the same WAL is used for
	// all memtables until DB restart. Therefore:
	// - The current WAL contains unflushed data (from the active memtable)
	// - Advancing LogNumber would cause that data to be skipped on recovery
	// - LogNumber is only safely advanced when we create a new WAL (on DB open)
	//
	// If/when we implement WAL rotation (like RocksDB), the immutable memtable's
	// nextLogNumber should be used here to advance LogNumber.
	// Reference: RocksDB v10.7.5 db/flush_job.cc:206 (SetLogNumber)
	//
	// CRITICAL: Use the largest sequence from the flushed SST, not db.seq.
	// Between memtable switch and flush completion, new writes to the active memtable
	// increment db.seq. If we use db.seq here, LastSequence will include sequences
	// that are NOT in the flushed SST. With DisableWAL, those sequences are lost on
	// crash but LastSequence preserves them, causing sequence reuse and collisions.
	//
	// LastSequence must be MONOTONIC (never decrease). Use max of flushed SST's
	// largest sequence and the previous LastSequence to ensure this property.
	// This ensures LastSequence never decreases after crash recovery.
	newLastSeq := meta.FD.LargestSeqno
	prevLastSeq := manifest.SequenceNumber(db.versions.LastSequence())
	if prevLastSeq > newLastSeq {
		newLastSeq = prevLastSeq
	}

	edit := &manifest.VersionEdit{
		HasLastSequence: true,
		LastSequence:    newLastSeq,
		// HasLogNumber intentionally NOT set - don't advance LogNumber during flush
	}
	edit.NewFiles = append(edit.NewFiles, manifest.NewFileEntry{
		Level: 0, // Flush always goes to L0
		Meta:  meta,
	})

	// Whitebox [crashtest]: crash before manifest update — SST orphaned
	testutil.MaybeKill(testutil.KPFlushUpdateManifest0)

	// Apply the version edit
	if err := db.versions.LogAndApply(edit); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("failed to log version edit: %w", err)
	}

	// CRITICAL: LogAndApply writes to MANIFEST but doesn't update in-memory lastSequence.
	// We must update it here to ensure subsequent flushes use the correct base value.
	db.versions.SetLastSequence(uint64(newLastSeq))

	// Whitebox [crashtest]: crash after manifest update — flush complete
	testutil.MaybeKill(testutil.KPFlushUpdateManifest1)

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
