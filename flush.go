package rockyardkv

// flush.go implements the flush operation that writes memtable to SST files.
//
// Reference: RocksDB v10.7.5
//   - db/flush_job.h
//   - db/flush_job.cc
//
// # Whitebox Testing Hooks
//
// This file contains sync points (requires -tags synctest) and kill points
// (requires -tags crashtest) for whitebox testing. In production builds,
// these compile to no-ops with zero overhead. See docs/testing/README.md for usage.

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/aalhour/rockyardkv/internal/flush"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/vfs"
)

// Compile-time check that dbImpl implements flush.DB interface.
var _ flush.DB = (*dbImpl)(nil)

// NextFileNumber implements flush.DB.
func (db *dbImpl) NextFileNumber() uint64 {
	return db.versions.NextFileNumber()
}

// SSTFilePath implements flush.DB.
func (db *dbImpl) SSTFilePath(fileNum uint64) string {
	return db.sstFilePath(fileNum)
}

// FS implements flush.DB.
func (db *dbImpl) FS() vfs.FS {
	return db.fs
}

// DBPath implements flush.DB.
func (db *dbImpl) DBPath() string {
	return db.name
}

// ComparatorName implements flush.DB.
func (db *dbImpl) ComparatorName() string {
	return db.comparator.Name()
}

// sstFilePath returns the path to an SST file.
func (db *dbImpl) sstFilePath(number uint64) string {
	return filepath.Join(db.name, sstFileName(number))
}

// sstFileName returns the filename for an SST file.
func sstFileName(number uint64) string {
	return fmt.Sprintf("%06d.sst", number)
}

// doFlush performs the actual flush of the immutable memtable.
// This is called from the background flush goroutine or synchronously.
func (db *dbImpl) doFlush() error {
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
	job := flush.NewJob(db, imm)
	meta, err := job.Run()
	if err != nil {
		if errors.Is(err, flush.ErrNoOutput) {
			// Empty flush is a no-op but still clears the immutable memtable.
			db.mu.Lock()
			db.imm = nil
			if db.immCond != nil {
				db.immCond.Broadcast()
			}
			db.mu.Unlock()
			return nil
		}
		// Flush failed. Set background error and broadcast to unblock any waiters.
		// Without this, goroutines waiting on immCond.Wait() would block forever.
		db.mu.Lock()
		if db.backgroundError == nil {
			db.backgroundError = err
		}
		// Broadcast to wake up any goroutines waiting for immutable memtable to clear.
		// They will check backgroundError and return the error.
		if db.immCond != nil {
			db.immCond.Broadcast()
		}
		db.mu.Unlock()
		db.logger.Warnf("[flush] flush job failed: %v", err)
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
func (db *dbImpl) backgroundFlush() {
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
