package rockyardkv

// recovery.go implements WAL recovery/replay.
//
// Reference: RocksDB v10.7.5
//   - db/db_impl/db_impl_open.cc (RecoverLogFiles)
//   - db/db_impl/db_impl_write.cc


import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"

	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/memtable"
	"github.com/aalhour/rockyardkv/internal/wal"
)

// logFileRegex matches log file names like "000001.log"
var logFileRegex = regexp.MustCompile(`^(\d{6})\.log$`)

// sstFileRegex matches SST file names like "000001.sst"
var sstFileRegex = regexp.MustCompile(`^(\d{6})\.sst$`)

// replayWAL replays all WAL files that haven't been flushed yet.
// This recovers any writes that were made but not yet persisted to SST files.
func (db *DBImpl) replayWAL() error {
	// Get the minimum log number we need to replay from
	// Any log file with number >= minLogNumber might contain unflushed data
	minLogNumber := db.versions.LogNumber()

	// Find all log files in the database directory
	logFiles, err := db.findLogFiles()
	if err != nil {
		return fmt.Errorf("failed to find log files: %w", err)
	}

	// Filter to only files >= minLogNumber
	var toReplay []uint64
	for _, num := range logFiles {
		if num >= minLogNumber {
			toReplay = append(toReplay, num)
		}
	}

	// Sort by file number (oldest first)
	slices.Sort(toReplay)

	// Create memtable for recovery with the configured comparator
	var memCmp memtable.Comparator
	if db.comparator != nil {
		memCmp = db.comparator.Compare
	}
	db.mem = memtable.NewMemTable(memCmp)

	// Replay each log file
	maxSeq := db.seq
	for _, logNum := range toReplay {
		seq, err := db.replayLogFile(logNum)
		if err != nil {
			db.logger.Warnf("[recovery] failed to replay log %d: %v", logNum, err)
			return fmt.Errorf("failed to replay log %d: %w", logNum, err)
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	// Update sequence number to max seen
	db.seq = maxSeq

	if len(toReplay) > 0 {
		db.logger.Infof("[recovery] replayed %d WAL files, max sequence: %d", len(toReplay), maxSeq)
	}

	return nil
}

// findLogFiles returns all log file numbers in the database directory.
func (db *DBImpl) findLogFiles() ([]uint64, error) {
	entries, err := db.fs.ListDir(db.name)
	if err != nil {
		return nil, err
	}

	var logFiles []uint64
	for _, entry := range entries {
		matches := logFileRegex.FindStringSubmatch(entry)
		if matches == nil {
			continue
		}

		num, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			continue
		}

		logFiles = append(logFiles, num)
	}

	return logFiles, nil
}

// replayLogFile replays a single log file and returns the max sequence number seen.
func (db *DBImpl) replayLogFile(logNum uint64) (uint64, error) {
	logPath := db.logFilePath(logNum)

	// Open the log file
	file, err := db.fs.Open(logPath)
	if err != nil {
		// Log file might not exist if it was just created but not written to
		return db.seq, nil
	}
	defer func() { _ = file.Close() }()

	// Create WAL reader
	reader := wal.NewReader(file, nil /* reporter */, true /* checksum */, logNum)

	maxSeq := db.seq

	// Read all records
	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Log corruption - we can either fail or continue
			// For now, stop at first error (strict mode)
			return maxSeq, fmt.Errorf("log read error: %w", err)
		}

		// Parse the record as a WriteBatch
		wb, err := batch.NewFromData(record)
		if err != nil {
			return maxSeq, fmt.Errorf("failed to decode batch: %w", err)
		}

		// Get the sequence number from the batch
		batchSeq := wb.Sequence()
		batchCount := wb.Count()

		// Update sequence number
		if batchSeq+uint64(batchCount) > maxSeq {
			maxSeq = batchSeq + uint64(batchCount)
		}

		// Apply the batch to memtable
		// Note: lockHeld=true because we're called from recover() which holds db.mu
		handler := &memtableInserter{
			db:         db,
			sequence:   batchSeq,
			defaultMem: db.mem,
			lockHeld:   true,
		}
		if err := wb.Iterate(handler); err != nil {
			return maxSeq, fmt.Errorf("failed to apply batch: %w", err)
		}
	}

	return maxSeq, nil
}

// deleteOrphanedSSTFiles removes SST files that aren't referenced in the MANIFEST.
// This is critical for preventing internal key collisions after crash recovery.
//
// Scenario:
//  1. Flush writes SST file and syncs it
//  2. Crash occurs before MANIFEST update is synced
//  3. faultfs drops unsynced MANIFEST write
//  4. SST file exists but isn't in MANIFEST (orphaned)
//  5. On recovery, LastSequence from old MANIFEST is used
//  6. New writes reuse sequence numbers from orphaned SST → COLLISION
//
// Failure policy:
//   - Directory listing failure: fails Open() hard (corruption suspected)
//   - Individual file deletion failure: logs warning, continues best-effort
//
// The best-effort approach is chosen because a locked file (e.g., Windows)
// doesn't cause collisions if the DB is not shared. Failing Open() hard for
// a single undeletable orphan would be overly disruptive.
//
// Reference: RocksDB db/db_impl/db_impl_files.cc DeleteObsoleteFiles
func (db *DBImpl) deleteOrphanedSSTFiles() error {
	// Get all SST file numbers referenced in the current version
	liveFiles := make(map[uint64]bool)
	version := db.versions.Current()
	if version != nil {
		for level := range version.NumLevels() {
			files := version.Files(level)
			for _, f := range files {
				liveFiles[f.FD.GetNumber()] = true
			}
		}
	}

	// Find all SST files in the directory
	entries, err := db.fs.ListDir(db.name)
	if err != nil {
		return fmt.Errorf("failed to list directory: %w", err)
	}

	orphanCount := 0
	for _, entry := range entries {
		matches := sstFileRegex.FindStringSubmatch(entry)
		if matches == nil {
			continue
		}

		num, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			continue
		}

		// If not in live files, it's orphaned - delete it
		if !liveFiles[num] {
			sstPath := db.sstFilePath(num)
			if err := db.fs.Remove(sstPath); err != nil {
				// Best-effort: log warning but continue (see doc comment for policy)
				db.logger.Warnf("[recovery] failed to delete orphaned SST %s: %v (continuing best-effort)", sstPath, err)
				continue
			}
			orphanCount++
		}
	}

	// Note: orphanCount > 0 is expected in crash recovery testing (faultfs).
	_ = orphanCount

	return nil
}

// RecoverLogFile is a helper to parse a log file for testing.
func RecoverLogFile(fs any, path string) ([]*batch.WriteBatch, error) {
	// This is for testing - not used in production
	return nil, nil
}

// walRecoveryHandler applies recovered operations to the memtable.
// Reserved for future WAL recovery implementation.
type walRecoveryHandler struct {
	mem      *memtable.MemTable
	sequence uint64
}

// Compile-time check that walRecoveryHandler implements batch.Handler
var _ batch.Handler = (*walRecoveryHandler)(nil)

func (h *walRecoveryHandler) Put(key, value []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeValue, key, value)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) Delete(key []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeDeletion, key, nil)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) SingleDelete(key []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeSingleDeletion, key, nil)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) Merge(key, value []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeMerge, key, value)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) DeleteRange(startKey, endKey []byte) error {
	h.mem.AddRangeTombstone(dbformat.SequenceNumber(h.sequence), startKey, endKey)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	// TODO: Support column family recovery
	h.mem.AddRangeTombstone(dbformat.SequenceNumber(h.sequence), startKey, endKey)
	h.sequence++
	return nil
}

func (h *walRecoveryHandler) PutCF(cfID uint32, key, value []byte) error {
	return h.Put(key, value)
}

func (h *walRecoveryHandler) DeleteCF(cfID uint32, key []byte) error {
	return h.Delete(key)
}

func (h *walRecoveryHandler) MergeCF(cfID uint32, key, value []byte) error {
	return h.Merge(key, value)
}

func (h *walRecoveryHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	return h.SingleDelete(key)
}

func (h *walRecoveryHandler) LogData(blob []byte) {
	// Ignored
}
