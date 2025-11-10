// external_sst_ingestion.go implements external SST file ingestion.
//
// IngestExternalFile adds SST files created by SstFileWriter to the database.
// Files are moved into the database directory and added to the LSM tree.
//
// Reference: RocksDB v10.7.5
//   - db/external_sst_file_ingestion_job.h
//   - db/external_sst_file_ingestion_job.cc
package db

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/version"
)

// Ingestion errors
var (
	// ErrIngestOverlapMemtable is returned when ingesting files overlap with the memtable
	// and allow_blocking_flush is false.
	ErrIngestOverlapMemtable = errors.New("ingest: files overlap with memtable and blocking flush not allowed")

	// ErrIngestInvalidFile is returned when an ingested file is invalid or corrupted.
	ErrIngestInvalidFile = errors.New("ingest: invalid or corrupted SST file")

	// ErrIngestEmptyFile is returned when an ingested file has no entries.
	ErrIngestEmptyFile = errors.New("ingest: SST file has no entries")

	// ErrIngestFilesOverlap is returned when ingested files overlap and global seqno is disabled.
	ErrIngestFilesOverlap = errors.New("ingest: ingested files overlap and allow_global_seqno is false")

	// ErrIngestNotBottommostLevel is returned when fail_if_not_bottommost_level is set
	// but files cannot be placed in the bottommost level.
	ErrIngestNotBottommostLevel = errors.New("ingest: files cannot be placed in bottommost level")
)

// IngestExternalFileOptions configures the behavior of IngestExternalFile.
// This matches the C++ RocksDB IngestExternalFileOptions structure.
type IngestExternalFileOptions struct {
	// MoveFiles: if true, move the files instead of copying them.
	// The input files will be unlinked after successful ingestion.
	MoveFiles bool

	// SnapshotConsistency: if false, ingested file keys could appear in
	// existing snapshots that were created before the file was ingested.
	SnapshotConsistency bool

	// AllowGlobalSeqNo: enables assigning a global sequence number to each
	// ingested file. If false, we will use the sequence numbers in the
	// ingested file as is.
	AllowGlobalSeqNo bool

	// AllowBlockingFlush: if true, IngestExternalFile() will trigger and
	// block for flushing memtable(s) if there is overlap between ingested
	// files and memtable(s). If false, ingestion will fail if overlap exists.
	AllowBlockingFlush bool

	// IngestBehind: if true, duplicate keys in the file being ingested will
	// be skipped rather than overwriting existing data. All files will be
	// ingested at the bottommost level with seqno=0.
	IngestBehind bool

	// FailIfNotBottommostLevel: if true, ingestion will fail if files cannot
	// be placed in the bottommost level.
	FailIfNotBottommostLevel bool

	// VerifyChecksumsBeforeIngest: if true, verify the checksums of each
	// block of the external SST file before ingestion.
	VerifyChecksumsBeforeIngest bool
}

// DefaultIngestExternalFileOptions returns the default ingestion options.
func DefaultIngestExternalFileOptions() IngestExternalFileOptions {
	return IngestExternalFileOptions{
		MoveFiles:                   false,
		SnapshotConsistency:         true,
		AllowGlobalSeqNo:            true,
		AllowBlockingFlush:          true,
		IngestBehind:                false,
		FailIfNotBottommostLevel:    false,
		VerifyChecksumsBeforeIngest: false,
	}
}

// ingestedFileInfo holds information about a file being ingested.
type ingestedFileInfo struct {
	externalPath string // Path to the external file
	internalPath string // Path where the file will be stored in the DB
	fileNumber   uint64 // Assigned file number
	fileSize     uint64 // Size of the file
	smallestKey  []byte // Smallest user key
	largestKey   []byte // Largest user key
	targetLevel  int    // Level where file will be placed
	globalSeqNo  uint64 // Assigned global sequence number
}

// IngestExternalFile loads external SST files into the database.
//
// The files must be SST files created by SstFileWriter or from another
// RocksDB instance. The files will be copied (or moved) into the database
// directory and added to the appropriate level in the LSM tree.
//
// Keys in the ingested files will be visible after this call returns.
// If snapshot_consistency is true, the ingested keys will not be visible
// in snapshots created before this call.
func (db *DBImpl) IngestExternalFile(paths []string, opts IngestExternalFileOptions) error {
	return db.IngestExternalFileCF(nil, paths, opts)
}

// IngestExternalFileCF loads external SST files into a specific column family.
func (db *DBImpl) IngestExternalFileCF(cf ColumnFamilyHandle, paths []string, opts IngestExternalFileOptions) error {
	if len(paths) == 0 {
		return nil
	}

	// Step 1: Verify and collect information about all files
	files, err := db.verifyAndPrepareIngestFiles(paths, opts)
	if err != nil {
		return err
	}

	// Step 2: Check for overlap between ingested files (if not allowing global seqno)
	if !opts.AllowGlobalSeqNo && len(files) > 1 {
		if err := db.checkIngestedFilesOverlap(files); err != nil {
			return err
		}
	}

	// Step 3: Acquire DB mutex for the rest of the operation
	db.mu.Lock()
	defer db.mu.Unlock()

	// Step 4: Check for overlap with memtable
	if db.checkMemtableOverlap(files) {
		if !opts.AllowBlockingFlush {
			return ErrIngestOverlapMemtable
		}
		// Flush memtable
		db.mu.Unlock()
		if err := db.Flush(DefaultFlushOptions()); err != nil {
			db.mu.Lock()
			return fmt.Errorf("ingest: failed to flush memtable: %w", err)
		}
		db.mu.Lock()
	}

	// Step 5: Assign file numbers and determine target levels
	for i := range files {
		files[i].fileNumber = db.versions.NextFileNumber()
		files[i].internalPath = db.sstFilePath(files[i].fileNumber)
	}

	// Step 6: Assign global sequence numbers
	if err := db.assignGlobalSeqNos(files, opts); err != nil {
		return err
	}

	// Step 7: Determine target levels
	if err := db.determineTargetLevels(files, opts); err != nil {
		return err
	}

	// Step 8: Copy/move files to DB directory
	if err := db.installIngestedFiles(files, opts); err != nil {
		return err
	}

	// Step 9: Update MANIFEST
	if err := db.updateManifestForIngest(files, cf); err != nil {
		// Cleanup copied files on failure
		for _, f := range files {
			os.Remove(f.internalPath)
		}
		return err
	}

	return nil
}

// verifyAndPrepareIngestFiles verifies each file is a valid SST and collects metadata.
func (db *DBImpl) verifyAndPrepareIngestFiles(paths []string, opts IngestExternalFileOptions) ([]*ingestedFileInfo, error) {
	files := make([]*ingestedFileInfo, 0, len(paths))

	for _, path := range paths {
		info, err := db.verifyIngestFile(path, opts)
		if err != nil {
			return nil, fmt.Errorf("ingest: file %s: %w", path, err)
		}
		files = append(files, info)
	}

	// Sort files by smallest key for consistent ordering
	sort.Slice(files, func(i, j int) bool {
		return bytes.Compare(files[i].smallestKey, files[j].smallestKey) < 0
	})

	return files, nil
}

// verifyIngestFile verifies a single SST file and returns its metadata.
func (db *DBImpl) verifyIngestFile(path string, opts IngestExternalFileOptions) (*ingestedFileInfo, error) {
	// Open and read the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Create a readable file wrapper
	wrapper := &osFileWrapper{f: file, size: stat.Size()}

	// Create a table reader to validate and get metadata
	readerOpts := table.ReaderOptions{
		VerifyChecksums: opts.VerifyChecksumsBeforeIngest,
	}
	reader, err := table.Open(wrapper, readerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to read SST file: %w", err)
	}

	// Get the key range by iterating
	iter := reader.NewIterator()

	// Get smallest key
	iter.SeekToFirst()
	if !iter.Valid() {
		return nil, ErrIngestEmptyFile
	}
	smallestKey := ingestExtractUserKey(iter.Key())

	// Get largest key
	iter.SeekToLast()
	if !iter.Valid() {
		return nil, ErrIngestInvalidFile
	}
	largestKey := ingestExtractUserKey(iter.Key())

	return &ingestedFileInfo{
		externalPath: path,
		fileSize:     uint64(stat.Size()),
		smallestKey:  append([]byte(nil), smallestKey...),
		largestKey:   append([]byte(nil), largestKey...),
	}, nil
}

// osFileWrapper wraps an os.File to implement table.ReadableFile
type osFileWrapper struct {
	f    *os.File
	size int64
}

func (w *osFileWrapper) ReadAt(p []byte, off int64) (int, error) {
	return w.f.ReadAt(p, off)
}

func (w *osFileWrapper) Size() int64 {
	return w.size
}

// checkIngestedFilesOverlap checks if any ingested files overlap with each other.
func (db *DBImpl) checkIngestedFilesOverlap(files []*ingestedFileInfo) error {
	// Files are sorted by smallest key
	for i := 1; i < len(files); i++ {
		// Check if previous file's largest key >= current file's smallest key
		if bytes.Compare(files[i-1].largestKey, files[i].smallestKey) >= 0 {
			return ErrIngestFilesOverlap
		}
	}
	return nil
}

// checkMemtableOverlap checks if any ingested file overlaps with the memtable.
func (db *DBImpl) checkMemtableOverlap(files []*ingestedFileInfo) bool {
	if db.mem == nil || db.mem.ApproximateMemoryUsage() == 0 {
		return false
	}

	// Get memtable key range
	memSmallest, memLargest := db.getMemtableKeyRange()
	if memSmallest == nil || memLargest == nil {
		return false
	}

	// Check each file for overlap
	for _, f := range files {
		if ingestRangesOverlap(f.smallestKey, f.largestKey, memSmallest, memLargest) {
			return true
		}
	}

	return false
}

// getMemtableKeyRange returns the smallest and largest user keys in the memtable.
func (db *DBImpl) getMemtableKeyRange() (smallest, largest []byte) {
	if db.mem == nil {
		return nil, nil
	}

	iter := db.mem.NewIterator()

	iter.SeekToFirst()
	if !iter.Valid() {
		return nil, nil
	}
	smallest = ingestExtractUserKey(iter.Key())

	iter.SeekToLast()
	if !iter.Valid() {
		return nil, nil
	}
	largest = ingestExtractUserKey(iter.Key())

	return append([]byte(nil), smallest...), append([]byte(nil), largest...)
}

// ingestRangesOverlap checks if two key ranges overlap.
func ingestRangesOverlap(aMin, aMax, bMin, bMax []byte) bool {
	return bytes.Compare(aMin, bMax) <= 0 && bytes.Compare(bMin, aMax) <= 0
}

// assignGlobalSeqNos assigns global sequence numbers to ingested files.
func (db *DBImpl) assignGlobalSeqNos(files []*ingestedFileInfo, opts IngestExternalFileOptions) error { //nolint:unparam // Error return kept for API consistency
	if opts.IngestBehind {
		// Ingest behind: all files get seqno 0
		for _, f := range files {
			f.globalSeqNo = 0
		}
		return nil
	}

	if !opts.SnapshotConsistency {
		// No snapshot consistency required
		for _, f := range files {
			f.globalSeqNo = 0
		}
		return nil
	}

	// Assign sequence numbers in order (newer files get higher seqno)
	// This ensures that if files overlap, later files will overwrite earlier ones
	baseSeq := atomic.AddUint64(&db.seq, uint64(len(files)))
	for i, f := range files {
		f.globalSeqNo = baseSeq - uint64(len(files)) + uint64(i) + 1
	}

	return nil
}

// determineTargetLevels finds the appropriate level for each ingested file.
func (db *DBImpl) determineTargetLevels(files []*ingestedFileInfo, opts IngestExternalFileOptions) error {
	current := db.versions.Current()
	if current == nil {
		// No version yet, place in L0
		for _, f := range files {
			f.targetLevel = 0
		}
		return nil
	}
	defer current.Unref()

	numLevels := version.MaxNumLevels

	for _, f := range files {
		if opts.IngestBehind {
			// Ingest behind: always place in last level
			f.targetLevel = numLevels - 1
		} else {
			// Find the lowest level where the file doesn't overlap
			targetLevel := 0
			for level := range numLevels {
				if db.levelOverlapsFile(current, level, f.smallestKey, f.largestKey) {
					// Overlap at this level, can't go lower
					break
				}
				targetLevel = level
			}
			f.targetLevel = targetLevel

			if opts.FailIfNotBottommostLevel && f.targetLevel != numLevels-1 {
				return ErrIngestNotBottommostLevel
			}
		}
	}

	return nil
}

// levelOverlapsFile checks if a level has any files overlapping with the given key range.
func (db *DBImpl) levelOverlapsFile(v *version.Version, level int, smallest, largest []byte) bool {
	files := v.Files(level)
	for _, f := range files {
		fileSmallest := dbformat.ExtractUserKey(f.Smallest)
		fileLargest := dbformat.ExtractUserKey(f.Largest)
		if ingestRangesOverlap(smallest, largest, fileSmallest, fileLargest) {
			return true
		}
	}
	return false
}

// installIngestedFiles copies or moves files to the DB directory.
func (db *DBImpl) installIngestedFiles(files []*ingestedFileInfo, opts IngestExternalFileOptions) error {
	for _, f := range files {
		if opts.MoveFiles {
			// Try to rename (move) the file
			if err := os.Rename(f.externalPath, f.internalPath); err != nil {
				// Fall back to copy
				if err := ingestCopyFile(f.externalPath, f.internalPath); err != nil {
					return fmt.Errorf("failed to copy file %s: %w", f.externalPath, err)
				}
				// Remove original after successful copy
				os.Remove(f.externalPath)
			}
		} else {
			// Copy the file
			if err := ingestCopyFile(f.externalPath, f.internalPath); err != nil {
				return fmt.Errorf("failed to copy file %s: %w", f.externalPath, err)
			}
		}
	}
	return nil
}

// updateManifestForIngest adds the ingested files to the MANIFEST.
func (db *DBImpl) updateManifestForIngest(files []*ingestedFileInfo, _ ColumnFamilyHandle) error {
	edit := manifest.NewVersionEdit()

	for _, f := range files {
		// Create internal keys for smallest/largest
		smallestInternal := dbformat.NewInternalKey(f.smallestKey, dbformat.SequenceNumber(f.globalSeqNo), dbformat.TypeValue)
		largestInternal := dbformat.NewInternalKey(f.largestKey, dbformat.SequenceNumber(f.globalSeqNo), dbformat.TypeValue)

		fileMeta := &manifest.FileMetaData{
			FD: manifest.FileDescriptor{
				PackedNumberAndPathID: manifest.PackFileNumberAndPathID(f.fileNumber, 0),
				FileSize:              f.fileSize,
				SmallestSeqno:         manifest.SequenceNumber(f.globalSeqNo),
				LargestSeqno:          manifest.SequenceNumber(f.globalSeqNo),
			},
			Smallest: smallestInternal,
			Largest:  largestInternal,
		}

		edit.AddFile(f.targetLevel, fileMeta)
	}

	// Apply the edit to the version set
	return db.versions.LogAndApply(edit)
}

// ingestCopyFile copies a file from src to dst.
func ingestCopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	// Create parent directories if needed
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		os.Remove(dst)
		return err
	}

	return dstFile.Sync()
}

// ingestExtractUserKey extracts the user key from an internal key.
func ingestExtractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}
