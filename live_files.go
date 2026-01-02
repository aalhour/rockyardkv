package rockyardkv

// live_files.go implements live file metadata and management for backups.
//
// Reference: RocksDB v10.7.5.
//   include/rocksdb/metadata.h - LiveFileMetaData, SstFileMetaData structs
//   db/db_filesnapshot.cc - GetLiveFiles, GetLiveFilesMetaData implementations
//   include/rocksdb/db.h - API definitions

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
)

// LiveFileMetaData describes a live SST file in the database.
// Reference: RocksDB v10.7.5 include/rocksdb/metadata.h lines 168-172
type LiveFileMetaData struct {
	// Name is the file name (without the directory path).
	Name string

	// Directory is the directory containing the file.
	Directory string

	// FileNumber is the file number.
	FileNumber uint64

	// Size is the file size in bytes.
	Size uint64

	// ColumnFamilyName is the name of the column family this file belongs to.
	ColumnFamilyName string

	// Level is the level at which this file resides.
	Level int

	// SmallestKey is the smallest user key in the file.
	SmallestKey []byte

	// LargestKey is the largest user key in the file.
	LargestKey []byte

	// SmallestSeqno is the smallest sequence number in the file.
	SmallestSeqno uint64

	// LargestSeqno is the largest sequence number in the file.
	LargestSeqno uint64

	// NumEntries is the number of entries in the file.
	NumEntries uint64

	// NumDeletions is the number of deletion entries in the file.
	NumDeletions uint64

	// BeingCompacted is true if the file is currently being compacted.
	BeingCompacted bool
}

// GetLiveFiles returns a list of all files in the database except WAL files.
// Reference: RocksDB v10.7.5 db/db_filesnapshot.cc GetLiveFiles()
func (db *dbImpl) GetLiveFiles(flushMemtable bool) ([]string, uint64, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, 0, ErrDBClosed
	}
	db.mu.RUnlock()

	// Flush memtable if requested
	if flushMemtable {
		if err := db.Flush(&FlushOptions{Wait: true}); err != nil {
			// Ignore "already exists" errors
			if err.Error() != "db: immutable memtable already exists" {
				return nil, 0, err
			}
		}
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	var files []string

	// Add CURRENT file
	files = append(files, "/CURRENT")

	// Add MANIFEST file
	if db.versions != nil {
		manifestNum := db.versions.ManifestFileNumber()
		manifestName := fmt.Sprintf("MANIFEST-%06d", manifestNum)
		files = append(files, "/"+manifestName)

		// Get MANIFEST file size
		manifestPath := filepath.Join(db.name, manifestName)
		info, err := db.fs.Stat(manifestPath)
		var manifestSize uint64
		if err == nil {
			manifestSize = uint64(info.Size())
		}

		// Add SST files from all levels
		current := db.versions.Current()
		if current != nil {
			for level := range current.NumLevels() {
				levelFiles := current.Files(level)
				for _, f := range levelFiles {
					sstName := fmt.Sprintf("%06d.sst", f.FD.GetNumber())
					files = append(files, "/"+sstName)
				}
			}
		}

		// Add OPTIONS file if exists
		files = append(files, "/OPTIONS-000000")

		return files, manifestSize, nil
	}

	return files, 0, nil
}

// GetLiveFilesMetaData returns metadata about all live SST files.
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc GetLiveFilesMetaData()
func (db *dbImpl) GetLiveFilesMetaData() []LiveFileMetaData {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed || db.versions == nil {
		return nil
	}

	var metadata []LiveFileMetaData

	current := db.versions.Current()
	if current == nil {
		return metadata
	}

	// Iterate through all levels
	for level := range current.NumLevels() {
		files := current.Files(level)
		for _, f := range files {
			meta := LiveFileMetaData{
				Name:             fmt.Sprintf("%06d.sst", f.FD.GetNumber()),
				Directory:        db.name,
				FileNumber:       f.FD.GetNumber(),
				Size:             f.FD.FileSize,
				ColumnFamilyName: "default",
				Level:            level,
				SmallestKey:      f.Smallest, // Internal key
				LargestKey:       f.Largest,  // Internal key
				SmallestSeqno:    uint64(f.FD.SmallestSeqno),
				LargestSeqno:     uint64(f.FD.LargestSeqno),
				BeingCompacted:   f.BeingCompacted,
			}
			metadata = append(metadata, meta)
		}
	}

	return metadata
}

// fileDeletionDisabled tracks whether file deletion is disabled.
// Uses atomic operations for thread safety.
var fileDeletionDisabledCount atomic.Int32

// DisableFileDeletions prevents file deletions.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h DisableFileDeletions()
func (db *dbImpl) DisableFileDeletions() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	db.mu.RUnlock()

	fileDeletionDisabledCount.Add(1)
	return nil
}

// EnableFileDeletions re-enables file deletions.
// Reference: RocksDB v10.7.5 include/rocksdb/db.h EnableFileDeletions()
func (db *dbImpl) EnableFileDeletions() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	db.mu.RUnlock()

	// Decrement but don't go below 0
	for {
		current := fileDeletionDisabledCount.Load()
		if current <= 0 {
			return nil
		}
		if fileDeletionDisabledCount.CompareAndSwap(current, current-1) {
			return nil
		}
	}
}

// IsFileDeletionsDisabled returns true if file deletions are disabled.
func IsFileDeletionsDisabled() bool {
	return fileDeletionDisabledCount.Load() > 0
}

// PauseBackgroundWork pauses all background work.
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc PauseBackgroundWork()
func (db *dbImpl) PauseBackgroundWork() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	bgWork := db.bgWork
	db.mu.RUnlock()

	if bgWork != nil {
		bgWork.pause()
	}
	return nil
}

// ContinueBackgroundWork resumes background work.
// Reference: RocksDB v10.7.5 db/db_impl/db_impl.cc ContinueBackgroundWork()
func (db *dbImpl) ContinueBackgroundWork() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrDBClosed
	}
	bgWork := db.bgWork
	db.mu.RUnlock()

	if bgWork != nil {
		bgWork.resume()
	}
	return nil
}
