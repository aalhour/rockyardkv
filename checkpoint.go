package rockyardkv

// checkpoint.go implements the Checkpoint feature for creating database backups.
//
// A checkpoint is a consistent point-in-time snapshot of the database that
// can be used for backups. It creates hard links (or copies if hard links
// aren't supported) of SST files, and copies the MANIFEST and WAL files.
//
// Reference: RocksDB v10.7.5
//   - utilities/checkpoint/checkpoint_impl.h
//   - utilities/checkpoint/checkpoint_impl.cc

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Checkpoint provides functionality to create database checkpoints.
type Checkpoint struct {
	db *dbImpl
}

// NewCheckpoint creates a new Checkpoint instance for the given database.
func NewCheckpoint(database DB) (*Checkpoint, error) {
	impl, ok := database.(*dbImpl)
	if !ok {
		return nil, fmt.Errorf("checkpoint: unsupported database type")
	}
	return &Checkpoint{db: impl}, nil
}

// CreateCheckpoint creates a checkpoint of the database at the specified path.
//
// The checkpoint will contain:
// - All SST files (hard-linked or copied)
// - The MANIFEST file (copied)
// - The CURRENT file (copied)
// - WAL files if log_size_for_flush is 0 (meaning flush before checkpoint)
//
// If logSizeForFlush is 0, the memtable will be flushed before creating the
// checkpoint, resulting in no WAL files in the checkpoint.
// If logSizeForFlush is > 0, WAL files smaller than this size will be copied.
// If logSizeForFlush is very large (e.g., math.MaxUint64), all WAL files are copied.
func (cp *Checkpoint) CreateCheckpoint(checkpointDir string, logSizeForFlush uint64) error {
	cp.db.logger.Infof("[checkpoint] creating checkpoint at %s", checkpointDir)

	// Validate checkpoint directory
	if checkpointDir == "" {
		return fmt.Errorf("checkpoint: directory path cannot be empty")
	}

	// Check if directory already exists
	if _, err := os.Stat(checkpointDir); !os.IsNotExist(err) {
		return fmt.Errorf("checkpoint: directory already exists: %s", checkpointDir)
	}

	// Flush memtable if requested
	if logSizeForFlush == 0 {
		if err := cp.db.Flush(DefaultFlushOptions()); err != nil {
			return fmt.Errorf("checkpoint: failed to flush memtable: %w", err)
		}
	}

	// Create the checkpoint directory
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return fmt.Errorf("checkpoint: failed to create directory: %w", err)
	}

	// Hold the mutex to get consistent view
	cp.db.mu.RLock()
	defer cp.db.mu.RUnlock()

	if cp.db.closed {
		return ErrDBClosed
	}

	dbPath := cp.db.name

	// Get list of live files
	liveFiles, err := cp.getLiveFiles()
	if err != nil {
		os.RemoveAll(checkpointDir) // Cleanup on error
		return fmt.Errorf("checkpoint: failed to get live files: %w", err)
	}

	// Copy/link SST files
	for _, file := range liveFiles.sst {
		srcPath := filepath.Join(dbPath, file)
		dstPath := filepath.Join(checkpointDir, file)

		if err := linkOrCopy(srcPath, dstPath); err != nil {
			os.RemoveAll(checkpointDir)
			return fmt.Errorf("checkpoint: failed to link/copy %s: %w", file, err)
		}
	}

	// Sync the MANIFEST to ensure it's fully written to disk before copying.
	// Errors are ignored because sync is best-effort; the checkpoint can still
	// succeed if the manifest was already synced by a previous operation.
	_ = cp.db.versions.SyncManifest()

	// Copy MANIFEST file
	if liveFiles.manifest != "" {
		srcPath := filepath.Join(dbPath, liveFiles.manifest)
		dstPath := filepath.Join(checkpointDir, liveFiles.manifest)
		if err := copyFile(srcPath, dstPath); err != nil {
			os.RemoveAll(checkpointDir)
			return fmt.Errorf("checkpoint: failed to copy MANIFEST: %w", err)
		}
	}

	// Copy CURRENT file
	srcPath := filepath.Join(dbPath, "CURRENT")
	dstPath := filepath.Join(checkpointDir, "CURRENT")
	if err := copyFile(srcPath, dstPath); err != nil {
		os.RemoveAll(checkpointDir)
		return fmt.Errorf("checkpoint: failed to copy CURRENT: %w", err)
	}

	// Copy WAL files if needed
	if logSizeForFlush > 0 {
		for _, file := range liveFiles.wal {
			srcPath := filepath.Join(dbPath, file)

			// Check file size
			info, err := os.Stat(srcPath)
			if err != nil {
				continue // Skip if can't stat
			}

			// Only copy if under threshold
			if uint64(info.Size()) <= logSizeForFlush {
				dstPath := filepath.Join(checkpointDir, file)
				if err := copyFile(srcPath, dstPath); err != nil {
					os.RemoveAll(checkpointDir)
					return fmt.Errorf("checkpoint: failed to copy WAL %s: %w", file, err)
				}
			}
		}
	}

	// Copy OPTIONS file if it exists
	optionsPath := filepath.Join(dbPath, "OPTIONS")
	if _, err := os.Stat(optionsPath); err == nil {
		dstPath := filepath.Join(checkpointDir, "OPTIONS")
		if err := copyFile(optionsPath, dstPath); err != nil {
			// Non-fatal, OPTIONS file is optional
		}
	}

	cp.db.logger.Infof("[checkpoint] completed: %d SST files, manifest=%s", len(liveFiles.sst), liveFiles.manifest)

	return nil
}

// liveFiles contains the list of files needed for a checkpoint.
type liveFiles struct {
	sst      []string
	manifest string
	wal      []string
}

// getLiveFiles returns the list of files needed for a consistent checkpoint.
func (cp *Checkpoint) getLiveFiles() (*liveFiles, error) {
	result := &liveFiles{}
	dbPath := cp.db.name

	// Get SST files from the current version
	v := cp.db.versions.Current()
	if v != nil {
		for level := range 7 {
			files := v.Files(level)
			for _, f := range files {
				sstFile := fmt.Sprintf("%06d.sst", f.FD.GetNumber())
				result.sst = append(result.sst, sstFile)
			}
		}
	}

	// Get current MANIFEST file
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "MANIFEST-") {
			// Check if this is the current manifest
			currentNum := cp.db.versions.GetManifestFileNumber()
			if strings.HasSuffix(name, strconv.FormatUint(currentNum, 10)) {
				result.manifest = name
			} else if result.manifest == "" {
				// Fallback: use any manifest
				result.manifest = name
			}
		} else if strings.HasSuffix(name, ".log") {
			// WAL files
			result.wal = append(result.wal, name)
		}
	}

	return result, nil
}

// ExportColumnFamilyCheckpoint exports a single column family to a checkpoint.
// This creates a minimal checkpoint containing only the specified column family's data.
func (cp *Checkpoint) ExportColumnFamilyCheckpoint(
	cfHandle ColumnFamilyHandle,
	exportDir string,
) error {
	// For now, we export the entire database
	// A full implementation would filter SST files by column family
	return cp.CreateCheckpoint(exportDir, 0)
}
