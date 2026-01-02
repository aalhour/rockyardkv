package rockyardkv

// backup.go implements BackupEngine for creating and managing database backups.
//
// BackupEngine provides:
// - Hot backups (no need to stop the database)
// - Multiple backups in a single backup directory
// - Incremental backups (shared SST files between backups)
// - Backup listing and deletion
// - Restore to a new location
//
// Reference: RocksDB v10.7.5 utilities/backup/backup_engine.cc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// BackupEngine manages database backups.
type BackupEngine struct {
	backupDir string
	db        *dbImpl
}

// BackupInfo contains information about a backup.
type BackupInfo struct {
	ID        uint32    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Size      int64     `json:"size"`
	NumFiles  int       `json:"num_files"`
}

// backupMeta is the internal metadata format for a backup.
type backupMeta struct {
	ID           uint32   `json:"id"`
	Timestamp    int64    `json:"timestamp"`
	Files        []string `json:"files"`
	ManifestFile string   `json:"manifest_file"`
	LogFiles     []string `json:"log_files"`
	SequenceNum  uint64   `json:"sequence_num"`
	TotalSize    int64    `json:"total_size"`
}

// CreateBackupEngine creates a BackupEngine for the given database.
// The backup directory must be different from the database directory.
func CreateBackupEngine(db DB, backupDir string) (*BackupEngine, error) {
	impl, ok := db.(*dbImpl)
	if !ok {
		return nil, fmt.Errorf("db: unsupported database type for backup")
	}

	// Ensure backup directory exists
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("db: failed to create backup directory: %w", err)
	}

	// Create shared files directory
	sharedDir := filepath.Join(backupDir, "shared")
	if err := os.MkdirAll(sharedDir, 0755); err != nil {
		return nil, fmt.Errorf("db: failed to create shared directory: %w", err)
	}

	// Create meta directory
	metaDir := filepath.Join(backupDir, "meta")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("db: failed to create meta directory: %w", err)
	}

	return &BackupEngine{
		backupDir: backupDir,
		db:        impl,
	}, nil
}

// CreateNewBackup creates a new backup of the database.
func (be *BackupEngine) CreateNewBackup() (*BackupInfo, error) {
	// Get next backup ID
	backupID, err := be.getNextBackupID()
	if err != nil {
		return nil, err
	}

	be.db.logger.Infof("[backup] creating backup %d", backupID)

	// Hold lock to get consistent state
	be.db.mu.Lock()

	v := be.db.versions.Current()
	if v == nil {
		be.db.mu.Unlock()
		return nil, fmt.Errorf("db: no current version")
	}

	// Collect SST files
	var sstFiles []string
	for level := range v.NumLevels() {
		for _, meta := range v.Files(level) {
			sstFiles = append(sstFiles, fmt.Sprintf("%06d.sst", meta.FD.GetNumber()))
		}
	}

	manifestNum := be.db.versions.ManifestFileNumber()
	manifestFile := fmt.Sprintf("MANIFEST-%06d", manifestNum)
	logFileNum := be.db.logFileNumber
	logFile := fmt.Sprintf("%06d.log", logFileNum)
	seqNum := be.db.seq
	dbPath := be.db.name

	be.db.mu.Unlock()

	sharedDir := filepath.Join(be.backupDir, "shared")
	var totalSize int64

	// Copy/link SST files to shared directory
	for _, sst := range sstFiles {
		srcPath := filepath.Join(dbPath, sst)
		dstPath := filepath.Join(sharedDir, sst)

		// Check if file already exists in shared (incremental backup)
		if _, err := os.Stat(dstPath); os.IsNotExist(err) {
			// Copy file
			if err := copyFile(srcPath, dstPath); err != nil {
				return nil, fmt.Errorf("db: failed to backup SST file %s: %w", sst, err)
			}
		}

		// Count size
		if info, err := os.Stat(dstPath); err == nil {
			totalSize += info.Size()
		}
	}

	// Create backup-specific directory for manifest and logs
	backupMetaDir := filepath.Join(be.backupDir, "meta", fmt.Sprintf("%d", backupID))
	if err := os.MkdirAll(backupMetaDir, 0755); err != nil {
		return nil, fmt.Errorf("db: failed to create backup meta dir: %w", err)
	}

	// Copy MANIFEST
	srcManifest := filepath.Join(dbPath, manifestFile)
	dstManifest := filepath.Join(backupMetaDir, manifestFile)
	if err := copyFile(srcManifest, dstManifest); err != nil {
		return nil, fmt.Errorf("db: failed to backup MANIFEST: %w", err)
	}
	if info, err := os.Stat(dstManifest); err == nil {
		totalSize += info.Size()
	}

	// Copy current WAL
	var logFiles []string
	srcLog := filepath.Join(dbPath, logFile)
	if _, err := os.Stat(srcLog); err == nil {
		dstLog := filepath.Join(backupMetaDir, logFile)
		if err := copyFile(srcLog, dstLog); err != nil {
			return nil, fmt.Errorf("db: failed to backup WAL: %w", err)
		}
		logFiles = append(logFiles, logFile)
		if info, err := os.Stat(dstLog); err == nil {
			totalSize += info.Size()
		}
	}

	// Create backup metadata
	meta := &backupMeta{
		ID:           backupID,
		Timestamp:    time.Now().Unix(),
		Files:        sstFiles,
		ManifestFile: manifestFile,
		LogFiles:     logFiles,
		SequenceNum:  seqNum,
		TotalSize:    totalSize,
	}

	// Write metadata file
	metaPath := filepath.Join(be.backupDir, "meta", fmt.Sprintf("backup_%d.json", backupID))
	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("db: failed to marshal backup metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return nil, fmt.Errorf("db: failed to write backup metadata: %w", err)
	}

	info := &BackupInfo{
		ID:        backupID,
		Timestamp: time.Unix(meta.Timestamp, 0),
		Size:      totalSize,
		NumFiles:  len(sstFiles) + 1 + len(logFiles), // SST + manifest + logs
	}

	be.db.logger.Infof("[backup] completed backup %d: %d files, %d bytes", backupID, info.NumFiles, totalSize)

	return info, nil
}

// GetBackupInfo returns information about all available backups.
func (be *BackupEngine) GetBackupInfo() ([]BackupInfo, error) {
	metaDir := filepath.Join(be.backupDir, "meta")
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return nil, fmt.Errorf("db: failed to read backup meta directory: %w", err)
	}

	var infos []BackupInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		metaPath := filepath.Join(metaDir, entry.Name())
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}

		var meta backupMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}

		infos = append(infos, BackupInfo{
			ID:        meta.ID,
			Timestamp: time.Unix(meta.Timestamp, 0),
			Size:      meta.TotalSize,
			NumFiles:  len(meta.Files) + 1 + len(meta.LogFiles),
		})
	}

	// Sort by ID
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].ID < infos[j].ID
	})

	return infos, nil
}

// RestoreDBFromBackup restores the database from a backup.
// The restore directory should not exist.
func (be *BackupEngine) RestoreDBFromBackup(backupID uint32, restoreDir string) error {
	be.db.logger.Infof("[backup] restoring backup %d to %s", backupID, restoreDir)

	// Check restore directory doesn't exist
	if _, err := os.Stat(restoreDir); !os.IsNotExist(err) {
		if err == nil {
			return fmt.Errorf("db: restore directory already exists: %s", restoreDir)
		}
		return err
	}

	// Read backup metadata
	metaPath := filepath.Join(be.backupDir, "meta", fmt.Sprintf("backup_%d.json", backupID))
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return fmt.Errorf("db: backup %d not found: %w", backupID, err)
	}

	var meta backupMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("db: failed to parse backup metadata: %w", err)
	}

	// Create restore directory
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		return fmt.Errorf("db: failed to create restore directory: %w", err)
	}

	// Cleanup on failure
	success := false
	defer func() {
		if !success {
			os.RemoveAll(restoreDir)
		}
	}()

	sharedDir := filepath.Join(be.backupDir, "shared")
	backupMetaDir := filepath.Join(be.backupDir, "meta", fmt.Sprintf("%d", backupID))

	// Copy SST files from shared
	for _, sst := range meta.Files {
		srcPath := filepath.Join(sharedDir, sst)
		dstPath := filepath.Join(restoreDir, sst)
		if err := copyFile(srcPath, dstPath); err != nil {
			return fmt.Errorf("db: failed to restore SST file %s: %w", sst, err)
		}
	}

	// Copy MANIFEST
	srcManifest := filepath.Join(backupMetaDir, meta.ManifestFile)
	dstManifest := filepath.Join(restoreDir, meta.ManifestFile)
	if err := copyFile(srcManifest, dstManifest); err != nil {
		return fmt.Errorf("db: failed to restore MANIFEST: %w", err)
	}

	// Create CURRENT file
	currentPath := filepath.Join(restoreDir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte(meta.ManifestFile+"\n"), 0644); err != nil {
		return fmt.Errorf("db: failed to create CURRENT: %w", err)
	}

	// Copy WAL files
	for _, logFile := range meta.LogFiles {
		srcLog := filepath.Join(backupMetaDir, logFile)
		dstLog := filepath.Join(restoreDir, logFile)
		if err := copyFile(srcLog, dstLog); err != nil {
			return fmt.Errorf("db: failed to restore WAL: %w", err)
		}
	}

	// Sync directory
	dir, err := os.Open(restoreDir)
	if err == nil {
		_ = dir.Sync() // Best effort sync
		_ = dir.Close()
	}

	be.db.logger.Infof("[backup] restore completed: %d SST files, manifest=%s", len(meta.Files), meta.ManifestFile)

	success = true
	return nil
}

// DeleteBackup deletes a backup.
func (be *BackupEngine) DeleteBackup(backupID uint32) error {
	// Read backup metadata to know which files to check
	metaPath := filepath.Join(be.backupDir, "meta", fmt.Sprintf("backup_%d.json", backupID))
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return fmt.Errorf("db: backup %d not found", backupID)
	}

	// Delete backup-specific metadata directory
	backupMetaDir := filepath.Join(be.backupDir, "meta", fmt.Sprintf("%d", backupID))
	os.RemoveAll(backupMetaDir)

	// Delete metadata file
	os.Remove(metaPath)

	// Note: We don't delete shared SST files as they may be used by other backups.
	// A separate GarbageCollect() method could be added to clean up unreferenced files.

	return nil
}

// PurgeOldBackups keeps the most recent N backups and deletes the rest.
func (be *BackupEngine) PurgeOldBackups(numToKeep int) (int, error) {
	infos, err := be.GetBackupInfo()
	if err != nil {
		return 0, err
	}

	if len(infos) <= numToKeep {
		return 0, nil
	}

	// Delete oldest backups
	deleted := 0
	for i := range len(infos) - numToKeep {
		if err := be.DeleteBackup(infos[i].ID); err == nil {
			deleted++
		}
	}

	return deleted, nil
}

// getNextBackupID returns the next available backup ID.
func (be *BackupEngine) getNextBackupID() (uint32, error) {
	infos, err := be.GetBackupInfo()
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 1, nil // Contract: Use 1 as the first backup ID when the backup directory doesn't exist yet.
		}
		return 0, err
	}

	if len(infos) == 0 {
		return 1, nil
	}

	return infos[len(infos)-1].ID + 1, nil
}

// Close closes the backup engine.
func (be *BackupEngine) Close() error {
	// No resources to clean up currently
	return nil
}
