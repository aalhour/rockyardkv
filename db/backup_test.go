package db_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/db"
)

func TestBackupEngineBasic(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")
	restorePath := filepath.Join(dir, "restored")

	// Create and populate database
	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write some data
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		value := fmt.Appendf(nil, "value%03d", i)
		if err := database.Put(nil, key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush to create SST files
	if err := database.Flush(nil); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Create backup engine
	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	// Create backup
	info, err := backupEngine.CreateNewBackup()
	if err != nil {
		t.Fatalf("CreateNewBackup failed: %v", err)
	}

	t.Logf("Created backup %d: %d files, %d bytes", info.ID, info.NumFiles, info.Size)

	// Close original database
	database.Close()

	// Restore from backup
	if err := backupEngine.RestoreDBFromBackup(info.ID, restorePath); err != nil {
		t.Fatalf("RestoreDBFromBackup failed: %v", err)
	}

	// Open restored database
	restoredDB, err := db.Open(restorePath, opts)
	if err != nil {
		t.Fatalf("Failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Verify data
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		expectedValue := fmt.Appendf(nil, "value%03d", i)

		value, err := restoredDB.Get(nil, key)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", key, err)
			continue
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Get(%s) = %s, want %s", key, value, expectedValue)
		}
	}
}

func TestBackupEngineMultipleBackups(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_multi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	// Create multiple backups
	for batch := range 3 {
		// Write data
		for i := range 20 {
			key := fmt.Appendf(nil, "batch%d_key%03d", batch, i)
			value := fmt.Appendf(nil, "batch%d_value%03d", batch, i)
			if err := database.Put(nil, key, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		database.Flush(nil)

		// Create backup
		info, err := backupEngine.CreateNewBackup()
		if err != nil {
			t.Fatalf("CreateNewBackup failed: %v", err)
		}
		t.Logf("Created backup %d", info.ID)
	}

	// List backups
	infos, err := backupEngine.GetBackupInfo()
	if err != nil {
		t.Fatalf("GetBackupInfo failed: %v", err)
	}

	if len(infos) != 3 {
		t.Errorf("Expected 3 backups, got %d", len(infos))
	}

	// Verify backup IDs are sequential
	for i, info := range infos {
		expectedID := uint32(i + 1)
		if info.ID != expectedID {
			t.Errorf("Backup %d has ID %d, expected %d", i, info.ID, expectedID)
		}
	}
}

func TestBackupEnginePurgeOldBackups(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_purge_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	// Create 5 backups
	for i := range 5 {
		key := fmt.Appendf(nil, "key%d", i)
		value := []byte("value")
		database.Put(nil, key, value)
		database.Flush(nil)
		if _, err := backupEngine.CreateNewBackup(); err != nil {
			t.Fatalf("CreateNewBackup failed: %v", err)
		}
	}

	// Verify 5 backups exist
	infos, _ := backupEngine.GetBackupInfo()
	if len(infos) != 5 {
		t.Fatalf("Expected 5 backups, got %d", len(infos))
	}

	// Purge to keep only 2
	deleted, err := backupEngine.PurgeOldBackups(2)
	if err != nil {
		t.Fatalf("PurgeOldBackups failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("Expected to delete 3 backups, deleted %d", deleted)
	}

	// Verify only 2 remain
	infos, _ = backupEngine.GetBackupInfo()
	if len(infos) != 2 {
		t.Errorf("Expected 2 backups after purge, got %d", len(infos))
	}

	// Should be the most recent (IDs 4 and 5)
	if infos[0].ID != 4 || infos[1].ID != 5 {
		t.Errorf("Expected backups 4 and 5 to remain, got %d and %d", infos[0].ID, infos[1].ID)
	}
}

func TestBackupEngineRestoreNonExistent(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_restore_missing_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")
	restorePath := filepath.Join(dir, "restored")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	// Try to restore non-existent backup
	err = backupEngine.RestoreDBFromBackup(999, restorePath)
	if err == nil {
		t.Fatal("Expected error when restoring non-existent backup")
	}
}

func TestBackupEngineRestoreToExistingDir(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_restore_exists_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")
	restorePath := filepath.Join(dir, "restored")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	key := []byte("key")
	value := []byte("value")
	database.Put(nil, key, value)
	database.Flush(nil)

	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	info, err := backupEngine.CreateNewBackup()
	if err != nil {
		t.Fatalf("CreateNewBackup failed: %v", err)
	}

	// Create restore directory before restoring
	if err := os.MkdirAll(restorePath, 0755); err != nil {
		t.Fatalf("Failed to create restore dir: %v", err)
	}

	// Should fail because directory exists
	err = backupEngine.RestoreDBFromBackup(info.ID, restorePath)
	if err == nil {
		t.Fatal("Expected error when restoring to existing directory")
	}
}

func TestBackupEngineIncrementalBackup(t *testing.T) {
	dir, err := os.MkdirTemp("", "backup_incremental_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "db")
	backupPath := filepath.Join(dir, "backups")
	restorePath := filepath.Join(dir, "restored")

	opts := db.DefaultOptions()
	opts.CreateIfMissing = true

	database, err := db.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	backupEngine, err := db.CreateBackupEngine(database, backupPath)
	if err != nil {
		t.Fatalf("CreateBackupEngine failed: %v", err)
	}
	defer backupEngine.Close()

	// First backup: write keys 0-49
	for i := range 50 {
		key := fmt.Appendf(nil, "key%03d", i)
		value := []byte("first_backup")
		database.Put(nil, key, value)
	}
	database.Flush(nil)
	info1, err := backupEngine.CreateNewBackup()
	if err != nil {
		t.Fatalf("CreateNewBackup 1 failed: %v", err)
	}

	// Second backup (incremental): write keys 50-99 (new keys)
	for i := 50; i < 100; i++ {
		key := fmt.Appendf(nil, "key%03d", i)
		value := []byte("second_backup")
		database.Put(nil, key, value)
	}
	database.Flush(nil)
	info2, err := backupEngine.CreateNewBackup()
	if err != nil {
		t.Fatalf("CreateNewBackup 2 failed: %v", err)
	}

	t.Logf("Backup 1: %d files, %d bytes", info1.NumFiles, info1.Size)
	t.Logf("Backup 2: %d files, %d bytes", info2.NumFiles, info2.Size)

	database.Close()

	// Restore from second backup and verify all data
	if err := backupEngine.RestoreDBFromBackup(info2.ID, restorePath); err != nil {
		t.Fatalf("RestoreDBFromBackup failed: %v", err)
	}

	restoredDB, err := db.Open(restorePath, opts)
	if err != nil {
		t.Fatalf("Failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Verify all 100 keys exist (0-49 from first backup, 50-99 from second)
	for i := range 100 {
		key := fmt.Appendf(nil, "key%03d", i)
		val, err := restoredDB.Get(nil, key)
		if err != nil {
			t.Errorf("Key %s not found in restored database: %v", key, err)
			continue
		}
		// Verify correct value
		expectedValue := "first_backup"
		if i >= 50 {
			expectedValue = "second_backup"
		}
		if string(val) != expectedValue {
			t.Errorf("Key %s has wrong value: got %s, want %s", key, val, expectedValue)
		}
	}
}
