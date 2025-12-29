package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFaultInjectionFS_Create(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	n, err := f.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Write returned %d, want 5", n)
	}

	f.Close()

	// Verify file exists
	if !fs.Exists(path) {
		t.Error("File should exist")
	}
}

func TestFaultInjectionFS_InjectWriteError(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")

	// Inject write error for this path
	fs.InjectWriteError(path)

	_, err := fs.Create(path)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected ErrInjectedWriteError, got %v", err)
	}

	// Clear errors
	fs.ClearErrors()

	// Should work now
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed after clearing errors: %v", err)
	}
	f.Close()
}

func TestFaultInjectionFS_InjectReadError(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("content"), 0644)

	// Inject read error for this path
	fs.InjectReadError(path)

	_, err := fs.Open(path)
	if !errors.Is(err, ErrInjectedReadError) {
		t.Errorf("Expected ErrInjectedReadError, got %v", err)
	}

	_, err = fs.OpenRandomAccess(path)
	if !errors.Is(err, ErrInjectedReadError) {
		t.Errorf("Expected ErrInjectedReadError for random access, got %v", err)
	}
}

func TestFaultInjectionFS_InjectSyncError(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	f.Write([]byte("hello"))

	// Inject sync error
	fs.InjectSyncError()

	err = f.Sync()
	if !errors.Is(err, ErrInjectedSyncError) {
		t.Errorf("Expected ErrInjectedSyncError, got %v", err)
	}

	f.Close()
}

func TestFaultInjectionFS_TrackSyncState(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write without sync
	f.Write([]byte("hello"))

	absPath, _ := filepath.Abs(path)
	syncedPos, currentPos, ok := fs.GetFileState(absPath)
	if !ok {
		t.Fatal("File state should exist")
	}
	if syncedPos != 0 {
		t.Errorf("syncedPos = %d, want 0", syncedPos)
	}
	if currentPos != 5 {
		t.Errorf("currentPos = %d, want 5", currentPos)
	}

	// Sync the file
	f.Sync()

	syncedPos, currentPos, _ = fs.GetFileState(absPath)
	if syncedPos != 5 {
		t.Errorf("syncedPos after sync = %d, want 5", syncedPos)
	}
	if currentPos != 5 {
		t.Errorf("currentPos after sync = %d, want 5", currentPos)
	}

	f.Close()
}

func TestFaultInjectionFS_DropUnsyncedData(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write and sync first part
	f.Write([]byte("hello"))
	f.Sync()

	// Write more without sync
	f.Write([]byte(" world"))
	f.Close()

	// Verify file has all content
	data, _ := os.ReadFile(path)
	if string(data) != "hello world" {
		t.Errorf("Content before drop = %q, want 'hello world'", data)
	}

	// Simulate crash - drop unsynced data
	fs.DropUnsyncedData()

	// File should be truncated to synced position
	data, _ = os.ReadFile(path)
	if string(data) != "hello" {
		t.Errorf("Content after drop = %q, want 'hello'", data)
	}
}

func TestFaultInjectionFS_SetFilesystemActive(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Deactivate filesystem
	fs.SetFilesystemActive(false)

	path := filepath.Join(dir, "test.txt")
	_, err := fs.Create(path)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected ErrInjectedWriteError when inactive, got %v", err)
	}

	// Reactivate
	fs.SetFilesystemActive(true)

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed after reactivation: %v", err)
	}
	f.Close()
}

func TestFaultInjectionFS_Rename(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	oldPath := filepath.Join(dir, "old.txt")
	newPath := filepath.Join(dir, "new.txt")

	f, _ := fs.Create(oldPath)
	f.Write([]byte("content"))
	f.Sync()
	f.Close()

	// Check state exists for old path
	absOld, _ := filepath.Abs(oldPath)
	_, _, ok := fs.GetFileState(absOld)
	if !ok {
		t.Error("State should exist for old path")
	}

	// Rename
	if err := fs.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// State should be transferred to new path
	absNew, _ := filepath.Abs(newPath)
	_, _, ok = fs.GetFileState(absNew)
	if !ok {
		t.Error("State should exist for new path after rename")
	}

	_, _, ok = fs.GetFileState(absOld)
	if ok {
		t.Error("State should not exist for old path after rename")
	}
}

func TestFaultInjectionFS_Remove(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, _ := fs.Create(path)
	f.Write([]byte("content"))
	f.Close()

	absPath, _ := filepath.Abs(path)
	_, _, ok := fs.GetFileState(absPath)
	if !ok {
		t.Error("State should exist before remove")
	}

	fs.Remove(path)

	_, _, ok = fs.GetFileState(absPath)
	if ok {
		t.Error("State should not exist after remove")
	}
}

func TestFaultInjectionFS_InjectErrorForAllPaths(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Inject write error for all paths (empty path means all)
	fs.InjectWriteError("")

	path1 := filepath.Join(dir, "test1.txt")
	path2 := filepath.Join(dir, "test2.txt")

	_, err := fs.Create(path1)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected error for path1, got %v", err)
	}

	_, err = fs.Create(path2)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected error for path2, got %v", err)
	}
}

func TestFaultInjectionFS_Truncate(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, _ := fs.Create(path)
	f.Write([]byte("hello world"))
	f.Sync()

	absPath, _ := filepath.Abs(path)
	syncedPos, currentPos, _ := fs.GetFileState(absPath)
	if syncedPos != 11 || currentPos != 11 {
		t.Errorf("State before truncate: synced=%d, current=%d", syncedPos, currentPos)
	}

	// Truncate
	f.Truncate(5)

	syncedPos, currentPos, _ = fs.GetFileState(absPath)
	if syncedPos != 5 {
		t.Errorf("syncedPos after truncate = %d, want 5", syncedPos)
	}
	if currentPos != 5 {
		t.Errorf("currentPos after truncate = %d, want 5", currentPos)
	}

	f.Close()
}

func TestFaultInjectionFS_SyncDir(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "test.txt")
	f, _ := fs.Create(path)
	f.Write([]byte("hello"))
	f.Sync()
	f.Close()

	// Sync the directory
	if err := fs.SyncDir(dir); err != nil {
		t.Fatalf("SyncDir failed: %v", err)
	}

	// After SyncDir, file's dirSynced should be true
	// This is an internal state that affects DeleteUnsyncedFiles behavior
}

func TestFaultInjectionFS_MkdirAll(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	path := filepath.Join(dir, "a", "b", "c")

	if err := fs.MkdirAll(path, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	if !fs.Exists(path) {
		t.Error("Directory should exist")
	}

	// Test with inactive filesystem
	fs.SetFilesystemActive(false)
	path2 := filepath.Join(dir, "d", "e")
	err := fs.MkdirAll(path2, 0755)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Expected error when inactive, got %v", err)
	}
}

func TestFaultInjectionFS_PassthroughMethods(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Create a file
	path := filepath.Join(dir, "test.txt")
	os.WriteFile(path, []byte("content"), 0644)

	// Test Stat
	info, err := fs.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 7 {
		t.Errorf("Size = %d, want 7", info.Size())
	}

	// Test Exists
	if !fs.Exists(path) {
		t.Error("Exists should return true")
	}

	// Test ListDir
	names, err := fs.ListDir(dir)
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}
	if len(names) != 1 || names[0] != "test.txt" {
		t.Errorf("ListDir = %v, want [test.txt]", names)
	}

	// Test Lock
	lock, err := fs.Lock(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	lock.Close()
}

// TestFaultInjectionFS_Rename_NotDurableWithoutDirSync tests that renames
// are not durable until the parent directory is synced.
//
// Contract: A rename without SyncDir creates a pending rename that can be reverted.
func TestFaultInjectionFS_Rename_NotDurableWithoutDirSync(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Create and sync a file
	oldPath := filepath.Join(dir, "MANIFEST-000001")
	f, err := fs.Create(oldPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	f.Write([]byte("manifest content"))
	f.Sync()
	f.Close()

	// Create CURRENT pointing to the old manifest
	currentPath := filepath.Join(dir, "CURRENT")
	curFile, _ := fs.Create(currentPath)
	curFile.Write([]byte("MANIFEST-000001\n"))
	curFile.Sync()
	curFile.Close()

	// Sync directory to make initial state durable
	fs.SyncDir(dir)

	// Now create a new manifest and update CURRENT via rename
	newManifestPath := filepath.Join(dir, "MANIFEST-000002")
	mf, _ := fs.Create(newManifestPath)
	mf.Write([]byte("new manifest content"))
	mf.Sync()
	mf.Close()

	// Write new CURRENT.tmp and rename to CURRENT (simulating CURRENT update)
	tmpPath := filepath.Join(dir, "CURRENT.tmp")
	tmp, _ := fs.Create(tmpPath)
	tmp.Write([]byte("MANIFEST-000002\n"))
	tmp.Sync()
	tmp.Close()

	// Rename CURRENT.tmp -> CURRENT
	if err := fs.Rename(tmpPath, currentPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// At this point, rename is NOT durable (no SyncDir called)
	if !fs.HasPendingRenames() {
		t.Error("Should have pending renames after Rename without SyncDir")
	}
	if fs.PendingRenameCount() != 1 {
		t.Errorf("PendingRenameCount = %d, want 1", fs.PendingRenameCount())
	}

	// Simulate crash by reverting unsynced renames
	fs.RevertUnsyncedRenames()

	// CURRENT should not exist or should have reverted
	// (In this case, the old CURRENT was overwritten, so it might be deleted)
	// The key point is: pending renames are reverted
	if fs.HasPendingRenames() {
		t.Error("Should have no pending renames after RevertUnsyncedRenames")
	}
}

// TestFaultInjectionFS_SyncDir_MakesRenamesDurable tests that SyncDir makes
// pending renames durable.
//
// Contract: A rename followed by SyncDir has no pending renames and survives crash simulation.
func TestFaultInjectionFS_SyncDir_MakesRenamesDurable(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Create and sync a file
	oldPath := filepath.Join(dir, "old.txt")
	f, _ := fs.Create(oldPath)
	f.Write([]byte("content"))
	f.Sync()
	f.Close()

	// Rename to new path
	newPath := filepath.Join(dir, "new.txt")
	fs.Rename(oldPath, newPath)

	// Verify pending rename exists
	if !fs.HasPendingRenames() {
		t.Error("Should have pending renames after Rename")
	}

	// Call SyncDir to make the rename durable
	fs.SyncDir(dir)

	// Pending rename should be cleared
	if fs.HasPendingRenames() {
		t.Error("Should have no pending renames after SyncDir")
	}

	// Verify the new file still exists at new path
	if !fs.Exists(newPath) {
		t.Error("New file should exist after SyncDir")
	}
}

// TestFaultInjectionFS_SyncDirLieMode_DoesNotMakeRenamesDurable tests that
// SyncDir in lie mode returns success but does NOT clear pending renames.
//
// Contract: In lie mode, SyncDir succeeds but renames remain pending.
// RevertUnsyncedRenames will still revert them on crash.
func TestFaultInjectionFS_SyncDirLieMode_DoesNotMakeRenamesDurable(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Enable lie mode
	fs.SetSyncDirLieMode(true)

	// Verify lie mode is enabled
	if !fs.IsSyncDirLieModeEnabled() {
		t.Error("SyncDirLieMode should be enabled")
	}

	// Create and sync a file
	oldPath := filepath.Join(dir, "old.txt")
	f, _ := fs.Create(oldPath)
	f.Write([]byte("content"))
	f.Sync()
	f.Close()

	// Rename to new path
	newPath := filepath.Join(dir, "new.txt")
	fs.Rename(oldPath, newPath)

	// Verify pending rename exists
	if !fs.HasPendingRenames() {
		t.Error("Should have pending renames after Rename")
	}
	if fs.PendingRenameCount() != 1 {
		t.Errorf("PendingRenameCount = %d, want 1", fs.PendingRenameCount())
	}

	// Call SyncDir - should return success but NOT clear pending renames
	err := fs.SyncDir(dir)
	if err != nil {
		t.Errorf("SyncDir should succeed in lie mode, got: %v", err)
	}

	// Pending renames should still exist (lie mode)
	if !fs.HasPendingRenames() {
		t.Error("Should STILL have pending renames after SyncDir in lie mode")
	}
	if fs.PendingRenameCount() != 1 {
		t.Errorf("PendingRenameCount = %d, want 1 (unchanged)", fs.PendingRenameCount())
	}

	// File should exist on disk (the rename happened)
	if !fs.Exists(newPath) {
		t.Error("New file should exist on disk")
	}

	// Simulate crash - renames should be reverted
	fs.RevertUnsyncedRenames()

	// Pending renames should be cleared now
	if fs.HasPendingRenames() {
		t.Error("Should have no pending renames after RevertUnsyncedRenames")
	}

	// File at new path should not exist (reverted)
	if fs.Exists(newPath) {
		t.Error("New file should NOT exist after crash (rename reverted)")
	}

	// File at old path should exist (reverted)
	if !fs.Exists(oldPath) {
		t.Error("Old file should exist after crash (rename reverted)")
	}
}

// TestFaultInjectionFS_SyncDirLieMode_ToggleOff tests that disabling lie mode
// restores normal SyncDir behavior.
//
// Contract: After disabling lie mode, SyncDir clears pending renames normally.
func TestFaultInjectionFS_SyncDirLieMode_ToggleOff(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Enable then disable lie mode
	fs.SetSyncDirLieMode(true)
	fs.SetSyncDirLieMode(false)

	if fs.IsSyncDirLieModeEnabled() {
		t.Error("SyncDirLieMode should be disabled")
	}

	// Create and rename a file
	oldPath := filepath.Join(dir, "old.txt")
	f, _ := fs.Create(oldPath)
	f.Write([]byte("content"))
	f.Sync()
	f.Close()

	newPath := filepath.Join(dir, "new.txt")
	fs.Rename(oldPath, newPath)

	// SyncDir should clear pending renames (normal mode)
	fs.SyncDir(dir)

	if fs.HasPendingRenames() {
		t.Error("Should have no pending renames after SyncDir in normal mode")
	}
}

// TestFaultInjectionFS_FileSyncLieMode_AllFiles tests that file sync lie mode
// causes Sync() to return success but NOT mark data as synced.
//
// Contract: In lie mode, Sync succeeds but DropUnsyncedData loses the data.
func TestFaultInjectionFS_FileSyncLieMode_AllFiles(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Enable lie mode for all files (empty pattern)
	fs.SetFileSyncLieMode(true, "")

	if !fs.IsFileSyncLieModeEnabled() {
		t.Error("FileSyncLieMode should be enabled")
	}

	// Create and write to a file
	path := filepath.Join(dir, "test.log")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	_, err = f.Write([]byte("important data"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Sync should succeed
	err = f.Sync()
	if err != nil {
		t.Fatalf("Sync should succeed in lie mode: %v", err)
	}

	f.Close()

	// Get file state - data should NOT be marked as synced
	syncedPos, currentPos, ok := fs.GetFileState(path)
	if !ok {
		t.Fatal("File state should exist")
	}

	t.Logf("syncedPos=%d, currentPos=%d", syncedPos, currentPos)

	if syncedPos == currentPos {
		t.Error("In lie mode, syncedPos should NOT equal currentPos after Sync")
	}

	// Simulate crash - data should be lost
	fs.DropUnsyncedData()

	// Read the file - it should be truncated to syncedPos (0)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if len(content) != 0 {
		t.Errorf("File should be empty after crash, got %d bytes", len(content))
	}
}

// TestFaultInjectionFS_FileSyncLieMode_PatternMatch tests that file sync lie mode
// only affects files matching the specified pattern.
//
// Contract: Lie mode with pattern ".log" only lies for .log files.
func TestFaultInjectionFS_FileSyncLieMode_PatternMatch(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Enable lie mode only for .log files
	fs.SetFileSyncLieMode(true, ".log")

	if fs.GetFileSyncLiePattern() != ".log" {
		t.Errorf("Pattern should be '.log', got %q", fs.GetFileSyncLiePattern())
	}

	// Create a .log file - should be affected by lie mode
	logPath := filepath.Join(dir, "test.log")
	logFile, _ := fs.Create(logPath)
	logFile.Write([]byte("log data"))
	logFile.Sync() // Lies
	logFile.Close()

	// Create a .sst file - should NOT be affected by lie mode
	sstPath := filepath.Join(dir, "test.sst")
	sstFile, _ := fs.Create(sstPath)
	sstFile.Write([]byte("sst data"))
	sstFile.Sync() // Does NOT lie
	sstFile.Close()

	// Simulate crash
	fs.DropUnsyncedData()

	// .log file should be empty (lie mode)
	logContent, _ := os.ReadFile(logPath)
	if len(logContent) != 0 {
		t.Errorf(".log file should be empty after crash, got %d bytes", len(logContent))
	}

	// .sst file should have data (sync was honest)
	sstContent, _ := os.ReadFile(sstPath)
	if string(sstContent) != "sst data" {
		t.Errorf(".sst file should have data after crash, got %q", sstContent)
	}
}

// TestFaultInjectionFS_FileSyncLieMode_ManifestPattern tests lie mode for MANIFEST files.
//
// Contract: Lie mode with pattern "MANIFEST" only lies for MANIFEST files.
func TestFaultInjectionFS_FileSyncLieMode_ManifestPattern(t *testing.T) {
	dir := t.TempDir()
	base := Default()
	fs := NewFaultInjectionFS(base)

	// Enable lie mode only for MANIFEST files
	fs.SetFileSyncLieMode(true, "MANIFEST")

	// Create a MANIFEST file - should be affected by lie mode
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	manifestFile, _ := fs.Create(manifestPath)
	manifestFile.Write([]byte("manifest data"))
	manifestFile.Sync() // Lies
	manifestFile.Close()

	// Create a regular file - should NOT be affected
	regularPath := filepath.Join(dir, "regular.txt")
	regularFile, _ := fs.Create(regularPath)
	regularFile.Write([]byte("regular data"))
	regularFile.Sync() // Does NOT lie
	regularFile.Close()

	// Simulate crash
	fs.DropUnsyncedData()

	// MANIFEST file should be empty (lie mode)
	manifestContent, _ := os.ReadFile(manifestPath)
	if len(manifestContent) != 0 {
		t.Errorf("MANIFEST file should be empty after crash, got %d bytes", len(manifestContent))
	}

	// Regular file should have data
	regularContent, _ := os.ReadFile(regularPath)
	if string(regularContent) != "regular data" {
		t.Errorf("Regular file should have data after crash, got %q", regularContent)
	}
}

// =============================================================================
// Rename Anomaly Mode Tests
// =============================================================================

// TestFaultInjectionFS_RenameDoubleNameMode_BothNamesExist verifies that
// with double-name mode enabled, after crash both old and new names exist.
//
// Contract: Rename succeeds, but after SimulateCrashWithRenameAnomalies(),
// both the original path and the renamed path exist with the same content.
func TestFaultInjectionFS_RenameDoubleNameMode_BothNamesExist(t *testing.T) {
	dir := t.TempDir()
	faultFS := NewFaultInjectionFS(Default())

	// Enable double-name mode for CURRENT files
	faultFS.SetRenameDoubleNameMode(true, "CURRENT")

	oldPath := filepath.Join(dir, "CURRENT.tmp")
	newPath := filepath.Join(dir, "CURRENT")

	// Create and write to the temp file
	f, err := faultFS.Create(oldPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	content := []byte("MANIFEST-000002\n")
	if _, err := f.Write(content); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Rename the file (this records the pending rename)
	if err := faultFS.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify rename worked normally
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Fatal("New path should exist after rename")
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatal("Old path should not exist after normal rename")
	}

	// Simulate crash with rename anomalies
	if err := faultFS.SimulateCrashWithRenameAnomalies(); err != nil {
		t.Fatalf("SimulateCrashWithRenameAnomalies failed: %v", err)
	}

	// After double-name anomaly, BOTH paths should exist
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Error("New path should exist after double-name crash")
	}
	if _, err := os.Stat(oldPath); os.IsNotExist(err) {
		t.Error("Old path should ALSO exist after double-name crash")
	}

	// Both should have the same content
	newContent, _ := os.ReadFile(newPath)
	oldContent, _ := os.ReadFile(oldPath)
	if string(newContent) != string(content) || string(oldContent) != string(content) {
		t.Errorf("Both paths should have same content. new=%q, old=%q", newContent, oldContent)
	}
}

// TestFaultInjectionFS_RenameNeitherNameMode_NeitherExists verifies that
// with neither-name mode enabled, after crash neither old nor new name exists.
//
// Contract: Rename succeeds, but after SimulateCrashWithRenameAnomalies(),
// neither the original path nor the renamed path exists.
func TestFaultInjectionFS_RenameNeitherNameMode_NeitherExists(t *testing.T) {
	dir := t.TempDir()
	faultFS := NewFaultInjectionFS(Default())

	// Enable neither-name mode for CURRENT files
	faultFS.SetRenameNeitherNameMode(true, "CURRENT")

	oldPath := filepath.Join(dir, "CURRENT.tmp")
	newPath := filepath.Join(dir, "CURRENT")

	// Create and write to the temp file
	f, err := faultFS.Create(oldPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	content := []byte("MANIFEST-000002\n")
	if _, err := f.Write(content); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Rename the file (this records the pending rename)
	if err := faultFS.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify rename worked normally
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		t.Fatal("New path should exist after rename")
	}

	// Simulate crash with rename anomalies
	if err := faultFS.SimulateCrashWithRenameAnomalies(); err != nil {
		t.Fatalf("SimulateCrashWithRenameAnomalies failed: %v", err)
	}

	// After neither-name anomaly, NEITHER path should exist
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Error("New path should NOT exist after neither-name crash")
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("Old path should NOT exist after neither-name crash")
	}
}

// TestFaultInjectionFS_RenameAnomalyMode_PatternMatching verifies that
// anomaly modes only affect files matching the specified pattern.
//
// Contract: Files not matching the pattern are not affected by anomaly mode.
func TestFaultInjectionFS_RenameAnomalyMode_PatternMatching(t *testing.T) {
	dir := t.TempDir()
	faultFS := NewFaultInjectionFS(Default())

	// Enable neither-name mode only for .sst files
	faultFS.SetRenameNeitherNameMode(true, ".sst")

	// Create and rename an SST file (should be affected)
	sstOldPath := filepath.Join(dir, "000001.sst.tmp")
	sstNewPath := filepath.Join(dir, "000001.sst")
	f1, _ := faultFS.Create(sstOldPath)
	f1.Write([]byte("sst data"))
	f1.Close()
	faultFS.Rename(sstOldPath, sstNewPath)

	// Create and rename a non-SST file (should NOT be affected)
	otherOldPath := filepath.Join(dir, "CURRENT.tmp")
	otherNewPath := filepath.Join(dir, "CURRENT")
	f2, _ := faultFS.Create(otherOldPath)
	f2.Write([]byte("current data"))
	f2.Close()
	faultFS.Rename(otherOldPath, otherNewPath)

	// Simulate crash
	faultFS.SimulateCrashWithRenameAnomalies()

	// SST file: neither should exist
	if _, err := os.Stat(sstNewPath); !os.IsNotExist(err) {
		t.Error("SST new path should not exist (neither-name mode)")
	}
	if _, err := os.Stat(sstOldPath); !os.IsNotExist(err) {
		t.Error("SST old path should not exist (neither-name mode)")
	}

	// CURRENT file: new path should still exist (not affected)
	if _, err := os.Stat(otherNewPath); os.IsNotExist(err) {
		t.Error("CURRENT new path should exist (not affected by .sst pattern)")
	}
}
