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
