package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestFaultInjectionDeleteUnsyncedFiles tests DeleteUnsyncedFiles
func TestFaultInjectionDeleteUnsyncedFiles(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file that won't be synced
	filePath := filepath.Join(dir, "unsynced.txt")
	f, err := fs.Create(filePath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	f.Write([]byte("test data"))
	f.Close()

	// File exists before DeleteUnsyncedFiles
	if !fs.Exists(filePath) {
		t.Error("file should exist before DeleteUnsyncedFiles")
	}

	// Delete unsynced files
	if err := fs.DeleteUnsyncedFiles(); err != nil {
		t.Fatalf("DeleteUnsyncedFiles failed: %v", err)
	}
}

// TestFaultInjectionRemoveAll tests RemoveAll
func TestFaultInjectionRemoveAll(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a nested directory structure
	subdir := filepath.Join(dir, "sub", "nested")
	if err := fs.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Create a file in the nested directory
	filePath := filepath.Join(subdir, "test.txt")
	f, err := fs.Create(filePath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	f.Close()

	// RemoveAll
	topDir := filepath.Join(dir, "sub")
	if err := fs.RemoveAll(topDir); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify it's gone
	if fs.Exists(topDir) {
		t.Error("directory should be removed after RemoveAll")
	}
}

// TestFaultInjectionOpenWithError tests Open with injected error
func TestFaultInjectionOpenWithError(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file first
	filePath := filepath.Join(dir, "test.txt")
	f, _ := fs.Create(filePath)
	f.Write([]byte("test"))
	f.Close()

	// Inject read error for this specific path
	fs.InjectReadError(filePath)

	// Try to open - should fail
	_, err := fs.Open(filePath)
	if !errors.Is(err, ErrInjectedReadError) {
		t.Errorf("Open with injected error: got %v, want %v", err, ErrInjectedReadError)
	}

	// Clear error and try again
	fs.ClearErrors()
	seqFile, err := fs.Open(filePath)
	if err != nil {
		t.Fatalf("Open after ClearErrors failed: %v", err)
	}
	seqFile.Close()
}

// TestFaultInjectionOpenRandomAccessWithError tests OpenRandomAccess with injected error
func TestFaultInjectionOpenRandomAccessWithError(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file first
	filePath := filepath.Join(dir, "test.txt")
	f, _ := fs.Create(filePath)
	f.Write([]byte("test data for random access"))
	f.Close()

	// Inject read error for this specific path
	fs.InjectReadError(filePath)

	// Try to open for random access - should fail
	_, err := fs.OpenRandomAccess(filePath)
	if !errors.Is(err, ErrInjectedReadError) {
		t.Errorf("OpenRandomAccess with injected error: got %v, want %v", err, ErrInjectedReadError)
	}

	// Clear error and try again
	fs.ClearErrors()
	raFile, err := fs.OpenRandomAccess(filePath)
	if err != nil {
		t.Fatalf("OpenRandomAccess after ClearErrors failed: %v", err)
	}
	raFile.Close()
}

// TestFaultInjectionRenameFilesystemInactive tests Rename when filesystem is inactive
func TestFaultInjectionRenameFilesystemInactive(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file
	filePath := filepath.Join(dir, "original.txt")
	newPath := filepath.Join(dir, "renamed.txt")
	f, _ := fs.Create(filePath)
	f.Close()

	// Deactivate filesystem
	fs.SetFilesystemActive(false)

	// Try to rename - should fail
	err := fs.Rename(filePath, newPath)
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Rename with inactive FS: got %v, want %v", err, ErrInjectedWriteError)
	}

	// Reactivate and try again
	fs.SetFilesystemActive(true)
	err = fs.Rename(filePath, newPath)
	if err != nil {
		t.Fatalf("Rename after reactivation failed: %v", err)
	}
}

// TestFaultInjectionWriteWithError tests Write with injected error
func TestFaultInjectionWriteWithError(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file
	filePath := filepath.Join(dir, "write_test.txt")
	f, err := fs.Create(filePath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write successfully first
	n, err := f.Write([]byte("initial data"))
	if err != nil {
		t.Fatalf("initial Write failed: %v", err)
	}
	if n != 12 {
		t.Errorf("Write returned %d, want 12", n)
	}

	// Inject write error (for all paths)
	fs.InjectWriteError("")

	// Write should fail now
	_, err = f.Write([]byte("more data"))
	if !errors.Is(err, ErrInjectedWriteError) {
		t.Errorf("Write with injected error: got %v, want %v", err, ErrInjectedWriteError)
	}

	f.Close()
}

// TestFaultInjectionSyncWithError tests Sync with injected error
func TestFaultInjectionSyncWithError(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file
	filePath := filepath.Join(dir, "sync_test.txt")
	f, _ := fs.Create(filePath)
	f.Write([]byte("data to sync"))

	// Inject sync error
	fs.InjectSyncError()

	// Sync should fail
	err := f.Sync()
	if !errors.Is(err, ErrInjectedSyncError) {
		t.Errorf("Sync with injected error: got %v, want %v", err, ErrInjectedSyncError)
	}

	// Clear errors and sync should work
	fs.ClearErrors()
	err = f.Sync()
	if err != nil {
		t.Errorf("Sync after ClearErrors failed: %v", err)
	}

	f.Close()
}

// TestDefaultFSLock tests the Lock functionality
func TestDefaultFSLock(t *testing.T) {
	dir := t.TempDir()
	fs := Default()

	lockPath := filepath.Join(dir, "LOCK")

	// Create the lock file first
	f, _ := os.Create(lockPath)
	f.Close()

	// Lock the file
	lock, err := fs.Lock(lockPath)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Close (unlock)
	err = lock.Close()
	if err != nil {
		t.Errorf("Close (unlock) failed: %v", err)
	}
}

// TestDefaultFSStat tests Stat functionality
func TestDefaultFSStat(t *testing.T) {
	dir := t.TempDir()
	fs := Default()

	// Create a file
	filePath := filepath.Join(dir, "stat_test.txt")
	f, _ := os.Create(filePath)
	f.Write([]byte("test content"))
	f.Close()

	// Stat the file
	info, err := fs.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size() != 12 {
		t.Errorf("Stat size = %d, want 12", info.Size())
	}

	// Stat non-existent file
	_, err = fs.Stat(filepath.Join(dir, "nonexistent"))
	if err == nil {
		t.Error("Stat on non-existent file should fail")
	}
}

// TestDefaultFSListDir tests ListDir functionality
func TestDefaultFSListDir(t *testing.T) {
	dir := t.TempDir()
	fs := Default()

	// Create some files
	for i := range 3 {
		f, _ := os.Create(filepath.Join(dir, string(rune('a'+i))+".txt"))
		f.Close()
	}

	// List directory
	entries, err := fs.ListDir(dir)
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("ListDir returned %d entries, want 3", len(entries))
	}
}

// TestFaultInjectionFileAppend tests the Append method on FaultInjectionFile
func TestFaultInjectionFileAppend(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file
	filePath := filepath.Join(dir, "append_test.txt")
	f, err := fs.Create(filePath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some initial data
	_, err = f.Write([]byte("initial"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Append data - Append returns only error
	err = f.Append([]byte(" appended"))
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}

	f.Close()
}

// TestFaultInjectionFileSize tests the Size method on FaultInjectionFile
func TestFaultInjectionFileSize(t *testing.T) {
	dir := t.TempDir()
	baseFS := Default()
	fs := NewFaultInjectionFS(baseFS)

	// Create a file with content
	filePath := filepath.Join(dir, "size_test.txt")
	f, err := fs.Create(filePath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	_, err = f.Write([]byte("test data"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Get size - cast to faultWritableFile to access Size method
	fwf := f.(*faultWritableFile)
	size, err := fwf.Size()
	if err != nil {
		t.Errorf("Size failed: %v", err)
	}
	if size != 9 {
		t.Errorf("Size = %d, want 9", size)
	}

	f.Close()
}

// TestGoroutineFaultManagerGlobalErrorRates tests the global error rate functions
func TestGoroutineFaultManagerGlobalErrorRates(t *testing.T) {
	manager := NewGoroutineFaultManager()

	// Test SetGlobalWriteErrorRate
	manager.SetGlobalWriteErrorRate(2) // 1 in 2 chance
	// Can't easily test the actual rate, but at least cover the function

	// Test SetGlobalSyncErrorRate
	manager.SetGlobalSyncErrorRate(2) // 1 in 2 chance
	// Can't easily test the actual rate, but at least cover the function

	// Reset rates
	manager.SetGlobalWriteErrorRate(0)
	manager.SetGlobalSyncErrorRate(0)
}
