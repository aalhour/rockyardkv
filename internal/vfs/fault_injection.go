// Package vfs provides filesystem abstractions including fault injection for testing.
//
// FaultInjectionFS wraps a real filesystem and allows injecting errors
// and simulating crashes for testing recovery code.
//
// Reference: RocksDB v10.7.5
//   - utilities/fault_injection_fs.h
//   - utilities/fault_injection_fs.cc
package vfs

import (
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	// ErrInjectedReadError is returned when a read error is injected.
	ErrInjectedReadError = errors.New("vfs: injected read error")

	// ErrInjectedWriteError is returned when a write error is injected.
	ErrInjectedWriteError = errors.New("vfs: injected write error")

	// ErrInjectedSyncError is returned when a sync error is injected.
	ErrInjectedSyncError = errors.New("vfs: injected sync error")
)

// FaultInjectionFS wraps an FS and allows injecting errors.
// It tracks unsynced data per file to simulate data loss on crash.
//
// Directory entry durability: Entries created by Rename are not durable
// until SyncDir is called on the parent directory. On simulated crash,
// pending renames (without dir sync) are reverted.
type FaultInjectionFS struct {
	base FS

	mu sync.RWMutex

	// Per-file state tracking
	fileState map[string]*fileState

	// Pending renames that are not yet durable (no SyncDir after rename).
	// Maps new path -> old path (empty string if file was created, not renamed).
	pendingRenames map[string]string

	// Error injection flags
	injectReadError  bool
	injectWriteError bool
	injectSyncError  bool
	readErrorPath    string
	writeErrorPath   string

	// Whether to drop unsynced data on "crash"
	filesystemActive bool

	// SyncDir lie mode: SyncDir() returns success but does not make renames durable.
	// This simulates filesystems that report directory fsync success but still lose
	// directory entries on crash. Use for testing "DB must recover to older consistent
	// state or fail loud" behavior under N05 fault models.
	syncDirLieMode bool

	// File sync lie mode: Sync() returns success but does not mark data as synced.
	// On crash, unsynced data is lost. This simulates filesystems that lie about
	// fsync completion. Use with fileSyncLiePattern to target specific file types.
	fileSyncLieMode    bool
	fileSyncLiePattern string // If set, only lie for files matching this pattern (e.g., ".log", "MANIFEST", ".sst")

	// Rename anomaly modes: Simulate real-world filesystem behaviors where renames
	// are non-atomic or have unexpected outcomes after power loss.
	//
	// renameDoubleNameMode: Rename succeeds but both old and new names exist after crash.
	// This simulates a filesystem where the new directory entry is created but the old
	// entry isn't removed before crash.
	renameDoubleNameMode    bool
	renameDoubleNamePattern string // If set, only apply to files matching this pattern

	// renameNeitherNameMode: Rename succeeds but neither name exists after crash.
	// This simulates a filesystem where both directory entries are lost.
	renameNeitherNameMode    bool
	renameNeitherNamePattern string // If set, only apply to files matching this pattern
}

// fileState tracks the sync state of a file.
type fileState struct {
	pos          int64  // Current file position
	syncedPos    int64  // Position up to which data is synced
	unsyncedData []byte // Data written but not synced
	dirSynced    bool   // Whether parent directory is synced
}

// NewFaultInjectionFS creates a new fault-injecting filesystem wrapper.
func NewFaultInjectionFS(base FS) *FaultInjectionFS {
	return &FaultInjectionFS{
		base:             base,
		fileState:        make(map[string]*fileState),
		pendingRenames:   make(map[string]string),
		filesystemActive: true,
	}
}

// trackPendingRename records a rename that needs SyncDir to become durable.
// Caller must hold fs.mu.
func (fs *FaultInjectionFS) trackPendingRename(oldPath, newPath string) {
	fs.pendingRenames[newPath] = oldPath
}

// SetFilesystemActive enables or disables the filesystem.
// When disabled, all writes fail. Used to simulate crash.
func (fs *FaultInjectionFS) SetFilesystemActive(active bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.filesystemActive = active
}

// InjectReadError sets up read error injection for the given path.
func (fs *FaultInjectionFS) InjectReadError(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.injectReadError = true
	fs.readErrorPath = path
}

// InjectWriteError sets up write error injection for the given path.
func (fs *FaultInjectionFS) InjectWriteError(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.injectWriteError = true
	fs.writeErrorPath = path
}

// InjectSyncError sets up sync error injection.
func (fs *FaultInjectionFS) InjectSyncError() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.injectSyncError = true
}

// ClearErrors clears all error injection.
func (fs *FaultInjectionFS) ClearErrors() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.injectReadError = false
	fs.injectWriteError = false
	fs.injectSyncError = false
	fs.readErrorPath = ""
	fs.writeErrorPath = ""
}

// DropUnsyncedData simulates a crash by dropping all unsynced data.
// This truncates all files to their last synced position.
func (fs *FaultInjectionFS) DropUnsyncedData() error {
	fs.mu.Lock()
	states := make(map[string]*fileState)
	maps.Copy(states, fs.fileState)
	fs.mu.Unlock()

	for path, state := range states {
		if state.syncedPos < state.pos {
			// Truncate file to synced position
			f, err := os.OpenFile(path, os.O_RDWR, 0644)
			if err != nil {
				continue // File may not exist
			}
			_ = f.Truncate(state.syncedPos) // Best-effort truncation
			_ = f.Close()

			// Update state
			fs.mu.Lock()
			if s, ok := fs.fileState[path]; ok {
				s.pos = state.syncedPos
				s.unsyncedData = nil
			}
			fs.mu.Unlock()
		}
	}
	return nil
}

// DeleteUnsyncedFiles removes files that were created but never synced.
func (fs *FaultInjectionFS) DeleteUnsyncedFiles() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for path, state := range fs.fileState {
		if !state.dirSynced {
			// File exists but parent directory was never synced
			os.Remove(path)
			delete(fs.fileState, path)
		}
	}
	return nil
}

// GetFileState returns the tracked state for a file.
func (fs *FaultInjectionFS) GetFileState(path string) (syncedPos, currentPos int64, ok bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	state, exists := fs.fileState[path]
	if !exists {
		return 0, 0, false
	}
	return state.syncedPos, state.pos, true
}

// Create creates a new writable file with fault injection.
func (fs *FaultInjectionFS) Create(name string) (WritableFile, error) {
	fs.mu.RLock()
	if !fs.filesystemActive {
		fs.mu.RUnlock()
		return nil, ErrInjectedWriteError
	}
	if fs.injectWriteError && (fs.writeErrorPath == "" || fs.writeErrorPath == name) {
		fs.mu.RUnlock()
		return nil, ErrInjectedWriteError
	}
	fs.mu.RUnlock()

	baseFile, err := fs.base.Create(name)
	if err != nil {
		return nil, err
	}

	absPath, _ := filepath.Abs(name)

	fs.mu.Lock()
	fs.fileState[absPath] = &fileState{
		pos:       0,
		syncedPos: 0,
		dirSynced: false,
	}
	fs.mu.Unlock()

	return &faultWritableFile{
		base: baseFile,
		fs:   fs,
		path: absPath,
	}, nil
}

// Open opens an existing file for sequential reading.
func (fs *FaultInjectionFS) Open(name string) (SequentialFile, error) {
	fs.mu.RLock()
	if fs.injectReadError && (fs.readErrorPath == "" || fs.readErrorPath == name) {
		fs.mu.RUnlock()
		return nil, ErrInjectedReadError
	}
	fs.mu.RUnlock()

	return fs.base.Open(name)
}

// OpenRandomAccess opens an existing file for random access reading.
func (fs *FaultInjectionFS) OpenRandomAccess(name string) (RandomAccessFile, error) {
	fs.mu.RLock()
	if fs.injectReadError && (fs.readErrorPath == "" || fs.readErrorPath == name) {
		fs.mu.RUnlock()
		return nil, ErrInjectedReadError
	}
	fs.mu.RUnlock()

	return fs.base.OpenRandomAccess(name)
}

// Rename atomically renames a file.
// The new directory entry is NOT durable until SyncDir is called on the parent directory.
// If a crash occurs before SyncDir, the renamed file may disappear or revert to the old name.
func (fs *FaultInjectionFS) Rename(oldname, newname string) error {
	fs.mu.RLock()
	if !fs.filesystemActive {
		fs.mu.RUnlock()
		return ErrInjectedWriteError
	}
	fs.mu.RUnlock()

	err := fs.base.Rename(oldname, newname)
	if err != nil {
		return err
	}

	// Update file state tracking.
	// Mark the new path as NOT directory-synced until SyncDir is called.
	// This models the fact that directory entries created by rename are not
	// durable until the parent directory is synced.
	fs.mu.Lock()
	absOld, _ := filepath.Abs(oldname)
	absNew, _ := filepath.Abs(newname)
	if state, ok := fs.fileState[absOld]; ok {
		// Copy the state, but reset dirSynced for the new path
		// The rename creates a new directory entry that isn't durable yet
		newState := &fileState{
			pos:          state.pos,
			syncedPos:    state.syncedPos,
			unsyncedData: state.unsyncedData,
			dirSynced:    false, // new directory entry not durable
		}
		fs.fileState[absNew] = newState
		delete(fs.fileState, absOld)

		// Track pending renames for potential revert
		fs.trackPendingRename(absOld, absNew)
	} else {
		// File wasn't tracked, create new tracking entry
		fs.fileState[absNew] = &fileState{
			pos:       0,
			syncedPos: 0,
			dirSynced: false, // not durable until dir sync
		}
		fs.trackPendingRename("", absNew)
	}
	fs.mu.Unlock()

	return nil
}

// Remove deletes a file.
func (fs *FaultInjectionFS) Remove(name string) error {
	err := fs.base.Remove(name)
	if err != nil {
		return err
	}

	fs.mu.Lock()
	absPath, _ := filepath.Abs(name)
	delete(fs.fileState, absPath)
	fs.mu.Unlock()

	return nil
}

// RemoveAll removes a directory and all its contents.
func (fs *FaultInjectionFS) RemoveAll(path string) error {
	return fs.base.RemoveAll(path)
}

// MkdirAll creates a directory and all parent directories.
func (fs *FaultInjectionFS) MkdirAll(path string, perm os.FileMode) error {
	fs.mu.RLock()
	if !fs.filesystemActive {
		fs.mu.RUnlock()
		return ErrInjectedWriteError
	}
	fs.mu.RUnlock()

	return fs.base.MkdirAll(path, perm)
}

// Stat returns file info.
func (fs *FaultInjectionFS) Stat(name string) (os.FileInfo, error) {
	return fs.base.Stat(name)
}

// Exists returns true if the file exists.
func (fs *FaultInjectionFS) Exists(name string) bool {
	return fs.base.Exists(name)
}

// ListDir lists files in a directory.
func (fs *FaultInjectionFS) ListDir(path string) ([]string, error) {
	return fs.base.ListDir(path)
}

// Lock acquires an exclusive lock on a file.
func (fs *FaultInjectionFS) Lock(name string) (io.Closer, error) {
	return fs.base.Lock(name)
}

// faultWritableFile wraps WritableFile with fault injection.
type faultWritableFile struct {
	base WritableFile
	fs   *FaultInjectionFS
	path string
}

func (f *faultWritableFile) Write(p []byte) (int, error) {
	f.fs.mu.RLock()
	if !f.fs.filesystemActive {
		f.fs.mu.RUnlock()
		return 0, ErrInjectedWriteError
	}
	if f.fs.injectWriteError && (f.fs.writeErrorPath == "" || f.fs.writeErrorPath == f.path) {
		f.fs.mu.RUnlock()
		return 0, ErrInjectedWriteError
	}
	f.fs.mu.RUnlock()

	n, err := f.base.Write(p)
	if err != nil {
		return n, err
	}

	// Track unsynced data
	f.fs.mu.Lock()
	if state, ok := f.fs.fileState[f.path]; ok {
		state.pos += int64(n)
		state.unsyncedData = append(state.unsyncedData, p[:n]...)
	}
	f.fs.mu.Unlock()

	return n, nil
}

func (f *faultWritableFile) Close() error {
	return f.base.Close()
}

func (f *faultWritableFile) Sync() error {
	f.fs.mu.RLock()
	if f.fs.injectSyncError {
		f.fs.mu.RUnlock()
		return ErrInjectedSyncError
	}
	fileSyncLieMode := f.fs.fileSyncLieMode
	fileSyncLiePattern := f.fs.fileSyncLiePattern
	f.fs.mu.RUnlock()

	err := f.base.Sync()
	if err != nil {
		return err
	}

	// Check if we should lie about this sync
	shouldLie := fileSyncLieMode && f.matchesLiePattern(fileSyncLiePattern)

	if shouldLie {
		// Lie mode: return success but do NOT mark data as synced.
		// On crash, unsynced data will be lost.
		return nil
	}

	// Normal mode: mark data as synced
	f.fs.mu.Lock()
	if state, ok := f.fs.fileState[f.path]; ok {
		state.syncedPos = state.pos
		state.unsyncedData = nil
	}
	f.fs.mu.Unlock()

	return nil
}

// matchesLiePattern checks if this file matches the lie pattern.
func (f *faultWritableFile) matchesLiePattern(pattern string) bool {
	if pattern == "" {
		return true // empty pattern matches all files
	}
	// Check if filename contains the pattern
	return containsPattern(f.path, pattern)
}

// containsPattern is a simple substring check for pattern matching.
func containsPattern(path, pattern string) bool {
	for i := 0; i <= len(path)-len(pattern); i++ {
		if path[i:i+len(pattern)] == pattern {
			return true
		}
	}
	return false
}

func (f *faultWritableFile) Append(data []byte) error {
	_, err := f.Write(data)
	return err
}

func (f *faultWritableFile) Truncate(size int64) error {
	f.fs.mu.RLock()
	if !f.fs.filesystemActive {
		f.fs.mu.RUnlock()
		return ErrInjectedWriteError
	}
	f.fs.mu.RUnlock()

	err := f.base.Truncate(size)
	if err != nil {
		return err
	}

	// Update state
	f.fs.mu.Lock()
	if state, ok := f.fs.fileState[f.path]; ok {
		if size < state.syncedPos {
			state.syncedPos = size
		}
		state.pos = size
		state.unsyncedData = nil
	}
	f.fs.mu.Unlock()

	return nil
}

func (f *faultWritableFile) Size() (int64, error) {
	return f.base.Size()
}

// SyncDir marks the directory as synced.
// This is important for durability of file creation and rename.
// After SyncDir, pending renames in this directory become durable.
//
// In "lie mode" (SetSyncDirLieMode(true)), this method returns success but
// does NOT make renames durable. On simulated crash, RevertUnsyncedRenames()
// will still revert those renames. This models filesystems that lie about
// directory fsync completion.
func (fs *FaultInjectionFS) SyncDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	absPath, _ := filepath.Abs(path)

	// Mark all files in this directory as having their directory synced.
	// This is done even in lie mode (affects file state tracking).
	for filePath, state := range fs.fileState {
		fileDir := filepath.Dir(filePath)
		if fileDir == absPath {
			state.dirSynced = true
		}
	}

	// In lie mode: return success but do NOT clear pending renames.
	// This simulates a filesystem that reports fsync success but still
	// loses directory entries on crash.
	if fs.syncDirLieMode {
		return nil
	}

	// Normal mode: clear pending renames for files in this directory
	// (they are now durable)
	for newPath := range fs.pendingRenames {
		fileDir := filepath.Dir(newPath)
		if fileDir == absPath {
			delete(fs.pendingRenames, newPath)
		}
	}

	return nil
}

// RevertUnsyncedRenames simulates crash behavior for directory entry durability.
// Renames that were not followed by SyncDir are reverted:
// - If the rename had an original path, the file is renamed back
// - If the rename was from a new file (no original), the file is deleted
func (fs *FaultInjectionFS) RevertUnsyncedRenames() error {
	fs.mu.Lock()
	pendingCopy := make(map[string]string)
	maps.Copy(pendingCopy, fs.pendingRenames)
	fs.mu.Unlock()

	for newPath, oldPath := range pendingCopy {
		if oldPath == "" {
			// File was created (not renamed from existing), delete it
			if err := os.Remove(newPath); err != nil && !os.IsNotExist(err) {
				// Best effort, continue
			}
		} else {
			// File was renamed, revert to old name
			if err := os.Rename(newPath, oldPath); err != nil && !os.IsNotExist(err) {
				// Best effort, continue
			}
		}

		// Clean up tracking
		fs.mu.Lock()
		delete(fs.pendingRenames, newPath)
		if state, ok := fs.fileState[newPath]; ok {
			if oldPath != "" {
				fs.fileState[oldPath] = state
			}
			delete(fs.fileState, newPath)
		}
		fs.mu.Unlock()
	}

	return nil
}

// HasPendingRenames returns true if there are renames waiting for SyncDir.
func (fs *FaultInjectionFS) HasPendingRenames() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.pendingRenames) > 0
}

// PendingRenameCount returns the number of pending (unsynced) renames.
func (fs *FaultInjectionFS) PendingRenameCount() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.pendingRenames)
}

// SetSyncDirLieMode enables or disables SyncDir lie mode.
// When enabled, SyncDir() returns success but does NOT make renames durable.
// This simulates filesystems that lie about directory fsync completion.
// Use for testing recovery behavior under N05 fault models where the OS
// reports success but data is still lost on crash.
func (fs *FaultInjectionFS) SetSyncDirLieMode(enabled bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.syncDirLieMode = enabled
}

// IsSyncDirLieModeEnabled returns true if SyncDir lie mode is active.
func (fs *FaultInjectionFS) IsSyncDirLieModeEnabled() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.syncDirLieMode
}

// SetFileSyncLieMode enables or disables file Sync lie mode.
// When enabled, Sync() returns success but does NOT mark data as synced.
// On simulated crash (DropUnsyncedData), unsynced data is lost.
//
// Use pattern to target specific file types:
//   - "" (empty): lie for ALL files
//   - ".log": lie for WAL files only
//   - "MANIFEST": lie for MANIFEST files only
//   - ".sst": lie for SST files only
func (fs *FaultInjectionFS) SetFileSyncLieMode(enabled bool, pattern string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.fileSyncLieMode = enabled
	fs.fileSyncLiePattern = pattern
}

// IsFileSyncLieModeEnabled returns true if file Sync lie mode is active.
func (fs *FaultInjectionFS) IsFileSyncLieModeEnabled() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.fileSyncLieMode
}

// GetFileSyncLiePattern returns the current file sync lie pattern.
func (fs *FaultInjectionFS) GetFileSyncLiePattern() string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.fileSyncLiePattern
}

// SetRenameDoubleNameMode enables/disables "both names exist" rename anomaly mode.
// When enabled, Rename() records both old and new paths in pendingRenames.
// After SimulateCrash(), both paths will exist. Use pattern to target specific files.
func (fs *FaultInjectionFS) SetRenameDoubleNameMode(enabled bool, pattern string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.renameDoubleNameMode = enabled
	fs.renameDoubleNamePattern = pattern
}

// IsRenameDoubleNameModeEnabled returns true if "both names exist" mode is active.
func (fs *FaultInjectionFS) IsRenameDoubleNameModeEnabled() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.renameDoubleNameMode
}

// SetRenameNeitherNameMode enables/disables "neither name exists" rename anomaly mode.
// When enabled, after SimulateCrash(), neither old nor new path exists. Use pattern
// to target specific files.
func (fs *FaultInjectionFS) SetRenameNeitherNameMode(enabled bool, pattern string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.renameNeitherNameMode = enabled
	fs.renameNeitherNamePattern = pattern
}

// IsRenameNeitherNameModeEnabled returns true if "neither name exists" mode is active.
func (fs *FaultInjectionFS) IsRenameNeitherNameModeEnabled() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.renameNeitherNameMode
}

// SimulateCrashWithRenameAnomalies applies rename anomaly modes to the filesystem.
// Call this before DropUnsyncedData to set up the rename anomaly state.
// For double-name mode: copies new file content to old path.
// For neither-name mode: deletes both paths.
func (fs *FaultInjectionFS) SimulateCrashWithRenameAnomalies() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for newPath, oldPath := range fs.pendingRenames {
		// Check if this rename matches the anomaly patterns
		matchesDouble := fs.renameDoubleNameMode &&
			(fs.renameDoubleNamePattern == "" || strings.Contains(newPath, fs.renameDoubleNamePattern))
		matchesNeither := fs.renameNeitherNameMode &&
			(fs.renameNeitherNamePattern == "" || strings.Contains(newPath, fs.renameNeitherNamePattern))

		if matchesDouble {
			// Double-name mode: copy new file content to old path (both exist)
			newContent, err := os.ReadFile(newPath)
			if err == nil {
				// Create old path with same content
				_ = os.WriteFile(oldPath, newContent, 0644)
			}
		} else if matchesNeither {
			// Neither-name mode: delete both paths
			_ = os.Remove(oldPath)
			_ = os.Remove(newPath)
		}
	}

	return nil
}
