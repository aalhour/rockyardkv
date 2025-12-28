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
type FaultInjectionFS struct {
	base FS

	mu sync.RWMutex

	// Per-file state tracking
	fileState map[string]*fileState

	// Error injection flags
	injectReadError  bool
	injectWriteError bool
	injectSyncError  bool
	readErrorPath    string
	writeErrorPath   string

	// Whether to drop unsynced data on "crash"
	filesystemActive bool
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
		filesystemActive: true,
	}
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

// OpenAppend opens an existing file for appending with fault injection tracking.
func (fs *FaultInjectionFS) OpenAppend(name string) (WritableFile, error) {
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

	baseFile, err := fs.base.OpenAppend(name)
	if err != nil {
		return nil, err
	}

	absPath, _ := filepath.Abs(name)
	size, _ := baseFile.Size()

	fs.mu.Lock()
	// Existing file: directory entry already exists, so mark as dirSynced.
	fs.fileState[absPath] = &fileState{
		pos:       size,
		syncedPos: size,
		dirSynced: true,
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

	// Update file state tracking
	fs.mu.Lock()
	absOld, _ := filepath.Abs(oldname)
	absNew, _ := filepath.Abs(newname)
	if state, ok := fs.fileState[absOld]; ok {
		fs.fileState[absNew] = state
		delete(fs.fileState, absOld)
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
	f.fs.mu.RUnlock()

	err := f.base.Sync()
	if err != nil {
		return err
	}

	// Mark data as synced
	f.fs.mu.Lock()
	if state, ok := f.fs.fileState[f.path]; ok {
		state.syncedPos = state.pos
		state.unsyncedData = nil
	}
	f.fs.mu.Unlock()

	return nil
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
// In RocksDB, this is important for durability of file creation.
func (fs *FaultInjectionFS) SyncDir(path string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Mark all files in this directory as having their directory synced
	absPath, _ := filepath.Abs(path)
	for filePath, state := range fs.fileState {
		fileDir := filepath.Dir(filePath)
		if fileDir == absPath {
			state.dirSynced = true
		}
	}
	return nil
}
