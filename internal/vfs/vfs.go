// Package vfs provides a virtual filesystem abstraction layer.
//
// This allows RockyardKV to:
// - Use the real OS filesystem in production
// - Use a memory filesystem for testing
// - Use a fault-injection filesystem for crash testing
//
// Reference: RocksDB v10.7.5 include/rocksdb/file_system.h
package vfs

import (
	"io"
	"os"
)

// FS is the main filesystem interface.
type FS interface {
	// Create creates a new writable file.
	// If the file already exists, it is truncated.
	Create(name string) (WritableFile, error)

	// Open opens an existing file for reading.
	Open(name string) (SequentialFile, error)

	// OpenRandomAccess opens an existing file for random access reading.
	OpenRandomAccess(name string) (RandomAccessFile, error)

	// Rename atomically renames a file.
	Rename(oldname, newname string) error

	// Remove deletes a file.
	Remove(name string) error

	// RemoveAll removes a directory and all its contents.
	RemoveAll(path string) error

	// MkdirAll creates a directory and all parent directories.
	MkdirAll(path string, perm os.FileMode) error

	// Stat returns file info.
	Stat(name string) (os.FileInfo, error)

	// Exists returns true if the file exists.
	Exists(name string) bool

	// ListDir lists files in a directory.
	ListDir(path string) ([]string, error)

	// Lock acquires an exclusive lock on a file.
	// Returns a Locker that must be closed to release the lock.
	Lock(name string) (io.Closer, error)

	// SyncDir syncs a directory to ensure metadata changes are durable.
	// This is required after file rename to ensure the rename is durable.
	// Reference: RocksDB file/filename.cc SetCurrentFile calls FsyncWithDirOptions.
	SyncDir(path string) error
}

// WritableFile is a file that can be written to.
type WritableFile interface {
	io.Writer
	io.Closer

	// Sync flushes the file contents to stable storage.
	Sync() error

	// Append appends data to the file.
	// For most implementations, this is the same as Write.
	Append(data []byte) error

	// Truncate changes the size of the file.
	Truncate(size int64) error

	// Size returns the current file size.
	Size() (int64, error)
}

// SequentialFile is a file that can be read sequentially.
type SequentialFile interface {
	io.Reader
	io.Closer

	// Skip skips n bytes.
	Skip(n int64) error
}

// RandomAccessFile is a file that can be read at any offset.
type RandomAccessFile interface {
	io.ReaderAt
	io.Closer

	// Size returns the file size.
	Size() int64
}

// osFS implements FS using the OS filesystem.
type osFS struct{}

// Default returns the default OS filesystem.
func Default() FS {
	return &osFS{}
}

func (fs *osFS) Create(name string) (WritableFile, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	return &osWritableFile{f: f}, nil
}

func (fs *osFS) Open(name string) (SequentialFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &osSequentialFile{f: f}, nil
}

func (fs *osFS) OpenRandomAccess(name string) (RandomAccessFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &osRandomAccessFile{f: f, size: info.Size()}, nil
}

func (fs *osFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (fs *osFS) Remove(name string) error {
	return os.Remove(name)
}

func (fs *osFS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (fs *osFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *osFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *osFS) Exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (fs *osFS) ListDir(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names, nil
}

func (fs *osFS) Lock(name string) (io.Closer, error) {
	return lockFile(name)
}

func (fs *osFS) SyncDir(path string) error {
	// Open directory for syncing
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// osWritableFile wraps os.File for WritableFile interface.
type osWritableFile struct {
	f *os.File
}

func (wf *osWritableFile) Write(p []byte) (int, error) {
	return wf.f.Write(p)
}

func (wf *osWritableFile) Close() error {
	return wf.f.Close()
}

func (wf *osWritableFile) Sync() error {
	return wf.f.Sync()
}

func (wf *osWritableFile) Append(data []byte) error {
	_, err := wf.f.Write(data)
	return err
}

func (wf *osWritableFile) Truncate(size int64) error {
	return wf.f.Truncate(size)
}

func (wf *osWritableFile) Size() (int64, error) {
	info, err := wf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// osSequentialFile wraps os.File for SequentialFile interface.
type osSequentialFile struct {
	f *os.File
}

func (sf *osSequentialFile) Read(p []byte) (int, error) {
	return sf.f.Read(p)
}

func (sf *osSequentialFile) Close() error {
	return sf.f.Close()
}

func (sf *osSequentialFile) Skip(n int64) error {
	_, err := sf.f.Seek(n, io.SeekCurrent)
	return err
}

// osRandomAccessFile wraps os.File for RandomAccessFile interface.
type osRandomAccessFile struct {
	f    *os.File
	size int64
}

func (rf *osRandomAccessFile) ReadAt(p []byte, off int64) (int, error) {
	return rf.f.ReadAt(p, off)
}

func (rf *osRandomAccessFile) Close() error {
	return rf.f.Close()
}

func (rf *osRandomAccessFile) Size() int64 {
	return rf.size
}
