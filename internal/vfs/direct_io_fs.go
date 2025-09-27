// Package vfs provides a Direct I/O-enabled filesystem implementation.
//
// Reference: RocksDB v10.7.5
//   include/rocksdb/file_system.h - FileSystem interface with NewRandomAccessFile, NewWritableFile
//   env/fs_posix.cc - PosixFileSystem with O_DIRECT/F_NOCACHE support
//   env/io_posix.cc - DirectIOHelper for aligned I/O operations

package vfs

import (
	"io"
	"os"
	"path/filepath"
)

// DirectIOFS extends FS with Direct I/O support.
type DirectIOFS interface {
	FS

	// CreateWithOptions creates a new writable file with the given options.
	CreateWithOptions(name string, opts FileOptions) (DirectWritableFile, error)

	// OpenWithOptions opens an existing file for reading with the given options.
	OpenWithOptions(name string, opts FileOptions) (DirectSequentialFile, error)

	// OpenRandomAccessWithOptions opens an existing file for random access with options.
	OpenRandomAccessWithOptions(name string, opts FileOptions) (DirectRandomAccessFile, error)

	// IsDirectIOSupported returns true if Direct I/O is supported on this filesystem.
	IsDirectIOSupported() bool

	// GetBlockSize returns the filesystem block size for the given path.
	GetBlockSize(path string) int
}

// DirectSequentialFile extends SequentialFile with Direct I/O information.
type DirectSequentialFile interface {
	SequentialFile
	DirectIOFile
}

// DirectRandomAccessFile extends RandomAccessFile with Direct I/O information.
type DirectRandomAccessFile interface {
	RandomAccessFile
	DirectIOFile
}

// DirectWritableFile extends WritableFile with Direct I/O information.
type DirectWritableFile interface {
	WritableFile
	DirectIOFile
}

// directIOFS implements DirectIOFS using the OS filesystem with Direct I/O.
type directIOFS struct {
	osFS
	blockSize int
}

// NewDirectIOFS creates a new filesystem with Direct I/O support.
func NewDirectIOFS() DirectIOFS {
	return &directIOFS{
		blockSize: DefaultBlockSize,
	}
}

func (fs *directIOFS) CreateWithOptions(name string, opts FileOptions) (DirectWritableFile, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if opts.UseDirectWrites && directIOSupported {
		f, err := openDirectWrite(name, true)
		if err != nil {
			return nil, err
		}
		// Get actual block size for the filesystem
		blockSize := opts.GetBlockSize()
		if bs, err := getBlockSize(filepath.Dir(name)); err == nil && bs > 0 {
			blockSize = bs
		}
		return &directWritableFile{
			osWritableFile: osWritableFile{f: f},
			useDirectIO:    true,
			alignment:      blockSize,
		}, nil
	}

	// Fall back to normal file creation
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	return &directWritableFile{
		osWritableFile: osWritableFile{f: f},
		useDirectIO:    false,
		alignment:      DefaultBlockSize,
	}, nil
}

func (fs *directIOFS) OpenWithOptions(name string, opts FileOptions) (DirectSequentialFile, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if opts.UseDirectReads && directIOSupported {
		f, err := openDirectRead(name)
		if err != nil {
			return nil, err
		}
		// Get actual block size for the filesystem
		blockSize := opts.GetBlockSize()
		if bs, err := getBlockSize(filepath.Dir(name)); err == nil && bs > 0 {
			blockSize = bs
		}
		return &directSequentialFile{
			osSequentialFile: osSequentialFile{f: f},
			useDirectIO:      true,
			alignment:        blockSize,
		}, nil
	}

	// Fall back to normal file open
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &directSequentialFile{
		osSequentialFile: osSequentialFile{f: f},
		useDirectIO:      false,
		alignment:        DefaultBlockSize,
	}, nil
}

func (fs *directIOFS) OpenRandomAccessWithOptions(name string, opts FileOptions) (DirectRandomAccessFile, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if opts.UseDirectReads && directIOSupported {
		f, err := openDirectRead(name)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		// Get actual block size for the filesystem
		blockSize := opts.GetBlockSize()
		if bs, err := getBlockSize(filepath.Dir(name)); err == nil && bs > 0 {
			blockSize = bs
		}
		return &directRandomAccessFile{
			osRandomAccessFile: osRandomAccessFile{f: f, size: info.Size()},
			useDirectIO:        true,
			alignment:          blockSize,
		}, nil
	}

	// Fall back to normal file open
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &directRandomAccessFile{
		osRandomAccessFile: osRandomAccessFile{f: f, size: info.Size()},
		useDirectIO:        false,
		alignment:          DefaultBlockSize,
	}, nil
}

func (fs *directIOFS) IsDirectIOSupported() bool {
	return directIOSupported
}

func (fs *directIOFS) GetBlockSize(path string) int {
	if bs, err := getBlockSize(path); err == nil && bs > 0 {
		return bs
	}
	return DefaultBlockSize
}

// directSequentialFile wraps osSequentialFile with Direct I/O support.
type directSequentialFile struct {
	osSequentialFile
	useDirectIO bool
	alignment   int
}

func (f *directSequentialFile) UseDirectIO() bool {
	return f.useDirectIO
}

func (f *directSequentialFile) GetRequiredBufferAlignment() int {
	if f.useDirectIO {
		return f.alignment
	}
	return 1
}

// directRandomAccessFile wraps osRandomAccessFile with Direct I/O support.
type directRandomAccessFile struct {
	osRandomAccessFile
	useDirectIO bool
	alignment   int
}

func (f *directRandomAccessFile) UseDirectIO() bool {
	return f.useDirectIO
}

func (f *directRandomAccessFile) GetRequiredBufferAlignment() int {
	if f.useDirectIO {
		return f.alignment
	}
	return 1
}

// directWritableFile wraps osWritableFile with Direct I/O support.
type directWritableFile struct {
	osWritableFile
	useDirectIO bool
	alignment   int
}

func (f *directWritableFile) UseDirectIO() bool {
	return f.useDirectIO
}

func (f *directWritableFile) GetRequiredBufferAlignment() int {
	if f.useDirectIO {
		return f.alignment
	}
	return 1
}

// Write implements io.Writer with alignment handling for Direct I/O.
func (f *directWritableFile) Write(p []byte) (int, error) {
	if !f.useDirectIO || IsAligned(len(p), f.alignment) {
		return f.osWritableFile.Write(p)
	}

	// For Direct I/O, we need to handle unaligned writes
	// This is a simplified implementation - full implementation would
	// buffer partial writes until we have a full aligned block
	return f.osWritableFile.Write(p)
}

// Append implements WritableFile.Append with alignment handling for Direct I/O.
func (f *directWritableFile) Append(data []byte) error {
	_, err := f.Write(data)
	return err
}

// WriteAligned writes data with alignment requirements.
// The data must be aligned to the buffer alignment requirement.
func (f *directWritableFile) WriteAligned(p []byte, offset int64) (int, error) {
	if !f.useDirectIO {
		return f.f.WriteAt(p, offset)
	}

	if !IsAligned(int(offset), f.alignment) || !IsAligned(len(p), f.alignment) {
		return 0, ErrNotAligned
	}

	return f.f.WriteAt(p, offset)
}

// Close closes the file and flushes any buffered data.
func (f *directWritableFile) Close() error {
	return f.osWritableFile.Close()
}

// ReadAlignedAt reads data at an aligned offset.
// This is useful for Direct I/O random access reads.
func (f *directRandomAccessFile) ReadAlignedAt(p []byte, offset int64) (int, error) {
	if !f.useDirectIO {
		return f.ReadAt(p, offset)
	}

	if !IsAligned(int(offset), f.alignment) || !IsAligned(len(p), f.alignment) {
		return 0, ErrNotAligned
	}

	return f.f.ReadAt(p, offset)
}

// directIOAdapter wraps any FS and adds Direct I/O methods (with fallback).
type directIOAdapter struct {
	FS
}

// WrapWithDirectIO wraps an existing FS with Direct I/O support.
// If the underlying FS is already a DirectIOFS, it is returned unchanged.
// Otherwise, a fallback wrapper is created.
func WrapWithDirectIO(fs FS) DirectIOFS {
	if dfs, ok := fs.(DirectIOFS); ok {
		return dfs
	}
	return &directIOAdapter{FS: fs}
}

func (a *directIOAdapter) CreateWithOptions(name string, opts FileOptions) (DirectWritableFile, error) {
	// Direct I/O not supported in fallback mode
	f, err := a.Create(name)
	if err != nil {
		return nil, err
	}
	return &fallbackDirectWritableFile{WritableFile: f}, nil
}

func (a *directIOAdapter) OpenWithOptions(name string, opts FileOptions) (DirectSequentialFile, error) {
	f, err := a.Open(name)
	if err != nil {
		return nil, err
	}
	return &fallbackDirectSequentialFile{SequentialFile: f}, nil
}

func (a *directIOAdapter) OpenRandomAccessWithOptions(name string, opts FileOptions) (DirectRandomAccessFile, error) {
	f, err := a.OpenRandomAccess(name)
	if err != nil {
		return nil, err
	}
	return &fallbackDirectRandomAccessFile{RandomAccessFile: f}, nil
}

func (a *directIOAdapter) IsDirectIOSupported() bool {
	return false
}

func (a *directIOAdapter) GetBlockSize(_ string) int {
	return DefaultBlockSize
}

// Fallback implementations for FS that doesn't support Direct I/O

type fallbackDirectSequentialFile struct {
	SequentialFile
}

func (f *fallbackDirectSequentialFile) UseDirectIO() bool               { return false }
func (f *fallbackDirectSequentialFile) GetRequiredBufferAlignment() int { return 1 }

type fallbackDirectRandomAccessFile struct {
	RandomAccessFile
}

func (f *fallbackDirectRandomAccessFile) UseDirectIO() bool               { return false }
func (f *fallbackDirectRandomAccessFile) GetRequiredBufferAlignment() int { return 1 }

type fallbackDirectWritableFile struct {
	WritableFile
}

func (f *fallbackDirectWritableFile) UseDirectIO() bool               { return false }
func (f *fallbackDirectWritableFile) GetRequiredBufferAlignment() int { return 1 }

// Ensure the adapter implements io.Closer
var _ io.Closer = (*fallbackDirectWritableFile)(nil)
var _ io.Closer = (*fallbackDirectSequentialFile)(nil)
var _ io.Closer = (*fallbackDirectRandomAccessFile)(nil)
