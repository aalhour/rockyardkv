//go:build linux

// Package vfs provides Direct I/O support on Linux using O_DIRECT.
//
// Reference: RocksDB v10.7.5
//   env/fs_posix.cc - Linux uses O_DIRECT flag (lines 172-175, 223-226, 297-301)
//   env/io_posix.h - PosixRandomAccessFile, PosixSequentialFile with use_direct_io_
//   env/io_posix.cc - Alignment handling and direct I/O reads/writes

package vfs

import (
	"os"
	"syscall"
)

// directIOSupported indicates whether Direct I/O is supported on this platform.
const directIOSupported = true

// openDirectRead opens a file for reading with O_DIRECT on Linux.
func openDirectRead(name string) (*os.File, error) {
	// O_DIRECT bypasses the kernel page cache
	fd, err := syscall.Open(name, syscall.O_RDONLY|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// openDirectWrite opens a file for writing with O_DIRECT on Linux.
func openDirectWrite(name string, create bool) (*os.File, error) {
	flags := syscall.O_WRONLY | syscall.O_DIRECT
	if create {
		flags |= syscall.O_CREAT | syscall.O_TRUNC
	}
	fd, err := syscall.Open(name, flags, 0644)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// openDirectRW opens a file for read/write with O_DIRECT on Linux.
// Currently unused but available for future DirectIOFS.OpenRW implementation.
func openDirectRW(name string, create bool) (*os.File, error) { //nolint:unused // reserved for future use
	flags := syscall.O_RDWR | syscall.O_DIRECT
	if create {
		flags |= syscall.O_CREAT
	}
	fd, err := syscall.Open(name, flags, 0644)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// getBlockSize returns the filesystem block size for alignment requirements.
func getBlockSize(path string) (int, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		// Fall back to default on error (e.g., path doesn't exist yet)
		return DefaultBlockSize, nil //nolint:nilerr // intentional fallback
	}
	return int(stat.Bsize), nil
}
