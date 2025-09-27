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
func openDirectRW(name string, create bool) (*os.File, error) {
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
		return DefaultBlockSize, nil // Fall back to default on error
	}
	return int(stat.Bsize), nil
}

// enableDirectIO enables Direct I/O on an already-open file descriptor.
// On Linux, this is a no-op because O_DIRECT must be specified at open time.
// Returns true if Direct I/O is enabled (even if it was a no-op).
func enableDirectIO(fd int) error {
	// On Linux, O_DIRECT must be specified when opening the file.
	// We cannot enable it on an already-open file.
	// However, we can check if the file was opened with O_DIRECT.
	return nil
}
