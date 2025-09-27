//go:build darwin

// Package vfs provides Direct I/O support on macOS using F_NOCACHE.
//
// Reference: RocksDB v10.7.5
//   env/fs_posix.cc - macOS uses fcntl(fd, F_NOCACHE, 1) (lines 191-196, 261-266, 336-340)
//   env/io_posix.h - PosixRandomAccessFile, PosixSequentialFile with use_direct_io_
//   env/io_posix.cc - Alignment handling and direct I/O reads/writes
//
// On macOS, there is no O_DIRECT flag. Instead, we use F_NOCACHE via fcntl
// to disable page caching for a file descriptor. This achieves similar
// behavior to Direct I/O on Linux.

package vfs

import (
	"os"
	"syscall"
)

// directIOSupported indicates whether Direct I/O is supported on this platform.
const directIOSupported = true

// FNocache is the macOS fcntl command to disable page cache for a file.
// This is equivalent to O_DIRECT on Linux.
// Value matches macOS sys/fcntl.h F_NOCACHE.
const FNocache = 48

// openDirectRead opens a file for reading with F_NOCACHE on macOS.
func openDirectRead(name string) (*os.File, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	// Enable F_NOCACHE to bypass the page cache
	if err := enableDirectIO(int(f.Fd())); err != nil {
		_ = f.Close()
		return nil, err
	}

	return f, nil
}

// openDirectWrite opens a file for writing with F_NOCACHE on macOS.
func openDirectWrite(name string, create bool) (*os.File, error) {
	var f *os.File
	var err error

	if create {
		f, err = os.Create(name)
	} else {
		f, err = os.OpenFile(name, os.O_WRONLY, 0644)
	}
	if err != nil {
		return nil, err
	}

	// Enable F_NOCACHE to bypass the page cache
	if err := enableDirectIO(int(f.Fd())); err != nil {
		_ = f.Close()
		return nil, err
	}

	return f, nil
}

// openDirectRW opens a file for read/write with F_NOCACHE on macOS.
// Currently unused but available for future DirectIOFS.OpenRW implementation.
func openDirectRW(name string, create bool) (*os.File, error) { //nolint:unused // reserved for future use
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}

	f, err := os.OpenFile(name, flags, 0644)
	if err != nil {
		return nil, err
	}

	// Enable F_NOCACHE to bypass the page cache
	if err := enableDirectIO(int(f.Fd())); err != nil {
		_ = f.Close()
		return nil, err
	}

	return f, nil
}

// getBlockSize returns the filesystem block size for alignment requirements.
func getBlockSize(path string) (int, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		// Fall back to default block size on error - this is not a fatal error
		// as Direct I/O can still work with the default alignment
		return DefaultBlockSize, nil //nolint:nilerr // fallback to non-direct IO on failure
	}
	return int(stat.Bsize), nil
}

// enableDirectIO enables F_NOCACHE on a file descriptor.
// This tells macOS to bypass the page cache for this file.
func enableDirectIO(fd int) error {
	// Use syscall.Syscall to call fcntl with FNocache
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(FNocache), uintptr(1))
	if errno != 0 {
		return errno
	}
	return nil
}
