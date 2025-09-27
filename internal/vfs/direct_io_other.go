//go:build !linux && !darwin

// Package vfs provides a stub for Direct I/O on unsupported platforms.
//
// Reference: RocksDB v10.7.5
//   env/fs_posix.cc - Platform-specific Direct I/O handling
//
// This file provides stub implementations for platforms that don't support
// Direct I/O (e.g., Windows, OpenBSD, Solaris). Files will be opened
// normally without Direct I/O, and the UseDirectIO method will return false.

package vfs

import (
	"os"
)

// directIOSupported indicates whether Direct I/O is supported on this platform.
const directIOSupported = false

// openDirectRead opens a file for reading without Direct I/O on unsupported platforms.
func openDirectRead(name string) (*os.File, error) {
	return os.Open(name)
}

// openDirectWrite opens a file for writing without Direct I/O on unsupported platforms.
func openDirectWrite(name string, create bool) (*os.File, error) {
	if create {
		return os.Create(name)
	}
	return os.OpenFile(name, os.O_WRONLY, 0644)
}

// openDirectRW opens a file for read/write without Direct I/O on unsupported platforms.
func openDirectRW(name string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	return os.OpenFile(name, flags, 0644)
}

// getBlockSize returns the default block size on unsupported platforms.
func getBlockSize(_ string) (int, error) {
	return DefaultBlockSize, nil
}

// enableDirectIO is a no-op on unsupported platforms.
func enableDirectIO(_ int) error {
	return ErrDirectIONotSupported
}
