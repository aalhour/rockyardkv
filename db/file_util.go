// Package db provides the main database interface and implementation.
// This file provides utility functions for file operations.
//
// Reference: RocksDB v10.7.5
//   - file/file_util.h
//   - file/file_util.cc
package db

import (
	"io"
	"os"
)

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	return dstFile.Sync()
}

// linkOrCopy tries to create a hard link, falling back to copy if not supported.
func linkOrCopy(src, dst string) error {
	// Try hard link first (most efficient)
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}

	// Fall back to copy
	return copyFile(src, dst)
}
