//go:build !windows

// lock.go implements file locking on Unix systems.
//
// Reference: RocksDB v10.7.5
//   - env/env_posix.cc (PosixEnv::LockFile)
//   - env/io_posix.cc
package vfs

import (
	"io"
	"os"
	"syscall"
)

// fileLock implements file locking on Unix systems.
type fileLock struct {
	f *os.File
}

// lockFile acquires an exclusive lock on the named file.
func lockFile(name string) (io.Closer, error) {
	// Create or open the lock file
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Try to acquire an exclusive lock
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &fileLock{f: f}, nil
}

func (l *fileLock) Close() error {
	// Release the lock (ignore error - file will be closed anyway)
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
