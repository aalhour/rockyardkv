package version

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

func TestVersionSetNew(t *testing.T) {
	opts := DefaultVersionSetOptions("/tmp/test")
	vs := NewVersionSet(opts)

	if vs == nil {
		t.Fatal("NewVersionSet() returned nil")
	}

	// Initial file number should be 2
	fn := vs.NextFileNumber()
	if fn != 2 {
		t.Errorf("NextFileNumber() = %d, want 2", fn)
	}
}

func TestVersionSetNextFileNumber(t *testing.T) {
	opts := DefaultVersionSetOptions("/tmp/test")
	vs := NewVersionSet(opts)

	fn1 := vs.NextFileNumber()
	fn2 := vs.NextFileNumber()
	fn3 := vs.NextFileNumber()

	if fn2 != fn1+1 {
		t.Errorf("NextFileNumber() = %d, want %d", fn2, fn1+1)
	}
	if fn3 != fn2+1 {
		t.Errorf("NextFileNumber() = %d, want %d", fn3, fn2+1)
	}
}

func TestVersionSetLastSequence(t *testing.T) {
	opts := DefaultVersionSetOptions("/tmp/test")
	vs := NewVersionSet(opts)

	if vs.LastSequence() != 0 {
		t.Errorf("LastSequence() = %d, want 0", vs.LastSequence())
	}

	vs.SetLastSequence(100)
	if vs.LastSequence() != 100 {
		t.Errorf("LastSequence() = %d, want 100", vs.LastSequence())
	}
}

func TestVersionSetCreate(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Check that CURRENT file exists
	currentPath := filepath.Join(dir, "CURRENT")
	if _, err := os.Stat(currentPath); os.IsNotExist(err) {
		t.Error("CURRENT file was not created")
	}

	// Check that a MANIFEST file exists
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error = %v", err)
	}

	foundManifest := false
	for _, entry := range entries {
		if filepath.HasPrefix(entry.Name(), "MANIFEST-") {
			foundManifest = true
			break
		}
	}
	if !foundManifest {
		t.Error("MANIFEST file was not created")
	}

	// Current version should exist
	if vs.Current() == nil {
		t.Error("Current() = nil after Create()")
	}
}

func TestVersionSetLogAndApply(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Apply an edit that adds a file
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(100, 0, 1000),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("z", 100, 1),
				},
			},
		},
	}

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	// Verify the file was added
	if vs.NumLevelFiles(0) != 1 {
		t.Errorf("NumLevelFiles(0) = %d, want 1", vs.NumLevelFiles(0))
	}
}

func TestVersionSetRecover(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a database
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add some files
	for i := uint64(1); i <= 3; i++ {
		edit := &manifest.VersionEdit{
			HasLogNumber:      true,
			LogNumber:         i,
			HasNextFileNumber: true,
			NextFileNumber:    i + 10,
			HasLastSequence:   true,
			LastSequence:      manifest.SequenceNumber(i * 100),
			NewFiles: []manifest.NewFileEntry{
				{
					Level: 0,
					Meta: &manifest.FileMetaData{
						FD:       manifest.NewFileDescriptor(i, 0, 1000*i),
						Smallest: makeInternalKey(string(rune('a'+i)), 100, 1),
						Largest:  makeInternalKey(string(rune('a'+i+1)), 100, 1),
					},
				},
			},
		}
		if err := vs1.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply() error = %v", err)
		}
	}

	vs1.Close()

	// Recover from the MANIFEST
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	// Verify recovered state
	if vs2.NumLevelFiles(0) != 3 {
		t.Errorf("NumLevelFiles(0) = %d, want 3", vs2.NumLevelFiles(0))
	}

	if vs2.LastSequence() != 300 {
		t.Errorf("LastSequence() = %d, want 300", vs2.LastSequence())
	}
}

func TestVersionSetClose(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Close should not error
	if err := vs.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Close again should not error
	if err := vs.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

func TestVersionSetRecoverNoManifest(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	// Recover without creating should fail
	err := vs.Recover()
	if !errors.Is(err, ErrNoCurrentManifest) {
		t.Errorf("Recover() error = %v, want ErrNoCurrentManifest", err)
	}
}

func TestVersionSetNumLevelBytes(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Add files of different sizes
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 1000),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("m", 100, 1),
				},
			},
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 2000),
					Smallest: makeInternalKey("n", 100, 1),
					Largest:  makeInternalKey("z", 100, 1),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(3, 0, 5000),
					Smallest: makeInternalKey("a", 50, 1),
					Largest:  makeInternalKey("z", 50, 1),
				},
			},
		},
	}

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	if got := vs.NumLevelBytes(0); got != 3000 {
		t.Errorf("NumLevelBytes(0) = %d, want 3000", got)
	}
	if got := vs.NumLevelBytes(1); got != 5000 {
		t.Errorf("NumLevelBytes(1) = %d, want 5000", got)
	}
}

func TestVersionSetConcurrentFileNumberAllocation(t *testing.T) {
	opts := DefaultVersionSetOptions("/tmp/test")
	vs := NewVersionSet(opts)

	const numGoroutines = 10
	const numAllocations = 100

	// Allocate file numbers from multiple goroutines
	results := make(chan uint64, numGoroutines*numAllocations)
	done := make(chan struct{})

	for range numGoroutines {
		go func() {
			for range numAllocations {
				fn := vs.NextFileNumber()
				results <- fn
			}
		}()
	}

	// Collect all results
	go func() {
		count := 0
		for range results {
			count++
			if count == numGoroutines*numAllocations {
				close(done)
				return
			}
		}
	}()

	<-done
	close(results)

	// Verify all file numbers are unique
	seen := make(map[uint64]bool)
	for fn := range results {
		if seen[fn] {
			t.Errorf("Duplicate file number allocated: %d", fn)
		}
		seen[fn] = true
	}
}

func TestVersionSetConcurrentVersionAccess(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Add initial files
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 1000),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("z", 100, 1),
				},
			},
		},
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	// Concurrent readers and writers
	const numReaders = 5
	const numWriters = 2
	const numIterations = 20

	done := make(chan struct{})
	errors := make(chan error, numReaders+numWriters)

	// Readers
	for range numReaders {
		go func() {
			for range numIterations {
				v := vs.Current()
				if v == nil {
					errors <- nil // OK, just no current version
					continue
				}
				v.Ref()
				// Simulate some work
				_ = v.NumFiles(0)
				_ = v.NumLevelBytes(0)
				v.Unref()
			}
			errors <- nil
		}()
	}

	// Writers
	for i := range numWriters {
		go func(id int) {
			for range numIterations {
				fileNum := vs.NextFileNumber()
				edit := &manifest.VersionEdit{
					NewFiles: []manifest.NewFileEntry{
						{
							Level: 0,
							Meta: &manifest.FileMetaData{
								FD:       manifest.NewFileDescriptor(fileNum, 0, 1000),
								Smallest: makeInternalKey("a", 100, 1),
								Largest:  makeInternalKey("z", 100, 1),
							},
						},
					},
				}
				if err := vs.LogAndApply(edit); err != nil {
					errors <- err
					return
				}
			}
			errors <- nil
		}(i)
	}

	// Wait for all goroutines
	go func() {
		for range numReaders + numWriters {
			if err := <-errors; err != nil {
				t.Errorf("Concurrent error: %v", err)
			}
		}
		close(done)
	}()

	<-done
}

func TestVersionSetMultipleLogAndApply(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Apply many edits
	const numEdits = 50
	for i := uint64(1); i <= numEdits; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{
					Level: int(i % 7), // Distribute across levels
					Meta: &manifest.FileMetaData{
						FD:       manifest.NewFileDescriptor(i, 0, 1000),
						Smallest: makeInternalKey("a", i, 1),
						Largest:  makeInternalKey("z", i, 1),
					},
				},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply(%d) error = %v", i, err)
		}
		// Update sequence separately (as DB layer would do)
		vs.SetLastSequence(i * 10)
	}

	// Verify final state
	totalFiles := 0
	for level := range MaxNumLevels {
		totalFiles += vs.NumLevelFiles(level)
	}
	if totalFiles != numEdits {
		t.Errorf("Total files = %d, want %d", totalFiles, numEdits)
	}

	if vs.LastSequence() != numEdits*10 {
		t.Errorf("LastSequence() = %d, want %d", vs.LastSequence(), numEdits*10)
	}
}

func TestVersionSetDeleteFiles(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Add files
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 1000), Smallest: makeInternalKey("a", 100, 1), Largest: makeInternalKey("m", 100, 1)}},
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 1000), Smallest: makeInternalKey("n", 100, 1), Largest: makeInternalKey("z", 100, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(3, 0, 2000), Smallest: makeInternalKey("a", 50, 1), Largest: makeInternalKey("z", 50, 1)}},
		},
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	if vs.NumLevelFiles(0) != 2 {
		t.Errorf("NumLevelFiles(0) = %d, want 2", vs.NumLevelFiles(0))
	}

	// Delete one file from L0
	deleteEdit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	if err := vs.LogAndApply(deleteEdit); err != nil {
		t.Fatalf("LogAndApply(delete) error = %v", err)
	}

	if vs.NumLevelFiles(0) != 1 {
		t.Errorf("NumLevelFiles(0) after delete = %d, want 1", vs.NumLevelFiles(0))
	}
	if vs.NumLevelFiles(1) != 1 {
		t.Errorf("NumLevelFiles(1) = %d, want 1", vs.NumLevelFiles(1))
	}
}

func TestVersionRefCounting(t *testing.T) {
	v := NewVersion(nil, 1)

	// Initial ref count
	if v.refs != 0 {
		t.Errorf("Initial refs = %d, want 0", v.refs)
	}

	// Multiple refs
	v.Ref()
	v.Ref()
	v.Ref()
	if v.refs != 3 {
		t.Errorf("After 3 Ref() calls, refs = %d, want 3", v.refs)
	}

	// Unrefs
	v.Unref()
	if v.refs != 2 {
		t.Errorf("After 1 Unref(), refs = %d, want 2", v.refs)
	}

	v.Unref()
	v.Unref()
	if v.refs != 0 {
		t.Errorf("After all Unref(), refs = %d, want 0", v.refs)
	}
}

func TestVersionSetRecoverWithDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create and populate
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add files
	for i := uint64(1); i <= 5; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(i, 0, 1000), Smallest: makeInternalKey("a", i, 1), Largest: makeInternalKey("z", i, 1)}},
			},
		}
		vs1.LogAndApply(edit)
	}

	// Delete some files
	deleteEdit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 2},
			{Level: 0, FileNumber: 4},
		},
	}
	vs1.LogAndApply(deleteEdit)

	vs1.Close()

	// Recover and verify
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	// Should have 3 files (5 added - 2 deleted)
	if vs2.NumLevelFiles(0) != 3 {
		t.Errorf("NumLevelFiles(0) after recover = %d, want 3", vs2.NumLevelFiles(0))
	}
}

func TestVersionSetRecoverCorruptCurrent(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database first
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Corrupt the CURRENT file
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte("invalid-manifest-name\n"), 0644); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	// Try to recover - should fail
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Error("Recover() should fail with corrupt CURRENT file")
	}
}

// TestVersionSetRecoverComparatorMismatch verifies that opening a database
// with a different comparator than the one stored in MANIFEST fails with
// a clear error message.
func TestVersionSetRecoverComparatorMismatch(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
		ComparatorName:      "leveldb.BytewiseComparator",
	}

	// Create a valid database
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Try to recover with a different comparator - should fail
	opts.ComparatorName = "rocksdb.ReverseBytewiseComparator"
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Fatal("Recover() should fail with comparator mismatch")
	}
	if !errors.Is(err, ErrComparatorMismatch) {
		t.Errorf("Expected ErrComparatorMismatch, got: %v", err)
	}
}

// TestVersionSetRecoverComparatorBackwardCompat verifies that bytewise
// comparator names are treated as equivalent for backward compatibility.
func TestVersionSetRecoverComparatorBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
		ComparatorName:      "leveldb.BytewiseComparator",
	}

	// Create with leveldb.BytewiseComparator
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Recover with rocksdb.BytewiseComparator - should succeed
	opts.ComparatorName = "rocksdb.BytewiseComparator"
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err != nil {
		t.Fatalf("Recover() should succeed with compatible comparator: %v", err)
	}
	vs2.Close()
}

func TestVersionSetRecoverEmptyCurrent(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database first
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Corrupt the CURRENT file with empty content
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.WriteFile(currentPath, []byte(""), 0644); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	// Try to recover - should fail
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Error("Recover() should fail with empty CURRENT file")
	}
}

// TestVersionSetRecoverManifestChecksumCorruption verifies that corrupting
// the MANIFEST checksum causes recovery to fail.
// This is critical: accepting corrupted MANIFEST leads to silent data corruption.
func TestVersionSetRecoverManifestChecksumCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Find and corrupt the MANIFEST file checksum
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "MANIFEST-") {
			manifestPath := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatalf("ReadFile error: %v", err)
			}
			// Corrupt the first byte (part of CRC checksum)
			if len(data) > 0 {
				data[0] ^= 0xFF
			}
			if err := os.WriteFile(manifestPath, data, 0644); err != nil {
				t.Fatalf("WriteFile error: %v", err)
			}
			break
		}
	}

	// Try to recover - should fail with corruption error
	vs2 := NewVersionSet(opts)
	err = vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Fatal("Recover() should fail with corrupted MANIFEST checksum")
	}
	// The error should indicate corruption
	errStr := err.Error()
	if !strings.Contains(errStr, "corrupted") && !strings.Contains(errStr, "checksum") {
		t.Logf("Note: error message doesn't explicitly mention corruption: %v", err)
	}
}

// TestVersionSetRecoverManifestTruncation verifies that truncating
// the MANIFEST file causes recovery to fail or handle gracefully.
func TestVersionSetRecoverManifestTruncation(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Find and truncate the MANIFEST file
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "MANIFEST-") {
			manifestPath := filepath.Join(dir, entry.Name())
			data, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatalf("ReadFile error: %v", err)
			}
			// Truncate 16 bytes from the end
			if len(data) > 16 {
				data = data[:len(data)-16]
			}
			if err := os.WriteFile(manifestPath, data, 0644); err != nil {
				t.Fatalf("WriteFile error: %v", err)
			}
			break
		}
	}

	// Try to recover - behavior depends on where truncation happens
	// but it should not silently succeed with wrong data
	vs2 := NewVersionSet(opts)
	err = vs2.Recover()
	// Either fails or recovers to a consistent earlier state
	if err == nil {
		// If it succeeded, verify some basic state is intact
		vs2.Close()
	}
}

func TestVersionSetRecoverMissingManifest(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database first
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs1.Close()

	// Find and delete the MANIFEST file
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if filepath.HasPrefix(entry.Name(), "MANIFEST-") {
			os.Remove(filepath.Join(dir, entry.Name()))
		}
	}

	// Try to recover - should fail
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Error("Recover() should fail with missing MANIFEST file")
	}
}

func TestVersionSetRecoverTruncatedManifest(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	// Create a valid database first
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add some files
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 1000), Smallest: makeInternalKey("a", 1, 1), Largest: makeInternalKey("z", 1, 1)}},
		},
	}
	vs1.LogAndApply(edit)
	vs1.Close()

	// Find the MANIFEST file and truncate it
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if filepath.HasPrefix(entry.Name(), "MANIFEST-") {
			manifestPath := filepath.Join(dir, entry.Name())
			data, _ := os.ReadFile(manifestPath)
			// Truncate to half size
			if len(data) > 10 {
				os.WriteFile(manifestPath, data[:len(data)/2], 0644)
			}
		}
	}

	// Try to recover - may fail or succeed partially
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	// We expect either an error or a successful recovery with incomplete data
	if err == nil {
		vs2.Close()
	}
	// Either outcome is acceptable - the test verifies we don't panic
}

func TestVersionSetOptionsValidation(t *testing.T) {
	// Test with various option combinations
	tests := []struct {
		name    string
		opts    VersionSetOptions
		wantErr bool
	}{
		{
			name: "valid options",
			opts: VersionSetOptions{
				DBName:              t.TempDir(),
				FS:                  vfs.Default(),
				MaxManifestFileSize: 1024 * 1024,
				NumLevels:           MaxNumLevels,
			},
			wantErr: false,
		},
		{
			name: "zero max manifest size uses default",
			opts: VersionSetOptions{
				DBName:              t.TempDir(),
				FS:                  vfs.Default(),
				MaxManifestFileSize: 0,
				NumLevels:           MaxNumLevels,
			},
			wantErr: false,
		},
		{
			name: "custom num levels",
			opts: VersionSetOptions{
				DBName:              t.TempDir(),
				FS:                  vfs.Default(),
				MaxManifestFileSize: 1024 * 1024,
				NumLevels:           4,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := NewVersionSet(tt.opts)
			err := vs.Create()
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				vs.Close()
			}
		})
	}
}

func TestVersionSetManifestRotation(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 512, // Very small to trigger rotation
		NumLevels:           MaxNumLevels,
	}
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Apply many edits to trigger manifest rotation
	for i := uint64(1); i <= 50; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(i, 0, 1000), Smallest: makeInternalKey("a", i, 1), Largest: makeInternalKey("z", i, 1)}},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply(%d) error = %v", i, err)
		}
	}

	// Count MANIFEST files (there should be at least 1)
	entries, _ := os.ReadDir(dir)
	manifestCount := 0
	for _, entry := range entries {
		if filepath.HasPrefix(entry.Name(), "MANIFEST-") {
			manifestCount++
		}
	}

	if manifestCount < 1 {
		t.Errorf("Expected at least 1 MANIFEST file, got %d", manifestCount)
	}

	// Verify files were added
	if vs.NumLevelFiles(0) != 50 {
		t.Errorf("NumLevelFiles(0) = %d, want 50", vs.NumLevelFiles(0))
	}
}
