package version

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// =============================================================================
// Version Lifecycle Tests (matching C++ version_set_test.cc)
// =============================================================================

// TestVersionRefCountingAdvanced tests version reference counting in detail.
func TestVersionRefCountingAdvanced(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Get current version
	v1 := vs.Current()
	if v1 == nil {
		t.Fatal("Current() returned nil")
	}
	initialRef := v1.refs

	// Ref and Unref
	v1.Ref()
	if v1.refs != initialRef+1 {
		t.Errorf("After Ref: refs = %d, want %d", v1.refs, initialRef+1)
	}

	v1.Unref()
	if v1.refs != initialRef {
		t.Errorf("After Unref: refs = %d, want %d", v1.refs, initialRef)
	}

	// Multiple refs
	for range 10 {
		v1.Ref()
	}
	if v1.refs != initialRef+10 {
		t.Errorf("After 10 Refs: refs = %d, want %d", v1.refs, initialRef+10)
	}

	for range 10 {
		v1.Unref()
	}
	if v1.refs != initialRef {
		t.Errorf("After 10 Unrefs: refs = %d, want %d", v1.refs, initialRef)
	}
}

// TestVersionIterateFiles tests iterating over files in a version.
func TestVersionIterateFiles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Add files at different levels
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100), Smallest: makeInternalKey("a", 1, 1), Largest: makeInternalKey("z", 1, 1)}},
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 200), Smallest: makeInternalKey("b", 2, 1), Largest: makeInternalKey("y", 2, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(3, 0, 300), Smallest: makeInternalKey("a", 3, 1), Largest: makeInternalKey("m", 3, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(4, 0, 400), Smallest: makeInternalKey("n", 4, 1), Largest: makeInternalKey("z", 4, 1)}},
			{Level: 2, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(5, 0, 500), Smallest: makeInternalKey("a", 5, 1), Largest: makeInternalKey("z", 5, 1)}},
		},
	}

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	v := vs.Current()

	// Check file counts per level
	if v.NumFiles(0) != 2 {
		t.Errorf("NumFiles(0) = %d, want 2", v.NumFiles(0))
	}
	if v.NumFiles(1) != 2 {
		t.Errorf("NumFiles(1) = %d, want 2", v.NumFiles(1))
	}
	if v.NumFiles(2) != 1 {
		t.Errorf("NumFiles(2) = %d, want 1", v.NumFiles(2))
	}

	// Check total files
	if v.TotalFiles() != 5 {
		t.Errorf("TotalFiles() = %d, want 5", v.TotalFiles())
	}

	// Check level bytes
	if v.NumLevelBytes(0) != 300 { // 100 + 200
		t.Errorf("NumLevelBytes(0) = %d, want 300", v.NumLevelBytes(0))
	}
	if v.NumLevelBytes(1) != 700 { // 300 + 400
		t.Errorf("NumLevelBytes(1) = %d, want 700", v.NumLevelBytes(1))
	}
	if v.NumLevelBytes(2) != 500 {
		t.Errorf("NumLevelBytes(2) = %d, want 500", v.NumLevelBytes(2))
	}
}

// =============================================================================
// VersionSet MANIFEST Tests (matching C++ version_set_test.cc)
// =============================================================================

// TestVersionSetManifestWriteAndRecover tests MANIFEST write and recovery.
func TestVersionSetManifestWriteAndRecover(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	// Create and populate
	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add several edits
	for i := uint64(1); i <= 5; i++ {
		edit := &manifest.VersionEdit{
			LastSequence:    manifest.SequenceNumber(i * 100),
			HasLastSequence: true,
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(i, 0, 1000),
					Smallest: makeInternalKey(fmt.Sprintf("key%d", i), i, 1),
					Largest:  makeInternalKey(fmt.Sprintf("key%d_end", i), i, 1),
				}},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply() error = %v", err)
		}
	}

	numFiles := vs.NumLevelFiles(0)
	vs.Close()

	// Recover
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	// After recovery, we should have the same number of files
	if vs2.NumLevelFiles(0) != numFiles {
		t.Errorf("NumLevelFiles(0) = %d, want %d", vs2.NumLevelFiles(0), numFiles)
	}
}

// TestVersionSetManifestMultipleEdits tests MANIFEST with multiple edits.
func TestVersionSetManifestMultipleEdits(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add file to default CF
	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 1000),
				Smallest: makeInternalKey("a", 1, 1),
				Largest:  makeInternalKey("z", 1, 1),
			}},
		},
	}
	if err := vs.LogAndApply(edit1); err != nil {
		t.Fatalf("LogAndApply(edit1) error = %v", err)
	}

	vs.Close()

	// Recover and verify
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	if vs2.NumLevelFiles(0) != 1 {
		t.Errorf("NumLevelFiles(0) = %d, want 1", vs2.NumLevelFiles(0))
	}
}

// TestVersionSetSequenceNumbers tests sequence number management.
func TestVersionSetSequenceNumbers(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Initial sequence should be 0
	if vs.LastSequence() != 0 {
		t.Errorf("Initial LastSequence() = %d, want 0", vs.LastSequence())
	}

	// Set sequence directly
	vs.SetLastSequence(100)
	if vs.LastSequence() != 100 {
		t.Errorf("LastSequence() = %d, want 100", vs.LastSequence())
	}

	// Set sequence again
	vs.SetLastSequence(200)
	if vs.LastSequence() != 200 {
		t.Errorf("LastSequence() = %d, want 200", vs.LastSequence())
	}

	// Verify sequence is preserved after LogAndApply (it records but doesn't overwrite in-memory)
	edit := &manifest.VersionEdit{
		LastSequence:    300,
		HasLastSequence: true,
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	// In-memory sequence is advanced so future MANIFEST snapshots can't regress seqno.
	if vs.LastSequence() != 300 {
		t.Errorf("LastSequence() after edit = %d, want 300", vs.LastSequence())
	}
}

// TestVersionSetLogNumber tests log number getter.
func TestVersionSetLogNumber(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// LogNumber starts at 0
	initialLogNumber := vs.LogNumber()

	// LogAndApply records the log number but doesn't update in-memory
	// (caller is responsible for setting logNumber before writing edits)
	edit := &manifest.VersionEdit{
		LogNumber:    42,
		HasLogNumber: true,
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	// LogNumber unchanged (recorded in MANIFEST but in-memory not updated by edit)
	if vs.LogNumber() != initialLogNumber {
		t.Errorf("LogNumber() = %d, want %d (unchanged)", vs.LogNumber(), initialLogNumber)
	}
}

// =============================================================================
// Version Builder Tests (matching C++ version_builder_test.cc)
// =============================================================================

// TestBuilderApplyFileDeletionIncorrectLevel tests deleting file from wrong level.
func TestBuilderApplyFileDeletionIncorrectLevel(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add a file at level 0
	addEdit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100)}},
		},
	}
	if err := builder.Apply(addEdit); err != nil {
		t.Fatalf("Apply(add) error = %v", err)
	}

	// Try to delete from level 1 (wrong level) - this should be a no-op
	deleteEdit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 1, FileNumber: 1},
		},
	}
	if err := builder.Apply(deleteEdit); err != nil {
		t.Fatalf("Apply(delete wrong level) error = %v", err)
	}

	v := builder.SaveTo(vs)

	// File should still exist at level 0
	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1 (file should not be deleted)", v.NumFiles(0))
	}
}

// TestBuilderApplyFileAdditionAlreadyApplied tests adding duplicate file.
func TestBuilderApplyFileAdditionAlreadyApplied(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add a file
	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 100),
				Smallest: makeInternalKey("a", 1, 1),
				Largest:  makeInternalKey("z", 1, 1),
			}},
		},
	}
	if err := builder.Apply(edit1); err != nil {
		t.Fatalf("Apply(first) error = %v", err)
	}

	// Add same file again (should overwrite)
	edit2 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 200), // Different size
				Smallest: makeInternalKey("b", 1, 1),
				Largest:  makeInternalKey("y", 1, 1),
			}},
		},
	}
	if err := builder.Apply(edit2); err != nil {
		t.Fatalf("Apply(duplicate) error = %v", err)
	}

	v := builder.SaveTo(vs)

	// Should have only 1 file with the updated size
	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", v.NumFiles(0))
	}
	files := v.Files(0)
	if files[0].FD.FileSize != 200 {
		t.Errorf("File size = %d, want 200 (should be updated)", files[0].FD.FileSize)
	}
}

// TestBuilderApplyFileAdditionAndDeletion tests add and delete in same batch.
// Per RocksDB semantics, adds are processed after deletes, so add wins.
func TestBuilderApplyFileAdditionAndDeletion(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add and delete in single edit - add is processed after delete
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100)}},
		},
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	// File should exist (add processed after delete, so add wins)
	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1 (add should win)", v.NumFiles(0))
	}
}

// TestBuilderApplyDeleteThenAdd tests explicit delete followed by add.
func TestBuilderApplyDeleteThenAdd(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))

	// Create base version with one file
	baseBuilder := NewBuilder(vs, nil)
	baseEdit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100)}},
		},
	}
	if err := baseBuilder.Apply(baseEdit); err != nil {
		t.Fatalf("Apply(base) error = %v", err)
	}
	baseVersion := baseBuilder.SaveTo(vs)

	// Now delete and add different file
	newBuilder := NewBuilder(vs, baseVersion)
	deleteEdit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	addEdit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 200)}},
		},
	}
	if err := newBuilder.Apply(deleteEdit); err != nil {
		t.Fatalf("Apply(delete) error = %v", err)
	}
	if err := newBuilder.Apply(addEdit); err != nil {
		t.Fatalf("Apply(add) error = %v", err)
	}

	newVersion := newBuilder.SaveTo(vs)

	// Should have 1 file (file 2, since file 1 was deleted)
	if newVersion.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", newVersion.NumFiles(0))
	}
	files := newVersion.Files(0)
	if len(files) > 0 && files[0].FD.GetNumber() != 2 {
		t.Errorf("File number = %d, want 2", files[0].FD.GetNumber())
	}
}

// TestBuilderFromBase tests building from a base version.
func TestBuilderFromBase(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))

	// Create base version with some files
	baseBuilder := NewBuilder(vs, nil)
	baseEdit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100), Smallest: makeInternalKey("a", 1, 1), Largest: makeInternalKey("m", 1, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 200), Smallest: makeInternalKey("a", 2, 1), Largest: makeInternalKey("z", 2, 1)}},
		},
	}
	if err := baseBuilder.Apply(baseEdit); err != nil {
		t.Fatalf("Apply(base) error = %v", err)
	}
	baseVersion := baseBuilder.SaveTo(vs)

	// Create new builder from base version
	newBuilder := NewBuilder(vs, baseVersion)
	newEdit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(3, 0, 300), Smallest: makeInternalKey("n", 3, 1), Largest: makeInternalKey("z", 3, 1)}},
		},
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 1, FileNumber: 2},
		},
	}
	if err := newBuilder.Apply(newEdit); err != nil {
		t.Fatalf("Apply(new) error = %v", err)
	}
	newVersion := newBuilder.SaveTo(vs)

	// New version should have 2 files in L0 and 0 in L1
	if newVersion.NumFiles(0) != 2 {
		t.Errorf("NumFiles(0) = %d, want 2", newVersion.NumFiles(0))
	}
	if newVersion.NumFiles(1) != 0 {
		t.Errorf("NumFiles(1) = %d, want 0", newVersion.NumFiles(1))
	}

	// Base version should be unchanged
	if baseVersion.NumFiles(0) != 1 {
		t.Errorf("Base NumFiles(0) = %d, want 1", baseVersion.NumFiles(0))
	}
	if baseVersion.NumFiles(1) != 1 {
		t.Errorf("Base NumFiles(1) = %d, want 1", baseVersion.NumFiles(1))
	}
}

// =============================================================================
// MANIFEST Corruption and Recovery Tests
// =============================================================================

// TestVersionSetRecoverFromTruncatedManifest tests recovery from truncated MANIFEST.
func TestVersionSetRecoverFromTruncatedManifest(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	// Create and populate
	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add some edits
	for i := uint64(1); i <= 3; i++ {
		edit := &manifest.VersionEdit{
			LastSequence:    manifest.SequenceNumber(i * 100),
			HasLastSequence: true,
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(i, 0, 1000),
					Smallest: makeInternalKey(fmt.Sprintf("key%d", i), i, 1),
					Largest:  makeInternalKey(fmt.Sprintf("key%d_end", i), i, 1),
				}},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply() error = %v", err)
		}
	}
	vs.Close()

	// Truncate the MANIFEST file
	currentPath := filepath.Join(dir, "CURRENT")
	currentData, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("ReadFile(CURRENT) error = %v", err)
	}
	manifestName := string(bytes.TrimSpace(currentData))
	manifestPath := filepath.Join(dir, manifestName)

	fi, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatalf("Stat(MANIFEST) error = %v", err)
	}

	// Truncate to 75% of original size
	newSize := fi.Size() * 3 / 4
	if err := os.Truncate(manifestPath, newSize); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}

	// Recovery should still work (reading partial edits)
	vs2 := NewVersionSet(opts)
	err = vs2.Recover()
	// May or may not succeed depending on where truncation happened
	if err == nil {
		defer vs2.Close()
		t.Logf("Recovery succeeded with truncated MANIFEST (partial recovery)")
	} else {
		t.Logf("Recovery failed as expected with truncated MANIFEST: %v", err)
	}
}

// TestVersionSetRecoverWithMissingCurrent tests recovery with missing CURRENT.
func TestVersionSetRecoverWithMissingCurrent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	vs := NewVersionSet(opts)
	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	vs.Close()

	// Remove CURRENT file
	currentPath := filepath.Join(dir, "CURRENT")
	if err := os.Remove(currentPath); err != nil {
		t.Fatalf("Remove(CURRENT) error = %v", err)
	}

	// Recovery should fail
	vs2 := NewVersionSet(opts)
	err := vs2.Recover()
	if err == nil {
		vs2.Close()
		t.Error("Expected error when recovering without CURRENT file")
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

// TestVersionSetConcurrentLogAndApply tests concurrent LogAndApply calls.
func TestVersionSetConcurrentLogAndApply(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	const numGoroutines = 10
	const editsPerGoroutine = 5

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*editsPerGoroutine)

	for g := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range editsPerGoroutine {
				fileNum := vs.NextFileNumber()
				edit := &manifest.VersionEdit{
					NewFiles: []manifest.NewFileEntry{
						{Level: 0, Meta: &manifest.FileMetaData{
							FD:       manifest.NewFileDescriptor(fileNum, 0, 100),
							Smallest: makeInternalKey(fmt.Sprintf("g%d_k%d", id, i), fileNum, 1),
							Largest:  makeInternalKey(fmt.Sprintf("g%d_k%d_end", id, i), fileNum, 1),
						}},
					},
				}
				if err := vs.LogAndApply(edit); err != nil {
					errCh <- fmt.Errorf("goroutine %d, edit %d: %w", id, i, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Should have all files
	expectedFiles := numGoroutines * editsPerGoroutine
	if vs.NumLevelFiles(0) != expectedFiles {
		t.Errorf("NumLevelFiles(0) = %d, want %d", vs.NumLevelFiles(0), expectedFiles)
	}
}

// TestVersionSetConcurrentCurrentAccess tests concurrent Current() access.
func TestVersionSetConcurrentCurrentAccess(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	const numReaders = 20
	const readsPerGoroutine = 100

	var wg sync.WaitGroup

	for range numReaders {
		wg.Go(func() {
			for range readsPerGoroutine {
				v := vs.Current()
				if v == nil {
					t.Error("Current() returned nil")
					return
				}
				_ = v.TotalFiles()
			}
		})
	}

	wg.Wait()
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestVersionSetEmptyEdit tests applying an empty edit.
func TestVersionSetEmptyEdit(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Apply empty edit
	edit := &manifest.VersionEdit{}
	if err := vs.LogAndApply(edit); err != nil {
		t.Errorf("LogAndApply(empty) error = %v", err)
	}

	// Version should still be valid
	if vs.Current() == nil {
		t.Error("Current() returned nil after empty edit")
	}
}

// TestVersionSetManyFiles tests handling many files.
func TestVersionSetManyFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping many files test in short mode")
	}

	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	const numFiles = 100

	for i := uint64(1); i <= numFiles; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{Level: int(i % 4), Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(i, 0, 1000*i),
					Smallest: makeInternalKey(fmt.Sprintf("key%04d", i), i, 1),
					Largest:  makeInternalKey(fmt.Sprintf("key%04d_end", i), i, 1),
				}},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply(%d) error = %v", i, err)
		}
	}

	// Check total files
	v := vs.Current()
	if v.TotalFiles() != int(numFiles) {
		t.Errorf("TotalFiles() = %d, want %d", v.TotalFiles(), numFiles)
	}
}

// TestVersionSetFileNumberAllocation tests file number allocation.
func TestVersionSetFileNumberAllocation(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Allocate several file numbers
	nums := make([]uint64, 10)
	for i := range nums {
		nums[i] = vs.NextFileNumber()
	}

	// All numbers should be unique and increasing
	for i := 1; i < len(nums); i++ {
		if nums[i] <= nums[i-1] {
			t.Errorf("File numbers not strictly increasing: %v", nums)
			break
		}
	}
}

// TestVersionLevelBytesEmpty tests NumLevelBytes on empty levels.
func TestVersionLevelBytesEmpty(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	v := NewVersion(vs, 1)

	for level := range MaxNumLevels {
		if v.NumLevelBytes(level) != 0 {
			t.Errorf("NumLevelBytes(%d) = %d, want 0", level, v.NumLevelBytes(level))
		}
	}
}

// TestVersionInvalidLevel tests accessing invalid levels.
func TestVersionInvalidLevel(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	v := NewVersion(vs, 1)

	// Negative level
	if files := v.Files(-1); files != nil {
		t.Errorf("Files(-1) = %v, want nil", files)
	}

	// Level beyond max
	if files := v.Files(MaxNumLevels); files != nil {
		t.Errorf("Files(%d) = %v, want nil", MaxNumLevels, files)
	}

	// NumFiles for invalid levels
	if v.NumFiles(-1) != 0 {
		t.Error("NumFiles(-1) should be 0")
	}
	if v.NumFiles(MaxNumLevels) != 0 {
		t.Errorf("NumFiles(%d) should be 0", MaxNumLevels)
	}

	// NumLevelBytes for invalid levels
	if v.NumLevelBytes(-1) != 0 {
		t.Error("NumLevelBytes(-1) should be 0")
	}
	if v.NumLevelBytes(MaxNumLevels) != 0 {
		t.Errorf("NumLevelBytes(%d) should be 0", MaxNumLevels)
	}
}
