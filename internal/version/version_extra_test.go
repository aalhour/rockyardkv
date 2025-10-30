package version

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// =============================================================================
// Additional Version Builder Tests (matching C++ version_builder_test.cc)
// =============================================================================

// TestBuilderApplyMultipleEdits tests applying multiple edits in sequence.
func TestBuilderApplyMultipleEdits(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// First edit: add files
	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100), Smallest: makeInternalKey("a", 1, 1), Largest: makeInternalKey("m", 1, 1)}},
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 100), Smallest: makeInternalKey("n", 2, 1), Largest: makeInternalKey("z", 2, 1)}},
		},
	}
	if err := builder.Apply(edit1); err != nil {
		t.Fatalf("Apply(edit1) error = %v", err)
	}

	// Second edit: add more files
	edit2 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(3, 0, 200), Smallest: makeInternalKey("a", 3, 1), Largest: makeInternalKey("z", 3, 1)}},
		},
	}
	if err := builder.Apply(edit2); err != nil {
		t.Fatalf("Apply(edit2) error = %v", err)
	}

	// Third edit: delete a file
	edit3 := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	if err := builder.Apply(edit3); err != nil {
		t.Fatalf("Apply(edit3) error = %v", err)
	}

	v := builder.SaveTo(vs)

	// Should have 1 file in L0 (file 2) and 1 in L1 (file 3)
	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", v.NumFiles(0))
	}
	if v.NumFiles(1) != 1 {
		t.Errorf("NumFiles(1) = %d, want 1", v.NumFiles(1))
	}
}

// TestBuilderApplyFileDeletionNotInLSMTree tests deleting non-existent file.
func TestBuilderApplyFileDeletionNotInLSMTree(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Try to delete a file that doesn't exist
	edit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 999},
		},
	}

	// This should not error - just be a no-op
	if err := builder.Apply(edit); err != nil {
		t.Errorf("Apply(delete non-existent) error = %v", err)
	}

	v := builder.SaveTo(vs)
	if v.NumFiles(0) != 0 {
		t.Errorf("NumFiles(0) = %d, want 0", v.NumFiles(0))
	}
}

// TestBuilderFileOverwrite tests overwriting a file with same number.
func TestBuilderFileOverwrite(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add a file
	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 100),
				Smallest: makeInternalKey("a", 1, 1),
				Largest:  makeInternalKey("m", 1, 1),
			}},
		},
	}
	if err := builder.Apply(edit1); err != nil {
		t.Fatalf("Apply(first) error = %v", err)
	}

	// Overwrite with same file number but different metadata
	edit2 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 500), // Different size
				Smallest: makeInternalKey("x", 1, 1),            // Different range
				Largest:  makeInternalKey("z", 1, 1),
			}},
		},
	}
	if err := builder.Apply(edit2); err != nil {
		t.Fatalf("Apply(overwrite) error = %v", err)
	}

	v := builder.SaveTo(vs)

	files := v.Files(0)
	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}

	// Should have the new metadata
	if files[0].FD.FileSize != 500 {
		t.Errorf("FileSize = %d, want 500", files[0].FD.FileSize)
	}
}

// TestBuilderFilesAtMultipleLevels tests adding files at all levels.
func TestBuilderFilesAtMultipleLevels(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add one file at each level
	var entries []manifest.NewFileEntry
	for level := range MaxNumLevels {
		entries = append(entries, manifest.NewFileEntry{
			Level: level,
			Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(uint64(level+1), 0, uint64(100*(level+1))),
				Smallest: makeInternalKey(fmt.Sprintf("level%d_a", level), uint64(level+1), 1),
				Largest:  makeInternalKey(fmt.Sprintf("level%d_z", level), uint64(level+1), 1),
			},
		})
	}

	edit := &manifest.VersionEdit{NewFiles: entries}
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	// Each level should have 1 file
	for level := range MaxNumLevels {
		if v.NumFiles(level) != 1 {
			t.Errorf("NumFiles(%d) = %d, want 1", level, v.NumFiles(level))
		}
	}

	// Total should be MaxNumLevels
	if v.TotalFiles() != MaxNumLevels {
		t.Errorf("TotalFiles() = %d, want %d", v.TotalFiles(), MaxNumLevels)
	}
}

// =============================================================================
// Version Consistency Tests
// =============================================================================

// TestVersionConsistencyAfterMultipleUpdates tests version consistency.
func TestVersionConsistencyAfterMultipleUpdates(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Apply many edits
	for i := uint64(1); i <= 10; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(i, 0, 100),
					Smallest: makeInternalKey(fmt.Sprintf("key%d", i), i, 1),
					Largest:  makeInternalKey(fmt.Sprintf("key%d_end", i), i, 1),
				}},
			},
		}
		if err := vs.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply(%d) error = %v", i, err)
		}

		// Version should be consistent
		v := vs.Current()
		if v.NumFiles(0) != int(i) {
			t.Errorf("After edit %d: NumFiles(0) = %d, want %d", i, v.NumFiles(0), i)
		}
	}
}

// TestVersionFilesOrdering tests that files are properly ordered.
func TestVersionFilesOrdering(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add files out of order (by file number for L0)
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(5, 0, 100), Smallest: makeInternalKey("a", 5, 1), Largest: makeInternalKey("z", 5, 1)}},
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 100), Smallest: makeInternalKey("b", 2, 1), Largest: makeInternalKey("y", 2, 1)}},
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(8, 0, 100), Smallest: makeInternalKey("c", 8, 1), Largest: makeInternalKey("x", 8, 1)}},
		},
	}
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)
	files := v.Files(0)

	// L0 files should be sorted by file number (oldest first)
	if len(files) != 3 {
		t.Fatalf("Expected 3 files, got %d", len(files))
	}

	if files[0].FD.GetNumber() != 2 {
		t.Errorf("First file number = %d, want 2", files[0].FD.GetNumber())
	}
	if files[1].FD.GetNumber() != 5 {
		t.Errorf("Second file number = %d, want 5", files[1].FD.GetNumber())
	}
	if files[2].FD.GetNumber() != 8 {
		t.Errorf("Third file number = %d, want 8", files[2].FD.GetNumber())
	}
}

// TestVersionL1FilesOrderedByKey tests L1+ files are ordered by key.
func TestVersionL1FilesOrderedByKey(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add files out of key order at L1
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100), Smallest: makeInternalKey("m", 1, 1), Largest: makeInternalKey("p", 1, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 100), Smallest: makeInternalKey("a", 2, 1), Largest: makeInternalKey("c", 2, 1)}},
			{Level: 1, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(3, 0, 100), Smallest: makeInternalKey("x", 3, 1), Largest: makeInternalKey("z", 3, 1)}},
		},
	}
	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)
	files := v.Files(1)

	// L1+ files should be sorted by smallest key
	if len(files) != 3 {
		t.Fatalf("Expected 3 files, got %d", len(files))
	}

	// File with smallest="a" should be first
	if files[0].FD.GetNumber() != 2 {
		t.Errorf("First file (a-c) number = %d, want 2", files[0].FD.GetNumber())
	}
	// File with smallest="m" should be second
	if files[1].FD.GetNumber() != 1 {
		t.Errorf("Second file (m-p) number = %d, want 1", files[1].FD.GetNumber())
	}
	// File with smallest="x" should be third
	if files[2].FD.GetNumber() != 3 {
		t.Errorf("Third file (x-z) number = %d, want 3", files[2].FD.GetNumber())
	}
}

// =============================================================================
// Concurrent Version Tests
// =============================================================================

// TestConcurrentVersionReads tests concurrent reads of version.
func TestConcurrentVersionReads(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer vs.Close()

	// Add some files
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 100), Smallest: makeInternalKey("a", 1, 1), Largest: makeInternalKey("z", 1, 1)}},
		},
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	const numReaders = 50
	const readsPerGoroutine = 100

	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for range numReaders {
		wg.Go(func() {
			for range readsPerGoroutine {
				v := vs.Current()
				if v == nil {
					errors <- fmt.Errorf("Current() returned nil")
					return
				}
				_ = v.NumFiles(0)
				_ = v.TotalFiles()
				_ = v.NumLevelBytes(0)
			}
		})
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestVersionSetRecoverMultipleTimes tests multiple recoveries.
func TestVersionSetRecoverMultipleTimes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	// Create initial state
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{Level: 0, Meta: &manifest.FileMetaData{
				FD:       manifest.NewFileDescriptor(1, 0, 100),
				Smallest: makeInternalKey("a", 1, 1),
				Largest:  makeInternalKey("z", 1, 1),
			}},
		},
	}
	if err := vs1.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}
	vs1.Close()

	// Recover multiple times
	for i := range 3 {
		vs := NewVersionSet(opts)
		if err := vs.Recover(); err != nil {
			t.Fatalf("Recover(%d) error = %v", i, err)
		}
		if vs.NumLevelFiles(0) != 1 {
			t.Errorf("Recover(%d): NumLevelFiles(0) = %d, want 1", i, vs.NumLevelFiles(0))
		}
		vs.Close()
	}
}

// TestVersionSetNextFileNumberPersistence tests file number persistence via edits.
func TestVersionSetNextFileNumberPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)

	// Create and add files (which persists file numbers)
	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Add files to persist file numbers in MANIFEST
	for i := uint64(1); i <= 5; i++ {
		fileNum := vs1.NextFileNumber()
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(fileNum, 0, 100),
					Smallest: makeInternalKey(fmt.Sprintf("k%d", i), fileNum, 1),
					Largest:  makeInternalKey(fmt.Sprintf("k%d_end", i), fileNum, 1),
				}},
			},
			HasNextFileNumber: true,
			NextFileNumber:    vs1.NextFileNumber(),
		}
		if err := vs1.LogAndApply(edit); err != nil {
			t.Fatalf("LogAndApply(%d) error = %v", i, err)
		}
	}

	numFilesBeforeClose := vs1.NumLevelFiles(0)
	vs1.Close()

	// Recover and check state
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	// Should have same number of files
	if vs2.NumLevelFiles(0) != numFilesBeforeClose {
		t.Errorf("NumLevelFiles(0) = %d, want %d", vs2.NumLevelFiles(0), numFilesBeforeClose)
	}
}
