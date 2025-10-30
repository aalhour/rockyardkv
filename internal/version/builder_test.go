package version

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

func TestBuilderEmpty(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	v := builder.SaveTo(vs)

	if v.TotalFiles() != 0 {
		t.Errorf("TotalFiles() = %d, want 0", v.TotalFiles())
	}
}

func TestBuilderAddFiles(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Create an edit that adds files
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 100),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("z", 100, 1),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 200),
					Smallest: makeInternalKey("b", 99, 1),
					Largest:  makeInternalKey("y", 99, 1),
				},
			},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", v.NumFiles(0))
	}
	if v.NumFiles(1) != 1 {
		t.Errorf("NumFiles(1) = %d, want 1", v.NumFiles(1))
	}
	if v.TotalFiles() != 2 {
		t.Errorf("TotalFiles() = %d, want 2", v.TotalFiles())
	}
}

func TestBuilderDeleteFiles(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))

	// Create base version with some files
	base := NewVersion(vs, 1)
	base.files[0] = []*manifest.FileMetaData{
		{
			FD:       manifest.NewFileDescriptor(1, 0, 100),
			Smallest: makeInternalKey("a", 100, 1),
			Largest:  makeInternalKey("m", 100, 1),
		},
		{
			FD:       manifest.NewFileDescriptor(2, 0, 200),
			Smallest: makeInternalKey("n", 100, 1),
			Largest:  makeInternalKey("z", 100, 1),
		},
	}

	// Create edit that deletes one file
	builder := NewBuilder(vs, base)
	edit := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	if v.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", v.NumFiles(0))
	}

	// Verify the remaining file is file 2
	files := v.Files(0)
	if files[0].FD.GetNumber() != 2 {
		t.Errorf("Remaining file number = %d, want 2", files[0].FD.GetNumber())
	}
}

func TestBuilderAddThenDelete(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add a file
	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 100),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("z", 100, 1),
				},
			},
		},
	}
	if err := builder.Apply(edit1); err != nil {
		t.Fatalf("Apply(edit1) error = %v", err)
	}

	// Delete the same file
	edit2 := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	if err := builder.Apply(edit2); err != nil {
		t.Fatalf("Apply(edit2) error = %v", err)
	}

	v := builder.SaveTo(vs)

	// File should not be present
	if v.NumFiles(0) != 0 {
		t.Errorf("NumFiles(0) = %d, want 0", v.NumFiles(0))
	}
}

func TestBuilderSortingL0(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add L0 files in arbitrary order - they should be sorted by file number (oldest first)
	// This is required because L0 files may overlap and we need to search newest first
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(3, 0, 100),
					Smallest: makeInternalKey("z", 100, 1),
					Largest:  makeInternalKey("zz", 100, 1),
				},
			},
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 100),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("m", 100, 1),
				},
			},
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 100),
					Smallest: makeInternalKey("n", 100, 1),
					Largest:  makeInternalKey("y", 100, 1),
				},
			},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	// L0 files should be sorted by file number (oldest first), not by smallest key
	files := v.Files(0)
	if len(files) != 3 {
		t.Fatalf("NumFiles(0) = %d, want 3", len(files))
	}

	// File 1 (oldest) should be first
	if files[0].FD.GetNumber() != 1 {
		t.Errorf("First file number = %d, want 1", files[0].FD.GetNumber())
	}
	// File 2 should be second
	if files[1].FD.GetNumber() != 2 {
		t.Errorf("Second file number = %d, want 2", files[1].FD.GetNumber())
	}
	// File 3 (newest) should be third
	if files[2].FD.GetNumber() != 3 {
		t.Errorf("Third file number = %d, want 3", files[2].FD.GetNumber())
	}
}

func TestBuilderSortingL1(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Add L1 files in arbitrary order - they should be sorted by smallest key
	// This is appropriate for L1+ where files don't overlap
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 100),
					Smallest: makeInternalKey("z", 100, 1),
					Largest:  makeInternalKey("zz", 100, 1),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 100),
					Smallest: makeInternalKey("a", 100, 1),
					Largest:  makeInternalKey("m", 100, 1),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(3, 0, 100),
					Smallest: makeInternalKey("n", 100, 1),
					Largest:  makeInternalKey("y", 100, 1),
				},
			},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	v := builder.SaveTo(vs)

	// L1+ files should be sorted by smallest key
	files := v.Files(1)
	if len(files) != 3 {
		t.Fatalf("NumFiles(1) = %d, want 3", len(files))
	}

	// File 2 (smallest=a) should be first
	if files[0].FD.GetNumber() != 2 {
		t.Errorf("First file number = %d, want 2", files[0].FD.GetNumber())
	}
	// File 3 (smallest=n) should be second
	if files[1].FD.GetNumber() != 3 {
		t.Errorf("Second file number = %d, want 3", files[1].FD.GetNumber())
	}
	// File 1 (smallest=z) should be third
	if files[2].FD.GetNumber() != 1 {
		t.Errorf("Third file number = %d, want 1", files[2].FD.GetNumber())
	}
}

func TestBuilderMultipleEdits(t *testing.T) {
	vs := NewVersionSet(DefaultVersionSetOptions("/tmp/test"))
	builder := NewBuilder(vs, nil)

	// Apply multiple edits
	for i := uint64(1); i <= 5; i++ {
		edit := &manifest.VersionEdit{
			NewFiles: []manifest.NewFileEntry{
				{
					Level: int(i % MaxNumLevels),
					Meta: &manifest.FileMetaData{
						FD:       manifest.NewFileDescriptor(i, 0, 100*i),
						Smallest: makeInternalKey(string(rune('a'+i)), 100, 1),
						Largest:  makeInternalKey(string(rune('a'+i+1)), 100, 1),
					},
				},
			},
		}
		if err := builder.Apply(edit); err != nil {
			t.Fatalf("Apply() error = %v", err)
		}
	}

	v := builder.SaveTo(vs)

	if v.TotalFiles() != 5 {
		t.Errorf("TotalFiles() = %d, want 5", v.TotalFiles())
	}
}
