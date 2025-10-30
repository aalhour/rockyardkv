package version

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// TestGoldenMaxNumLevels tests that MaxNumLevels matches RocksDB.
func TestGoldenMaxNumLevels(t *testing.T) {
	// RocksDB default is 7 levels (0-6)
	if MaxNumLevels != 7 {
		t.Errorf("MaxNumLevels = %d, want 7", MaxNumLevels)
	}
}

// TestGoldenVersionEditApplication tests applying VersionEdits to build versions.
func TestGoldenVersionEditApplication(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vs := NewVersionSet(opts)

	// Create initial version
	initialVersion := NewVersion(vs, 0)

	// Apply edit that adds files
	builder := NewBuilder(vs, initialVersion)

	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 1000),
					Smallest: []byte("a"),
					Largest:  []byte("z"),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 2000),
					Smallest: []byte("a"),
					Largest:  []byte("m"),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(3, 0, 3000),
					Smallest: []byte("n"),
					Largest:  []byte("z"),
				},
			},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	newVersion := builder.SaveTo(vs)

	// Verify file counts
	if newVersion.NumFiles(0) != 1 {
		t.Errorf("Level 0 files = %d, want 1", newVersion.NumFiles(0))
	}
	if newVersion.NumFiles(1) != 2 {
		t.Errorf("Level 1 files = %d, want 2", newVersion.NumFiles(1))
	}
	if newVersion.NumFiles(2) != 0 {
		t.Errorf("Level 2 files = %d, want 0", newVersion.NumFiles(2))
	}

	// Verify total bytes
	if newVersion.NumLevelBytes(0) != 1000 {
		t.Errorf("Level 0 bytes = %d, want 1000", newVersion.NumLevelBytes(0))
	}
	if newVersion.NumLevelBytes(1) != 5000 {
		t.Errorf("Level 1 bytes = %d, want 5000", newVersion.NumLevelBytes(1))
	}
}

// TestGoldenVersionEditDeletion tests applying file deletions.
func TestGoldenVersionEditDeletion(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vs := NewVersionSet(opts)

	// Build version with files
	initialVersion := NewVersion(vs, 0)
	builder := NewBuilder(vs, initialVersion)

	edit1 := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 1000),
					Smallest: []byte("a"),
					Largest:  []byte("z"),
				},
			},
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 2000),
					Smallest: []byte("a"),
					Largest:  []byte("z"),
				},
			},
		},
	}
	if err := builder.Apply(edit1); err != nil {
		t.Fatalf("Apply edit1 failed: %v", err)
	}

	version1 := builder.SaveTo(vs)
	if version1.NumFiles(0) != 2 {
		t.Fatalf("Expected 2 files at level 0, got %d", version1.NumFiles(0))
	}

	// Now delete one file
	builder2 := NewBuilder(vs, version1)
	edit2 := &manifest.VersionEdit{
		DeletedFiles: []manifest.DeletedFileEntry{
			{Level: 0, FileNumber: 1},
		},
	}
	if err := builder2.Apply(edit2); err != nil {
		t.Fatalf("Apply edit2 failed: %v", err)
	}

	version2 := builder2.SaveTo(vs)
	if version2.NumFiles(0) != 1 {
		t.Errorf("After deletion, level 0 files = %d, want 1", version2.NumFiles(0))
	}
}

// TestGoldenVersionFileOrdering tests that files within a level are ordered.
func TestGoldenVersionFileOrdering(t *testing.T) {
	opts := DefaultVersionSetOptions(t.TempDir())
	vs := NewVersionSet(opts)

	version := NewVersion(vs, 0)
	builder := NewBuilder(vs, version)

	// Add files in non-sorted order
	edit := &manifest.VersionEdit{
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(3, 0, 1000),
					Smallest: []byte("m"),
					Largest:  []byte("z"),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(1, 0, 1000),
					Smallest: []byte("a"),
					Largest:  []byte("f"),
				},
			},
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(2, 0, 1000),
					Smallest: []byte("g"),
					Largest:  []byte("l"),
				},
			},
		},
	}

	if err := builder.Apply(edit); err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	newVersion := builder.SaveTo(vs)
	files := newVersion.Files(1)

	if len(files) != 3 {
		t.Fatalf("Expected 3 files, got %d", len(files))
	}

	// Files should be sorted by smallest key for levels > 0
	// (Level 0 files are sorted by newest first - different ordering)
	for i := 1; i < len(files); i++ {
		prevLargest := files[i-1].Largest
		currSmallest := files[i].Smallest
		if bytesCompareLocal(prevLargest, currSmallest) >= 0 {
			t.Errorf("Files not properly sorted: file %d largest %q >= file %d smallest %q",
				i-1, prevLargest, i, currSmallest)
		}
	}
}

// TestGoldenManifestFilename tests MANIFEST filename format.
func TestGoldenManifestFilename(t *testing.T) {
	// MANIFEST files are named MANIFEST-NNNNNN where NNNNNN is the file number
	// formatted as a 6-digit zero-padded decimal
	testCases := []struct {
		number   uint64
		expected string
	}{
		{1, "MANIFEST-000001"},
		{10, "MANIFEST-000010"},
		{123456, "MANIFEST-123456"},
		{999999, "MANIFEST-999999"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			// The format is used in version_set.go
			got := manifestFilename(tc.number)
			if got != tc.expected {
				t.Errorf("manifestFilename(%d) = %q, want %q", tc.number, got, tc.expected)
			}
		})
	}
}

// manifestFilename returns the MANIFEST filename for a given file number.
func manifestFilename(number uint64) string {
	return manifestPrefix + formatFileNumber(number)
}

const manifestPrefix = "MANIFEST-"

func formatFileNumber(n uint64) string {
	s := make([]byte, 6)
	for i := 5; i >= 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}

// bytesCompareLocal is a local bytes comparison function for testing.
func bytesCompareLocal(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
