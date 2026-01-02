package version

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/vfs"
)

// TestVersionSetManifestFileNumber tests the ManifestFileNumber method
func TestVersionSetManifestFileNumber(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	// Before Create, manifest number should be 0
	if num := vs.ManifestFileNumber(); num != 0 {
		t.Errorf("ManifestFileNumber before Create = %d, want 0", num)
	}

	// Create the version set
	if err := vs.Create(); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer vs.Close()

	// After Create, manifest number should be set
	if num := vs.ManifestFileNumber(); num == 0 {
		t.Error("ManifestFileNumber after Create = 0, want > 0")
	}
}

// TestVersionSetLogAndApplyCoverage tests LogAndApply with file additions
func TestVersionSetLogAndApplyCoverage(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	if err := vs.Create(); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer vs.Close()

	// Create an edit that adds a file
	edit := &manifest.VersionEdit{
		HasLogNumber:      true,
		LogNumber:         1,
		HasLastSequence:   true,
		LastSequence:      100,
		HasNextFileNumber: true,
		NextFileNumber:    10,
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 1,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(5, 0, 1000),
					Smallest: []byte("aaa\x00\x00\x00\x00\x00\x00\x00\x01"),
					Largest:  []byte("zzz\x00\x00\x00\x00\x00\x00\x00\x01"),
				},
			},
		},
	}

	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply failed: %v", err)
	}

	// Verify the file was added
	current := vs.Current()
	if current.NumFiles(1) != 1 {
		t.Errorf("NumFiles(1) = %d, want 1", current.NumFiles(1))
	}
}

// TestVersionSetNumLevelFilesNoCurrent tests NumLevelFiles with no current version
func TestVersionSetNumLevelFilesNoCurrent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	// Without Create, current is nil
	if num := vs.NumLevelFiles(0); num != 0 {
		t.Errorf("NumLevelFiles(0) without current = %d, want 0", num)
	}
}

// TestVersionSetNumLevelBytesNoCurrent tests NumLevelBytes with no current version
func TestVersionSetNumLevelBytesNoCurrent(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	// Without Create, current is nil
	if bytes := vs.NumLevelBytes(0); bytes != 0 {
		t.Errorf("NumLevelBytes(0) without current = %d, want 0", bytes)
	}
}

// TestVersionSetRecoverCoverage tests Recover with a valid manifest
func TestVersionSetRecoverCoverage(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	// Create initial version
	if err := vs.Create(); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Add a file
	edit := &manifest.VersionEdit{
		HasLogNumber:      true,
		LogNumber:         1,
		HasLastSequence:   true,
		LastSequence:      100,
		HasNextFileNumber: true,
		NextFileNumber:    10,
	}
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply failed: %v", err)
	}
	vs.Close()

	// Recover
	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	defer vs2.Close()

	// Verify recovered state
	if vs2.LogNumber() != 1 {
		t.Errorf("LogNumber after recover = %d, want 1", vs2.LogNumber())
	}
}

// TestVersionSetRecoverNoCurrentFile tests Recover when CURRENT file doesn't exist
func TestVersionSetRecoverNoCurrentFile(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	err := vs.Recover()
	if !errors.Is(err, ErrNoCurrentManifest) {
		t.Errorf("Recover without CURRENT file: got %v, want %v", err, ErrNoCurrentManifest)
	}
}

// TestVersionSetRecoverInvalidManifestName tests Recover with invalid manifest name
func TestVersionSetRecoverInvalidManifestName(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()

	// Create a CURRENT file with invalid content
	currentPath := filepath.Join(dir, "CURRENT")
	f, _ := fs.Create(currentPath)
	f.Write([]byte("INVALID-NAME\n"))
	f.Close()

	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	err := vs.Recover()
	if !errors.Is(err, ErrInvalidManifest) {
		t.Errorf("Recover with invalid manifest name: got %v, want %v", err, ErrInvalidManifest)
	}
}

// TestLogAndApplyWritesSnapshot tests LogAndApply when manifest needs to be created.
// This exercises the writeSnapshot() function.
func TestLogAndApplyWritesSnapshot(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultVersionSetOptions(dir)
	vs := NewVersionSet(opts)

	// Create initial version, but don't use vs.Create() - set up manually
	// so that we can call LogAndApply with manifestWriter == nil
	vs.mu.Lock()
	vs.current = NewVersion(vs, vs.NextVersionNumber())
	vs.current.Ref()
	vs.appendVersion(vs.current)
	vs.mu.Unlock()

	// Add a file to the current version first
	edit := &manifest.VersionEdit{
		HasLogNumber:      true,
		LogNumber:         1,
		HasLastSequence:   true,
		LastSequence:      100,
		HasNextFileNumber: true,
		NextFileNumber:    10,
		NewFiles: []manifest.NewFileEntry{
			{
				Level: 0,
				Meta: &manifest.FileMetaData{
					FD:       manifest.NewFileDescriptor(5, 0, 1000),
					Smallest: []byte("aaa\x00\x00\x00\x00\x00\x00\x00\x01"),
					Largest:  []byte("zzz\x00\x00\x00\x00\x00\x00\x00\x01"),
				},
			},
		},
	}

	// This will trigger writeSnapshot because manifestWriter is nil
	if err := vs.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply failed: %v", err)
	}
	defer vs.Close()

	// Verify the file was added
	current := vs.Current()
	if current.NumFiles(0) != 1 {
		t.Errorf("NumFiles(0) = %d, want 1", current.NumFiles(0))
	}
}
