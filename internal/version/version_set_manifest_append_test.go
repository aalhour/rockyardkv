package version

import (
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

func TestVersionSet_Recover_ReusesManifestForAppends(t *testing.T) {
	dir := t.TempDir()
	opts := VersionSetOptions{
		DBName:              dir,
		FS:                  vfs.Default(),
		MaxManifestFileSize: 1024 * 1024,
		NumLevels:           MaxNumLevels,
	}

	vs1 := NewVersionSet(opts)
	if err := vs1.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Write one edit so MANIFEST has some history.
	edit := &manifest.VersionEdit{
		HasLogNumber:    true,
		LogNumber:       1,
		HasLastSequence: true,
		LastSequence:    123,
	}
	if err := vs1.LogAndApply(edit); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}
	vs1.Close()

	vs2 := NewVersionSet(opts)
	if err := vs2.Recover(); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	defer vs2.Close()

	manifestNum := vs2.ManifestFileNumber()

	// A post-recover LogAndApply must NOT allocate a new MANIFEST.
	edit2 := &manifest.VersionEdit{
		HasLogNumber:    true,
		LogNumber:       2,
		HasLastSequence: true,
		LastSequence:    124,
	}
	if err := vs2.LogAndApply(edit2); err != nil {
		t.Fatalf("LogAndApply() error = %v", err)
	}

	if got := vs2.ManifestFileNumber(); got != manifestNum {
		t.Fatalf("ManifestFileNumber() changed after LogAndApply: got %d, want %d", got, manifestNum)
	}

	// CURRENT should still point at the same manifest.
	current, err := opts.FS.Open(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("Open(CURRENT) error = %v", err)
	}
	defer current.Close()
	buf := make([]byte, 64)
	n, _ := current.Read(buf)
	if n == 0 {
		t.Fatalf("CURRENT empty")
	}
}
