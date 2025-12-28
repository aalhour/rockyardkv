package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
	"github.com/aalhour/rockyardkv/internal/wal"
)

func TestCollisionCheck_FindsCollisionAcrossLiveSSTs(t *testing.T) {
	dir := t.TempDir()

	// Create two SSTs with the exact same internal key but different values.
	internalKey := []byte("userkey........" + "\x99\x00\x00\x00\x00\x00\x00\x00") // seq=0x99 type=0
	// Ensure internal key has >= 8-byte trailer; exact contents don't matter for tool.
	internalKey = append([]byte("key0000000000000001"), []byte{0x99, 0, 0, 0, 0, 0, 0, 0}...)

	dbDirPath := filepath.Join(dir, "db")
	if err := os.MkdirAll(dbDirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	sst1 := filepath.Join(dbDirPath, "000001.sst")
	sst2 := filepath.Join(dbDirPath, "000002.sst")
	writeTestSST(t, sst1, internalKey, []byte("value-one"))
	writeTestSST(t, sst2, internalKey, []byte("value-two"))

	manifestName := "MANIFEST-000001"
	writeCurrent(t, dbDirPath, manifestName)
	writeManifest(t, filepath.Join(dbDirPath, manifestName), []manifest.VersionEdit{
		{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 1)}},
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 1)}},
			},
		},
	})

	*dbDir = dbDirPath
	err := cmdCollisionCheck()
	if err == nil {
		t.Fatalf("expected collision error, got nil")
	}
	ce, ok := err.(*collisionError)
	if !ok {
		t.Fatalf("expected *collisionError, got %T: %v", err, err)
	}
	if ce.file1 == ce.file2 {
		t.Fatalf("expected two different files, got %q", ce.file1)
	}
	if !strings.HasSuffix(ce.file1, ".sst") || !strings.HasSuffix(ce.file2, ".sst") {
		t.Fatalf("expected sst filenames, got %q and %q", ce.file1, ce.file2)
	}
	if ce.value1Hex == ce.value2Hex {
		t.Fatalf("expected different values, got %q", ce.value1Hex)
	}
	if ce.internalKeyHex == "" {
		t.Fatalf("expected internalKeyHex to be set")
	}
}

func TestCollisionCheck_IgnoresDeletedSSTs(t *testing.T) {
	dir := t.TempDir()
	dbDirPath := filepath.Join(dir, "db")
	if err := os.MkdirAll(dbDirPath, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	internalKey := append([]byte("key0000000000000001"), []byte{0x99, 0, 0, 0, 0, 0, 0, 0}...)

	// Two SSTs collide, but then one is deleted in MANIFEST.
	sst1 := filepath.Join(dbDirPath, "000001.sst")
	sst2 := filepath.Join(dbDirPath, "000002.sst")
	writeTestSST(t, sst1, internalKey, []byte("value-one"))
	writeTestSST(t, sst2, internalKey, []byte("value-two"))

	manifestName := "MANIFEST-000001"
	writeCurrent(t, dbDirPath, manifestName)
	writeManifest(t, filepath.Join(dbDirPath, manifestName), []manifest.VersionEdit{
		{
			NewFiles: []manifest.NewFileEntry{
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(1, 0, 1)}},
				{Level: 0, Meta: &manifest.FileMetaData{FD: manifest.NewFileDescriptor(2, 0, 1)}},
			},
		},
		{
			DeletedFiles: []manifest.DeletedFileEntry{
				{Level: 0, FileNumber: 2},
			},
		},
	})

	*dbDir = dbDirPath
	if err := cmdCollisionCheck(); err != nil {
		t.Fatalf("expected no collision across live set, got %T: %v", err, err)
	}
}

func writeCurrent(t *testing.T, dbDirPath, manifestName string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dbDirPath, "CURRENT"), []byte(manifestName+"\n"), 0o644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}
}

func writeManifest(t *testing.T, path string, edits []manifest.VersionEdit) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create manifest: %v", err)
	}
	defer func() { _ = f.Close() }()

	w := wal.NewWriter(f, 1, false)
	for i := range edits {
		encoded := edits[i].EncodeTo()
		if _, err := w.AddRecord(encoded); err != nil {
			t.Fatalf("AddRecord: %v", err)
		}
	}
}

func writeTestSST(t *testing.T, path string, internalKey, value []byte) {
	t.Helper()
	fs := vfs.Default()
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("create sst: %v", err)
	}
	defer func() { _ = f.Close() }()

	b := table.NewTableBuilder(f, table.DefaultBuilderOptions())
	if err := b.Add(internalKey, value); err != nil {
		b.Abandon()
		t.Fatalf("builder add: %v", err)
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("builder finish: %v", err)
	}
}


