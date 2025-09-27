package vfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestOSFS_Create(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	n, err := f.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Write returned %d, want 5", n)
	}

	if err := f.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists and has correct content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Content = %q, want 'hello'", data)
	}
}

func TestOSFS_Open(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	// Create file first
	if err := os.WriteFile(path, []byte("hello world"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open for sequential reading
	f, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 5 || string(buf) != "hello" {
		t.Errorf("Read = %q, want 'hello'", buf[:n])
	}
}

func TestOSFS_OpenRandomAccess(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	// Create file first
	if err := os.WriteFile(path, []byte("hello world"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open for random access
	f, err := fs.OpenRandomAccess(path)
	if err != nil {
		t.Fatalf("OpenRandomAccess failed: %v", err)
	}
	defer f.Close()

	if f.Size() != 11 {
		t.Errorf("Size = %d, want 11", f.Size())
	}

	// Read from offset 6
	buf := make([]byte, 5)
	n, err := f.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != 5 || string(buf) != "world" {
		t.Errorf("ReadAt = %q, want 'world'", buf[:n])
	}
}

func TestOSFS_Rename(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.txt")
	newPath := filepath.Join(dir, "new.txt")

	if err := os.WriteFile(oldPath, []byte("content"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := fs.Rename(oldPath, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if fs.Exists(oldPath) {
		t.Error("Old file should not exist after rename")
	}
	if !fs.Exists(newPath) {
		t.Error("New file should exist after rename")
	}

	data, _ := os.ReadFile(newPath)
	if string(data) != "content" {
		t.Errorf("Content = %q, want 'content'", data)
	}
}

func TestOSFS_Remove(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := fs.Remove(path); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if fs.Exists(path) {
		t.Error("File should not exist after Remove")
	}
}

func TestOSFS_MkdirAll(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "b", "c")

	if err := fs.MkdirAll(path, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	if !fs.Exists(path) {
		t.Error("Directory should exist after MkdirAll")
	}
}

func TestOSFS_ListDir(t *testing.T) {
	fs := Default()
	dir := t.TempDir()

	// Create some files
	for i := range 3 {
		path := filepath.Join(dir, string(rune('a'+i))+".txt")
		os.WriteFile(path, []byte("content"), 0644)
	}

	names, err := fs.ListDir(dir)
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}

	if len(names) != 3 {
		t.Errorf("ListDir returned %d entries, want 3", len(names))
	}
}

func TestOSFS_Stat(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := os.WriteFile(path, []byte("hello"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	info, err := fs.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info.Size() != 5 {
		t.Errorf("Size = %d, want 5", info.Size())
	}
}

func TestOSFS_Exists(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if fs.Exists(path) {
		t.Error("File should not exist yet")
	}

	os.WriteFile(path, []byte("content"), 0644)

	if !fs.Exists(path) {
		t.Error("File should exist after creation")
	}
}

func TestWritableFile_Append(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := f.Append([]byte("hello")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := f.Append([]byte(" world")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	f.Close()

	data, _ := os.ReadFile(path)
	if string(data) != "hello world" {
		t.Errorf("Content = %q, want 'hello world'", data)
	}
}

func TestWritableFile_Size(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer f.Close()

	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Initial size = %d, want 0", size)
	}

	f.Write([]byte("hello"))

	size, err = f.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != 5 {
		t.Errorf("Size after write = %d, want 5", size)
	}
}

func TestWritableFile_Truncate(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	f.Write([]byte("hello world"))

	if err := f.Truncate(5); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	f.Close()

	data, _ := os.ReadFile(path)
	if string(data) != "hello" {
		t.Errorf("Content after truncate = %q, want 'hello'", data)
	}
}

func TestSequentialFile_Skip(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	os.WriteFile(path, []byte("hello world"), 0644)

	f, err := fs.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer f.Close()

	if err := f.Skip(6); err != nil {
		t.Fatalf("Skip failed: %v", err)
	}

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("Read after skip = %q, want 'world'", buf[:n])
	}
}

func TestOSFS_Lock(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "LOCK")

	lock1, err := fs.Lock(lockPath)
	if err != nil {
		t.Fatalf("First lock failed: %v", err)
	}

	// Second lock should fail
	_, err = fs.Lock(lockPath)
	if err == nil {
		t.Error("Second lock should fail")
	}

	// Release first lock
	if err := lock1.Close(); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Now should be able to acquire
	lock2, err := fs.Lock(lockPath)
	if err != nil {
		t.Fatalf("Lock after release failed: %v", err)
	}
	lock2.Close()
}

func TestOSFS_RemoveAll(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir")

	// Create a nested directory structure
	os.MkdirAll(filepath.Join(path, "a", "b"), 0755)
	os.WriteFile(filepath.Join(path, "file.txt"), []byte("content"), 0644)
	os.WriteFile(filepath.Join(path, "a", "file.txt"), []byte("content"), 0644)

	if err := fs.RemoveAll(path); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	if fs.Exists(path) {
		t.Error("Directory should not exist after RemoveAll")
	}
}

func TestLargeFileReadWrite(t *testing.T) {
	fs := Default()
	dir := t.TempDir()
	path := filepath.Join(dir, "large.bin")

	// Create 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	f.Close()

	// Read back
	rf, err := fs.OpenRandomAccess(path)
	if err != nil {
		t.Fatalf("OpenRandomAccess failed: %v", err)
	}
	defer rf.Close()

	readData := make([]byte, len(data))
	n, err := rf.ReadAt(readData, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Read %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(data, readData) {
		t.Error("Data mismatch")
	}
}
