package testutil

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArtifactBundle_Creation(t *testing.T) {
	dir := t.TempDir()

	ab, err := NewArtifactBundle(dir, "TestExample", 12345)
	if err != nil {
		t.Fatalf("NewArtifactBundle failed: %v", err)
	}

	if ab.RunDir != dir {
		t.Errorf("RunDir = %q, want %q", ab.RunDir, dir)
	}
	if ab.RunInfo.TestName != "TestExample" {
		t.Errorf("TestName = %q, want %q", ab.RunInfo.TestName, "TestExample")
	}
	if ab.RunInfo.Seed != 12345 {
		t.Errorf("Seed = %d, want 12345", ab.RunInfo.Seed)
	}
}

func TestArtifactBundle_RecordFailure(t *testing.T) {
	dir := t.TempDir()

	// Create a mock database directory
	dbPath := filepath.Join(dir, "testdb")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		t.Fatalf("Failed to create mock db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "CURRENT"), []byte("MANIFEST-000001\n"), 0644); err != nil {
		t.Fatalf("Failed to write CURRENT: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "MANIFEST-000001"), []byte("manifest data"), 0644); err != nil {
		t.Fatalf("Failed to write MANIFEST: %v", err)
	}

	// Create expected state file
	expectedStatePath := filepath.Join(dir, "expected_state.bin")
	if err := os.WriteFile(expectedStatePath, []byte("expected state data"), 0644); err != nil {
		t.Fatalf("Failed to write expected state: %v", err)
	}

	// Create artifact bundle
	runDir := filepath.Join(dir, "artifacts")
	ab, err := NewArtifactBundle(runDir, "TestFailure", 99999)
	if err != nil {
		t.Fatalf("NewArtifactBundle failed: %v", err)
	}

	ab.SetDBPath(dbPath)
	ab.SetExpectedStatePath(expectedStatePath)
	ab.SetVersion("v0.1.2")
	ab.SetFlag("duration", "10m")
	ab.SetFlag("threads", 4)

	// Record failure
	testErr := errors.New("verification mismatch")
	if err := ab.RecordFailure(testErr, 5*time.Minute); err != nil {
		t.Fatalf("RecordFailure failed: %v", err)
	}

	// Verify run.json exists and has correct content
	runJSONPath := filepath.Join(runDir, "run.json")
	data, err := os.ReadFile(runJSONPath)
	if err != nil {
		t.Fatalf("Failed to read run.json: %v", err)
	}

	var runInfo RunInfo
	if err := json.Unmarshal(data, &runInfo); err != nil {
		t.Fatalf("Failed to parse run.json: %v", err)
	}

	if runInfo.TestName != "TestFailure" {
		t.Errorf("run.json TestName = %q, want %q", runInfo.TestName, "TestFailure")
	}
	if runInfo.Seed != 99999 {
		t.Errorf("run.json Seed = %d, want 99999", runInfo.Seed)
	}
	if runInfo.Passed {
		t.Error("run.json Passed = true, want false")
	}
	if runInfo.Error != "verification mismatch" {
		t.Errorf("run.json Error = %q, want %q", runInfo.Error, "verification mismatch")
	}
	if runInfo.Version != "v0.1.2" {
		t.Errorf("run.json Version = %q, want %q", runInfo.Version, "v0.1.2")
	}

	// Verify DB was copied
	dbCopyPath := filepath.Join(runDir, "db")
	if _, err := os.Stat(dbCopyPath); os.IsNotExist(err) {
		t.Error("DB directory was not copied")
	}
	currentCopy := filepath.Join(dbCopyPath, "CURRENT")
	if _, err := os.Stat(currentCopy); os.IsNotExist(err) {
		t.Error("CURRENT file was not copied")
	}

	// Verify expected state was copied
	expectedCopy := filepath.Join(runDir, "expected_state.bin")
	if _, err := os.Stat(expectedCopy); os.IsNotExist(err) {
		t.Error("expected_state.bin was not copied")
	}
}

func TestArtifactBundle_RecordSuccess(t *testing.T) {
	dir := t.TempDir()

	ab, err := NewArtifactBundle(dir, "TestSuccess", 11111)
	if err != nil {
		t.Fatalf("NewArtifactBundle failed: %v", err)
	}

	ab.RecordSuccess(10 * time.Second)

	if !ab.RunInfo.Passed {
		t.Error("Passed = false, want true")
	}
	if ab.RunInfo.Elapsed != "10s" {
		t.Errorf("Elapsed = %q, want %q", ab.RunInfo.Elapsed, "10s")
	}
}

func TestOutputCapture(t *testing.T) {
	dir := t.TempDir()

	oc, err := NewOutputCapture(dir)
	if err != nil {
		t.Fatalf("NewOutputCapture failed: %v", err)
	}

	// Write to stdout and stderr
	stdoutWriter := oc.StdoutWriter()
	stderrWriter := oc.StderrWriter()

	_, _ = stdoutWriter.Write([]byte("stdout message\n"))
	_, _ = stderrWriter.Write([]byte("stderr message\n"))

	// Close and verify
	if err := oc.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read back
	stdoutData, err := os.ReadFile(oc.StdoutPath())
	if err != nil {
		t.Fatalf("Read stdout failed: %v", err)
	}
	if string(stdoutData) != "stdout message\n" {
		t.Errorf("stdout content = %q, want %q", stdoutData, "stdout message\n")
	}

	stderrData, err := os.ReadFile(oc.StderrPath())
	if err != nil {
		t.Fatalf("Read stderr failed: %v", err)
	}
	if string(stderrData) != "stderr message\n" {
		t.Errorf("stderr content = %q, want %q", stderrData, "stderr message\n")
	}
}

func TestTeeWriter(t *testing.T) {
	dir := t.TempDir()

	file1, err := os.Create(filepath.Join(dir, "file1.txt"))
	if err != nil {
		t.Fatalf("Create file1 failed: %v", err)
	}
	defer file1.Close()

	file2, err := os.Create(filepath.Join(dir, "file2.txt"))
	if err != nil {
		t.Fatalf("Create file2 failed: %v", err)
	}
	defer file2.Close()

	tee := NewTeeWriter(file1, file2)
	_, err = tee.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("TeeWriter.Write failed: %v", err)
	}

	// Close files
	file1.Close()
	file2.Close()

	// Verify both have the content
	data1, _ := os.ReadFile(filepath.Join(dir, "file1.txt"))
	data2, _ := os.ReadFile(filepath.Join(dir, "file2.txt"))

	if string(data1) != "hello world" {
		t.Errorf("file1 content = %q, want %q", data1, "hello world")
	}
	if string(data2) != "hello world" {
		t.Errorf("file2 content = %q, want %q", data2, "hello world")
	}
}

func TestCopyDir(t *testing.T) {
	dir := t.TempDir()

	// Create source directory with nested structure
	srcDir := filepath.Join(dir, "src")
	if err := os.MkdirAll(filepath.Join(srcDir, "subdir"), 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Copy
	dstDir := filepath.Join(dir, "dst")
	if err := copyDir(srcDir, dstDir); err != nil {
		t.Fatalf("copyDir failed: %v", err)
	}

	// Verify
	data1, err := os.ReadFile(filepath.Join(dstDir, "file1.txt"))
	if err != nil {
		t.Fatalf("Read file1.txt failed: %v", err)
	}
	if string(data1) != "content1" {
		t.Errorf("file1.txt content = %q, want %q", data1, "content1")
	}

	data2, err := os.ReadFile(filepath.Join(dstDir, "subdir", "file2.txt"))
	if err != nil {
		t.Fatalf("Read subdir/file2.txt failed: %v", err)
	}
	if string(data2) != "content2" {
		t.Errorf("subdir/file2.txt content = %q, want %q", data2, "content2")
	}
}
