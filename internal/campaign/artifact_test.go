package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestComputeFingerprint(t *testing.T) {
	// Same instance/seed/failure should produce same fingerprint
	fp1 := ComputeFingerprint("test.instance", 12345, "exit_error", "non-zero exit code: 1", "")
	fp2 := ComputeFingerprint("test.instance", 12345, "exit_error", "non-zero exit code: 1", "")

	if fp1 != fp2 {
		t.Errorf("same failure should produce same fingerprint: %q != %q", fp1, fp2)
	}

	// Different instance should produce different fingerprint
	fp3 := ComputeFingerprint("other.instance", 12345, "exit_error", "non-zero exit code: 1", "")
	if fp1 == fp3 {
		t.Errorf("different instances should produce different fingerprints: %q == %q", fp1, fp3)
	}

	// Different seed should produce different fingerprint
	fp4 := ComputeFingerprint("test.instance", 99999, "exit_error", "non-zero exit code: 1", "")
	if fp1 == fp4 {
		t.Errorf("different seeds should produce different fingerprints: %q == %q", fp1, fp4)
	}

	// Different failure kind should produce different fingerprint
	fp5 := ComputeFingerprint("test.instance", 12345, "timeout", "non-zero exit code: 1", "")
	if fp1 == fp5 {
		t.Errorf("different failure kinds should produce different fingerprints: %q == %q", fp1, fp5)
	}

	// Fingerprint should be 16 hex chars
	if len(fp1) != 16 {
		t.Errorf("fingerprint length = %d, want 16", len(fp1))
	}
}

func TestRunResultDuration(t *testing.T) {
	start := time.Now()
	end := start.Add(5 * time.Second)

	result := &RunResult{
		StartTime: start,
		EndTime:   end,
	}

	duration := result.Duration()
	if duration != 5*time.Second {
		t.Errorf("Duration() = %v, want %v", duration, 5*time.Second)
	}
}

func TestWriteRunArtifact(t *testing.T) {
	tmpDir := t.TempDir()

	instance := &Instance{
		Name: "test.instance",
	}

	result := &RunResult{
		Instance:      instance,
		Seed:          12345,
		RunDir:        tmpDir,
		StartTime:     time.Now(),
		EndTime:       time.Now().Add(1 * time.Second),
		ExitCode:      0,
		Passed:        true,
		FailureReason: "",
		Fingerprint:   "",
	}

	err := WriteRunArtifact(result)
	if err != nil {
		t.Fatalf("WriteRunArtifact() error = %v", err)
	}

	// Read and verify
	data, err := os.ReadFile(filepath.Join(tmpDir, "run.json"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var artifact RunArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if artifact.Instance != "test.instance" {
		t.Errorf("artifact.Instance = %q, want %q", artifact.Instance, "test.instance")
	}
	if artifact.Seed != 12345 {
		t.Errorf("artifact.Seed = %d, want %d", artifact.Seed, 12345)
	}
	if !artifact.Passed {
		t.Error("artifact.Passed should be true")
	}
}

func TestEnsureDir(t *testing.T) {
	tmpDir := t.TempDir()
	newDir := filepath.Join(tmpDir, "a", "b", "c")

	err := EnsureDir(newDir)
	if err != nil {
		t.Fatalf("EnsureDir() error = %v", err)
	}

	info, err := os.Stat(newDir)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if !info.IsDir() {
		t.Error("EnsureDir() should create a directory")
	}
}

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create source file
	srcPath := filepath.Join(tmpDir, "src.txt")
	content := []byte("hello world")
	if err := os.WriteFile(srcPath, content, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	// Copy
	dstPath := filepath.Join(tmpDir, "dst.txt")
	if err := CopyFile(srcPath, dstPath); err != nil {
		t.Fatalf("CopyFile() error = %v", err)
	}

	// Verify
	copied, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(copied) != string(content) {
		t.Errorf("copied content = %q, want %q", string(copied), string(content))
	}
}
