package campaign

import (
	"os"
	"path/filepath"
	"testing"
)

// Contract: classifyFailureKind categorizes failures by exit code and message content.
func TestClassifyFailureKind(t *testing.T) {
	tests := []struct {
		name          string
		failureReason string
		exitCode      int
		want          string
	}{
		{"exit code -1 without timeout reason", "", -1, "exit_error"},
		{"timeout exit code", "timeout", -1, "timeout"},
		{"sigkill", "", 137, "killed"},
		{"sigterm", "", 143, "terminated"},
		{"oracle failure", "oracle checkconsistency failed", 1, "oracle_failure"},
		{"consistency check", "consistency check failed", 1, "oracle_failure"},
		{"verification failure", "final verification failed", 1, "verification_failure"},
		{"corruption detected", "corruption: bad block", 1, "corruption"},
		{"timeout in message", "timeout waiting for response", 1, "timeout"},
		{"generic exit error", "some other error", 1, "exit_error"},
		{"zero exit with reason", "warning message", 0, "exit_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyFailureKind(tt.failureReason, tt.exitCode)
			if got != tt.want {
				t.Errorf("classifyFailureKind(%q, %d) = %q, want %q",
					tt.failureReason, tt.exitCode, got, tt.want)
			}
		})
	}
}

// Contract: containsIgnoreCase performs case-insensitive substring matching.
func TestContainsIgnoreCase(t *testing.T) {
	tests := []struct {
		s      string
		substr string
		want   bool
	}{
		{"hello world", "world", true},
		{"Hello World", "hello", true},
		{"ORACLE", "oracle", true},
		{"oracle", "ORACLE", true},
		{"test", "missing", false},
		{"", "test", false},
		{"test", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.s+"_"+tt.substr, func(t *testing.T) {
			got := containsIgnoreCase(tt.s, tt.substr)
			if got != tt.want {
				t.Errorf("containsIgnoreCase(%q, %q) = %v, want %v",
					tt.s, tt.substr, got, tt.want)
			}
		})
	}
}

// Contract: discoverDBPath finds a database directory containing CURRENT.
func TestDiscoverDBPath(t *testing.T) {
	tmpDir := t.TempDir()

	r := &Runner{config: RunnerConfig{}}

	if got := r.discoverDBPath(tmpDir); got != "" {
		t.Errorf("discoverDBPath() should return empty for dir without db: %q", got)
	}

	dbDir := filepath.Join(tmpDir, "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := r.discoverDBPath(tmpDir); got != dbDir {
		t.Errorf("discoverDBPath() = %q, want %q", got, dbDir)
	}
}

// Contract: discoverDBPath recognizes the artifacts/db layout used by crashtest.
func TestDiscoverDBPath_ArtifactsLayout(t *testing.T) {
	tmpDir := t.TempDir()

	r := &Runner{config: RunnerConfig{}}

	dbDir := filepath.Join(tmpDir, "artifacts", "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := r.discoverDBPath(tmpDir); got != dbDir {
		t.Errorf("discoverDBPath() = %q, want %q", got, dbDir)
	}
}

// Contract: discoverDBPath recognizes the db_sync layout used by durability tests.
func TestDiscoverDBPath_SyncLayout(t *testing.T) {
	tmpDir := t.TempDir()

	r := &Runner{config: RunnerConfig{}}

	dbDir := filepath.Join(tmpDir, "db_sync")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := r.discoverDBPath(tmpDir); got != dbDir {
		t.Errorf("discoverDBPath() = %q, want %q", got, dbDir)
	}
}

// Contract: NewRunner sets default values for BinDir, Output, and timeouts.
func TestNewRunner_Defaults(t *testing.T) {
	config := RunnerConfig{
		Tier:    TierQuick,
		RunRoot: "/tmp/test",
	}

	r := NewRunner(config)

	if r.config.BinDir != "./bin" {
		t.Errorf("BinDir default = %q, want %q", r.config.BinDir, "./bin")
	}

	if r.config.Output == nil {
		t.Error("Output should default to os.Stdout")
	}

	if r.config.InstanceTimeout == 0 {
		t.Error("InstanceTimeout should be set from tier default")
	}

	if r.config.GlobalTimeout == 0 {
		t.Error("GlobalTimeout should be set from tier default")
	}
}

// Contract: NewRunner preserves explicitly provided configuration values.
func TestNewRunner_PreservesCustomValues(t *testing.T) {
	config := RunnerConfig{
		Tier:            TierQuick,
		RunRoot:         "/tmp/test",
		BinDir:          "/custom/bin",
		InstanceTimeout: 300,
		GlobalTimeout:   1800,
	}

	r := NewRunner(config)

	if r.config.BinDir != "/custom/bin" {
		t.Errorf("BinDir = %q, want %q", r.config.BinDir, "/custom/bin")
	}

	if r.config.InstanceTimeout != 300 {
		t.Errorf("InstanceTimeout = %d, want %d", r.config.InstanceTimeout, 300)
	}

	if r.config.GlobalTimeout != 1800 {
		t.Errorf("GlobalTimeout = %d, want %d", r.config.GlobalTimeout, 1800)
	}
}

// Contract: passedStr returns "PASS" for true and "FAIL" for false.
func TestPassedStr(t *testing.T) {
	if passedStr(true) != "PASS" {
		t.Error("passedStr(true) should return PASS")
	}
	if passedStr(false) != "FAIL" {
		t.Error("passedStr(false) should return FAIL")
	}
}

// Contract: writeOracleArtifacts writes stable files to oracle/ subdirectory.
// This test is hermetic: no shelling out to ldb, uses fake oracle result.
func TestRunner_writeOracleArtifacts_WritesStableFiles(t *testing.T) {
	runDir := t.TempDir()
	r := NewRunner(RunnerConfig{})

	// Fake oracle result with non-empty stdout/stderr
	result := &ToolResult{
		ExitCode: 0,
		Stdout:   "OK\n",
		Stderr:   "warning: some message\n",
	}

	if err := r.writeOracleArtifacts(runDir, result); err != nil {
		t.Fatalf("writeOracleArtifacts() error = %v", err)
	}

	// Assert stable file paths exist
	expectedFiles := []string{
		"oracle/ldb_checkconsistency.stdout.txt",
		"oracle/ldb_checkconsistency.stderr.txt",
		"oracle/ldb_checkconsistency.exitcode",
	}

	for _, f := range expectedFiles {
		path := filepath.Join(runDir, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s does not exist", f)
		}
	}

	// Verify exit code content
	exitCodePath := filepath.Join(runDir, "oracle/ldb_checkconsistency.exitcode")
	content, err := os.ReadFile(exitCodePath)
	if err != nil {
		t.Fatalf("ReadFile(exitcode) error = %v", err)
	}
	if string(content) != "0\n" {
		t.Errorf("exitcode content = %q, want %q", content, "0\n")
	}
}

// Contract: writeOracleArtifacts writes files even when stdout/stderr are empty.
// Avoid "paper success" where empty results don't produce evidence files.
func TestRunner_writeOracleArtifacts_WritesEmptyFiles(t *testing.T) {
	runDir := t.TempDir()
	r := NewRunner(RunnerConfig{})

	// Empty stdout/stderr
	result := &ToolResult{
		ExitCode: 1,
		Stdout:   "",
		Stderr:   "",
	}

	if err := r.writeOracleArtifacts(runDir, result); err != nil {
		t.Fatalf("writeOracleArtifacts() error = %v", err)
	}

	// All three files must exist even with empty output
	expectedFiles := []string{
		"oracle/ldb_checkconsistency.stdout.txt",
		"oracle/ldb_checkconsistency.stderr.txt",
		"oracle/ldb_checkconsistency.exitcode",
	}

	for _, f := range expectedFiles {
		path := filepath.Join(runDir, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("expected file %s must exist even with empty output", f)
		}
	}

	// Exit code file must contain "1\n"
	exitCodePath := filepath.Join(runDir, "oracle/ldb_checkconsistency.exitcode")
	content, err := os.ReadFile(exitCodePath)
	if err != nil {
		t.Fatalf("ReadFile(exitcode) error = %v", err)
	}
	if string(content) != "1\n" {
		t.Errorf("exitcode content = %q, want %q", content, "1\n")
	}

	// Stdout and stderr files must be empty (zero bytes)
	stdoutPath := filepath.Join(runDir, "oracle/ldb_checkconsistency.stdout.txt")
	stdoutContent, _ := os.ReadFile(stdoutPath)
	if len(stdoutContent) != 0 {
		t.Errorf("stdout file should be empty, got %d bytes", len(stdoutContent))
	}

	stderrPath := filepath.Join(runDir, "oracle/ldb_checkconsistency.stderr.txt")
	stderrContent, _ := os.ReadFile(stderrPath)
	if len(stderrContent) != 0 {
		t.Errorf("stderr file should be empty, got %d bytes", len(stderrContent))
	}
}
