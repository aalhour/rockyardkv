// Artifact bundle for test reproducibility.
//
// When a test fails, this utility creates a bundle containing:
// - run.json: Complete test configuration (flags, seeds, versions)
// - DB snapshot: Copy of database directory
// - expected_state.bin: Expected state oracle (if available)
// - stdout.log, stderr.log: Captured output
//
// Reference: RocksDB v10.7.5
//   - tools/db_crashtest.py (artifact collection on failure)
package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ArtifactBundle collects evidence on test failure for reproducibility.
type ArtifactBundle struct {
	// RunDir is the output directory for artifacts.
	RunDir string

	// RunInfo contains metadata about the test run.
	RunInfo RunInfo

	// DBPath is the path to the database directory (will be copied).
	DBPath string

	// ExpectedStatePath is the path to the expected state file (will be copied).
	ExpectedStatePath string

	// StdoutPath and StderrPath are paths to captured output logs.
	StdoutPath string
	StderrPath string
}

// RunInfo contains metadata about a test run.
type RunInfo struct {
	// Test identification
	TestName  string    `json:"test_name"`
	Timestamp time.Time `json:"timestamp"`

	// Version info
	Version   string `json:"version,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`

	// Reproducibility
	Seed int64 `json:"seed"`

	// Test configuration
	Flags map[string]any `json:"flags"`

	// Result
	Passed  bool   `json:"passed"`
	Error   string `json:"error,omitempty"`
	Elapsed string `json:"elapsed,omitempty"`
}

// NewArtifactBundle creates a new artifact bundle.
// If runDir is empty, a temporary directory will be created.
func NewArtifactBundle(runDir, testName string, seed int64) (*ArtifactBundle, error) {
	if runDir == "" {
		var err error
		runDir, err = os.MkdirTemp("", "rockyard-artifacts-*")
		if err != nil {
			return nil, fmt.Errorf("create artifact dir: %w", err)
		}
	} else {
		if err := os.MkdirAll(runDir, 0755); err != nil {
			return nil, fmt.Errorf("create run dir: %w", err)
		}
	}

	return &ArtifactBundle{
		RunDir: runDir,
		RunInfo: RunInfo{
			TestName:  testName,
			Timestamp: time.Now().UTC(),
			GoVersion: runtime.Version(),
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			Seed:      seed,
			Flags:     make(map[string]any),
			GitCommit: getGitCommit(),
		},
	}, nil
}

// SetFlag records a flag value.
func (ab *ArtifactBundle) SetFlag(name string, value any) {
	ab.RunInfo.Flags[name] = value
}

// SetFlags records multiple flags at once.
func (ab *ArtifactBundle) SetFlags(flags map[string]any) {
	maps.Copy(ab.RunInfo.Flags, flags)
}

// SetDBPath sets the database path to copy on failure.
func (ab *ArtifactBundle) SetDBPath(path string) {
	ab.DBPath = path
}

// SetExpectedStatePath sets the expected state file path to copy on failure.
func (ab *ArtifactBundle) SetExpectedStatePath(path string) {
	ab.ExpectedStatePath = path
}

// SetVersion sets the version string.
func (ab *ArtifactBundle) SetVersion(version string) {
	ab.RunInfo.Version = version
}

// RecordSuccess marks the test as passed.
func (ab *ArtifactBundle) RecordSuccess(elapsed time.Duration) {
	ab.RunInfo.Passed = true
	ab.RunInfo.Elapsed = elapsed.String()
}

// RecordFailure marks the test as failed and collects artifacts.
func (ab *ArtifactBundle) RecordFailure(err error, elapsed time.Duration) error {
	ab.RunInfo.Passed = false
	ab.RunInfo.Error = err.Error()
	ab.RunInfo.Elapsed = elapsed.String()

	return ab.collectArtifacts()
}

// collectArtifacts gathers all evidence into the run directory.
func (ab *ArtifactBundle) collectArtifacts() error {
	var errs []string

	// Write run.json
	if err := ab.writeRunJSON(); err != nil {
		errs = append(errs, fmt.Sprintf("run.json: %v", err))
	}

	// Copy database directory
	if ab.DBPath != "" {
		dbDest := filepath.Join(ab.RunDir, "db")
		if err := copyDir(ab.DBPath, dbDest); err != nil {
			errs = append(errs, fmt.Sprintf("db copy: %v", err))
		}
	}

	// Copy expected state file
	if ab.ExpectedStatePath != "" {
		if _, err := os.Stat(ab.ExpectedStatePath); err == nil {
			dest := filepath.Join(ab.RunDir, filepath.Base(ab.ExpectedStatePath))
			if err := copyFile(ab.ExpectedStatePath, dest); err != nil {
				errs = append(errs, fmt.Sprintf("expected state: %v", err))
			}
		}
	}

	// Copy stdout/stderr logs if they exist
	if ab.StdoutPath != "" {
		if _, err := os.Stat(ab.StdoutPath); err == nil {
			dest := filepath.Join(ab.RunDir, "stdout.log")
			if err := copyFile(ab.StdoutPath, dest); err != nil {
				errs = append(errs, fmt.Sprintf("stdout: %v", err))
			}
		}
	}
	if ab.StderrPath != "" {
		if _, err := os.Stat(ab.StderrPath); err == nil {
			dest := filepath.Join(ab.RunDir, "stderr.log")
			if err := copyFile(ab.StderrPath, dest); err != nil {
				errs = append(errs, fmt.Sprintf("stderr: %v", err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("artifact collection errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// writeRunJSON writes the run metadata to run.json.
func (ab *ArtifactBundle) writeRunJSON() error {
	data, err := json.MarshalIndent(ab.RunInfo, "", "  ")
	if err != nil {
		return err
	}

	path := filepath.Join(ab.RunDir, "run.json")
	return os.WriteFile(path, data, 0644)
}

// getGitCommit returns the current git commit hash.
func getGitCommit() string {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

// copyFile copies a single file from src to dst.
func copyFile(src, dst string) (err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := srcFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := dstFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	// Preserve permissions
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	return os.Chmod(dst, srcInfo.Mode())
}

// copyDir recursively copies a directory from src to dst.
func copyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// OutputCapture captures stdout/stderr to files.
type OutputCapture struct {
	stdoutFile *os.File
	stderrFile *os.File
	origStdout *os.File
	origStderr *os.File
}

// NewOutputCapture starts capturing stdout/stderr to files.
func NewOutputCapture(dir string) (*OutputCapture, error) {
	stdoutPath := filepath.Join(dir, "stdout.log")
	stderrPath := filepath.Join(dir, "stderr.log")

	stdoutFile, err := os.Create(stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("create stdout.log: %w", err)
	}

	stderrFile, err := os.Create(stderrPath)
	if err != nil {
		_ = stdoutFile.Close()
		return nil, fmt.Errorf("create stderr.log: %w", err)
	}

	// Save originals
	oc := &OutputCapture{
		stdoutFile: stdoutFile,
		stderrFile: stderrFile,
		origStdout: os.Stdout,
		origStderr: os.Stderr,
	}

	// Note: We don't redirect os.Stdout/os.Stderr because that would
	// prevent console output. Instead, callers should use TeeWriter
	// to write to both console and file.

	return oc, nil
}

// StdoutPath returns the path to the stdout log file.
func (oc *OutputCapture) StdoutPath() string {
	if oc.stdoutFile == nil {
		return ""
	}
	return oc.stdoutFile.Name()
}

// StderrPath returns the path to the stderr log file.
func (oc *OutputCapture) StderrPath() string {
	if oc.stderrFile == nil {
		return ""
	}
	return oc.stderrFile.Name()
}

// StdoutWriter returns a writer that writes to the stdout log file.
func (oc *OutputCapture) StdoutWriter() io.Writer {
	return oc.stdoutFile
}

// StderrWriter returns a writer that writes to the stderr log file.
func (oc *OutputCapture) StderrWriter() io.Writer {
	return oc.stderrFile
}

// Close closes the capture files.
func (oc *OutputCapture) Close() error {
	var errs []string
	if oc.stdoutFile != nil {
		if err := oc.stdoutFile.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if oc.stderrFile != nil {
		if err := oc.stderrFile.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close capture: %s", strings.Join(errs, "; "))
	}
	return nil
}

// TeeWriter writes to multiple writers.
type TeeWriter struct {
	writers []io.Writer
}

// NewTeeWriter creates a writer that writes to all provided writers.
func NewTeeWriter(writers ...io.Writer) *TeeWriter {
	return &TeeWriter{writers: writers}
}

// Write writes to all underlying writers.
func (tw *TeeWriter) Write(p []byte) (n int, err error) {
	for _, w := range tw.writers {
		n, err = w.Write(p)
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}
