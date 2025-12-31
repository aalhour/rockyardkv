package campaign

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// RunResult represents the outcome of a single instance run.
type RunResult struct {
	// Instance is the instance that was run.
	Instance *Instance

	// Seed is the seed value used for this run.
	Seed int64

	// RunDir is the directory containing all run artifacts.
	RunDir string

	// BinaryPath is the resolved path to the binary that was executed.
	BinaryPath string

	// StartTime is when the run started.
	StartTime time.Time

	// EndTime is when the run ended.
	EndTime time.Time

	// ExitCode is the process exit code.
	ExitCode int

	// Passed indicates if the run passed all stop conditions.
	Passed bool

	// FailureReason describes why the run failed (if it did).
	FailureReason string

	// FailureKind categorizes the failure type for fingerprinting.
	FailureKind string

	// Fingerprint is the failure fingerprint for deduplication.
	// Empty string if the run passed.
	Fingerprint string

	// IsDuplicate indicates if this failure fingerprint was already known.
	IsDuplicate bool

	// OracleResult is the result of oracle verification (if performed).
	OracleResult *ToolResult
}

// Duration returns the run duration.
func (r *RunResult) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// RunArtifact is the JSON structure written to run.json in each run directory.
type RunArtifact struct {
	Instance       string    `json:"instance"`
	Seed           int64     `json:"seed"`
	BinaryPath     string    `json:"binary_path"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	DurationMs     int64     `json:"duration_ms"`
	ExitCode       int       `json:"exit_code"`
	Passed         bool      `json:"passed"`
	Failure        string    `json:"failure,omitempty"`
	FailureKind    string    `json:"failure_kind,omitempty"`
	Fingerprint    string    `json:"fingerprint,omitempty"`
	IsDuplicate    bool      `json:"is_duplicate,omitempty"`
	OracleExitCode *int      `json:"oracle_exit_code,omitempty"`
	OracleOutput   string    `json:"oracle_output,omitempty"`
}

// WriteRunArtifact writes the run.json file to the run directory.
// Also writes duplicate_of.txt if the failure is a duplicate.
func WriteRunArtifact(result *RunResult) error {
	artifact := RunArtifact{
		Instance:    result.Instance.Name,
		Seed:        result.Seed,
		BinaryPath:  result.BinaryPath,
		StartTime:   result.StartTime,
		EndTime:     result.EndTime,
		DurationMs:  result.Duration().Milliseconds(),
		ExitCode:    result.ExitCode,
		Passed:      result.Passed,
		Failure:     result.FailureReason,
		FailureKind: result.FailureKind,
		Fingerprint: result.Fingerprint,
		IsDuplicate: result.IsDuplicate,
	}

	if result.OracleResult != nil {
		artifact.OracleExitCode = &result.OracleResult.ExitCode
		artifact.OracleOutput = result.OracleResult.Stdout + result.OracleResult.Stderr
	}

	path := filepath.Join(result.RunDir, "run.json")
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run artifact: %w", err)
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}

	// Write duplicate_of.txt marker if this is a duplicate failure
	if result.IsDuplicate && result.Fingerprint != "" {
		markerPath := filepath.Join(result.RunDir, "duplicate_of.txt")
		markerContent := fmt.Sprintf("fingerprint: %s\n", result.Fingerprint)
		if err := os.WriteFile(markerPath, []byte(markerContent), 0o644); err != nil {
			return fmt.Errorf("write duplicate marker: %w", err)
		}
	}

	return nil
}

// CampaignSummary is the JSON structure written to summary.json after a campaign.
type CampaignSummary struct {
	Tier         string       `json:"tier"`
	StartTime    time.Time    `json:"start_time"`
	EndTime      time.Time    `json:"end_time"`
	DurationMs   int64        `json:"duration_ms"`
	TotalRuns    int          `json:"total_runs"`
	PassedRuns   int          `json:"passed_runs"`
	FailedRuns   int          `json:"failed_runs"`
	UniqueErrors int          `json:"unique_errors"`
	AllPassed    bool         `json:"all_passed"`
	Runs         []RunSummary `json:"runs"`
}

// RunSummary is a brief summary of each run for the campaign summary.
type RunSummary struct {
	Instance    string `json:"instance"`
	Seed        int64  `json:"seed"`
	Passed      bool   `json:"passed"`
	Failure     string `json:"failure,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
}

// WriteCampaignSummary writes the summary.json file to the run root.
func WriteCampaignSummary(runRoot string, tier Tier, startTime, endTime time.Time, results []*RunResult) error {
	fingerprints := make(map[string]struct{})
	summary := CampaignSummary{
		Tier:       string(tier),
		StartTime:  startTime,
		EndTime:    endTime,
		DurationMs: endTime.Sub(startTime).Milliseconds(),
		TotalRuns:  len(results),
		AllPassed:  true,
	}

	for _, r := range results {
		rs := RunSummary{
			Instance:    r.Instance.Name,
			Seed:        r.Seed,
			Passed:      r.Passed,
			Failure:     r.FailureReason,
			Fingerprint: r.Fingerprint,
			DurationMs:  r.Duration().Milliseconds(),
		}
		summary.Runs = append(summary.Runs, rs)

		if r.Passed {
			summary.PassedRuns++
		} else {
			summary.FailedRuns++
			summary.AllPassed = false
			if r.Fingerprint != "" {
				fingerprints[r.Fingerprint] = struct{}{}
			}
		}
	}
	summary.UniqueErrors = len(fingerprints)

	path := filepath.Join(runRoot, "summary.json")
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal campaign summary: %w", err)
	}

	return os.WriteFile(path, data, 0o644)
}

// ComputeFingerprint computes a failure fingerprint that includes:
// - Instance name (to avoid collisions across instances)
// - Seed (to identify specific run)
// - Failure kind (enum-like category)
// - Failure reason (specific message)
// - Log tail (for extra signal)
//
// Uses SHA-256 truncated to 16 hex chars for uniqueness.
func ComputeFingerprint(instanceName string, seed int64, failureKind, failureReason, logPath string) string {
	h := sha256.New()

	// Include instance identity and seed
	h.Write([]byte(instanceName))
	h.Write([]byte(":"))
	h.Write([]byte(strconv.FormatInt(seed, 10)))
	h.Write([]byte(":"))

	// Include failure classification
	h.Write([]byte(failureKind))
	h.Write([]byte(":"))
	h.Write([]byte(failureReason))

	// Include key lines from log if available
	if logPath != "" {
		if f, err := os.Open(logPath); err == nil {
			defer func() { _ = f.Close() }()
			// Read last 4KB for fingerprinting
			const tailSize = 4096
			stat, _ := f.Stat()
			if stat.Size() > tailSize {
				_, _ = f.Seek(-tailSize, io.SeekEnd)
			}
			tail := make([]byte, tailSize)
			n, _ := f.Read(tail)
			if n > 0 {
				h.Write(tail[:n])
			}
		}
	}

	return hex.EncodeToString(h.Sum(nil))[:16]
}

// EnsureDir creates a directory if it does not exist.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
