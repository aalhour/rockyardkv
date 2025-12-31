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
	"strings"
	"time"
)

// SchemaVersion is the current version of the artifact schema.
// Bump rules:
//   - Major: interpretation changes (field meaning, fingerprint algorithm, pass/fail logic)
//   - Minor: additive fields that don't change meaning or pass/fail
//   - Patch: tooling bugfixes that don't change schema
const SchemaVersion = "1.1.0"

// FailureClass categorizes failures for governance reporting.
type FailureClass string

const (
	// FailureClassNone means the run passed.
	FailureClassNone FailureClass = ""
	// FailureClassNew is a new failure not previously seen.
	FailureClassNew FailureClass = "new_failure"
	// FailureClassKnown is a failure that matches a quarantined known failure.
	FailureClassKnown FailureClass = "known_failure"
	// FailureClassDuplicate is a repeat of a failure already seen in this campaign run.
	FailureClassDuplicate FailureClass = "duplicate"
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

	// FailureClass categorizes the failure for governance reporting.
	FailureClass FailureClass

	// QuarantinePolicy is the policy for this failure (if it's a known failure).
	QuarantinePolicy QuarantinePolicy

	// OracleResult is the result of oracle verification (if performed).
	OracleResult *ToolResult

	// TraceResult contains trace capture information (if enabled).
	TraceResult *TraceResult

	// MinimizeResult contains minimization results (if performed).
	MinimizeResult *MinimizeResult
}

// Duration returns the run duration.
func (r *RunResult) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// RunArtifact is the JSON structure written to run.json in each run directory.
type RunArtifact struct {
	SchemaVersion string    `json:"schema_version"`
	Instance      string    `json:"instance"`
	Seed          int64     `json:"seed"`
	BinaryPath    string    `json:"binary_path"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	DurationMs    int64     `json:"duration_ms"`
	ExitCode      int       `json:"exit_code"`
	Passed        bool      `json:"passed"`
	Failure       string    `json:"failure,omitempty"`
	FailureKind   string    `json:"failure_kind,omitempty"`
	Fingerprint   string    `json:"fingerprint,omitempty"`
	IsDuplicate   bool      `json:"is_duplicate,omitempty"`

	// Oracle check fields
	OracleExitCode *int   `json:"oracle_exit_code,omitempty"`
	OracleOutput   string `json:"oracle_output,omitempty"`

	// Trace capture fields
	TracePath        string `json:"trace_path,omitempty"`
	TraceBytesWriten int64  `json:"trace_bytes_written,omitempty"`
	TraceTruncated   bool   `json:"trace_truncated,omitempty"`
	ReplayCommand    string `json:"replay_command,omitempty"`

	// Minimization fields
	Minimized       bool            `json:"minimized,omitempty"`
	MinimizedResult *MinimizeResult `json:"minimized_result,omitempty"`

	// Tags for filtering (computed at write time)
	Tags *Tags `json:"tags,omitempty"`
}

// WriteRunArtifact writes the run.json file to the run directory.
// Also writes duplicate_of.txt if the failure is a duplicate.
func WriteRunArtifact(result *RunResult) error {
	// Compute tags for the instance
	tags := result.Instance.ComputeTags()

	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      result.Instance.Name,
		Seed:          result.Seed,
		BinaryPath:    result.BinaryPath,
		StartTime:     result.StartTime,
		EndTime:       result.EndTime,
		DurationMs:    result.Duration().Milliseconds(),
		ExitCode:      result.ExitCode,
		Passed:        result.Passed,
		Failure:       result.FailureReason,
		FailureKind:   result.FailureKind,
		Fingerprint:   result.Fingerprint,
		IsDuplicate:   result.IsDuplicate,
		Tags:          &tags,
	}

	if result.OracleResult != nil {
		artifact.OracleExitCode = &result.OracleResult.ExitCode
		artifact.OracleOutput = result.OracleResult.Stdout + result.OracleResult.Stderr
	}

	// Add trace fields
	if result.TraceResult != nil {
		artifact.TracePath = result.TraceResult.Path
		artifact.TraceBytesWriten = result.TraceResult.BytesWritten
		artifact.TraceTruncated = result.TraceResult.Truncated
		artifact.ReplayCommand = result.TraceResult.ReplayCommand
	}

	// Add minimize fields
	if result.MinimizeResult != nil {
		artifact.Minimized = result.MinimizeResult.Success
		artifact.MinimizedResult = result.MinimizeResult
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
	SchemaVersion string       `json:"schema_version"`
	Tier          string       `json:"tier"`
	StartTime     time.Time    `json:"start_time"`
	EndTime       time.Time    `json:"end_time"`
	DurationMs    int64        `json:"duration_ms"`
	TotalRuns     int          `json:"total_runs"`
	PassedRuns    int          `json:"passed_runs"`
	FailedRuns    int          `json:"failed_runs"`
	SkippedRuns   int          `json:"skipped_runs"`
	UniqueErrors  int          `json:"unique_errors"`
	AllPassed     bool         `json:"all_passed"`
	Runs          []RunSummary `json:"runs"`

	// Skipped instances and their reasons
	Skipped []SkipSummary `json:"skipped,omitempty"`

	// Governance fields for failure classification and deduplication
	NewFailures    int `json:"new_failures"`
	KnownFailures  int `json:"known_failures"`
	Duplicates     int `json:"duplicates"`
	Unquarantined  int `json:"unquarantined"`
	OracleRequired int `json:"oracle_required"`
	OracleGated    int `json:"oracle_gated"`
}

// SkipSummary records an instance that was skipped.
type SkipSummary struct {
	Instance string `json:"instance"`
	Reason   string `json:"reason"`
	IssueID  string `json:"issue_id,omitempty"`
}

// RunSummary is a brief summary of each run for the campaign summary.
type RunSummary struct {
	Instance     string       `json:"instance"`
	Seed         int64        `json:"seed"`
	Passed       bool         `json:"passed"`
	Failure      string       `json:"failure,omitempty"`
	Fingerprint  string       `json:"fingerprint,omitempty"`
	FailureClass FailureClass `json:"failure_class,omitempty"`
	DurationMs   int64        `json:"duration_ms"`
}

// WriteCampaignSummary writes the summary.json file to the run root.
func WriteCampaignSummary(runRoot string, tier Tier, startTime, endTime time.Time, results []*RunResult, skipped []SkipSummary) error {
	fingerprints := make(map[string]struct{})
	summary := CampaignSummary{
		SchemaVersion: SchemaVersion,
		Tier:          string(tier),
		StartTime:     startTime,
		EndTime:       endTime,
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		TotalRuns:     len(results),
		SkippedRuns:   len(skipped),
		Skipped:       skipped,
		AllPassed:     true,
	}

	for _, r := range results {
		rs := RunSummary{
			Instance:     r.Instance.Name,
			Seed:         r.Seed,
			Passed:       r.Passed,
			Failure:      r.FailureReason,
			Fingerprint:  r.Fingerprint,
			FailureClass: r.FailureClass,
			DurationMs:   r.Duration().Milliseconds(),
		}
		summary.Runs = append(summary.Runs, rs)

		// Count oracle stats
		if r.Instance.RequiresOracle {
			summary.OracleRequired++
		}

		if r.Passed {
			summary.PassedRuns++
		} else {
			summary.FailedRuns++
			summary.AllPassed = false
			if r.Fingerprint != "" {
				fingerprints[r.Fingerprint] = struct{}{}
			}

			// Classify failure for governance reporting
			switch r.FailureClass {
			case FailureClassNew:
				summary.NewFailures++
			case FailureClassKnown:
				summary.KnownFailures++
			case FailureClassDuplicate:
				summary.Duplicates++
				// Duplicate failures are, by definition, previously-seen fingerprints that are not
				// quarantined (otherwise they would be FailureClassKnown).
				summary.Unquarantined++
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

// ReadCampaignSummary reads summary.json from a run root.
func ReadCampaignSummary(runRoot string) (*CampaignSummary, error) {
	path := filepath.Join(runRoot, "summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s CampaignSummary
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// GovernanceReport is the machine-readable triage report for operators.
// Written to governance.json in the run root.
type GovernanceReport struct {
	SchemaVersion string `json:"schema_version"`

	// Summary counts
	TotalFailures    int `json:"total_failures"`
	NewFailures      int `json:"new_failures"`
	KnownFailures    int `json:"known_failures"`
	Duplicates       int `json:"duplicates"`
	Unquarantined    int `json:"unquarantined"`
	SkippedInstances int `json:"skipped_instances"`

	// Actionable items
	UnquarantinedDuplicates []GovernanceFailure `json:"unquarantined_duplicates,omitempty"`
	QuarantinedHits         []GovernanceFailure `json:"quarantined_hits,omitempty"`
	SkippedList             []SkipSummary       `json:"skipped,omitempty"`

	// Next steps for operators
	NextSteps string `json:"next_steps"`
}

// GovernanceFailure contains details about a failure for triage.
type GovernanceFailure struct {
	Instance    string `json:"instance"`
	Seed        int64  `json:"seed"`
	Fingerprint string `json:"fingerprint"`
	IssueID     string `json:"issue_id,omitempty"`
	FailureKind string `json:"failure_kind,omitempty"`
}

// WriteGovernanceReport writes the governance.json file to the run root.
// This artifact provides an at-a-glance triage view for operators.
func WriteGovernanceReport(runRoot string, results []*RunResult, skipped []SkipSummary, knownFailures *KnownFailures) error {
	report := GovernanceReport{
		SchemaVersion:    SchemaVersion,
		SkippedInstances: len(skipped),
		SkippedList:      skipped,
	}

	for _, r := range results {
		if r.Passed {
			continue
		}
		report.TotalFailures++

		switch r.FailureClass {
		case FailureClassNew:
			report.NewFailures++
		case FailureClassKnown:
			report.KnownFailures++
			// Add to quarantined hits
			issueID := ""
			if knownFailures != nil {
				if kf := knownFailures.Get(r.Fingerprint); kf != nil {
					issueID = kf.IssueID
				}
			}
			report.QuarantinedHits = append(report.QuarantinedHits, GovernanceFailure{
				Instance:    r.Instance.Name,
				Seed:        r.Seed,
				Fingerprint: r.Fingerprint,
				IssueID:     issueID,
				FailureKind: r.FailureKind,
			})
		case FailureClassDuplicate:
			report.Duplicates++
			report.Unquarantined++
			// Add to unquarantined duplicates
			report.UnquarantinedDuplicates = append(report.UnquarantinedDuplicates, GovernanceFailure{
				Instance:    r.Instance.Name,
				Seed:        r.Seed,
				Fingerprint: r.Fingerprint,
				FailureKind: r.FailureKind,
			})
		}
	}

	// Generate next steps message
	report.NextSteps = generateNextSteps(report)

	path := filepath.Join(runRoot, "governance.json")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal governance report: %w", err)
	}

	return os.WriteFile(path, data, 0o644)
}

// generateNextSteps creates an actionable message for operators.
func generateNextSteps(report GovernanceReport) string {
	if report.TotalFailures == 0 && report.SkippedInstances == 0 {
		return "All tests passed. No action required."
	}

	var steps []string

	if report.Unquarantined > 0 {
		steps = append(steps,
			"URGENT: Unquarantined repeat failures detected. "+
				"Either fix the underlying issues or add quarantine entries with issue IDs.")
	}

	if report.NewFailures > 0 {
		steps = append(steps,
			"New failures detected. Investigate and either fix or add to known-failures with issue tracking.")
	}

	if report.KnownFailures > 0 {
		steps = append(steps,
			"Known quarantined failures occurred. Verify linked issues are being tracked.")
	}

	if report.SkippedInstances > 0 {
		steps = append(steps,
			"Some instances were skipped due to skip policies. Review if they should be re-enabled.")
	}

	if len(steps) == 0 {
		return "Review results and take appropriate action."
	}

	var builder strings.Builder
	for i, step := range steps {
		builder.WriteString(fmt.Sprintf("%d. %s", i+1, step))
		if i < len(steps)-1 {
			builder.WriteString(" ")
		}
	}
	return builder.String()
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
