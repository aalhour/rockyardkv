package campaign

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RecheckResult captures the outcome of re-evaluating an existing run.
type RecheckResult struct {
	// RecheckTime is when the recheck was performed.
	RecheckTime time.Time `json:"recheck_time"`

	// RecheckSchemaVersion is the schema version used for this recheck.
	RecheckSchemaVersion string `json:"recheck_schema_version"`

	// OracleRecheck contains the oracle re-check outcome.
	OracleRecheck *OracleRecheckResult `json:"oracle_recheck,omitempty"`

	// MarkerRecheck contains the verification marker re-parse outcome.
	MarkerRecheck *MarkerRecheckResult `json:"marker_recheck,omitempty"`

	// FingerprintRecomputed is the recomputed fingerprint (if failure).
	FingerprintRecomputed string `json:"fingerprint_recomputed,omitempty"`

	// PolicyResult contains the pass/fail evaluation with current stop conditions.
	PolicyResult *PolicyRecheckResult `json:"policy_result"`
}

// OracleRecheckResult contains oracle tool re-check details.
type OracleRecheckResult struct {
	// Performed indicates if oracle check was run.
	Performed bool `json:"performed"`

	// Skipped indicates oracle check was skipped (not required or oracle unavailable).
	Skipped bool `json:"skipped,omitempty"`

	// SkipReason explains why oracle check was skipped.
	SkipReason string `json:"skip_reason,omitempty"`

	// OK indicates if the oracle check passed.
	OK bool `json:"ok"`

	// ExitCode is the oracle tool exit code.
	ExitCode int `json:"exit_code,omitempty"`

	// StdoutPath is the path to captured stdout.
	StdoutPath string `json:"stdout_path,omitempty"`

	// StderrPath is the path to captured stderr.
	StderrPath string `json:"stderr_path,omitempty"`

	// Summary is a brief inline summary.
	Summary string `json:"summary,omitempty"`
}

// MarkerRecheckResult contains verification marker re-parse details.
type MarkerRecheckResult struct {
	// Passed indicates if verification markers indicate success.
	Passed bool `json:"passed"`

	// Reason explains the result.
	Reason string `json:"reason"`
}

// PolicyRecheckResult contains stop-condition policy evaluation.
type PolicyRecheckResult struct {
	// Passed indicates if the run passes current policy.
	Passed bool `json:"passed"`

	// Reason explains why it passed or failed.
	Reason string `json:"reason"`

	// Verified indicates if the run can be marked as VERIFIED.
	// False when oracle is required but missing.
	Verified bool `json:"verified"`
}

// Rechecker re-evaluates existing run artifacts.
type Rechecker struct {
	Oracle         *Oracle
	StopConditions map[string]StopCondition // instance name -> stop condition
}

// NewRechecker creates a new Rechecker.
func NewRechecker(oracle *Oracle) *Rechecker {
	return &Rechecker{
		Oracle:         oracle,
		StopConditions: make(map[string]StopCondition),
	}
}

// RecheckRun re-evaluates a single run directory.
func (r *Rechecker) RecheckRun(runDir string) (*RecheckResult, error) {
	// Read existing run.json
	runJSONPath := filepath.Join(runDir, "run.json")
	data, err := os.ReadFile(runJSONPath)
	if err != nil {
		return nil, fmt.Errorf("read run.json: %w", err)
	}

	var artifact RunArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, fmt.Errorf("parse run.json: %w", err)
	}

	result := &RecheckResult{
		RecheckTime:          time.Now(),
		RecheckSchemaVersion: SchemaVersion,
	}

	// Determine stop conditions for this instance
	stop := r.getStopCondition(artifact.Instance)

	// Re-check oracle if required
	result.OracleRecheck = r.recheckOracle(runDir, stop)

	// Re-parse verification markers
	result.MarkerRecheck = r.recheckMarkers(runDir, artifact, stop)

	// Recompute fingerprint if failure
	if !artifact.Passed {
		logPath := filepath.Join(runDir, "output.log")
		result.FingerprintRecomputed = ComputeFingerprint(
			artifact.Instance,
			artifact.Seed,
			artifact.FailureKind,
			artifact.Failure,
			logPath,
		)
	}

	// Evaluate policy
	result.PolicyResult = r.evaluatePolicy(artifact, result, stop)

	// Write recheck.json
	recheckPath := filepath.Join(runDir, "recheck.json")
	recheckData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return result, fmt.Errorf("marshal recheck.json: %w", err)
	}
	if err := os.WriteFile(recheckPath, recheckData, 0o644); err != nil {
		return result, fmt.Errorf("write recheck.json: %w", err)
	}

	return result, nil
}

// getStopCondition returns the stop condition for an instance.
func (r *Rechecker) getStopCondition(instanceName string) StopCondition {
	if stop, ok := r.StopConditions[instanceName]; ok {
		return stop
	}
	return DefaultStopCondition()
}

// recheckOracle re-runs oracle consistency check.
func (r *Rechecker) recheckOracle(runDir string, stop StopCondition) *OracleRecheckResult {
	result := &OracleRecheckResult{}

	if !stop.RequireOracleCheckConsistencyOK {
		result.Skipped = true
		result.SkipReason = "oracle check not required by stop condition"
		return result
	}

	if r.Oracle == nil || !r.Oracle.Available() {
		result.Skipped = true
		result.SkipReason = "oracle not available"
		return result
	}

	// Find DB path
	dbPath := discoverDBPathInDir(runDir)
	if dbPath == "" {
		result.Skipped = true
		result.SkipReason = "no database directory found"
		return result
	}

	// Run oracle check
	result.Performed = true
	oracleResult := r.Oracle.CheckConsistency(dbPath)
	result.ExitCode = oracleResult.ExitCode
	result.OK = oracleResult.OK()
	result.Summary = strings.TrimSpace(oracleResult.Stdout)
	if len(result.Summary) > 200 {
		result.Summary = result.Summary[:200] + "..."
	}

	// Write oracle output to recheck directory
	oracleDir := filepath.Join(runDir, "recheck_oracle")
	_ = os.MkdirAll(oracleDir, 0o755)
	stdoutPath := filepath.Join(oracleDir, "stdout.txt")
	stderrPath := filepath.Join(oracleDir, "stderr.txt")
	_ = os.WriteFile(stdoutPath, []byte(oracleResult.Stdout), 0o644)
	_ = os.WriteFile(stderrPath, []byte(oracleResult.Stderr), 0o644)
	result.StdoutPath = stdoutPath
	result.StderrPath = stderrPath

	return result
}

// recheckMarkers re-parses log files for verification markers.
func (r *Rechecker) recheckMarkers(runDir string, artifact RunArtifact, stop StopCondition) *MarkerRecheckResult {
	result := &MarkerRecheckResult{}

	if !stop.RequireFinalVerificationPass {
		result.Passed = true
		result.Reason = "verification markers not required by stop condition"
		return result
	}

	// Find log file
	logPath := filepath.Join(runDir, "output.log")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		result.Passed = false
		result.Reason = "output.log not found"
		return result
	}

	// Determine tool from tags or instance name
	tool := determineTool(artifact)

	if finalVerificationPassed(tool, logPath) {
		result.Passed = true
		result.Reason = "final verification marker found"
	} else {
		result.Passed = false
		result.Reason = "final verification marker not found in output.log"
	}

	return result
}

// evaluatePolicy evaluates pass/fail using current stop conditions.
func (r *Rechecker) evaluatePolicy(artifact RunArtifact, recheck *RecheckResult, stop StopCondition) *PolicyRecheckResult {
	result := &PolicyRecheckResult{
		Passed:   true,
		Verified: true,
	}

	reasons := []string{}

	// Check termination
	if stop.RequireTermination && artifact.ExitCode == -1 {
		result.Passed = false
		reasons = append(reasons, "process did not terminate cleanly")
	}

	// Check oracle: if required, any skip or failure is a problem.
	// Contract: when oracle is required, Verified=false unless oracle check was performed and passed.
	if stop.RequireOracleCheckConsistencyOK {
		if recheck.OracleRecheck == nil {
			result.Verified = false
			reasons = append(reasons, "NOT VERIFIED: oracle required but check not performed")
		} else if recheck.OracleRecheck.Skipped {
			// Any skip reason when oracle is required means NOT VERIFIED
			result.Verified = false
			reasons = append(reasons, "NOT VERIFIED: "+recheck.OracleRecheck.SkipReason)
		} else if recheck.OracleRecheck.Performed && !recheck.OracleRecheck.OK {
			result.Passed = false
			reasons = append(reasons, "oracle consistency check failed")
		}
	}

	// Check verification markers
	if stop.RequireFinalVerificationPass {
		if recheck.MarkerRecheck != nil && !recheck.MarkerRecheck.Passed {
			result.Passed = false
			reasons = append(reasons, recheck.MarkerRecheck.Reason)
		}
	}

	if len(reasons) == 0 {
		result.Reason = "all stop conditions satisfied"
	} else {
		result.Reason = strings.Join(reasons, "; ")
	}

	return result
}

// discoverDBPathInDir finds a database directory within a run directory.
func discoverDBPathInDir(runDir string) string {
	// Check common locations
	candidates := []string{
		filepath.Join(runDir, "db"),
		filepath.Join(runDir, "artifacts", "db"),
	}

	for _, candidate := range candidates {
		currentPath := filepath.Join(candidate, "CURRENT")
		if _, err := os.Stat(currentPath); err == nil {
			return candidate
		}
	}

	// Walk to find CURRENT file
	var dbPath string
	_ = filepath.WalkDir(runDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err // Propagate error to stop walk
		}
		if d.Name() == "CURRENT" && !d.IsDir() {
			dbPath = filepath.Dir(path)
			return filepath.SkipAll
		}
		return nil
	})

	return dbPath
}

// determineTool infers the tool from artifact data.
func determineTool(artifact RunArtifact) Tool {
	if artifact.Tags != nil && artifact.Tags.Tool != "" {
		return Tool(artifact.Tags.Tool)
	}

	// Infer from instance name
	name := artifact.Instance
	if strings.HasPrefix(name, "stress") || strings.Contains(name, ".stress.") {
		return ToolStress
	}
	if strings.HasPrefix(name, "crash") || strings.Contains(name, ".crash.") {
		return ToolCrash
	}
	if strings.HasPrefix(name, "golden") {
		return ToolGolden
	}
	if strings.HasPrefix(name, "adversarial") || strings.Contains(name, ".adversarial.") {
		return ToolAdversarial
	}

	return ToolStress // default
}

// RecheckCampaign re-evaluates all runs in a campaign run root.
func (r *Rechecker) RecheckCampaign(runRoot string) ([]RecheckResult, error) {
	var results []RecheckResult

	// Find all run directories (those containing run.json)
	err := filepath.WalkDir(runRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr // Propagate error
		}
		if d.Name() == "run.json" && !d.IsDir() {
			runDir := filepath.Dir(path)
			result, recheckErr := r.RecheckRun(runDir)
			if recheckErr == nil {
				results = append(results, *result)
			}
			// Continue walking even on recheck error
		}
		return nil
	})

	return results, err
}
