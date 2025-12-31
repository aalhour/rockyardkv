package campaign

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RunnerConfig configures the campaign runner.
type RunnerConfig struct {
	// Tier is the intensity level.
	Tier Tier

	// RunRoot is the root directory for all run artifacts.
	RunRoot string

	// BinDir is the directory containing test binaries.
	// Defaults to "./bin" if empty.
	BinDir string

	// Oracle is the C++ oracle for consistency checks.
	// May be nil if oracle is not available.
	Oracle *Oracle

	// KnownFailures tracks failure fingerprints for deduplication.
	KnownFailures *KnownFailures

	// FailFast stops the campaign on the first failure.
	FailFast bool

	// Verbose enables verbose output.
	Verbose bool

	// Output is where to write progress messages.
	Output io.Writer

	// InstanceTimeout is the per-instance timeout in seconds.
	// If 0, uses the default for the tier.
	InstanceTimeout int

	// GlobalTimeout is the global campaign timeout in seconds.
	// If 0, uses the default for the tier.
	GlobalTimeout int
}

// Runner executes campaign instances.
type Runner struct {
	config RunnerConfig
}

// NewRunner creates a new campaign runner.
func NewRunner(config RunnerConfig) *Runner {
	if config.Output == nil {
		config.Output = os.Stdout
	}
	if config.BinDir == "" {
		config.BinDir = "./bin"
	}
	if config.InstanceTimeout == 0 {
		config.InstanceTimeout = InstanceTimeout(config.Tier)
	}
	if config.GlobalTimeout == 0 {
		config.GlobalTimeout = GlobalTimeout(config.Tier)
	}
	return &Runner{config: config}
}

// Run executes all instances for the configured tier.
// Returns the campaign summary and any error.
func (r *Runner) Run(ctx context.Context) (*CampaignSummary, error) {
	instances := GetInstances(r.config.Tier)
	return r.RunInstances(ctx, instances)
}

// RunGroup executes instances matching the group prefix.
// If group is empty, runs all instances for the tier.
// If group starts with "status.", runs status instances.
func (r *Runner) RunGroup(ctx context.Context, group string) (*CampaignSummary, error) {
	var instances []Instance

	if group == "" {
		instances = GetInstances(r.config.Tier)
	} else if len(group) >= 6 && group[:6] == "status" {
		instances = GetStatusInstances(group)
	} else {
		// Filter regular instances by group
		all := GetInstances(r.config.Tier)
		for _, inst := range all {
			if matchesGroup(inst.Name, group) {
				instances = append(instances, inst)
			}
		}
	}

	if len(instances) == 0 {
		return nil, fmt.Errorf("no instances match group %q", group)
	}

	return r.RunInstances(ctx, instances)
}

// RunInstances executes the specified instances.
func (r *Runner) RunInstances(ctx context.Context, instances []Instance) (*CampaignSummary, error) {
	startTime := time.Now()

	// Create run root
	if err := EnsureDir(r.config.RunRoot); err != nil {
		return nil, fmt.Errorf("create run root: %w", err)
	}

	// Gate check: verify oracle for instances that require it
	for i := range instances {
		if err := GateCheck(&instances[i], r.config.Oracle); err != nil {
			return nil, err
		}
	}

	var results []*RunResult

	// Run all instances
	stopNow := false
	for i := range instances {
		instance := &instances[i]

		// Run each seed
		for _, seed := range instance.Seeds {
			select {
			case <-ctx.Done():
				r.log("campaign cancelled")
				endTime := time.Now()
				if err := WriteCampaignSummary(r.config.RunRoot, r.config.Tier, startTime, endTime, results); err != nil {
					r.log("warning: failed to write campaign summary: %v", err)
				}
				return nil, ctx.Err()
			default:
			}

			result := r.runInstance(ctx, instance, seed)
			results = append(results, result)

			if err := WriteRunArtifact(result); err != nil {
				r.log("warning: failed to write run artifact: %v", err)
			}

			if !result.Passed && r.config.FailFast {
				r.log("fail-fast: stopping after first failure")
				stopNow = true
				break
			}
		}

		if stopNow {
			break
		}
	}

	endTime := time.Now()

	// Write campaign summary
	if err := WriteCampaignSummary(r.config.RunRoot, r.config.Tier, startTime, endTime, results); err != nil {
		r.log("warning: failed to write campaign summary: %v", err)
	}

	// Build summary struct
	fingerprints := make(map[string]struct{})
	summary := &CampaignSummary{
		Tier:       string(r.config.Tier),
		StartTime:  startTime,
		EndTime:    endTime,
		DurationMs: endTime.Sub(startTime).Milliseconds(),
		TotalRuns:  len(results),
		AllPassed:  true,
	}

	for _, result := range results {
		rs := RunSummary{
			Instance:    result.Instance.Name,
			Seed:        result.Seed,
			Passed:      result.Passed,
			Failure:     result.FailureReason,
			Fingerprint: result.Fingerprint,
			DurationMs:  result.Duration().Milliseconds(),
		}
		summary.Runs = append(summary.Runs, rs)

		if result.Passed {
			summary.PassedRuns++
		} else {
			summary.FailedRuns++
			summary.AllPassed = false
			if result.Fingerprint != "" {
				fingerprints[result.Fingerprint] = struct{}{}
			}
		}
	}
	summary.UniqueErrors = len(fingerprints)

	return summary, nil
}

// runInstance runs a single instance with a specific seed.
func (r *Runner) runInstance(ctx context.Context, instance *Instance, seed int64) *RunResult {
	runDir := instance.RunDir(r.config.RunRoot, seed)
	if err := EnsureDir(runDir); err != nil {
		return &RunResult{
			Instance:      instance,
			Seed:          seed,
			RunDir:        runDir,
			StartTime:     time.Now(),
			EndTime:       time.Now(),
			ExitCode:      -1,
			Passed:        false,
			FailureReason: fmt.Sprintf("create run dir: %v", err),
		}
	}

	r.log("running %s seed=%d", instance.Name, seed)

	startTime := time.Now()

	// Create timeout context
	timeout := time.Duration(r.config.InstanceTimeout) * time.Second
	instanceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Build command with resolved binary path
	binaryPath := instance.BinaryPath(r.config.BinDir)
	var cmd *exec.Cmd
	if instance.IsGoTest() {
		cmd = exec.CommandContext(instanceCtx, "go", instance.ResolveArgs(runDir, seed)...)
		binaryPath = "go" // go test uses system go
	} else {
		args := instance.ResolveArgs(runDir, seed)
		cmd = exec.CommandContext(instanceCtx, binaryPath, args...)
	}

	// Set environment
	cmd.Env = os.Environ()
	for k, v := range instance.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	// Capture output to log file
	logPath := filepath.Join(runDir, "output.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return &RunResult{
			Instance:      instance,
			Seed:          seed,
			RunDir:        runDir,
			StartTime:     startTime,
			EndTime:       time.Now(),
			ExitCode:      -1,
			Passed:        false,
			FailureReason: fmt.Sprintf("create log file: %v", err),
		}
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Run the command
	err = cmd.Run()
	_ = logFile.Close()
	endTime := time.Now()

	result := &RunResult{
		Instance:   instance,
		Seed:       seed,
		RunDir:     runDir,
		BinaryPath: binaryPath,
		StartTime:  startTime,
		EndTime:    endTime,
	}

	// Check exit code
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
		} else if instanceCtx.Err() == context.DeadlineExceeded {
			result.ExitCode = -1
			result.FailureReason = "timeout"
		} else {
			result.ExitCode = -1
			result.FailureReason = err.Error()
		}
	}

	// Check stop conditions
	r.checkStopConditions(result, instance, logPath)

	// Classify failure kind and compute fingerprint
	if !result.Passed && result.FailureReason != "" {
		result.FailureKind = classifyFailureKind(result.FailureReason, result.ExitCode)
		result.Fingerprint = ComputeFingerprint(
			instance.Name,
			seed,
			result.FailureKind,
			result.FailureReason,
			logPath,
		)

		if r.config.KnownFailures != nil && instance.Stop.DedupeByFingerprint {
			isDup := r.config.KnownFailures.IsDuplicate(result.Fingerprint)
			result.IsDuplicate = isDup
			if isDup {
				r.log("  duplicate failure: %s (fingerprint %s)", result.FailureReason, result.Fingerprint)
			} else {
				r.config.KnownFailures.Record(result.Fingerprint, instance.Name, startTime.Format(time.RFC3339))
				r.log("  NEW failure: %s (fingerprint %s)", result.FailureReason, result.Fingerprint)
			}
		}
	}

	if result.Passed {
		r.log("  PASS (%s)", result.Duration())
	} else {
		r.log("  FAIL: %s (%s)", result.FailureReason, result.Duration())
	}

	return result
}

// checkStopConditions evaluates the stop conditions and sets result.Passed.
func (r *Runner) checkStopConditions(result *RunResult, instance *Instance, logPath string) {
	stop := instance.Stop

	// For stresstest with fault injection, a non-zero exit can be expected due to
	// injected operational errors even if final verification is clean.
	// We only relax "exit code must be 0" for stresstest fault-injection runs.
	allowNonZeroExit := instance.Tool == ToolStress && instance.FaultModel.Kind != FaultNone

	// Check termination requirement (exit code policy)
	if stop.RequireTermination && result.ExitCode != 0 && !allowNonZeroExit {
		if result.FailureReason == "" {
			result.FailureReason = fmt.Sprintf("non-zero exit code: %d", result.ExitCode)
		}
		result.Passed = false
		return
	}

	// Check tool-specific final verification requirement.
	// This is the "signal" that the tool's end-of-run verification actually passed.
	if stop.RequireFinalVerificationPass {
		switch instance.Tool {
		case ToolStress, ToolCrash:
			if !finalVerificationPassed(instance.Tool, logPath) {
				if result.FailureReason == "" {
					result.FailureReason = "final verification not observed as passed in output log"
				}
				result.Passed = false
				return
			}
		default:
			// For other tools, RequireTermination + exit code policy is the signal.
			// Nothing additional to check here.
		}
	}

	// Check oracle consistency
	if stop.RequireOracleCheckConsistencyOK && instance.RequiresOracle && r.config.Oracle != nil {
		// Try common DB snapshot paths
		dbPath := r.discoverDBPath(result.RunDir)
		if dbPath == "" {
			if result.FailureReason == "" {
				result.FailureReason = "oracle checkconsistency required but database snapshot path was not found"
			}
			result.Passed = false
			return
		}

		oracleResult := r.config.Oracle.CheckConsistency(dbPath)
		result.OracleResult = oracleResult

		// Write oracle artifacts to oracle/ subdirectory
		if err := r.writeOracleArtifacts(result.RunDir, oracleResult); err != nil {
			r.log("warning: failed to write oracle artifacts: %v", err)
		}

		if !oracleResult.OK() {
			result.FailureReason = fmt.Sprintf("oracle checkconsistency failed: %s", oracleResult.Stderr)
			result.Passed = false
			return
		}
	}

	// If we get here and no failure was recorded, it passed
	if result.FailureReason == "" {
		result.Passed = true
	}
}

// finalVerificationPassed returns true if the tool's output log indicates that
// the tool's end-of-run verification passed.
//
// This is intentionally string-based to avoid tool-specific file contracts.
func finalVerificationPassed(tool Tool, logPath string) bool {
	if logPath == "" {
		return false
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		return false
	}
	s := string(data)
	switch tool {
	case ToolStress:
		// stresstest can exit non-zero due to expected injected operational errors
		// even when final verification is clean. Treat "0 failures" in the final
		// verification section as the success signal.
		return strings.Contains(s, "Running final verification") && strings.Contains(s, ", 0 failures")
	case ToolCrash:
		return strings.Contains(s, "Final verification passed")
	default:
		return false
	}
}

// discoverDBPath finds the database snapshot path in the run directory.
// Tries common layouts used by different tools.
func (r *Runner) discoverDBPath(runDir string) string {
	// Common paths used by test tools
	candidates := []string{
		filepath.Join(runDir, "db"),                     // Standard layout
		filepath.Join(runDir, "artifacts/db"),           // crashtest layout
		filepath.Join(runDir, "db_sync"),                // status.durability.wal_sync layout
		filepath.Join(runDir, "db_faultfs_disable_wal"), // disablewal layout
	}

	for _, path := range candidates {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			// Prefer the directory that actually contains CURRENT.
			if hasCurrentFile(path) {
				return path
			}
			// Some tools/DB paths create a nested "db/" subdirectory containing the actual DB.
			nested := filepath.Join(path, "db")
			if hasCurrentFile(nested) {
				return nested
			}
		}
	}

	return ""
}

func hasCurrentFile(dbDir string) bool {
	if dbDir == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(dbDir, "CURRENT"))
	return err == nil
}

// writeOracleArtifacts writes oracle output to stable files in oracle/ subdirectory.
func (r *Runner) writeOracleArtifacts(runDir string, result *ToolResult) error {
	oracleDir := filepath.Join(runDir, "oracle")
	if err := EnsureDir(oracleDir); err != nil {
		return err
	}

	// Write ldb checkconsistency output
	if result.Stdout != "" {
		if err := os.WriteFile(
			filepath.Join(oracleDir, "ldb_checkconsistency.stdout.txt"),
			[]byte(result.Stdout),
			0o644,
		); err != nil {
			return err
		}
	}

	if result.Stderr != "" {
		if err := os.WriteFile(
			filepath.Join(oracleDir, "ldb_checkconsistency.stderr.txt"),
			[]byte(result.Stderr),
			0o644,
		); err != nil {
			return err
		}
	}

	// Write exit code
	exitCodeContent := fmt.Sprintf("%d\n", result.ExitCode)
	return os.WriteFile(
		filepath.Join(oracleDir, "ldb_checkconsistency.exitcode"),
		[]byte(exitCodeContent),
		0o644,
	)
}

// log writes a message to the output.
func (r *Runner) log(format string, args ...any) {
	fmt.Fprintf(r.config.Output, format+"\n", args...)
}

// classifyFailureKind categorizes a failure for fingerprinting.
func classifyFailureKind(failureReason string, exitCode int) string {
	switch {
	case exitCode == -1:
		return "timeout"
	case exitCode == 137:
		return "killed" // SIGKILL
	case exitCode == 143:
		return "terminated" // SIGTERM
	case containsIgnoreCase(failureReason, "oracle") || containsIgnoreCase(failureReason, "consistency"):
		return "oracle_failure"
	case containsIgnoreCase(failureReason, "verification"):
		return "verification_failure"
	case containsIgnoreCase(failureReason, "corruption"):
		return "corruption"
	case containsIgnoreCase(failureReason, "timeout"):
		return "timeout"
	default:
		return "exit_error"
	}
}

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
