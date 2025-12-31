// runner.go implements the core campaign execution engine.
//
// The Runner orchestrates instance execution with:
//   - Tool invocation with timeout and cancellation
//   - Artifact persistence (run.json, logs, DB snapshots)
//   - Oracle gating (require ldb checkconsistency before pass)
//   - Failure fingerprinting and deduplication
//   - Skip policy enforcement
//   - Summary generation (summary.json, governance.json)
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

	// Trace controls trace capture behavior.
	Trace TraceConfig

	// Minimize controls minimization behavior.
	Minimize MinimizeConfig

	// Filter restricts which instances to run.
	// If nil, all instances are run.
	Filter *Filter

	// RequireQuarantine enforces that repeat failures must be quarantined.
	// If true, unquarantined duplicate failures cause the campaign to fail.
	RequireQuarantine bool

	// SkipPolicies defines instance-level skip policies.
	// Instances matching a skip policy are not run and are recorded as skipped.
	SkipPolicies *InstanceSkipPolicies
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
	instances = FilterInstances(instances, r.config.Filter)
	return r.RunInstances(ctx, instances)
}

// RunGroup executes instances matching the group prefix.
// If group is empty, runs all instances for the tier.
// If group starts with "status.", runs status instances.
// Special groups "status.composite" and "status.sweep" run composite/sweep instances.
func (r *Runner) RunGroup(ctx context.Context, group string) (*CampaignSummary, error) {
	// Handle composite instances
	if group == "status.composite" || (len(group) > 16 && group[:16] == "status.composite") {
		composites := StatusCompositeInstances()
		if group != "status.composite" {
			// Filter by specific composite name
			var filtered []CompositeInstance
			for _, c := range composites {
				if matchesGroup(c.Name, group) {
					filtered = append(filtered, c)
				}
			}
			composites = filtered
		}
		if len(composites) == 0 {
			return nil, fmt.Errorf("no composite instances match group %q", group)
		}
		return r.RunCompositeInstances(ctx, composites)
	}

	// Handle sweep instances
	if group == "status.sweep" || (len(group) > 12 && group[:12] == "status.sweep") {
		sweeps := StatusSweepInstances()
		if group != "status.sweep" {
			// Filter by specific sweep name
			var filtered []SweepInstance
			for _, s := range sweeps {
				if matchesGroup(s.Base.Name, group) {
					filtered = append(filtered, s)
				}
			}
			sweeps = filtered
		}
		if len(sweeps) == 0 {
			return nil, fmt.Errorf("no sweep instances match group %q", group)
		}
		return r.RunSweepInstances(ctx, sweeps)
	}

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

	// Apply tag filter if configured
	instances = FilterInstances(instances, r.config.Filter)
	if len(instances) == 0 {
		return nil, fmt.Errorf("no instances match group %q after applying filter", group)
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

	// Track skipped instances for summary reporting
	var skipped []SkipSummary

	// Gate check and skip policy: verify oracle and check skip policies
	var instancesToRun []Instance
	for i := range instances {
		instance := &instances[i]

		// Check skip policy first (before oracle gate)
		if r.config.SkipPolicies != nil {
			if skipResult := r.config.SkipPolicies.ShouldSkip(instance); skipResult != nil {
				r.log("⏭️  SKIP %s: %s", instance.Name, skipResult.Reason)
				skipped = append(skipped, SkipSummary{
					Instance: skipResult.InstanceName,
					Reason:   skipResult.Reason,
					IssueID:  skipResult.IssueID,
				})
				continue
			}
		}

		// Oracle gate check
		if err := GateCheck(instance, r.config.Oracle); err != nil {
			return nil, err
		}

		instancesToRun = append(instancesToRun, *instance)
	}

	var results []*RunResult

	// Run all non-skipped instances
	stopNow := false
	for i := range instancesToRun {
		instance := &instancesToRun[i]

		// Run each seed
		for _, seed := range instance.Seeds {
			select {
			case <-ctx.Done():
				r.log("campaign cancelled")
				endTime := time.Now()
				if err := WriteCampaignSummary(r.config.RunRoot, r.config.Tier, startTime, endTime, results, skipped); err != nil {
					r.log("warning: failed to write campaign summary: %v", err)
				}
				return nil, ctx.Err()
			default:
			}

			result := r.runInstance(ctx, instance, seed)

			// Run minimization if applicable
			if r.config.Minimize.Enabled && !result.Passed && !result.IsDuplicate {
				minimizer := NewMinimizer(r, r.config.Minimize)
				if minimizer.ShouldMinimize(result) {
					r.log("  minimizing failure...")
					minResult, err := minimizer.Minimize(ctx, result)
					if err != nil {
						r.log("  minimization failed: %v", err)
					} else {
						result.MinimizeResult = minResult
						if minResult.Success {
							r.log("  minimized to: duration=%s threads=%d keys=%d",
								minResult.FinalDuration, minResult.FinalThreads, minResult.FinalKeys)
						} else {
							r.log("  minimization did not reduce parameters")
						}
					}
				}
			}

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
	if err := WriteCampaignSummary(r.config.RunRoot, r.config.Tier, startTime, endTime, results, skipped); err != nil {
		r.log("warning: failed to write campaign summary: %v", err)
	}

	// Write governance report for artifact-first triage
	if err := WriteGovernanceReport(r.config.RunRoot, results, skipped, r.config.KnownFailures); err != nil {
		r.log("warning: failed to write governance report: %v", err)
	}

	// Return the same summary structure we persist to disk, so CLI output and enforcement
	// (e.g., -require-quarantine) reflect the canonical artifact.
	if summary, err := ReadCampaignSummary(r.config.RunRoot); err == nil {
		return summary, nil
	}

	// Fallback: return a minimal in-memory summary.
	fingerprints := make(map[string]struct{})
	summary := &CampaignSummary{
		Tier:        string(r.config.Tier),
		StartTime:   startTime,
		EndTime:     endTime,
		DurationMs:  endTime.Sub(startTime).Milliseconds(),
		TotalRuns:   len(results),
		SkippedRuns: len(skipped),
		AllPassed:   true,
	}
	for _, result := range results {
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

	// Create trace directory if trace capture is enabled
	traceConfig := r.config.Trace
	if r.config.Minimize.Enabled {
		// Auto-enable trace when minimization is enabled
		traceConfig.Enabled = true
	}
	if traceConfig.Enabled && instance.Tool == ToolStress {
		if err := EnsureTraceDir(runDir, traceConfig); err != nil {
			r.log("warning: failed to create trace dir: %v", err)
		}
	}

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
		// Inject trace args for stresstest if trace capture is enabled
		if traceConfig.Enabled && instance.Tool == ToolStress {
			args, _ = InjectTraceArgs(args, runDir, traceConfig)
		}
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
			result.FailureClass, result.QuarantinePolicy, result.IsDuplicate = r.classifyFailure(
				result.Fingerprint, instance.Name, startTime.Format(time.RFC3339))
		} else {
			// Default classification for runs without dedupe
			result.FailureClass = FailureClassNew
		}
	}

	// Collect trace result and check truncation
	if traceConfig.Enabled && instance.Tool == ToolStress {
		dbPath := r.discoverDBPath(runDir)
		result.TraceResult = CollectTraceResult(runDir, dbPath, r.config.BinDir, traceConfig)

		// Check trace size and write truncation marker if needed
		if result.TraceResult != nil && result.TraceResult.Path != "" {
			size, exceeded, err := CheckTraceSize(result.TraceResult.Path, traceConfig)
			if err == nil {
				result.TraceResult.BytesWritten = size
				if exceeded {
					result.TraceResult.Truncated = true
					if err := WriteTruncatedMarker(runDir, traceConfig, size); err != nil {
						r.log("warning: failed to write truncated marker: %v", err)
					}
				}
			}
		}

		// Write replay script if trace was captured (use TraceResult.Path, not local tracePath)
		if result.TraceResult != nil && result.TraceResult.Path != "" && dbPath != "" {
			if err := WriteReplayScript(runDir, result.TraceResult.Path, dbPath, r.config.BinDir); err != nil {
				r.log("warning: failed to write replay script: %v", err)
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
// Files are written even when stdout/stderr are empty to avoid "paper success"
// where empty results don't produce evidence files.
func (r *Runner) writeOracleArtifacts(runDir string, result *ToolResult) error {
	oracleDir := filepath.Join(runDir, "oracle")
	if err := EnsureDir(oracleDir); err != nil {
		return err
	}

	// Always write stdout file (even if empty)
	if err := os.WriteFile(
		filepath.Join(oracleDir, "ldb_checkconsistency.stdout.txt"),
		[]byte(result.Stdout),
		0o644,
	); err != nil {
		return err
	}

	// Always write stderr file (even if empty)
	if err := os.WriteFile(
		filepath.Join(oracleDir, "ldb_checkconsistency.stderr.txt"),
		[]byte(result.Stderr),
		0o644,
	); err != nil {
		return err
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

// classifyFailure determines the failure class and quarantine policy for a fingerprint.
// Returns (FailureClass, QuarantinePolicy, isDuplicate).
func (r *Runner) classifyFailure(fingerprint, instanceName, timestamp string) (FailureClass, QuarantinePolicy, bool) {
	kf := r.config.KnownFailures
	if kf == nil {
		return FailureClassNew, QuarantineNone, false
	}

	existing := kf.Get(fingerprint)
	if existing == nil {
		// First time seeing this fingerprint - record it
		kf.Record(fingerprint, instanceName, timestamp)
		r.log("  NEW failure (fingerprint %s)", fingerprint)
		return FailureClassNew, QuarantineNone, false
	}

	// Fingerprint is known
	if existing.Quarantine != QuarantineNone {
		// Known and quarantined
		r.log("  known failure: %s (quarantine=%s, issue=%s)", fingerprint, existing.Quarantine, existing.IssueID)
		kf.Record(fingerprint, instanceName, timestamp) // Update count
		return FailureClassKnown, existing.Quarantine, true
	}

	// Known but not quarantined - this is a duplicate
	r.log("  duplicate failure (fingerprint %s, seen %d times)", fingerprint, existing.Count+1)
	kf.Record(fingerprint, instanceName, timestamp) // Update count
	return FailureClassDuplicate, QuarantineNone, true
}

// classifyFailureKind categorizes a failure for fingerprinting.
func classifyFailureKind(failureReason string, exitCode int) string {
	switch {
	case exitCode == -1 && containsIgnoreCase(failureReason, "timeout"):
		return "timeout"
	case exitCode == -1:
		// When we fail to start the tool (for example, exec errors), ExitCode is -1.
		// Treat these as generic execution failures, not timeouts.
		return "exit_error"
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

// RunCompositeInstances executes composite (multi-step) instances with Phase-1-grade artifacts.
func (r *Runner) RunCompositeInstances(ctx context.Context, composites []CompositeInstance) (*CampaignSummary, error) {
	startTime := time.Now()

	// Create run root
	if err := EnsureDir(r.config.RunRoot); err != nil {
		return nil, fmt.Errorf("create run root: %w", err)
	}

	var results []*RunResult
	fingerprints := make(map[string]struct{})

	for _, composite := range composites {
		for _, seed := range composite.Seeds {
			result := r.runCompositeInstance(ctx, &composite, seed)
			results = append(results, result)

			if !result.Passed && result.Fingerprint != "" {
				fingerprints[result.Fingerprint] = struct{}{}
			}

			if !result.Passed && r.config.FailFast {
				break
			}
		}
		if r.config.FailFast && len(results) > 0 && !results[len(results)-1].Passed {
			break
		}
	}

	endTime := time.Now()

	// Write campaign summary file
	if err := WriteCampaignSummary(r.config.RunRoot, r.config.Tier, startTime, endTime, results, nil); err != nil {
		r.log("warning: failed to write campaign summary: %v", err)
	}

	// Return canonical persisted summary.
	if summary, err := ReadCampaignSummary(r.config.RunRoot); err == nil {
		return summary, nil
	}

	// Fallback: return a minimal in-memory summary.
	summary := &CampaignSummary{
		Tier:         string(r.config.Tier),
		StartTime:    startTime,
		EndTime:      endTime,
		DurationMs:   endTime.Sub(startTime).Milliseconds(),
		TotalRuns:    len(results),
		UniqueErrors: len(fingerprints),
		AllPassed:    true,
	}
	for _, result := range results {
		if result.Passed {
			summary.PassedRuns++
		} else {
			summary.FailedRuns++
			summary.AllPassed = false
		}
	}
	return summary, nil
}

// runCompositeInstance runs a multi-step composite instance with Phase-1-grade artifacts.
func (r *Runner) runCompositeInstance(ctx context.Context, composite *CompositeInstance, seed int64) *RunResult {
	runDir := composite.RunDir(r.config.RunRoot, seed)
	startTime := time.Now()

	if err := EnsureDir(runDir); err != nil {
		return &RunResult{
			Instance:      &composite.Instance,
			Seed:          seed,
			RunDir:        runDir,
			StartTime:     startTime,
			EndTime:       time.Now(),
			Passed:        false,
			FailureReason: fmt.Sprintf("create run dir: %v", err),
			ExitCode:      -1,
		}
	}

	// Strict oracle gating: check before running any steps
	if composite.RequiresOracle && (r.config.Oracle == nil || !r.config.Oracle.Available()) {
		result := &RunResult{
			Instance:      &composite.Instance,
			Seed:          seed,
			RunDir:        runDir,
			StartTime:     startTime,
			EndTime:       time.Now(),
			ExitCode:      -1,
			Passed:        false,
			FailureReason: "oracle required but not available",
		}
		// Write run.json even for failures
		if err := WriteRunArtifact(result); err != nil {
			r.log("warning: failed to write run artifact: %v", err)
		}
		return result
	}

	r.log("Running composite: %s (seed=%d)", composite.Name, seed)

	steps := composite.ToSteps()
	compositeResult := &CompositeResult{
		GatingPolicy: composite.GatingPolicy,
	}

	var lastDBPath string

	// Create timeout context for the entire composite instance
	timeout := time.Duration(r.config.InstanceTimeout) * time.Second
	instanceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for i, step := range steps {
		// Check context for timeout
		if instanceCtx.Err() != nil {
			stepResult := StepResult{
				StepName:      step.Name,
				Passed:        false,
				ExitCode:      -1,
				FailureReason: "timeout",
			}
			compositeResult.Steps = append(compositeResult.Steps, stepResult)
			break
		}

		// Check for per-step oracle requirement
		if step.RequiresOracle && (r.config.Oracle == nil || !r.config.Oracle.Available()) {
			stepResult := StepResult{
				StepName:      step.Name,
				Passed:        false,
				ExitCode:      -1,
				FailureReason: "oracle required but not available",
			}
			compositeResult.Steps = append(compositeResult.Steps, stepResult)
			break
		}

		// Per-step timeout (configurable, or fraction of instance timeout)
		// Minimum 30s per step
		stepTimeout := max(timeout/time.Duration(len(steps)), 30*time.Second)
		stepCtx, stepCancel := context.WithTimeout(instanceCtx, stepTimeout)

		stepResult := r.runStep(stepCtx, &step, runDir, seed, lastDBPath, composite.Stop)
		stepCancel()

		compositeResult.Steps = append(compositeResult.Steps, stepResult)

		// Update lastDBPath if this step discovered it
		if stepResult.DBPath != "" {
			lastDBPath = stepResult.DBPath
		}

		r.log("  Step %d/%d %s: %s (exit=%d, %dms)",
			i+1, len(steps), step.Name, passedStr(stepResult.Passed),
			stepResult.ExitCode, stepResult.DurationMs)

		// For GateAllSteps, stop on first failure
		if composite.GatingPolicy == GateAllSteps && !stepResult.Passed {
			break
		}
	}

	// Compute overall pass/fail based on gating policy
	compositeResult.ComputePassed()

	endTime := time.Now()

	// Build combined log path from step logs
	var combinedLogPath string
	if len(compositeResult.Steps) > 0 {
		combinedLogPath = compositeResult.Steps[len(compositeResult.Steps)-1].LogPath
	}

	// Determine exit code (last step's exit code)
	exitCode := 0
	if len(compositeResult.Steps) > 0 {
		exitCode = compositeResult.Steps[len(compositeResult.Steps)-1].ExitCode
	}

	// Build result
	result := &RunResult{
		Instance:      &composite.Instance,
		Seed:          seed,
		RunDir:        runDir,
		StartTime:     startTime,
		EndTime:       endTime,
		ExitCode:      exitCode,
		Passed:        compositeResult.Passed,
		FailureReason: compositeResult.FailureReason,
	}

	// Apply stop conditions if applicable
	if composite.Stop.RequireOracleCheckConsistencyOK && lastDBPath != "" && r.config.Oracle != nil {
		oracleResult := r.config.Oracle.CheckConsistency(lastDBPath)
		result.OracleResult = oracleResult
		if !oracleResult.OK() {
			result.Passed = false
			if result.FailureReason == "" {
				result.FailureReason = "oracle consistency check failed"
			}
		}
		// Write oracle artifacts
		if err := r.writeOracleArtifacts(runDir, oracleResult); err != nil {
			r.log("warning: failed to write oracle artifacts: %v", err)
		}
	}

	// Compute fingerprint and handle dedupe
	if !result.Passed && result.FailureReason != "" {
		result.FailureKind = classifyFailureKind(result.FailureReason, result.ExitCode)
		result.Fingerprint = ComputeFingerprint(
			composite.Name,
			seed,
			result.FailureKind,
			result.FailureReason,
			combinedLogPath,
		)

		if r.config.KnownFailures != nil && composite.Stop.DedupeByFingerprint {
			result.FailureClass, result.QuarantinePolicy, result.IsDuplicate = r.classifyFailure(
				result.Fingerprint, composite.Name, startTime.Format(time.RFC3339))
		} else {
			result.FailureClass = FailureClassNew
		}
	}

	// Write run.json (Phase-1-grade artifact)
	if err := WriteRunArtifact(result); err != nil {
		r.log("warning: failed to write run artifact: %v", err)
	}

	if result.Passed {
		r.log("  PASS (%s)", result.Duration())
	} else {
		r.log("  FAIL: %s (%s)", result.FailureReason, result.Duration())
	}

	return result
}

// runStep executes a single step of a composite instance.
// stop contains the composite instance's stop conditions for verification enforcement.
func (r *Runner) runStep(ctx context.Context, step *Step, runDir string, seed int64, dbPath string, stop StopCondition) StepResult {
	stepDir := StepRunDir(runDir, step.Name)
	if err := EnsureDir(stepDir); err != nil {
		return StepResult{
			StepName:      step.Name,
			Passed:        false,
			ExitCode:      -1,
			FailureReason: fmt.Sprintf("create step dir: %v", err),
		}
	}

	startTime := time.Now()

	// Resolve args
	args := ResolveStepArgs(step.Args, runDir, seed, dbPath)

	// Build command
	var binaryPath string
	switch step.Tool {
	case ToolSSTDump:
		binaryPath = filepath.Join(r.config.BinDir, "sstdump")
	case ToolCrash:
		binaryPath = filepath.Join(r.config.BinDir, "crashtest")
	case ToolStress:
		binaryPath = filepath.Join(r.config.BinDir, "stresstest")
	case ToolAdversarial:
		binaryPath = filepath.Join(r.config.BinDir, "adversarialtest")
	default:
		binaryPath = string(step.Tool)
	}

	// Execute
	logPath := filepath.Join(stepDir, "output.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return StepResult{
			StepName:      step.Name,
			Passed:        false,
			ExitCode:      -1,
			FailureReason: fmt.Sprintf("create log file: %v", err),
		}
	}
	defer func() { _ = logFile.Close() }()

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Add environment
	cmd.Env = os.Environ()
	for k, v := range step.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	err = cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	durationMs := time.Since(startTime).Milliseconds()

	result := StepResult{
		StepName:   step.Name,
		Passed:     exitCode == 0,
		ExitCode:   exitCode,
		DurationMs: durationMs,
		LogPath:    logPath,
	}

	if !result.Passed {
		result.FailureReason = fmt.Sprintf("exit code %d", exitCode)
	}

	// Enforce RequireFinalVerificationPass for tools that produce verification markers.
	// This matches the behavior of checkStopConditions() in RunInstances.
	if stop.RequireFinalVerificationPass && result.Passed {
		switch step.Tool {
		case ToolStress, ToolCrash:
			if !finalVerificationPassed(step.Tool, logPath) {
				result.Passed = false
				result.FailureReason = "final verification not observed as passed in output log"
			}
		}
	}

	// Discover DB path if requested
	if step.DiscoverDBPath {
		result.DBPath = r.discoverDBPath(runDir)
	}

	return result
}

// passedStr returns "PASS" or "FAIL" based on passed.
func passedStr(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}

// RunSweepInstances expands and runs sweep instances.
func (r *Runner) RunSweepInstances(ctx context.Context, sweeps []SweepInstance) (*CampaignSummary, error) {
	// Expand all sweeps into concrete instances
	var instances []Instance
	for _, sweep := range sweeps {
		expanded := sweep.Expand()
		instances = append(instances, expanded...)
	}

	// Run as regular instances
	return r.RunInstances(ctx, instances)
}
