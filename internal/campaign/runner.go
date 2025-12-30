package campaign

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// RunnerConfig configures the campaign runner.
type RunnerConfig struct {
	// Tier is the intensity level.
	Tier Tier

	// RunRoot is the root directory for all run artifacts.
	RunRoot string

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
				break
			}
		}

		if r.config.FailFast {
			// Check if any failure occurred
			for _, result := range results {
				if !result.Passed {
					break
				}
			}
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

	// Build command
	var cmd *exec.Cmd
	if instance.IsGoTest() {
		cmd = exec.CommandContext(instanceCtx, "go", instance.ResolveArgs(runDir, seed)...)
	} else {
		binary := instance.BinaryName()
		args := instance.ResolveArgs(runDir, seed)
		cmd = exec.CommandContext(instanceCtx, binary, args...)
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
		Instance:  instance,
		Seed:      seed,
		RunDir:    runDir,
		StartTime: startTime,
		EndTime:   endTime,
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
	r.checkStopConditions(result, instance)

	// Record fingerprint if failed
	if !result.Passed && result.FailureReason != "" {
		result.Fingerprint = ComputeFingerprint(result.FailureReason, logPath)

		if r.config.KnownFailures != nil && instance.Stop.DedupeByFingerprint {
			isDup := r.config.KnownFailures.IsDuplicate(result.Fingerprint)
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
func (r *Runner) checkStopConditions(result *RunResult, instance *Instance) {
	stop := instance.Stop

	// Check termination requirement
	if stop.RequireTermination && result.ExitCode != 0 {
		if result.FailureReason == "" {
			result.FailureReason = fmt.Sprintf("non-zero exit code: %d", result.ExitCode)
		}
		result.Passed = false
		return
	}

	// Check oracle consistency
	if stop.RequireOracleCheckConsistencyOK && instance.RequiresOracle && r.config.Oracle != nil {
		// Find the DB path in the run directory
		dbPath := filepath.Join(result.RunDir, "db")
		if _, err := os.Stat(dbPath); err == nil {
			oracleResult := r.config.Oracle.CheckConsistency(dbPath)
			result.OracleResult = oracleResult

			if !oracleResult.OK() {
				result.FailureReason = fmt.Sprintf("oracle checkconsistency failed: %s", oracleResult.Stderr)
				result.Passed = false
				return
			}
		}
	}

	// If we get here and no failure was recorded, it passed
	if result.FailureReason == "" {
		result.Passed = true
	}
}

// log writes a message to the output.
func (r *Runner) log(format string, args ...any) {
	fmt.Fprintf(r.config.Output, format+"\n", args...)
}
