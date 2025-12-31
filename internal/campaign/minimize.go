// minimize.go implements failure minimization for stresstest runs.
//
// When a stresstest fails, minimization attempts to reduce the reproduction
// parameters (duration, threads, keys) to find the smallest configuration
// that still reproduces the failure. This makes debugging faster.
//
// Reduction strategy: binary search on each dimension independently.
package campaign

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// MinBounds defines the minimum values for parameter reduction during minimization.
type MinBounds struct {
	// MinDuration is the minimum test duration.
	// Default: 5 seconds.
	MinDuration time.Duration

	// MinThreads is the minimum number of threads.
	// Default: 4.
	MinThreads int

	// MinKeys is the minimum number of keys.
	// Default: 500.
	MinKeys int
}

// DefaultMinBounds returns the Red Team approved minimization bounds.
func DefaultMinBounds() MinBounds {
	return MinBounds{
		MinDuration: 5 * time.Second,
		MinThreads:  4,
		MinKeys:     500,
	}
}

// MinimizeConfig controls the minimization process.
type MinimizeConfig struct {
	// Enabled controls whether minimization is active.
	Enabled bool

	// Bounds defines the minimum parameter values.
	Bounds MinBounds

	// AllowedFailureKinds is the set of failure kinds eligible for minimization.
	// Empty means all failure kinds are eligible.
	AllowedFailureKinds map[string]bool
}

// DefaultMinimizeConfig returns the default minimization configuration.
func DefaultMinimizeConfig() MinimizeConfig {
	return MinimizeConfig{
		Enabled: false,
		Bounds:  DefaultMinBounds(),
		AllowedFailureKinds: map[string]bool{
			"verification_failure": true,
			"oracle_failure":       true,
			"corruption":           true,
			"exit_error":           true,
		},
	}
}

// ReductionStep records a single step in the minimization process.
type ReductionStep struct {
	Parameter   string `json:"parameter"` // "duration", "threads", or "keys"
	OriginalVal string `json:"original_value"`
	ReducedVal  string `json:"reduced_value"`
	StillFails  bool   `json:"still_fails"`
	DurationMs  int64  `json:"duration_ms"`
}

// MinimizeResult captures the outcome of a minimization attempt.
type MinimizeResult struct {
	// Success indicates if minimization found a smaller reproducer.
	Success bool `json:"success"`

	// OriginalArgs are the original instance arguments.
	OriginalArgs []string `json:"original_args"`

	// MinimalArgs are the minimized arguments (if successful).
	MinimalArgs []string `json:"minimal_args,omitempty"`

	// Steps records each reduction attempt.
	Steps []ReductionStep `json:"steps"`

	// FinalDuration is the minimized duration.
	FinalDuration string `json:"final_duration,omitempty"`

	// FinalThreads is the minimized thread count.
	FinalThreads int `json:"final_threads,omitempty"`

	// FinalKeys is the minimized key count.
	FinalKeys int `json:"final_keys,omitempty"`

	// PreservedFailureKind is the failure kind class that was preserved across reduction.
	PreservedFailureKind string `json:"preserved_failure_kind,omitempty"`

	// TotalAttempts is the number of runs performed during minimization.
	TotalAttempts int `json:"total_attempts"`

	// TotalDurationMs is the total time spent minimizing.
	TotalDurationMs int64 `json:"total_duration_ms"`
}

// Minimizer reduces failing test cases to minimal parameters.
type Minimizer struct {
	runner        *Runner
	config        MinimizeConfig
	attemptNumber int // Counter for unique attempt directories
}

// NewMinimizer creates a new minimizer with the given runner and config.
func NewMinimizer(runner *Runner, config MinimizeConfig) *Minimizer {
	return &Minimizer{
		runner: runner,
		config: config,
	}
}

// ShouldMinimize returns true if the failure is eligible for minimization.
func (m *Minimizer) ShouldMinimize(result *RunResult) bool {
	if !m.config.Enabled {
		return false
	}

	// Only minimize new failures (not duplicates)
	if result.IsDuplicate {
		return false
	}

	// Only minimize failures (not passes)
	if result.Passed {
		return false
	}

	// Check if failure kind is in the allowlist
	if len(m.config.AllowedFailureKinds) > 0 {
		if !m.config.AllowedFailureKinds[result.FailureKind] {
			return false
		}
	}

	// Only minimize stresstest instances (they have the relevant parameters)
	if result.Instance.Tool != ToolStress {
		return false
	}

	return true
}

// Minimize attempts to reduce a failing instance to minimal parameters.
// It uses sequential reduction: duration → threads → keys.
// Within each parameter, it uses binary search.
func (m *Minimizer) Minimize(ctx context.Context, result *RunResult) (*MinimizeResult, error) {
	startTime := time.Now()

	minResult := &MinimizeResult{
		OriginalArgs:         result.Instance.Args,
		PreservedFailureKind: result.FailureKind,
	}

	// Parse current values from args
	currentDuration := m.parseDuration(result.Instance.Args)
	currentThreads := m.parseThreads(result.Instance.Args)
	currentKeys := m.parseKeys(result.Instance.Args)

	// Phase 1: Reduce duration
	bestDuration, steps := m.reduceDuration(ctx, result, currentDuration, currentThreads, currentKeys)
	minResult.Steps = append(minResult.Steps, steps...)
	minResult.TotalAttempts += len(steps)

	// Phase 2: Reduce threads (with minimized duration)
	bestThreads, steps := m.reduceThreads(ctx, result, bestDuration, currentThreads, currentKeys)
	minResult.Steps = append(minResult.Steps, steps...)
	minResult.TotalAttempts += len(steps)

	// Phase 3: Reduce keys (with minimized duration and threads)
	bestKeys, steps := m.reduceKeys(ctx, result, bestDuration, bestThreads, currentKeys)
	minResult.Steps = append(minResult.Steps, steps...)
	minResult.TotalAttempts += len(steps)

	// Build minimal args
	minResult.FinalDuration = bestDuration.String()
	minResult.FinalThreads = bestThreads
	minResult.FinalKeys = bestKeys
	minResult.MinimalArgs = m.buildArgs(result.Instance.Args, bestDuration, bestThreads, bestKeys)
	minResult.Success = bestDuration < currentDuration || bestThreads < currentThreads || bestKeys < currentKeys
	minResult.TotalDurationMs = time.Since(startTime).Milliseconds()

	return minResult, nil
}

// reduceDuration uses binary search to find the minimum duration that still fails.
func (m *Minimizer) reduceDuration(ctx context.Context, result *RunResult, current time.Duration, threads, keys int) (time.Duration, []ReductionStep) {
	var steps []ReductionStep
	minDur := m.config.Bounds.MinDuration
	best := current

	// Binary search: try halving until we find the minimum
	for current > minDur {
		select {
		case <-ctx.Done():
			return best, steps
		default:
		}

		// Try half the current duration
		candidate := max(current/2, minDur)

		step := ReductionStep{
			Parameter:   "duration",
			OriginalVal: current.String(),
			ReducedVal:  candidate.String(),
		}

		start := time.Now()
		stillFails := m.testWithParams(ctx, result, candidate, threads, keys)
		step.DurationMs = time.Since(start).Milliseconds()
		step.StillFails = stillFails

		steps = append(steps, step)

		if stillFails {
			best = candidate
			current = candidate
		} else {
			// Can't reduce further
			break
		}

		if candidate == minDur {
			break
		}
	}

	return best, steps
}

// reduceThreads uses binary search to find the minimum threads that still fails.
func (m *Minimizer) reduceThreads(ctx context.Context, result *RunResult, duration time.Duration, current, keys int) (int, []ReductionStep) {
	var steps []ReductionStep
	minThreads := m.config.Bounds.MinThreads
	best := current

	for current > minThreads {
		select {
		case <-ctx.Done():
			return best, steps
		default:
		}

		candidate := max(current/2, minThreads)

		step := ReductionStep{
			Parameter:   "threads",
			OriginalVal: fmt.Sprintf("%d", current),
			ReducedVal:  fmt.Sprintf("%d", candidate),
		}

		start := time.Now()
		stillFails := m.testWithParams(ctx, result, duration, candidate, keys)
		step.DurationMs = time.Since(start).Milliseconds()
		step.StillFails = stillFails

		steps = append(steps, step)

		if stillFails {
			best = candidate
			current = candidate
		} else {
			break
		}

		if candidate == minThreads {
			break
		}
	}

	return best, steps
}

// reduceKeys uses binary search to find the minimum keys that still fails.
func (m *Minimizer) reduceKeys(ctx context.Context, result *RunResult, duration time.Duration, threads, current int) (int, []ReductionStep) {
	var steps []ReductionStep
	minKeys := m.config.Bounds.MinKeys
	best := current

	for current > minKeys {
		select {
		case <-ctx.Done():
			return best, steps
		default:
		}

		candidate := max(current/2, minKeys)

		step := ReductionStep{
			Parameter:   "keys",
			OriginalVal: fmt.Sprintf("%d", current),
			ReducedVal:  fmt.Sprintf("%d", candidate),
		}

		start := time.Now()
		stillFails := m.testWithParams(ctx, result, duration, threads, candidate)
		step.DurationMs = time.Since(start).Milliseconds()
		step.StillFails = stillFails

		steps = append(steps, step)

		if stillFails {
			best = candidate
			current = candidate
		} else {
			break
		}

		if candidate == minKeys {
			break
		}
	}

	return best, steps
}

// testWithParams runs the instance with modified parameters and checks if it still fails.
func (m *Minimizer) testWithParams(ctx context.Context, original *RunResult, duration time.Duration, threads, keys int) bool {
	// Build modified args
	args := m.buildArgs(original.Instance.Args, duration, threads, keys)

	// Create a temporary instance with modified args and a unique name for the run directory
	m.attemptNumber++
	inst := *original.Instance
	inst.Args = args
	inst.Name = fmt.Sprintf("%s_minimize/attempt%03d", original.Instance.Name, m.attemptNumber)

	// Disable fingerprint deduplication for minimization runs to avoid polluting KnownFailures
	inst.Stop.DedupeByFingerprint = false

	result := m.runner.runInstance(ctx, &inst, original.Seed)

	// Check if it still fails with the same failure kind
	return !result.Passed && result.FailureKind == original.FailureKind
}

// buildArgs creates a new argument list with modified parameters.
func (m *Minimizer) buildArgs(original []string, duration time.Duration, threads, keys int) []string {
	args := make([]string, 0, len(original))

	for i := 0; i < len(original); i++ {
		arg := original[i]

		switch arg {
		case "-duration":
			args = append(args, "-duration", duration.String())
			i++ // Skip the next arg (original value)
		case "-threads":
			args = append(args, "-threads", fmt.Sprintf("%d", threads))
			i++
		case "-keys":
			args = append(args, "-keys", fmt.Sprintf("%d", keys))
			i++
		default:
			// Check for -duration=X style
			if len(arg) > 10 && arg[:10] == "-duration=" {
				args = append(args, fmt.Sprintf("-duration=%s", duration.String()))
			} else if len(arg) > 9 && arg[:9] == "-threads=" {
				args = append(args, fmt.Sprintf("-threads=%d", threads))
			} else if len(arg) > 6 && arg[:6] == "-keys=" {
				args = append(args, fmt.Sprintf("-keys=%d", keys))
			} else {
				args = append(args, arg)
			}
		}
	}

	return args
}

// parseDuration extracts the duration from instance args.
func (m *Minimizer) parseDuration(args []string) time.Duration {
	for i, arg := range args {
		if arg == "-duration" && i+1 < len(args) {
			d, _ := time.ParseDuration(args[i+1])
			return d
		}
		if len(arg) > 10 && arg[:10] == "-duration=" {
			d, _ := time.ParseDuration(arg[10:])
			return d
		}
	}
	return 20 * time.Second // Default
}

// parseThreads extracts the thread count from instance args.
func (m *Minimizer) parseThreads(args []string) int {
	for i, arg := range args {
		if arg == "-threads" && i+1 < len(args) {
			n, err := strconv.Atoi(args[i+1])
			if err == nil {
				return n
			}
		}
		if len(arg) > 9 && arg[:9] == "-threads=" {
			n, err := strconv.Atoi(arg[9:])
			if err == nil {
				return n
			}
		}
	}
	return 32 // Default
}

// parseKeys extracts the key count from instance args.
func (m *Minimizer) parseKeys(args []string) int {
	for i, arg := range args {
		if arg == "-keys" && i+1 < len(args) {
			n, err := strconv.Atoi(args[i+1])
			if err == nil {
				return n
			}
		}
		if len(arg) > 6 && arg[:6] == "-keys=" {
			n, err := strconv.Atoi(arg[6:])
			if err == nil {
				return n
			}
		}
	}
	return 5000 // Default
}
