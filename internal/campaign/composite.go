package campaign

import (
	"fmt"
	"path/filepath"
	"strings"
)

// GatingPolicy defines how multi-step instance results are combined.
type GatingPolicy string

const (
	// GateAllSteps fails if ANY step fails.
	GateAllSteps GatingPolicy = "all_steps"

	// GateLastStep fails ONLY if the last step fails.
	// Earlier step failures are recorded but don't fail the instance.
	GateLastStep GatingPolicy = "last_step"
)

// Step represents a single execution step in a composite instance.
type Step struct {
	// Name identifies this step (e.g., "crashtest", "collision-check").
	Name string

	// Tool is the binary to execute.
	Tool Tool

	// Args are the command-line arguments.
	// Supports placeholders: <RUN_DIR>, <SEED>, <DB_DIR>, <PREV_DB_DIR>.
	Args []string

	// Env are additional environment variables.
	Env map[string]string

	// RequiresOracle indicates if this step needs oracle tools.
	RequiresOracle bool

	// DiscoverDBPath indicates the runner should discover the DB path
	// from the previous step's artifacts and make it available as <DB_DIR>.
	DiscoverDBPath bool
}

// CompositeInstance extends Instance with multi-step execution support.
type CompositeInstance struct {
	Instance

	// Steps are the execution steps (in order).
	// If nil or empty, the Instance.Tool/Args are used as a single step.
	Steps []Step

	// GatingPolicy determines how step results combine.
	// Default: GateAllSteps (fail if ANY step fails).
	GatingPolicy GatingPolicy
}

// IsComposite returns true if this instance has multiple steps.
func (c *CompositeInstance) IsComposite() bool {
	return len(c.Steps) > 1
}

// ToSteps converts the instance to a list of steps.
// If no explicit steps, creates a single step from Instance fields.
func (c *CompositeInstance) ToSteps() []Step {
	if len(c.Steps) > 0 {
		return c.Steps
	}

	// Convert simple instance to single step
	return []Step{{
		Name:           c.Name,
		Tool:           c.Tool,
		Args:           c.Args,
		Env:            c.Env,
		RequiresOracle: c.RequiresOracle,
	}}
}

// StepResult captures the outcome of a single step execution.
type StepResult struct {
	// StepName identifies which step this result is for.
	StepName string

	// Passed indicates if this step succeeded.
	Passed bool

	// ExitCode is the process exit code.
	ExitCode int

	// FailureReason describes why the step failed (if applicable).
	FailureReason string

	// DurationMs is how long the step took.
	DurationMs int64

	// DBPath is the discovered DB path (if DiscoverDBPath was set).
	DBPath string

	// LogPath is the path to this step's log file.
	LogPath string
}

// CompositeResult captures the outcome of a composite instance.
type CompositeResult struct {
	// Steps contains results for each step.
	Steps []StepResult

	// Passed indicates if the composite instance passed per its gating policy.
	Passed bool

	// FailureReason summarizes why the instance failed.
	FailureReason string

	// GatingPolicy that was applied.
	GatingPolicy GatingPolicy
}

// ComputePassed evaluates the gating policy against step results.
func (c *CompositeResult) ComputePassed() {
	if len(c.Steps) == 0 {
		c.Passed = true
		return
	}

	switch c.GatingPolicy {
	case GateLastStep:
		// Only the last step's result matters
		lastStep := c.Steps[len(c.Steps)-1]
		c.Passed = lastStep.Passed
		if !c.Passed {
			c.FailureReason = fmt.Sprintf("step %q failed: %s", lastStep.StepName, lastStep.FailureReason)
		}

	case GateAllSteps:
		fallthrough
	default:
		// All steps must pass
		c.Passed = true
		for _, step := range c.Steps {
			if !step.Passed {
				c.Passed = false
				c.FailureReason = fmt.Sprintf("step %q failed: %s", step.StepName, step.FailureReason)
				break
			}
		}
	}
}

// StepRunDir returns the run directory for a specific step.
func StepRunDir(instanceRunDir string, stepName string) string {
	return filepath.Join(instanceRunDir, "steps", stepName)
}

// ResolveStepArgs returns args with placeholders replaced.
// dbPath is the DB path discovered from a previous step (or empty).
func ResolveStepArgs(args []string, runDir string, seed int64, dbPath string) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		arg = strings.ReplaceAll(arg, "<RUN_DIR>", runDir)
		arg = strings.ReplaceAll(arg, "<SEED>", fmt.Sprintf("%d", seed))
		if dbPath != "" {
			arg = strings.ReplaceAll(arg, "<DB_DIR>", dbPath)
			arg = strings.ReplaceAll(arg, "<PREV_DB_DIR>", dbPath)
		}
		result[i] = arg
	}
	return result
}
