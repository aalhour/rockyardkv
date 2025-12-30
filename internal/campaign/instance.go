package campaign

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Instance represents a single campaign test instance.
// Each instance defines a specific test configuration to run.
type Instance struct {
	// Name is the unique instance identifier.
	// Should be descriptive: "stress.read.corruption.1in7"
	Name string

	// Tier is the intensity level (quick or nightly).
	Tier Tier

	// RequiresOracle indicates if C++ oracle tools are required.
	// If true, the runner will fail fast if oracle is not configured.
	RequiresOracle bool

	// Tool is the test binary to execute.
	Tool Tool

	// Args are the command-line arguments for the tool.
	// Use "<RUN_DIR>" as a placeholder for the run directory.
	// Use "<SEED>" as a placeholder for the seed value.
	Args []string

	// Env are additional environment variables for the tool.
	Env map[string]string

	// Seeds are the seed values to run. Each seed produces a separate run.
	Seeds []int64

	// FaultModel describes the fault injection configuration.
	FaultModel FaultModel

	// Stop defines the stopping conditions for this instance.
	Stop StopCondition
}

// RunDir returns the run directory path for a specific seed.
func (i *Instance) RunDir(runRoot string, seed int64) string {
	return filepath.Join(runRoot, i.Name, fmt.Sprintf("seed_%d", seed))
}

// ResolveArgs returns the arguments with placeholders replaced.
func (i *Instance) ResolveArgs(runDir string, seed int64) []string {
	args := make([]string, len(i.Args))
	for idx, arg := range i.Args {
		arg = strings.ReplaceAll(arg, "<RUN_DIR>", runDir)
		arg = strings.ReplaceAll(arg, "<SEED>", fmt.Sprintf("%d", seed))
		args[idx] = arg
	}
	return args
}

// BinaryName returns the binary name for the tool.
func (i *Instance) BinaryName() string {
	switch i.Tool {
	case ToolStress:
		return "stresstest"
	case ToolCrash:
		return "crashtest"
	case ToolAdversarial:
		return "adversarialtest"
	case ToolGolden:
		return "go" // go test
	default:
		return string(i.Tool)
	}
}

// IsGoTest returns true if this instance runs via `go test`.
func (i *Instance) IsGoTest() bool {
	return i.Tool == ToolGolden
}
