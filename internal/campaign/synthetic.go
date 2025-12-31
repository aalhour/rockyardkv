package campaign

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SyntheticFailConfig configures the synthetic failure hook.
// This is used for CI testing of minimization and failure classification.
type SyntheticFailConfig struct {
	// Enabled activates the synthetic failure mode.
	Enabled bool

	// FailAfterOps causes failure after N operations.
	// Used to exercise minimization: minimizer should reduce N.
	FailAfterOps int

	// FailureKind is the classification for the synthetic failure.
	FailureKind string

	// FailureMessage is the human-readable failure reason.
	FailureMessage string
}

// SyntheticInstance returns a test-only instance that fails deterministically.
// This is gated behind ROCKYARDKV_SYNTHETIC_FAIL=1 env var to prevent accidental use.
//
// Usage:
//
//	ROCKYARDKV_SYNTHETIC_FAIL=1 bin/campaignrunner -group=synthetic -minimize
func SyntheticInstance() *Instance {
	if os.Getenv("ROCKYARDKV_SYNTHETIC_FAIL") != "1" {
		return nil
	}

	return &Instance{
		Name:           "synthetic.deterministic_fail",
		Tier:           TierQuick,
		RequiresOracle: false,
		Tool:           ToolStress,
		Args: []string{
			"-duration=10s",
			"-threads=1",
			"-keys=100",
			"-db", "<RUN_DIR>/db",
			"-run-dir", "<RUN_DIR>/artifacts",
			"-seed", "<SEED>",
			"-cleanup",
			"-v",
		},
		Seeds: []int64{1},
		FaultModel: FaultModel{
			Kind: FaultNone,
		},
		Stop: StopCondition{
			RequireTermination:           true,
			RequireFinalVerificationPass: true,
		},
	}
}

// RunSyntheticFailure executes a synthetic failure for CI testing.
// Returns a RunResult that simulates a deterministic, classifiable failure.
func RunSyntheticFailure(ctx context.Context, config SyntheticFailConfig, runDir string) *RunResult {
	startTime := time.Now()

	// Create run directory
	if err := EnsureDir(runDir); err != nil {
		return &RunResult{
			StartTime:     startTime,
			EndTime:       time.Now(),
			Passed:        false,
			FailureReason: fmt.Sprintf("failed to create run dir: %v", err),
			FailureKind:   "setup_error",
		}
	}

	// Write synthetic log
	logPath := filepath.Join(runDir, "synthetic.log")
	logContent := fmt.Sprintf("Synthetic failure after %d ops\nKind: %s\nMessage: %s\n",
		config.FailAfterOps, config.FailureKind, config.FailureMessage)
	_ = os.WriteFile(logPath, []byte(logContent), 0o644)

	endTime := time.Now()

	// Return a well-formed failure result
	return &RunResult{
		Instance: &Instance{
			Name: "synthetic.deterministic_fail",
		},
		Seed:      1,
		RunDir:    runDir,
		StartTime: startTime,
		EndTime:   endTime,
		ExitCode:  1,
		Passed:    false,

		// Stable, classifiable failure
		FailureReason: config.FailureMessage,
		FailureKind:   config.FailureKind,

		// Generate stable fingerprint for testing dedup
		Fingerprint: ComputeFingerprint(
			"synthetic.deterministic_fail",
			1,
			config.FailureKind,
			config.FailureMessage,
			"",
		),
	}
}
