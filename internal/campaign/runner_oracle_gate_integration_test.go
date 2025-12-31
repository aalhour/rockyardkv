package campaign

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Contract: If an instance requires the C++ oracle and the oracle is not available, the runner
// fails before creating any per-instance run directories or writing summary.json.
func TestRunner_RunInstances_OracleRequiredWithoutOracle_FailsWithoutCreatingRunDirs(t *testing.T) {
	runRoot := t.TempDir()

	r := NewRunner(RunnerConfig{
		Tier:    TierQuick,
		RunRoot: runRoot,
		Oracle:  nil, // oracle not available
	})

	inst := Instance{
		Name:           "stress.oracle.required.test",
		Tool:           ToolStress,
		RequiresOracle: true,
		Seeds:          []int64{1},
	}

	summary, err := r.RunInstances(context.Background(), []Instance{inst})
	if err == nil {
		t.Fatalf("expected RunInstances to fail when oracle is required but unavailable; got summary=%v", summary)
	}

	// Runner.RunInstances always creates the run root, but it must not create per-instance dirs.
	instanceDir := filepath.Join(runRoot, inst.Name)
	if _, statErr := os.Stat(instanceDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected per-instance directory to not exist: %s (statErr=%v)", instanceDir, statErr)
	}

	// No campaign-level summary should be written because the run never started.
	summaryPath := filepath.Join(runRoot, "summary.json")
	if _, statErr := os.Stat(summaryPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected summary.json to not exist: %s (statErr=%v)", summaryPath, statErr)
	}
}
