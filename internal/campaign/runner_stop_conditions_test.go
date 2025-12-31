package campaign

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunner_CheckStopConditions_AdversarialNonZeroExitFailsEvenWithFaultKind(t *testing.T) {
	r := NewRunner(RunnerConfig{Tier: TierQuick, RunRoot: t.TempDir()})

	inst := &Instance{
		Name: "status.adversarial.corruption",
		Tool: ToolAdversarial,
		FaultModel: FaultModel{
			Kind: FaultCorrupt,
		},
		Stop: StopCondition{
			RequireTermination:              true,
			RequireFinalVerificationPass:    false,
			RequireOracleCheckConsistencyOK: false,
			DedupeByFingerprint:             false,
		},
	}

	res := &RunResult{ExitCode: 2}
	r.checkStopConditions(res, inst, "")
	if res.Passed {
		t.Fatalf("expected non-zero exit to fail for adversarialtest even under fault kind")
	}
	if res.FailureReason == "" {
		t.Fatalf("expected failure reason to be set")
	}
}

func TestRunner_CheckStopConditions_StressFaultRun_NonZeroExitRequiresFinalVerificationMarker(t *testing.T) {
	r := NewRunner(RunnerConfig{Tier: TierQuick, RunRoot: t.TempDir()})

	inst := &Instance{
		Name: "stress.read.status.1in7",
		Tool: ToolStress,
		FaultModel: FaultModel{
			Kind: FaultRead,
		},
		Stop: StopCondition{
			RequireTermination:              true,
			RequireFinalVerificationPass:    true,
			RequireOracleCheckConsistencyOK: false,
			DedupeByFingerprint:             false,
		},
	}

	t.Run("marker present => pass", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "output.log")
		if err := os.WriteFile(logPath, []byte("...\n🔍 Running final verification...\n  Verified 5000 keys, 0 failures\n"), 0o644); err != nil {
			t.Fatalf("write log: %v", err)
		}

		res := &RunResult{ExitCode: 2}
		r.checkStopConditions(res, inst, logPath)
		if !res.Passed {
			t.Fatalf("expected stress fault run to pass when final verification passed marker is present; got failure=%q", res.FailureReason)
		}
	})

	t.Run("marker missing => fail", func(t *testing.T) {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "output.log")
		if err := os.WriteFile(logPath, []byte("...\n(no marker)\n"), 0o644); err != nil {
			t.Fatalf("write log: %v", err)
		}

		res := &RunResult{ExitCode: 2}
		r.checkStopConditions(res, inst, logPath)
		if res.Passed {
			t.Fatalf("expected stress fault run to fail when final verification marker is missing")
		}
	})
}

func TestRunner_CheckStopConditions_CrashRequiresFinalVerificationMarker(t *testing.T) {
	r := NewRunner(RunnerConfig{Tier: TierQuick, RunRoot: t.TempDir()})

	inst := &Instance{
		Name: "status.durability.wal_sync",
		Tool: ToolCrash,
		FaultModel: FaultModel{
			Kind: FaultCrash,
		},
		Stop: StopCondition{
			RequireTermination:              true,
			RequireFinalVerificationPass:    true,
			RequireOracleCheckConsistencyOK: false,
			DedupeByFingerprint:             false,
		},
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "output.log")
	if err := os.WriteFile(logPath, []byte("...\n(no final verification marker)\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	res := &RunResult{ExitCode: 0}
	r.checkStopConditions(res, inst, logPath)
	if res.Passed {
		t.Fatalf("expected crashtest to fail stop conditions if final verification marker is missing")
	}
}

func TestRunner_CheckStopConditions_OracleRequiredButDBPathMissingFails(t *testing.T) {
	r := NewRunner(RunnerConfig{
		Tier:    TierQuick,
		RunRoot: t.TempDir(),
		Oracle:  &Oracle{RocksDBPath: t.TempDir()},
	})

	inst := &Instance{
		Name:           "status.durability.wal_sync",
		Tool:           ToolCrash,
		RequiresOracle: true,
		Stop: StopCondition{
			RequireTermination:              true,
			RequireFinalVerificationPass:    false,
			RequireOracleCheckConsistencyOK: true,
			DedupeByFingerprint:             false,
		},
	}

	res := &RunResult{
		RunDir:   t.TempDir(), // empty; no db snapshot present
		ExitCode: 0,
	}
	// Note: logPath is unused since RequireFinalVerificationPass=false.
	r.checkStopConditions(res, inst, "")

	if res.Passed {
		t.Fatalf("expected oracle-required run to fail when db snapshot path cannot be discovered")
	}
	if res.FailureReason == "" {
		t.Fatalf("expected failure reason to be set")
	}
}

func TestRunner_discoverDBPath_PrefersDirectoryContainingCURRENT(t *testing.T) {
	r := NewRunner(RunnerConfig{Tier: TierQuick, RunRoot: t.TempDir()})
	runDir := t.TempDir()

	// Simulate stresstest/crashtest layout where the DB root is <RUN_DIR>/db/db.
	if err := os.MkdirAll(filepath.Join(runDir, "db", "db"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "db", "db", "CURRENT"), []byte("MANIFEST-000001\n"), 0o644); err != nil {
		t.Fatalf("write CURRENT: %v", err)
	}

	got := r.discoverDBPath(runDir)
	want := filepath.Join(runDir, "db", "db")
	if got != want {
		t.Fatalf("discoverDBPath()=%q, want %q", got, want)
	}
}
