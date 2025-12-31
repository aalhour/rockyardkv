package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Contract: NewRechecker creates a Rechecker with the given oracle.
func TestNewRechecker(t *testing.T) {
	r := NewRechecker(nil)
	if r == nil {
		t.Fatal("NewRechecker(nil) returned nil")
	}
	if r.StopConditions == nil {
		t.Error("StopConditions should be initialized")
	}
}

// Contract: discoverDBPathInDir finds a database directory containing CURRENT.
func TestDiscoverDBPathInDir(t *testing.T) {
	tmpDir := t.TempDir()

	// Create db/CURRENT
	dbDir := filepath.Join(tmpDir, "db")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dbDir, "CURRENT"), []byte("MANIFEST-000001"), 0o644); err != nil {
		t.Fatal(err)
	}

	path := discoverDBPathInDir(tmpDir)
	if path != dbDir {
		t.Errorf("discoverDBPathInDir() = %q, want %q", path, dbDir)
	}
}

// Contract: discoverDBPathInDir returns empty string when no CURRENT found.
func TestDiscoverDBPathInDir_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	path := discoverDBPathInDir(tmpDir)
	if path != "" {
		t.Errorf("discoverDBPathInDir() = %q, want empty", path)
	}
}

// Contract: determineTool infers tool from artifact tags.
func TestDetermineTool_FromTags(t *testing.T) {
	artifact := RunArtifact{
		Tags: &Tags{Tool: "crashtest"},
	}
	tool := determineTool(artifact)
	if tool != ToolCrash {
		t.Errorf("determineTool() = %v, want %v", tool, ToolCrash)
	}
}

// Contract: determineTool infers tool from instance name when no tags.
func TestDetermineTool_FromName(t *testing.T) {
	tests := []struct {
		instance string
		want     Tool
	}{
		{"stress.read.corruption", ToolStress},
		{"crash.loop.basic", ToolCrash},
		{"golden.compat", ToolGolden},
		{"adversarial.corruption", ToolAdversarial},
		{"unknown", ToolStress}, // default
	}

	for _, tt := range tests {
		artifact := RunArtifact{Instance: tt.instance}
		got := determineTool(artifact)
		if got != tt.want {
			t.Errorf("determineTool(%q) = %v, want %v", tt.instance, got, tt.want)
		}
	}
}

// Contract: RecheckRun creates recheck.json with correct fields.
func TestRechecker_RecheckRun(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a minimal run.json
	artifact := RunArtifact{
		SchemaVersion: "0.9.0",
		Instance:      "stress.test",
		Seed:          123,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now(),
		DurationMs:    60000,
		ExitCode:      0,
		Passed:        true,
	}

	data, _ := json.MarshalIndent(artifact, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "run.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRechecker(nil)
	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error = %v", err)
	}

	if result.RecheckSchemaVersion != SchemaVersion {
		t.Errorf("RecheckSchemaVersion = %q, want %q", result.RecheckSchemaVersion, SchemaVersion)
	}

	// Check recheck.json was written
	recheckPath := filepath.Join(tmpDir, "recheck.json")
	if _, err := os.Stat(recheckPath); os.IsNotExist(err) {
		t.Error("recheck.json was not created")
	}
}

// Contract: RecheckRun returns error for missing run.json.
func TestRechecker_RecheckRun_MissingRunJSON(t *testing.T) {
	tmpDir := t.TempDir()

	r := NewRechecker(nil)
	_, err := r.RecheckRun(tmpDir)
	if err == nil {
		t.Error("RecheckRun should fail for missing run.json")
	}
}

// Contract: recheckOracle skips when not required by stop condition.
func TestRechecker_RecheckOracle_SkipsWhenNotRequired(t *testing.T) {
	r := NewRechecker(nil)
	stop := StopCondition{RequireOracleCheckConsistencyOK: false}

	result := r.recheckOracle(t.TempDir(), stop)

	if !result.Skipped {
		t.Error("Should be skipped when oracle not required")
	}
	if result.SkipReason == "" {
		t.Error("SkipReason should be set")
	}
}

// Contract: recheckOracle skips when oracle not available.
func TestRechecker_RecheckOracle_SkipsWhenNoOracle(t *testing.T) {
	r := NewRechecker(nil) // nil oracle
	stop := StopCondition{RequireOracleCheckConsistencyOK: true}

	result := r.recheckOracle(t.TempDir(), stop)

	if !result.Skipped {
		t.Error("Should be skipped when oracle not available")
	}
}

// Contract: recheckMarkers returns passed=true when not required.
func TestRechecker_RecheckMarkers_PassedWhenNotRequired(t *testing.T) {
	r := NewRechecker(nil)
	artifact := RunArtifact{Instance: "stress.test"}
	stop := StopCondition{RequireFinalVerificationPass: false}

	result := r.recheckMarkers(t.TempDir(), artifact, stop)

	if !result.Passed {
		t.Error("Should pass when verification not required")
	}
}

// Contract: evaluatePolicy returns passed when all conditions satisfied.
func TestRechecker_EvaluatePolicy_AllSatisfied(t *testing.T) {
	r := NewRechecker(nil)
	artifact := RunArtifact{ExitCode: 0}
	recheck := &RecheckResult{
		OracleRecheck: &OracleRecheckResult{Skipped: true},
		MarkerRecheck: &MarkerRecheckResult{Passed: true},
	}
	stop := StopCondition{RequireTermination: true}

	result := r.evaluatePolicy(artifact, recheck, stop)

	if !result.Passed {
		t.Error("Should pass when all conditions satisfied")
	}
	if !result.Verified {
		t.Error("Should be verified when oracle not required")
	}
}

// Contract: evaluatePolicy fails when termination required but exitCode is -1.
func TestRechecker_EvaluatePolicy_TerminationFailed(t *testing.T) {
	r := NewRechecker(nil)
	artifact := RunArtifact{ExitCode: -1}
	recheck := &RecheckResult{}
	stop := StopCondition{RequireTermination: true}

	result := r.evaluatePolicy(artifact, recheck, stop)

	if result.Passed {
		t.Error("Should fail when termination required but exitCode is -1")
	}
}

// Contract: evaluatePolicy marks NOT VERIFIED when oracle required but unavailable.
func TestRechecker_EvaluatePolicy_OracleUnavailable(t *testing.T) {
	r := NewRechecker(nil)
	artifact := RunArtifact{ExitCode: 0}
	recheck := &RecheckResult{
		OracleRecheck: &OracleRecheckResult{
			Skipped:    true,
			SkipReason: "oracle not available",
		},
	}
	stop := StopCondition{RequireOracleCheckConsistencyOK: true}

	result := r.evaluatePolicy(artifact, recheck, stop)

	if result.Verified {
		t.Error("Should not be verified when oracle required but unavailable")
	}
}

// Contract: RecheckCampaign finds and processes all run.json files.
func TestRechecker_RecheckCampaign(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two run directories
	for _, name := range []string{"run1", "run2"} {
		runDir := filepath.Join(tmpDir, name)
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			t.Fatal(err)
		}
		artifact := RunArtifact{
			Instance: name,
			Passed:   true,
		}
		data, _ := json.MarshalIndent(artifact, "", "  ")
		if err := os.WriteFile(filepath.Join(runDir, "run.json"), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	r := NewRechecker(nil)
	results, err := r.RecheckCampaign(tmpDir)
	if err != nil {
		t.Fatalf("RecheckCampaign error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("RecheckCampaign found %d runs, want 2", len(results))
	}
}

// Contract: SchemaVersion is set correctly.
func TestSchemaVersion(t *testing.T) {
	if SchemaVersion == "" {
		t.Error("SchemaVersion should not be empty")
	}
	// Should be semver format
	if SchemaVersion[0] < '0' || SchemaVersion[0] > '9' {
		t.Errorf("SchemaVersion should start with a digit, got %q", SchemaVersion)
	}
}
