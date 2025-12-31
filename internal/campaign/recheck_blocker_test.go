package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Contract: Recheck marks NOT VERIFIED when oracle is required but unavailable.
func TestRecheck_OracleRequired_Unavailable_NotVerified(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a run.json that requires oracle
	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "status.durability.wal_sync",
		Seed:          123,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now(),
		DurationMs:    60000,
		ExitCode:      0,
		Passed:        true,
		Tags: &Tags{
			OracleRequired: true,
		},
	}
	data, _ := json.MarshalIndent(artifact, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "run.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create rechecker with no oracle
	r := NewRechecker(nil)
	r.StopConditions["status.durability.wal_sync"] = StopCondition{
		RequireOracleCheckConsistencyOK: true,
	}

	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	if result.PolicyResult.Verified {
		t.Error("should NOT be verified when oracle required but unavailable")
	}
}

// Contract: Recheck marks NOT VERIFIED when DB snapshot is missing.
func TestRecheck_DBSnapshotMissing_NotVerified(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a run.json (no db/ directory)
	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
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

	// Create rechecker with mock oracle requirement
	r := NewRechecker(nil)
	r.StopConditions["stress.test"] = StopCondition{
		RequireOracleCheckConsistencyOK: true,
	}

	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	// Oracle check should be skipped due to missing DB
	if result.OracleRecheck == nil {
		t.Fatal("OracleRecheck should not be nil")
	}

	if !result.OracleRecheck.Skipped {
		t.Error("oracle check should be skipped when DB is missing")
	}

	if result.OracleRecheck.SkipReason == "" {
		t.Error("SkipReason should explain why oracle check was skipped")
	}

	// Critical: when oracle is required but skipped for ANY reason, Verified must be false
	if result.PolicyResult.Verified {
		t.Errorf("should NOT be verified when oracle required but skipped (reason: %s)", result.OracleRecheck.SkipReason)
	}
}

// Contract: Recheck passes when oracle is not required.
func TestRecheck_OracleNotRequired_Verified(t *testing.T) {
	tmpDir := t.TempDir()

	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "stress.test.no.oracle",
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

	// Create rechecker with no oracle requirement
	r := NewRechecker(nil)
	r.StopConditions["stress.test.no.oracle"] = StopCondition{
		RequireOracleCheckConsistencyOK: false,
	}

	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	if !result.PolicyResult.Verified {
		t.Error("should be verified when oracle is not required")
	}
}

// Contract: Recheck recomputes fingerprint for failed runs.
func TestRecheck_FailedRun_RecomputesFingerprint(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a failed run.json
	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "stress.failing",
		Seed:          456,
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now(),
		DurationMs:    60000,
		ExitCode:      1,
		Passed:        false,
		Failure:       "test failure",
		FailureKind:   "assertion",
	}
	data, _ := json.MarshalIndent(artifact, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "run.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRechecker(nil)
	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	if result.FingerprintRecomputed == "" {
		t.Error("fingerprint should be recomputed for failed runs")
	}
}

// Contract: Recheck does not recompute fingerprint for passed runs.
func TestRecheck_PassedRun_NoFingerprint(t *testing.T) {
	tmpDir := t.TempDir()

	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "stress.passing",
		Seed:          789,
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
		t.Fatalf("RecheckRun error: %v", err)
	}

	if result.FingerprintRecomputed != "" {
		t.Error("fingerprint should not be recomputed for passed runs")
	}
}

// Contract: Recheck writes recheck.json with schema version.
func TestRecheck_WritesRecheckJSON_WithSchema(t *testing.T) {
	tmpDir := t.TempDir()

	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "stress.test",
		Seed:          123,
		Passed:        true,
	}
	data, _ := json.MarshalIndent(artifact, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "run.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRechecker(nil)
	_, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	// Read recheck.json
	recheckPath := filepath.Join(tmpDir, "recheck.json")
	recheckData, err := os.ReadFile(recheckPath)
	if err != nil {
		t.Fatalf("read recheck.json: %v", err)
	}

	var recheckResult RecheckResult
	if err := json.Unmarshal(recheckData, &recheckResult); err != nil {
		t.Fatalf("parse recheck.json: %v", err)
	}

	if recheckResult.RecheckSchemaVersion != SchemaVersion {
		t.Errorf("schema version = %q, want %q", recheckResult.RecheckSchemaVersion, SchemaVersion)
	}
}

// Contract: Recheck evaluates termination policy correctly.
func TestRecheck_TerminationPolicy_ExitCodeNegativeOne(t *testing.T) {
	tmpDir := t.TempDir()

	// Run with exit code -1 (did not terminate)
	artifact := RunArtifact{
		SchemaVersion: SchemaVersion,
		Instance:      "stress.hung",
		Seed:          123,
		ExitCode:      -1,
		Passed:        false,
	}
	data, _ := json.MarshalIndent(artifact, "", "  ")
	if err := os.WriteFile(filepath.Join(tmpDir, "run.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	r := NewRechecker(nil)
	r.StopConditions["stress.hung"] = StopCondition{
		RequireTermination: true,
	}

	result, err := r.RecheckRun(tmpDir)
	if err != nil {
		t.Fatalf("RecheckRun error: %v", err)
	}

	if result.PolicyResult.Passed {
		t.Error("should fail when RequireTermination is true and exit code is -1")
	}
}
