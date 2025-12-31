package campaign

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Contract: run.json schema is stable and parseable.
func TestArtifact_RunJSON_SchemaStable(t *testing.T) {
	tmpDir := t.TempDir()

	result := &RunResult{
		Instance: &Instance{
			Name:           "test.instance",
			Tier:           TierQuick,
			Tool:           ToolStress,
			RequiresOracle: true,
		},
		Seed:          12345,
		RunDir:        tmpDir,
		BinaryPath:    "/bin/stresstest",
		StartTime:     time.Now().Add(-time.Minute),
		EndTime:       time.Now(),
		ExitCode:      0,
		Passed:        true,
		FailureReason: "",
		FailureKind:   "",
		Fingerprint:   "",
		IsDuplicate:   false,
	}

	err := WriteRunArtifact(result)
	if err != nil {
		t.Fatalf("WriteRunArtifact: %v", err)
	}

	// Read and parse the artifact
	data, err := os.ReadFile(filepath.Join(tmpDir, "run.json"))
	if err != nil {
		t.Fatalf("read run.json: %v", err)
	}

	var artifact RunArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("parse run.json: %v", err)
	}

	// Verify required fields
	if artifact.SchemaVersion == "" {
		t.Error("schema_version must be present")
	}
	if artifact.SchemaVersion != SchemaVersion {
		t.Errorf("schema_version = %q, want %q", artifact.SchemaVersion, SchemaVersion)
	}
	if artifact.Instance != "test.instance" {
		t.Errorf("instance = %q, want %q", artifact.Instance, "test.instance")
	}
	if artifact.Seed != 12345 {
		t.Errorf("seed = %d, want %d", artifact.Seed, 12345)
	}
	if !artifact.Passed {
		t.Error("passed should be true")
	}
}

// Contract: run.json includes tags when instance is available.
func TestArtifact_RunJSON_IncludesTags(t *testing.T) {
	tmpDir := t.TempDir()

	result := &RunResult{
		Instance: &Instance{
			Name:           "status.durability.test",
			Tier:           TierNightly,
			Tool:           ToolCrash,
			RequiresOracle: true,
			FaultModel: FaultModel{
				Kind:  FaultCrash,
				Scope: ScopeGlobal,
			},
		},
		Seed:      999,
		RunDir:    tmpDir,
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Passed:    true,
	}

	if err := WriteRunArtifact(result); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(tmpDir, "run.json"))
	var artifact RunArtifact
	json.Unmarshal(data, &artifact)

	if artifact.Tags == nil {
		t.Fatal("tags should be present")
	}
	if artifact.Tags.Tier != "nightly" {
		t.Errorf("tags.tier = %q, want nightly", artifact.Tags.Tier)
	}
	if artifact.Tags.Tool != "crashtest" {
		t.Errorf("tags.tool = %q, want crashtest", artifact.Tags.Tool)
	}
	if !artifact.Tags.OracleRequired {
		t.Error("tags.oracle_required should be true")
	}
	if artifact.Tags.FaultKind != "crash" {
		t.Errorf("tags.fault_kind = %q, want crash", artifact.Tags.FaultKind)
	}
}

// Contract: summary.json AllPassed reflects actual run outcomes.
func TestArtifact_SummaryJSON_AllPassedAccurate(t *testing.T) {
	tmpDir := t.TempDir()

	// Create results with mixed outcomes
	results := []*RunResult{
		{
			Instance: &Instance{Name: "pass1"},
			Seed:     1,
			Passed:   true,
			RunDir:   filepath.Join(tmpDir, "run1"),
		},
		{
			Instance:      &Instance{Name: "fail1"},
			Seed:          2,
			Passed:        false,
			FailureReason: "test failure",
			RunDir:        filepath.Join(tmpDir, "run2"),
		},
	}

	startTime := time.Now().Add(-time.Minute)
	endTime := time.Now()

	err := WriteCampaignSummary(tmpDir, TierQuick, startTime, endTime, results, nil)
	if err != nil {
		t.Fatalf("WriteCampaignSummary: %v", err)
	}

	// Read and parse
	data, _ := os.ReadFile(filepath.Join(tmpDir, "summary.json"))
	var summary CampaignSummary
	json.Unmarshal(data, &summary)

	if summary.AllPassed {
		t.Error("AllPassed should be false when there are failures")
	}
	if summary.TotalRuns != 2 {
		t.Errorf("TotalRuns = %d, want 2", summary.TotalRuns)
	}
	if summary.PassedRuns != 1 {
		t.Errorf("PassedRuns = %d, want 1", summary.PassedRuns)
	}
	if summary.FailedRuns != 1 {
		t.Errorf("FailedRuns = %d, want 1", summary.FailedRuns)
	}
}

// Contract: summary.json includes schema version.
func TestArtifact_SummaryJSON_IncludesSchemaVersion(t *testing.T) {
	tmpDir := t.TempDir()

	results := []*RunResult{
		{
			Instance: &Instance{Name: "test"},
			Seed:     1,
			Passed:   true,
			RunDir:   filepath.Join(tmpDir, "run1"),
		},
	}

	err := WriteCampaignSummary(tmpDir, TierQuick, time.Now(), time.Now(), results, nil)
	if err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(tmpDir, "summary.json"))
	var summary CampaignSummary
	json.Unmarshal(data, &summary)

	if summary.SchemaVersion != SchemaVersion {
		t.Errorf("schema_version = %q, want %q", summary.SchemaVersion, SchemaVersion)
	}
}

// Contract: run.json records trace metadata when present.
func TestArtifact_RunJSON_TraceMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	result := &RunResult{
		Instance:  &Instance{Name: "trace.test"},
		Seed:      123,
		RunDir:    tmpDir,
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Passed:    true,
		TraceResult: &TraceResult{
			Path:          "/tmp/trace.bin",
			BytesWritten:  1024000,
			Truncated:     true,
			ReplayCommand: "traceanalyzer replay /tmp/trace.bin",
		},
	}

	if err := WriteRunArtifact(result); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(tmpDir, "run.json"))
	var artifact RunArtifact
	json.Unmarshal(data, &artifact)

	if artifact.TracePath != "/tmp/trace.bin" {
		t.Errorf("trace_path = %q, want /tmp/trace.bin", artifact.TracePath)
	}
	if artifact.TraceBytesWriten != 1024000 {
		t.Errorf("trace_bytes_written = %d, want 1024000", artifact.TraceBytesWriten)
	}
	if !artifact.TraceTruncated {
		t.Error("trace_truncated should be true")
	}
	if artifact.ReplayCommand == "" {
		t.Error("replay_command should be set")
	}
}

// Contract: run.json records minimize result when present.
func TestArtifact_RunJSON_MinimizeResult(t *testing.T) {
	tmpDir := t.TempDir()

	result := &RunResult{
		Instance:  &Instance{Name: "minimize.test"},
		Seed:      123,
		RunDir:    tmpDir,
		StartTime: time.Now(),
		EndTime:   time.Now(),
		Passed:    false,
		MinimizeResult: &MinimizeResult{
			Success:              true,
			OriginalArgs:         []string{"-duration", "60", "-threads", "16", "-keys", "10000"},
			MinimalArgs:          []string{"-duration", "5", "-threads", "4", "-keys", "500"},
			FinalDuration:        "5s",
			FinalThreads:         4,
			FinalKeys:            500,
			PreservedFailureKind: "assertion",
		},
	}

	if err := WriteRunArtifact(result); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(tmpDir, "run.json"))
	var artifact RunArtifact
	json.Unmarshal(data, &artifact)

	if !artifact.Minimized {
		t.Error("minimized should be true")
	}
	if artifact.MinimizedResult == nil {
		t.Fatal("minimized_result should be present")
	}
	if artifact.MinimizedResult.FinalThreads != 4 {
		t.Errorf("final_threads = %d, want 4", artifact.MinimizedResult.FinalThreads)
	}
}

// Contract: duplicate_of.txt is written for duplicate failures.
func TestArtifact_DuplicateMarker_Written(t *testing.T) {
	tmpDir := t.TempDir()

	result := &RunResult{
		Instance:      &Instance{Name: "dup.test"},
		Seed:          123,
		RunDir:        tmpDir,
		StartTime:     time.Now(),
		EndTime:       time.Now(),
		Passed:        false,
		FailureReason: "test failure",
		Fingerprint:   "abc123",
		IsDuplicate:   true,
	}

	if err := WriteRunArtifact(result); err != nil {
		t.Fatal(err)
	}

	// Check duplicate_of.txt exists
	dupPath := filepath.Join(tmpDir, "duplicate_of.txt")
	if _, err := os.Stat(dupPath); os.IsNotExist(err) {
		t.Error("duplicate_of.txt should be written for duplicate failures")
	}

	// Check content contains the fingerprint
	content, _ := os.ReadFile(dupPath)
	if len(content) == 0 {
		t.Error("duplicate_of.txt should not be empty")
	}
	// Content format may include prefix, just check fingerprint is present
	contentStr := string(content)
	if !strings.Contains(contentStr, "abc123") {
		t.Errorf("duplicate_of.txt should contain fingerprint 'abc123', got %q", contentStr)
	}
}
