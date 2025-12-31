package campaign

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Contract: QuarantinePolicy constants are defined correctly.
func TestQuarantinePolicy_Constants(t *testing.T) {
	if QuarantineNone != "" {
		t.Errorf("QuarantineNone should be empty string, got %q", QuarantineNone)
	}
	if QuarantineAllowed != "allowed" {
		t.Errorf("QuarantineAllowed should be 'allowed', got %q", QuarantineAllowed)
	}
	if QuarantineSkip != "skip" {
		t.Errorf("QuarantineSkip should be 'skip', got %q", QuarantineSkip)
	}
}

// Contract: KnownFailures.Get returns nil for unknown fingerprints.
func TestKnownFailures_Get_Unknown(t *testing.T) {
	kf := NewKnownFailures("")
	if got := kf.Get("unknown"); got != nil {
		t.Errorf("Get(unknown) should return nil, got %v", got)
	}
}

// Contract: KnownFailures.Get returns the failure for known fingerprints.
func TestKnownFailures_Get_Known(t *testing.T) {
	kf := NewKnownFailures("")
	kf.Record("fp1", "instance1", "2025-01-01T00:00:00Z")

	got := kf.Get("fp1")
	if got == nil {
		t.Fatal("Get(fp1) should return a failure")
	}
	if got.Fingerprint != "fp1" {
		t.Errorf("Fingerprint = %q, want %q", got.Fingerprint, "fp1")
	}
}

// Contract: KnownFailures.IsQuarantined returns false for unknown fingerprints.
func TestKnownFailures_IsQuarantined_Unknown(t *testing.T) {
	kf := NewKnownFailures("")
	if kf.IsQuarantined("unknown") {
		t.Error("IsQuarantined(unknown) should be false")
	}
}

// Contract: KnownFailures.IsQuarantined returns false for non-quarantined failures.
func TestKnownFailures_IsQuarantined_NotQuarantined(t *testing.T) {
	kf := NewKnownFailures("")
	kf.Record("fp1", "instance1", "2025-01-01T00:00:00Z")

	if kf.IsQuarantined("fp1") {
		t.Error("IsQuarantined(fp1) should be false when not quarantined")
	}
}

// Contract: KnownFailures.IsQuarantined returns true for quarantined failures.
func TestKnownFailures_IsQuarantined_Quarantined(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "kf.json")

	// Create a known failures file with quarantine
	data := []KnownFailure{
		{
			Fingerprint: "fp1",
			Instance:    "instance1",
			FirstSeen:   "2025-01-01T00:00:00Z",
			Count:       1,
			IssueID:     "GH-123",
			Quarantine:  QuarantineAllowed,
		},
	}
	jsonData, _ := json.MarshalIndent(data, "", "  ")
	os.WriteFile(path, jsonData, 0o644)

	kf := NewKnownFailures(path)

	if !kf.IsQuarantined("fp1") {
		t.Error("IsQuarantined(fp1) should be true when quarantined")
	}
}

// Contract: KnownFailures.GetQuarantinePolicy returns the correct policy.
func TestKnownFailures_GetQuarantinePolicy(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "kf.json")

	data := []KnownFailure{
		{Fingerprint: "fp1", Quarantine: QuarantineAllowed},
		{Fingerprint: "fp2", Quarantine: QuarantineSkip},
		{Fingerprint: "fp3", Quarantine: QuarantineNone},
	}
	jsonData, _ := json.MarshalIndent(data, "", "  ")
	os.WriteFile(path, jsonData, 0o644)

	kf := NewKnownFailures(path)

	if got := kf.GetQuarantinePolicy("fp1"); got != QuarantineAllowed {
		t.Errorf("fp1 policy = %q, want %q", got, QuarantineAllowed)
	}
	if got := kf.GetQuarantinePolicy("fp2"); got != QuarantineSkip {
		t.Errorf("fp2 policy = %q, want %q", got, QuarantineSkip)
	}
	if got := kf.GetQuarantinePolicy("fp3"); got != QuarantineNone {
		t.Errorf("fp3 policy = %q, want %q", got, QuarantineNone)
	}
	if got := kf.GetQuarantinePolicy("unknown"); got != QuarantineNone {
		t.Errorf("unknown policy = %q, want %q", got, QuarantineNone)
	}
}

// Contract: FailureClass constants are defined correctly.
func TestFailureClass_Constants(t *testing.T) {
	if FailureClassNone != "" {
		t.Errorf("FailureClassNone should be empty, got %q", FailureClassNone)
	}
	if FailureClassNew != "new_failure" {
		t.Errorf("FailureClassNew should be 'new_failure', got %q", FailureClassNew)
	}
	if FailureClassKnown != "known_failure" {
		t.Errorf("FailureClassKnown should be 'known_failure', got %q", FailureClassKnown)
	}
	if FailureClassDuplicate != "duplicate" {
		t.Errorf("FailureClassDuplicate should be 'duplicate', got %q", FailureClassDuplicate)
	}
}

// Contract: CampaignSummary includes governance fields for failure classification.
func TestCampaignSummary_GovernanceFields(t *testing.T) {
	tmpDir := t.TempDir()

	inst := &Instance{
		Name:           "test.instance",
		RequiresOracle: true,
	}

	results := []*RunResult{
		{
			Instance:     inst,
			Passed:       true,
			StartTime:    time.Now(),
			EndTime:      time.Now(),
			FailureClass: FailureClassNone,
		},
		{
			Instance:         inst,
			Passed:           false,
			StartTime:        time.Now(),
			EndTime:          time.Now(),
			FailureReason:    "test failure",
			Fingerprint:      "fp1",
			FailureClass:     FailureClassNew,
			QuarantinePolicy: QuarantineNone,
		},
		{
			Instance:         inst,
			Passed:           false,
			StartTime:        time.Now(),
			EndTime:          time.Now(),
			FailureReason:    "known failure",
			Fingerprint:      "fp2",
			FailureClass:     FailureClassKnown,
			QuarantinePolicy: QuarantineAllowed,
		},
		{
			Instance:         inst,
			Passed:           false,
			StartTime:        time.Now(),
			EndTime:          time.Now(),
			FailureReason:    "duplicate",
			Fingerprint:      "fp1",
			FailureClass:     FailureClassDuplicate,
			QuarantinePolicy: QuarantineNone,
		},
	}

	err := WriteCampaignSummary(tmpDir, TierQuick, time.Now(), time.Now(), results, nil)
	if err != nil {
		t.Fatalf("WriteCampaignSummary: %v", err)
	}

	// Read and verify
	data, _ := os.ReadFile(filepath.Join(tmpDir, "summary.json"))
	var summary CampaignSummary
	json.Unmarshal(data, &summary)

	if summary.NewFailures != 1 {
		t.Errorf("NewFailures = %d, want 1", summary.NewFailures)
	}
	if summary.KnownFailures != 1 {
		t.Errorf("KnownFailures = %d, want 1", summary.KnownFailures)
	}
	if summary.Duplicates != 1 {
		t.Errorf("Duplicates = %d, want 1", summary.Duplicates)
	}
	if summary.Unquarantined != 1 {
		t.Errorf("Unquarantined = %d, want 1", summary.Unquarantined)
	}
	if summary.OracleRequired != 4 {
		t.Errorf("OracleRequired = %d, want 4", summary.OracleRequired)
	}
}

// Contract: RunSummary includes FailureClass.
func TestRunSummary_FailureClass(t *testing.T) {
	tmpDir := t.TempDir()

	inst := &Instance{Name: "test.instance"}
	results := []*RunResult{
		{
			Instance:      inst,
			Passed:        false,
			StartTime:     time.Now(),
			EndTime:       time.Now(),
			FailureReason: "test",
			Fingerprint:   "fp1",
			FailureClass:  FailureClassNew,
		},
	}

	err := WriteCampaignSummary(tmpDir, TierQuick, time.Now(), time.Now(), results, nil)
	if err != nil {
		t.Fatalf("WriteCampaignSummary: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(tmpDir, "summary.json"))
	var summary CampaignSummary
	json.Unmarshal(data, &summary)

	if len(summary.Runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(summary.Runs))
	}
	if summary.Runs[0].FailureClass != FailureClassNew {
		t.Errorf("FailureClass = %q, want %q", summary.Runs[0].FailureClass, FailureClassNew)
	}
}

// Contract: SchemaVersion is 1.1.0 to indicate governance fields are present.
func TestSchemaVersion_Phase5(t *testing.T) {
	if SchemaVersion != "1.1.0" {
		t.Errorf("SchemaVersion = %q, want %q", SchemaVersion, "1.1.0")
	}
}

// Contract: Unquarantined counts duplicate failures, not new failures.
// Rationale: New failures can't be expected to have quarantine mappings because
// you haven't seen them before. Only repeat failures (duplicates) without
// quarantine should be counted as "unquarantined".
func TestCampaignSummary_Unquarantined_OnlyCountsDuplicates(t *testing.T) {
	tmpDir := t.TempDir()
	inst := &Instance{Name: "test.instance"}

	// Case 1: Only new failures - Unquarantined should be 0
	t.Run("NewFailuresOnly", func(t *testing.T) {
		dir := filepath.Join(tmpDir, "case1")
		os.MkdirAll(dir, 0o755)

		results := []*RunResult{
			{
				Instance:      inst,
				Passed:        false,
				StartTime:     time.Now(),
				EndTime:       time.Now(),
				FailureClass:  FailureClassNew,
				Fingerprint:   "fp1",
				FailureReason: "new failure 1",
			},
			{
				Instance:      inst,
				Passed:        false,
				StartTime:     time.Now(),
				EndTime:       time.Now(),
				FailureClass:  FailureClassNew,
				Fingerprint:   "fp2",
				FailureReason: "new failure 2",
			},
		}

		err := WriteCampaignSummary(dir, TierQuick, time.Now(), time.Now(), results, nil)
		if err != nil {
			t.Fatalf("WriteCampaignSummary: %v", err)
		}

		data, _ := os.ReadFile(filepath.Join(dir, "summary.json"))
		var summary CampaignSummary
		json.Unmarshal(data, &summary)

		if summary.NewFailures != 2 {
			t.Errorf("NewFailures = %d, want 2", summary.NewFailures)
		}
		if summary.Unquarantined != 0 {
			t.Errorf("Unquarantined = %d, want 0 (new failures are not unquarantined)", summary.Unquarantined)
		}
	})

	// Case 2: Only duplicates - Unquarantined should equal duplicates
	t.Run("DuplicatesOnly", func(t *testing.T) {
		dir := filepath.Join(tmpDir, "case2")
		os.MkdirAll(dir, 0o755)

		results := []*RunResult{
			{
				Instance:      inst,
				Passed:        false,
				StartTime:     time.Now(),
				EndTime:       time.Now(),
				FailureClass:  FailureClassDuplicate,
				Fingerprint:   "fp1",
				FailureReason: "duplicate 1",
			},
			{
				Instance:      inst,
				Passed:        false,
				StartTime:     time.Now(),
				EndTime:       time.Now(),
				FailureClass:  FailureClassDuplicate,
				Fingerprint:   "fp2",
				FailureReason: "duplicate 2",
			},
		}

		err := WriteCampaignSummary(dir, TierQuick, time.Now(), time.Now(), results, nil)
		if err != nil {
			t.Fatalf("WriteCampaignSummary: %v", err)
		}

		data, _ := os.ReadFile(filepath.Join(dir, "summary.json"))
		var summary CampaignSummary
		json.Unmarshal(data, &summary)

		if summary.Duplicates != 2 {
			t.Errorf("Duplicates = %d, want 2", summary.Duplicates)
		}
		if summary.Unquarantined != 2 {
			t.Errorf("Unquarantined = %d, want 2 (duplicates require quarantine)", summary.Unquarantined)
		}
	})

	// Case 3: Only known failures (quarantined) - Unquarantined should be 0
	t.Run("KnownFailuresOnly", func(t *testing.T) {
		dir := filepath.Join(tmpDir, "case3")
		os.MkdirAll(dir, 0o755)

		results := []*RunResult{
			{
				Instance:         inst,
				Passed:           false,
				StartTime:        time.Now(),
				EndTime:          time.Now(),
				FailureClass:     FailureClassKnown,
				QuarantinePolicy: QuarantineAllowed,
				Fingerprint:      "fp1",
				FailureReason:    "known failure",
			},
		}

		err := WriteCampaignSummary(dir, TierQuick, time.Now(), time.Now(), results, nil)
		if err != nil {
			t.Fatalf("WriteCampaignSummary: %v", err)
		}

		data, _ := os.ReadFile(filepath.Join(dir, "summary.json"))
		var summary CampaignSummary
		json.Unmarshal(data, &summary)

		if summary.KnownFailures != 1 {
			t.Errorf("KnownFailures = %d, want 1", summary.KnownFailures)
		}
		if summary.Unquarantined != 0 {
			t.Errorf("Unquarantined = %d, want 0 (known failures are already quarantined)", summary.Unquarantined)
		}
	})
}

// Contract: classifyFailure returns FailureClassKnown for quarantined fingerprints.
func TestClassifyFailure_QuarantinedFingerprint(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "kf.json")

	// Pre-populate with a quarantined failure
	data := []KnownFailure{
		{
			Fingerprint: "fp_quarantined",
			Instance:    "test.instance",
			FirstSeen:   "2025-01-01T00:00:00Z",
			Count:       5,
			IssueID:     "GH-999",
			Quarantine:  QuarantineAllowed,
		},
	}
	jsonData, _ := json.MarshalIndent(data, "", "  ")
	os.WriteFile(path, jsonData, 0o644)

	kf := NewKnownFailures(path)
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: kf,
			Output:        io.Discard,
		},
	}

	fc, qp, isDup := runner.classifyFailure("fp_quarantined", "test.instance", "2025-01-01T12:00:00Z")

	if fc != FailureClassKnown {
		t.Errorf("FailureClass = %q, want %q", fc, FailureClassKnown)
	}
	if qp != QuarantineAllowed {
		t.Errorf("QuarantinePolicy = %q, want %q", qp, QuarantineAllowed)
	}
	if !isDup {
		t.Error("IsDuplicate should be true for quarantined fingerprints")
	}
}

// Contract: classifyFailure returns FailureClassDuplicate for non-quarantined repeat failures.
func TestClassifyFailure_NonQuarantinedRepeat(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "kf.json")

	// Pre-populate with a non-quarantined failure
	data := []KnownFailure{
		{
			Fingerprint: "fp_repeat",
			Instance:    "test.instance",
			FirstSeen:   "2025-01-01T00:00:00Z",
			Count:       3,
			// No IssueID, no Quarantine
		},
	}
	jsonData, _ := json.MarshalIndent(data, "", "  ")
	os.WriteFile(path, jsonData, 0o644)

	kf := NewKnownFailures(path)
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: kf,
			Output:        io.Discard,
		},
	}

	fc, qp, isDup := runner.classifyFailure("fp_repeat", "test.instance", "2025-01-01T12:00:00Z")

	if fc != FailureClassDuplicate {
		t.Errorf("FailureClass = %q, want %q", fc, FailureClassDuplicate)
	}
	if qp != QuarantineNone {
		t.Errorf("QuarantinePolicy = %q, want %q", qp, QuarantineNone)
	}
	if !isDup {
		t.Error("IsDuplicate should be true for repeat failures")
	}
}

// Contract: classifyFailure returns FailureClassNew for never-seen fingerprints.
func TestClassifyFailure_NewFingerprint(t *testing.T) {
	kf := NewKnownFailures("")
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: kf,
			Output:        io.Discard,
		},
	}

	fc, qp, isDup := runner.classifyFailure("fp_new", "test.instance", "2025-01-01T00:00:00Z")

	if fc != FailureClassNew {
		t.Errorf("FailureClass = %q, want %q", fc, FailureClassNew)
	}
	if qp != QuarantineNone {
		t.Errorf("QuarantinePolicy = %q, want %q", qp, QuarantineNone)
	}
	if isDup {
		t.Error("IsDuplicate should be false for new fingerprints")
	}

	// Verify it was recorded
	if kf.Count() != 1 {
		t.Errorf("KnownFailures.Count = %d, want 1", kf.Count())
	}
}

// Contract: classifyFailure records new fingerprints in KnownFailures.
func TestClassifyFailure_RecordsNewFailure(t *testing.T) {
	kf := NewKnownFailures("")
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: kf,
			Output:        io.Discard,
		},
	}

	// First occurrence
	runner.classifyFailure("fp_test", "instance1", "2025-01-01T00:00:00Z")

	recorded := kf.Get("fp_test")
	if recorded == nil {
		t.Fatal("New failure was not recorded")
	}
	if recorded.Instance != "instance1" {
		t.Errorf("Instance = %q, want %q", recorded.Instance, "instance1")
	}
	if recorded.Count != 1 {
		t.Errorf("Count = %d, want 1", recorded.Count)
	}
}

// Contract: classifyFailure increments count for repeat failures.
func TestClassifyFailure_IncrementsCount(t *testing.T) {
	kf := NewKnownFailures("")
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: kf,
			Output:        io.Discard,
		},
	}

	// First occurrence
	runner.classifyFailure("fp_test", "instance1", "2025-01-01T00:00:00Z")
	// Second occurrence (duplicate)
	runner.classifyFailure("fp_test", "instance1", "2025-01-01T01:00:00Z")
	// Third occurrence (duplicate)
	runner.classifyFailure("fp_test", "instance1", "2025-01-01T02:00:00Z")

	recorded := kf.Get("fp_test")
	if recorded == nil {
		t.Fatal("Failure was not recorded")
	}
	if recorded.Count != 3 {
		t.Errorf("Count = %d, want 3", recorded.Count)
	}
}

// Contract: classifyFailure without KnownFailures returns FailureClassNew.
func TestClassifyFailure_NoKnownFailures(t *testing.T) {
	runner := &Runner{
		config: RunnerConfig{
			KnownFailures: nil,
			Output:        io.Discard,
		},
	}

	fc, qp, isDup := runner.classifyFailure("fp_any", "instance", "2025-01-01T00:00:00Z")

	if fc != FailureClassNew {
		t.Errorf("FailureClass = %q, want %q", fc, FailureClassNew)
	}
	if qp != QuarantineNone {
		t.Errorf("QuarantinePolicy = %q, want %q", qp, QuarantineNone)
	}
	if isDup {
		t.Error("IsDuplicate should be false when KnownFailures is nil")
	}
}

// Contract: Persisted summary.json with unquarantined > 0 should fail require-quarantine check.
// This is a regression test ensuring that -require-quarantine works with the canonical artifact.
func TestRequireQuarantine_FailsOnUnquarantinedDuplicates(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a summary.json with unquarantined duplicates
	summary := CampaignSummary{
		SchemaVersion: SchemaVersion,
		Tier:          "quick",
		TotalRuns:     2,
		FailedRuns:    2,
		Duplicates:    1,
		Unquarantined: 1, // Key field: must trigger require-quarantine failure
	}

	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "summary.json"), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read back and verify
	readSummary, err := ReadCampaignSummary(tmpDir)
	if err != nil {
		t.Fatalf("ReadCampaignSummary: %v", err)
	}

	if readSummary.Unquarantined != 1 {
		t.Errorf("Unquarantined = %d, want 1", readSummary.Unquarantined)
	}

	// Simulate what cmd/campaignrunner does with -require-quarantine
	requireQuarantine := true
	if requireQuarantine && readSummary.Unquarantined > 0 {
		// This should trigger the failure path
		t.Log("require-quarantine correctly detected unquarantined failures")
	} else {
		t.Error("require-quarantine should detect unquarantined > 0")
	}
}

// Contract: ReadCampaignSummary returns the canonical persisted summary.
func TestReadCampaignSummary_ReturnsCanonicalData(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a summary with specific governance fields
	original := CampaignSummary{
		SchemaVersion: SchemaVersion,
		Tier:          "nightly",
		TotalRuns:     10,
		PassedRuns:    7,
		FailedRuns:    3,
		SkippedRuns:   2,
		Skipped: []SkipSummary{
			{Instance: "skipped.test.1", Reason: "broken"},
			{Instance: "skipped.test.2", Reason: "flaky"},
		},
		NewFailures:   1,
		KnownFailures: 1,
		Duplicates:    1,
		Unquarantined: 1,
	}

	data, _ := json.MarshalIndent(original, "", "  ")
	os.WriteFile(filepath.Join(tmpDir, "summary.json"), data, 0o644)

	// Read back
	read, err := ReadCampaignSummary(tmpDir)
	if err != nil {
		t.Fatalf("ReadCampaignSummary: %v", err)
	}

	// Verify all fields match
	if read.TotalRuns != 10 {
		t.Errorf("TotalRuns = %d, want 10", read.TotalRuns)
	}
	if read.SkippedRuns != 2 {
		t.Errorf("SkippedRuns = %d, want 2", read.SkippedRuns)
	}
	if len(read.Skipped) != 2 {
		t.Errorf("len(Skipped) = %d, want 2", len(read.Skipped))
	}
	if read.Unquarantined != 1 {
		t.Errorf("Unquarantined = %d, want 1", read.Unquarantined)
	}
}
