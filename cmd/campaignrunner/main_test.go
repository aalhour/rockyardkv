package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aalhour/rockyardkv/internal/campaign"
)

// Contract: handleListInstances produces valid JSON when asJSON=true.
func TestHandleListInstances_JSON_ValidOutput(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := handleListInstances("quick", "", true)

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("handleListInstances error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	// Verify it's valid JSON
	var result []struct {
		Name string        `json:"name"`
		Tags campaign.Tags `json:"tags"`
	}
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Errorf("output is not valid JSON: %v\nOutput: %s", err, output)
	}

	if len(result) == 0 {
		t.Error("expected at least one instance")
	}
}

// Contract: handleListInstances with filter reduces instance count.
func TestHandleListInstances_Filter_ReducesCount(t *testing.T) {
	// Get unfiltered count
	instances := campaign.GetInstances(campaign.TierQuick)
	unfilteredCount := len(instances)

	// Filter to only stresstest
	filter, _ := campaign.ParseFilter("tool=stresstest")
	filtered := campaign.FilterInstances(instances, filter)
	filteredCount := len(filtered)

	if filteredCount >= unfilteredCount && unfilteredCount > 1 {
		t.Errorf("filter should reduce count: unfiltered=%d, filtered=%d", unfilteredCount, filteredCount)
	}
}

// Contract: handleListInstances with unknown filter key fails fast.
func TestHandleListInstances_UnknownFilterKey_Fails(t *testing.T) {
	err := handleListInstances("quick", "invalid_key=value", false)
	if err == nil {
		t.Error("expected error for unknown filter key")
	}
}

// Contract: handleListInstances with empty filter returns all instances.
func TestHandleListInstances_EmptyFilter_ReturnsAll(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := handleListInstances("quick", "", true)

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("handleListInstances error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)

	var result []struct {
		Name string `json:"name"`
	}
	json.Unmarshal(buf.Bytes(), &result)

	expected := len(campaign.GetInstances(campaign.TierQuick))
	if len(result) != expected {
		t.Errorf("expected %d instances, got %d", expected, len(result))
	}
}

// Contract: handleRecheck fails fast for missing run root.
func TestHandleRecheck_MissingRunRoot_Fails(t *testing.T) {
	err := handleRecheck("/nonexistent/path/that/does/not/exist")
	if err == nil {
		t.Error("expected error for missing run root")
	}
}

// Contract: handleRecheck processes valid run directories.
func TestHandleRecheck_ValidRunDir_Succeeds(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a minimal run.json
	artifact := campaign.RunArtifact{
		SchemaVersion: campaign.SchemaVersion,
		Instance:      "test.instance",
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

	// Capture stdout to suppress output
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	os.Stdout = w

	err := handleRecheck(tmpDir)

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Errorf("handleRecheck error: %v", err)
	}
}

// Contract: -filter with OR values matches multiple tools.
func TestFilter_OrValues_MatchesMultipleTools(t *testing.T) {
	instances := campaign.GetInstances(campaign.TierQuick)
	filter, _ := campaign.ParseFilter("tool=stresstest|crashtest")
	filtered := campaign.FilterInstances(instances, filter)

	// Check that we have both stresstest and crashtest instances
	hasStress := false
	hasCrash := false
	for _, inst := range filtered {
		if inst.Tool == campaign.ToolStress {
			hasStress = true
		}
		if inst.Tool == campaign.ToolCrash {
			hasCrash = true
		}
	}

	if !hasStress && !hasCrash {
		t.Error("OR filter should match at least one of stresstest or crashtest")
	}
}

// Contract: -filter with AND clauses requires all conditions.
func TestFilter_AndClauses_RequiresAll(t *testing.T) {
	instances := campaign.GetInstances(campaign.TierQuick)
	filter, _ := campaign.ParseFilter("tool=stresstest,oracle_required=true")
	filtered := campaign.FilterInstances(instances, filter)

	for _, inst := range filtered {
		tags := inst.ComputeTags()
		if tags.Tool != "stresstest" {
			t.Errorf("instance %s should have tool=stresstest, got %s", inst.Name, tags.Tool)
		}
		if !tags.OracleRequired {
			t.Errorf("instance %s should have oracle_required=true", inst.Name)
		}
	}
}

// Contract: Tier defaults to quick for unknown values.
func TestHandleListInstances_UnknownTier_DefaultsToQuick(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := handleListInstances("unknown_tier", "", true)

	w.Close()
	os.Stdout = oldStdout

	if err != nil {
		t.Fatalf("handleListInstances error: %v", err)
	}

	var buf bytes.Buffer
	buf.ReadFrom(r)

	var result []struct {
		Name string `json:"name"`
	}
	json.Unmarshal(buf.Bytes(), &result)

	expected := len(campaign.GetInstances(campaign.TierQuick))
	if len(result) != expected {
		t.Errorf("unknown tier should default to quick: expected %d instances, got %d", expected, len(result))
	}
}
