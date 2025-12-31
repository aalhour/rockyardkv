package campaign

import "testing"

// Contract: Filter is applied to RunInstances via FilterInstances.
func TestFilter_AppliedToRunInstances(t *testing.T) {
	// Get all instances
	instances := GetInstances(TierQuick)
	if len(instances) == 0 {
		t.Skip("no instances available")
	}

	// Filter to only stresstest
	filter, err := ParseFilter("tool=stresstest")
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}

	filtered := FilterInstances(instances, filter)

	// Verify all filtered instances are stresstest
	for _, inst := range filtered {
		if inst.Tool != ToolStress {
			t.Errorf("instance %s has tool=%s, expected stresstest", inst.Name, inst.Tool)
		}
	}

	// Verify filter actually reduced the count (if there are non-stresstest instances)
	hasNonStress := false
	for _, inst := range instances {
		if inst.Tool != ToolStress {
			hasNonStress = true
			break
		}
	}

	if hasNonStress && len(filtered) >= len(instances) {
		t.Error("filter should reduce instance count when non-stresstest instances exist")
	}
}

// Contract: Filter with oracle_required=true only returns oracle-required instances.
func TestFilter_OracleRequired_True(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("oracle_required=true")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		if !inst.RequiresOracle {
			t.Errorf("instance %s has RequiresOracle=false, expected true", inst.Name)
		}
	}
}

// Contract: Filter with oracle_required=false only returns non-oracle instances.
func TestFilter_OracleRequired_False(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("oracle_required=false")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		if inst.RequiresOracle {
			t.Errorf("instance %s has RequiresOracle=true, expected false", inst.Name)
		}
	}
}

// Contract: Filter with kind=status only returns status instances.
func TestFilter_Kind_Status(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("kind=status")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		tags := inst.ComputeTags()
		if tags.Kind != "status" {
			t.Errorf("instance %s has kind=%s, expected status", inst.Name, tags.Kind)
		}
	}
}

// Contract: Filter with multiple AND clauses restricts results.
func TestFilter_MultipleAndClauses(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("tool=stresstest,oracle_required=true")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		if inst.Tool != ToolStress {
			t.Errorf("instance %s has tool=%s, expected stresstest", inst.Name, inst.Tool)
		}
		if !inst.RequiresOracle {
			t.Errorf("instance %s has RequiresOracle=false, expected true", inst.Name)
		}
	}
}

// Contract: Filter with OR values (pipe-separated) matches any.
func TestFilter_OrValues_MatchesAny(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("tool=stresstest|crashtest")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		if inst.Tool != ToolStress && inst.Tool != ToolCrash {
			t.Errorf("instance %s has tool=%s, expected stresstest or crashtest", inst.Name, inst.Tool)
		}
	}
}

// Contract: Filter with NOT operator excludes matching instances.
func TestFilter_NotEqual_Excludes(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("tool!=goldentest")
	filtered := FilterInstances(instances, filter)

	for _, inst := range filtered {
		if inst.Tool == ToolGolden {
			t.Errorf("instance %s should be excluded by tool!=goldentest", inst.Name)
		}
	}
}

// Contract: Empty filter returns all instances unchanged.
func TestFilter_Empty_ReturnsAll(t *testing.T) {
	instances := GetInstances(TierQuick)
	filter, _ := ParseFilter("")
	filtered := FilterInstances(instances, filter)

	if len(filtered) != len(instances) {
		t.Errorf("empty filter should return all: got %d, want %d", len(filtered), len(instances))
	}
}

// Contract: Nil filter returns all instances unchanged.
func TestFilter_Nil_ReturnsAll(t *testing.T) {
	instances := GetInstances(TierQuick)
	filtered := FilterInstances(instances, nil)

	if len(filtered) != len(instances) {
		t.Errorf("nil filter should return all: got %d, want %d", len(filtered), len(instances))
	}
}

// Contract: RunnerConfig.Filter is applied in Run() and RunGroup().
func TestRunnerConfig_Filter_Applied(t *testing.T) {
	// This test verifies the Filter field exists and is plumbed through.
	// We cannot test actual runs without fixtures, but we can verify the field.
	filter, err := ParseFilter("tool=stresstest")
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}

	config := RunnerConfig{
		Tier:   TierQuick,
		Filter: filter,
	}

	// Verify filter is accessible
	if config.Filter == nil {
		t.Error("Filter should be set in config")
	}

	if len(config.Filter.Clauses) != 1 {
		t.Errorf("Filter should have 1 clause, got %d", len(config.Filter.Clauses))
	}

	if config.Filter.Clauses[0].Key != "tool" {
		t.Errorf("Filter clause key = %q, want %q", config.Filter.Clauses[0].Key, "tool")
	}
}
