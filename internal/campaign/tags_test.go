package campaign

import "testing"

// Contract: AllTagKeys returns all valid tag keys for filter validation.
func TestAllTagKeys(t *testing.T) {
	keys := AllTagKeys()

	expected := []string{
		"campaign",
		"tier",
		"tool",
		"kind",
		"oracle_required",
		"group",
		"fault_kind",
		"fault_scope",
	}

	if len(keys) != len(expected) {
		t.Errorf("AllTagKeys() length = %d, want %d", len(keys), len(expected))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, e := range expected {
		if !keySet[e] {
			t.Errorf("AllTagKeys() missing key %q", e)
		}
	}
}

// Contract: ComputeTags populates all required tag fields.
func TestInstance_ComputeTags(t *testing.T) {
	inst := Instance{
		Name:           "stress.read.corruption.1in7",
		Tier:           TierQuick,
		Tool:           ToolStress,
		RequiresOracle: true,
		FaultModel: FaultModel{
			Kind:  FaultRead,
			Scope: ScopeWorker,
		},
	}

	tags := inst.ComputeTags()

	if tags.Campaign != "C05" {
		t.Errorf("Campaign = %q, want %q", tags.Campaign, "C05")
	}
	if tags.Tier != "quick" {
		t.Errorf("Tier = %q, want %q", tags.Tier, "quick")
	}
	if tags.Tool != "stresstest" {
		t.Errorf("Tool = %q, want %q", tags.Tool, "stresstest")
	}
	if tags.Kind != "stress" {
		t.Errorf("Kind = %q, want %q", tags.Kind, "stress")
	}
	if !tags.OracleRequired {
		t.Error("OracleRequired should be true")
	}
	if tags.Group != "stress.read" {
		t.Errorf("Group = %q, want %q", tags.Group, "stress.read")
	}
	if tags.FaultKind != "read" {
		t.Errorf("FaultKind = %q, want %q", tags.FaultKind, "read")
	}
	if tags.FaultScope != "worker" {
		t.Errorf("FaultScope = %q, want %q", tags.FaultScope, "worker")
	}
}

// Contract: deriveKind returns "status" for status-prefixed instance names.
func TestDeriveKind_StatusPrefix(t *testing.T) {
	kind := deriveKind(ToolCrash, "status.durability.wal_sync")
	if kind != "status" {
		t.Errorf("deriveKind(ToolCrash, status.*) = %q, want %q", kind, "status")
	}
}

// Contract: deriveKind returns tool name for non-status instances.
func TestDeriveKind_NonStatus(t *testing.T) {
	tests := []struct {
		tool Tool
		name string
		want string
	}{
		{ToolStress, "stress.read.corruption", "stress"},
		{ToolCrash, "crash.loop.basic", "crash"},
		{ToolGolden, "golden.compat", "golden"},
		{ToolAdversarial, "adversarial.corruption", "adversarial"},
	}

	for _, tt := range tests {
		got := deriveKind(tt.tool, tt.name)
		if got != tt.want {
			t.Errorf("deriveKind(%s, %s) = %q, want %q", tt.tool, tt.name, got, tt.want)
		}
	}
}

// Contract: deriveGroup extracts the group prefix from instance names.
func TestDeriveGroup(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"status.durability.wal_sync", "status.durability"},
		{"stress.read.corruption", "stress.read"},
		{"crash", "crash"},
		{"golden.compat", "golden"},
	}

	for _, tt := range tests {
		got := deriveGroup(tt.name)
		if got != tt.want {
			t.Errorf("deriveGroup(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

// Contract: Tags.Get returns the correct value for all tag keys.
func TestTags_Get(t *testing.T) {
	tags := Tags{
		Campaign:       "C05",
		Tier:           "nightly",
		Tool:           "crashtest",
		Kind:           "crash",
		OracleRequired: true,
		Group:          "crash.loop",
		FaultKind:      "crash",
		FaultScope:     "global",
	}

	tests := []struct {
		key  string
		want string
	}{
		{"campaign", "C05"},
		{"tier", "nightly"},
		{"tool", "crashtest"},
		{"kind", "crash"},
		{"oracle_required", "true"},
		{"group", "crash.loop"},
		{"fault_kind", "crash"},
		{"fault_scope", "global"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		got := tags.Get(tt.key)
		if got != tt.want {
			t.Errorf("Tags.Get(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// Contract: Tags.Get returns "false" for OracleRequired when false.
func TestTags_Get_OracleFalse(t *testing.T) {
	tags := Tags{OracleRequired: false}
	if tags.Get("oracle_required") != "false" {
		t.Error("Tags.Get(oracle_required) should return 'false' when OracleRequired is false")
	}
}
