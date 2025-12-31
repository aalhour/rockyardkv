package campaign

import "testing"

// Contract: QuickInstances returns at least one instance.
func TestQuickInstances_NotEmpty(t *testing.T) {
	instances := QuickInstances()
	if len(instances) == 0 {
		t.Error("QuickInstances() should return at least one instance")
	}
}

// Contract: NightlyInstances returns at least one instance.
func TestNightlyInstances_NotEmpty(t *testing.T) {
	instances := NightlyInstances()
	if len(instances) == 0 {
		t.Error("NightlyInstances() should return at least one instance")
	}
}

// Contract: NightlyInstances includes all quick instances plus additional ones.
func TestNightlyInstances_IncludesQuick(t *testing.T) {
	quick := QuickInstances()
	nightly := NightlyInstances()

	if len(nightly) < len(quick) {
		t.Errorf("NightlyInstances() count (%d) should be >= QuickInstances() count (%d)",
			len(nightly), len(quick))
	}
}

// Contract: GetInstances(TierQuick) returns instances.
func TestGetInstances_Quick(t *testing.T) {
	instances := GetInstances(TierQuick)

	if len(instances) == 0 {
		t.Error("GetInstances(TierQuick) should return instances")
	}
}

// Contract: GetInstances(TierNightly) returns at least as many as quick tier.
func TestGetInstances_Nightly(t *testing.T) {
	instances := GetInstances(TierNightly)

	if len(instances) == 0 {
		t.Error("GetInstances(TierNightly) should return instances")
	}

	quick := GetInstances(TierQuick)
	if len(instances) < len(quick) {
		t.Errorf("GetInstances(TierNightly) count (%d) should be >= TierQuick (%d)",
			len(instances), len(quick))
	}
}

// Contract: GetInstances returns instances for unknown tiers (defaults to quick).
func TestGetInstances_UnknownTierDefaultsToQuick(t *testing.T) {
	instances := GetInstances(Tier("unknown"))

	if len(instances) == 0 {
		t.Error("GetInstances(unknown) should return instances (defaults to quick)")
	}
}

// Contract: All quick instances have Name, Tool, Seeds, and Tier set.
func TestQuickInstances_AllHaveRequiredFields(t *testing.T) {
	for _, inst := range QuickInstances() {
		if inst.Name == "" {
			t.Error("instance Name should not be empty")
		}
		if inst.Tool == "" {
			t.Errorf("instance %q Tool should not be empty", inst.Name)
		}
		if len(inst.Seeds) == 0 {
			t.Errorf("instance %q Seeds should not be empty", inst.Name)
		}
		if inst.Tier == "" {
			t.Errorf("instance %q Tier should not be empty", inst.Name)
		}
	}
}

// Contract: All nightly instances have Name, Tool, and Seeds set.
func TestNightlyInstances_AllHaveRequiredFields(t *testing.T) {
	for _, inst := range NightlyInstances() {
		if inst.Name == "" {
			t.Error("instance Name should not be empty")
		}
		if inst.Tool == "" {
			t.Errorf("instance %q Tool should not be empty", inst.Name)
		}
		if len(inst.Seeds) == 0 {
			t.Errorf("instance %q Seeds should not be empty", inst.Name)
		}
	}
}

// Contract: Quick instance names are unique.
func TestQuickInstances_UniqueNames(t *testing.T) {
	names := make(map[string]bool)
	for _, inst := range QuickInstances() {
		if names[inst.Name] {
			t.Errorf("duplicate instance name: %q", inst.Name)
		}
		names[inst.Name] = true
	}
}

// Contract: Nightly instance names are unique.
func TestNightlyInstances_UniqueNames(t *testing.T) {
	names := make(map[string]bool)
	for _, inst := range NightlyInstances() {
		if names[inst.Name] {
			t.Errorf("duplicate instance name: %q", inst.Name)
		}
		names[inst.Name] = true
	}
}

// Contract: GetInstances includes status instances for the tier.
func TestGetInstances_IncludesStatusInstances(t *testing.T) {
	quick := GetInstances(TierQuick)
	status := StatusInstances()

	quickNames := make(map[string]bool)
	for _, inst := range quick {
		quickNames[inst.Name] = true
	}

	for _, inst := range status {
		if inst.Tier == TierQuick && !quickNames[inst.Name] {
			t.Errorf("GetInstances(TierQuick) should include status instance %q", inst.Name)
		}
	}
}
