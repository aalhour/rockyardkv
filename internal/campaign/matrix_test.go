package campaign

import "testing"

func TestQuickInstances(t *testing.T) {
	instances := QuickInstances()

	if len(instances) == 0 {
		t.Fatal("QuickInstances() should return at least one instance")
	}

	// Verify all instances have required fields
	for _, inst := range instances {
		if inst.Name == "" {
			t.Error("instance Name should not be empty")
		}
		if inst.Tier != TierQuick {
			t.Errorf("instance %q Tier = %q, want %q", inst.Name, inst.Tier, TierQuick)
		}
		if inst.Tool == "" {
			t.Errorf("instance %q Tool should not be empty", inst.Name)
		}
		if len(inst.Seeds) == 0 {
			t.Errorf("instance %q Seeds should not be empty", inst.Name)
		}
	}
}

func TestNightlyInstances(t *testing.T) {
	instances := NightlyInstances()

	if len(instances) == 0 {
		t.Fatal("NightlyInstances() should return at least one instance")
	}

	// Nightly should include all quick instances plus more
	quickInstances := QuickInstances()
	if len(instances) < len(quickInstances) {
		t.Errorf("NightlyInstances() length = %d, should be >= QuickInstances() length %d",
			len(instances), len(quickInstances))
	}
}

func TestGetInstances(t *testing.T) {
	quick := GetInstances(TierQuick)
	nightly := GetInstances(TierNightly)

	if len(quick) == 0 {
		t.Error("GetInstances(TierQuick) should return instances")
	}
	if len(nightly) == 0 {
		t.Error("GetInstances(TierNightly) should return instances")
	}
	if len(nightly) < len(quick) {
		t.Error("nightly should have at least as many instances as quick")
	}
}

func TestInstanceNames(t *testing.T) {
	instances := QuickInstances()

	// Check for expected instance names
	names := make(map[string]bool)
	for _, inst := range instances {
		names[inst.Name] = true
	}

	expected := []string{
		"stress.read.corruption.1in7",
		"stress.read.status.1in7",
		"stress.write.status.1in7",
		"stress.sync.status.1in7",
		"crash.blackbox",
		"golden.compat",
	}

	for _, name := range expected {
		if !names[name] {
			t.Errorf("QuickInstances() should include %q", name)
		}
	}
}

func TestInstanceSeeds(t *testing.T) {
	instances := QuickInstances()

	for _, inst := range instances {
		// Check for duplicate seeds
		seen := make(map[int64]bool)
		for _, seed := range inst.Seeds {
			if seen[seed] {
				t.Errorf("instance %q has duplicate seed %d", inst.Name, seed)
			}
			seen[seed] = true
		}
	}
}
