package campaign

import "testing"

func TestStatusInstances(t *testing.T) {
	instances := StatusInstances()

	if len(instances) == 0 {
		t.Fatal("StatusInstances() should return at least one instance")
	}

	// Verify all instances have required fields
	for _, inst := range instances {
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

func TestStatusInstanceNames(t *testing.T) {
	instances := StatusInstances()

	names := make(map[string]bool)
	for _, inst := range instances {
		names[inst.Name] = true
	}

	expected := []string{
		"status.golden",
		"status.durability.wal_sync",
		"status.durability.wal_sync_sweep",
		"status.durability.disablewal_faultfs",
		"status.adversarial.corruption",
		"status.durability.internal_key_collision",
	}

	for _, name := range expected {
		if !names[name] {
			t.Errorf("StatusInstances() should include %q", name)
		}
	}
}

func TestGetStatusInstances_All(t *testing.T) {
	all := GetStatusInstances("")
	if len(all) != len(StatusInstances()) {
		t.Errorf("GetStatusInstances(\"\") length = %d, want %d", len(all), len(StatusInstances()))
	}
}

func TestGetStatusInstances_Durability(t *testing.T) {
	durability := GetStatusInstances("status.durability")

	if len(durability) == 0 {
		t.Fatal("GetStatusInstances(\"status.durability\") should return instances")
	}

	for _, inst := range durability {
		if len(inst.Name) < 18 || inst.Name[:18] != "status.durability." {
			t.Errorf("instance %q does not match prefix status.durability.", inst.Name)
		}
	}
}

func TestGetStatusInstances_Golden(t *testing.T) {
	golden := GetStatusInstances("status.golden")

	if len(golden) != 1 {
		t.Errorf("GetStatusInstances(\"status.golden\") length = %d, want 1", len(golden))
	}

	if len(golden) > 0 && golden[0].Name != "status.golden" {
		t.Errorf("golden[0].Name = %q, want %q", golden[0].Name, "status.golden")
	}
}

func TestMatchesGroup(t *testing.T) {
	tests := []struct {
		name     string
		group    string
		expected bool
	}{
		{"status.golden", "status", true},
		{"status.golden", "status.golden", true},
		{"status.golden", "status.durability", false},
		{"status.durability.wal_sync", "status.durability", true},
		{"stress.read.corruption", "stress", true},
		{"stress.read.corruption", "crash", false},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_"+tt.group, func(t *testing.T) {
			got := matchesGroup(tt.name, tt.group)
			if got != tt.expected {
				t.Errorf("matchesGroup(%q, %q) = %v, want %v", tt.name, tt.group, got, tt.expected)
			}
		})
	}
}

func TestAllGroups(t *testing.T) {
	groups := AllGroups()

	if len(groups) == 0 {
		t.Fatal("AllGroups() should return at least one group")
	}

	expected := []string{
		"stress",
		"crash",
		"status.golden",
		"status.durability",
		"status.adversarial",
	}

	groupMap := make(map[string]bool)
	for _, g := range groups {
		groupMap[g] = true
	}

	for _, e := range expected {
		if !groupMap[e] {
			t.Errorf("AllGroups() should include %q", e)
		}
	}
}
