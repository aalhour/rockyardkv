package campaign

import (
	"path/filepath"
	"testing"
)

// Contract: RunDir returns a path containing the instance name and seed.
func TestInstance_RunDir(t *testing.T) {
	inst := &Instance{Name: "stress.read.corruption"}

	got := inst.RunDir("/tmp/campaign", 12345)
	want := filepath.Join("/tmp/campaign", "stress.read.corruption", "seed_12345")

	if got != want {
		t.Errorf("RunDir() = %q, want %q", got, want)
	}
}

// Contract: ResolveArgs replaces <RUN_DIR> and <SEED> placeholders with actual values.
func TestInstance_ResolveArgs(t *testing.T) {
	inst := &Instance{
		Args: []string{
			"-run-dir", "<RUN_DIR>",
			"-seed", "<SEED>",
			"-duration=20s",
		},
	}

	got := inst.ResolveArgs("/tmp/run", 99999)
	want := []string{
		"-run-dir", "/tmp/run",
		"-seed", "99999",
		"-duration=20s",
	}

	if len(got) != len(want) {
		t.Fatalf("ResolveArgs() length = %d, want %d", len(got), len(want))
	}

	for i := range got {
		if got[i] != want[i] {
			t.Errorf("ResolveArgs()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// Contract: BinaryName returns the correct executable name for each tool type.
func TestInstance_BinaryName(t *testing.T) {
	tests := []struct {
		tool Tool
		want string
	}{
		{ToolStress, "stresstest"},
		{ToolCrash, "crashtest"},
		{ToolAdversarial, "adversarialtest"},
		{ToolGolden, "go"},
	}

	for _, tt := range tests {
		t.Run(string(tt.tool), func(t *testing.T) {
			inst := &Instance{Tool: tt.tool}
			if got := inst.BinaryName(); got != tt.want {
				t.Errorf("BinaryName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Contract: BinaryPath returns binDir-prefixed path for test binaries, "go" for golden.
func TestInstance_BinaryPath(t *testing.T) {
	tests := []struct {
		name   string
		tool   Tool
		binDir string
		want   string
	}{
		{"stress uses binDir", ToolStress, "./bin", filepath.Join("./bin", "stresstest")},
		{"crash uses binDir", ToolCrash, "/usr/local/bin", "/usr/local/bin/crashtest"},
		{"golden returns go", ToolGolden, "./bin", "go"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst := &Instance{Tool: tt.tool}
			if got := inst.BinaryPath(tt.binDir); got != tt.want {
				t.Errorf("BinaryPath(%q) = %q, want %q", tt.binDir, got, tt.want)
			}
		})
	}
}

// Contract: IsGoTest returns true only for ToolGolden.
func TestInstance_IsGoTest(t *testing.T) {
	tests := []struct {
		tool Tool
		want bool
	}{
		{ToolGolden, true},
		{ToolStress, false},
		{ToolCrash, false},
		{ToolAdversarial, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.tool), func(t *testing.T) {
			inst := &Instance{Tool: tt.tool}
			if got := inst.IsGoTest(); got != tt.want {
				t.Errorf("IsGoTest() = %v, want %v", got, tt.want)
			}
		})
	}
}
