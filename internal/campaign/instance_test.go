package campaign

import (
	"path/filepath"
	"testing"
)

func TestInstanceRunDir(t *testing.T) {
	inst := &Instance{
		Name: "stress.read.corruption.1in7",
	}

	runDir := inst.RunDir("/tmp/campaign", 12345)
	expected := filepath.Join("/tmp/campaign", "stress.read.corruption.1in7", "seed_12345")

	if runDir != expected {
		t.Errorf("RunDir() = %q, want %q", runDir, expected)
	}
}

func TestInstanceResolveArgs(t *testing.T) {
	inst := &Instance{
		Name: "test",
		Args: []string{"-run-dir", "<RUN_DIR>", "-seed", "<SEED>", "-v"},
	}

	resolved := inst.ResolveArgs("/tmp/run", 42)

	expected := []string{"-run-dir", "/tmp/run", "-seed", "42", "-v"}

	if len(resolved) != len(expected) {
		t.Fatalf("ResolveArgs() length = %d, want %d", len(resolved), len(expected))
	}

	for i, arg := range resolved {
		if arg != expected[i] {
			t.Errorf("ResolveArgs()[%d] = %q, want %q", i, arg, expected[i])
		}
	}
}

func TestInstanceBinaryName(t *testing.T) {
	tests := []struct {
		tool     Tool
		expected string
	}{
		{ToolStress, "stresstest"},
		{ToolCrash, "crashtest"},
		{ToolAdversarial, "adversarialtest"},
		{ToolGolden, "go"},
	}

	for _, tt := range tests {
		t.Run(string(tt.tool), func(t *testing.T) {
			inst := &Instance{Tool: tt.tool}
			got := inst.BinaryName()
			if got != tt.expected {
				t.Errorf("BinaryName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestInstanceIsGoTest(t *testing.T) {
	tests := []struct {
		tool     Tool
		expected bool
	}{
		{ToolStress, false},
		{ToolCrash, false},
		{ToolAdversarial, false},
		{ToolGolden, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.tool), func(t *testing.T) {
			inst := &Instance{Tool: tt.tool}
			got := inst.IsGoTest()
			if got != tt.expected {
				t.Errorf("IsGoTest() = %v, want %v", got, tt.expected)
			}
		})
	}
}
