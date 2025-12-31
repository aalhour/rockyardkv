package campaign

import (
	"testing"
)

// Contract: CompositeInstance.IsComposite returns true when Steps has multiple entries.
func TestCompositeInstance_IsComposite(t *testing.T) {
	tests := []struct {
		name     string
		steps    []Step
		expected bool
	}{
		{"nil steps", nil, false},
		{"empty steps", []Step{}, false},
		{"single step", []Step{{Name: "step1"}}, false},
		{"multiple steps", []Step{{Name: "step1"}, {Name: "step2"}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CompositeInstance{Steps: tt.steps}
			if got := c.IsComposite(); got != tt.expected {
				t.Errorf("IsComposite() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// Contract: CompositeInstance.ToSteps returns Steps if non-empty, else creates single step from Instance.
func TestCompositeInstance_ToSteps(t *testing.T) {
	t.Run("explicit steps", func(t *testing.T) {
		steps := []Step{{Name: "step1"}, {Name: "step2"}}
		c := CompositeInstance{Steps: steps}
		got := c.ToSteps()
		if len(got) != 2 {
			t.Fatalf("ToSteps() returned %d steps, want 2", len(got))
		}
		if got[0].Name != "step1" || got[1].Name != "step2" {
			t.Error("ToSteps() returned wrong step names")
		}
	})

	t.Run("implicit single step", func(t *testing.T) {
		c := CompositeInstance{
			Instance: Instance{
				Name:           "test",
				Tool:           ToolCrash,
				Args:           []string{"-v"},
				RequiresOracle: true,
			},
		}
		got := c.ToSteps()
		if len(got) != 1 {
			t.Fatalf("ToSteps() returned %d steps, want 1", len(got))
		}
		if got[0].Name != "test" {
			t.Errorf("ToSteps()[0].Name = %q, want %q", got[0].Name, "test")
		}
		if got[0].Tool != ToolCrash {
			t.Errorf("ToSteps()[0].Tool = %v, want %v", got[0].Tool, ToolCrash)
		}
		if !got[0].RequiresOracle {
			t.Error("ToSteps()[0].RequiresOracle = false, want true")
		}
	})
}

// Contract: CompositeResult.ComputePassed applies GateAllSteps policy correctly.
func TestCompositeResult_ComputePassed_AllSteps(t *testing.T) {
	tests := []struct {
		name     string
		steps    []StepResult
		expected bool
	}{
		{
			"all pass",
			[]StepResult{{Passed: true}, {Passed: true}},
			true,
		},
		{
			"first fails",
			[]StepResult{{Passed: false, StepName: "step1", FailureReason: "boom"}, {Passed: true}},
			false,
		},
		{
			"last fails",
			[]StepResult{{Passed: true}, {Passed: false, StepName: "step2", FailureReason: "crash"}},
			false,
		},
		{
			"both fail",
			[]StepResult{{Passed: false}, {Passed: false}},
			false,
		},
		{
			"empty steps",
			[]StepResult{},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := CompositeResult{Steps: tt.steps, GatingPolicy: GateAllSteps}
			r.ComputePassed()
			if r.Passed != tt.expected {
				t.Errorf("ComputePassed() Passed = %v, want %v", r.Passed, tt.expected)
			}
		})
	}
}

// Contract: CompositeResult.ComputePassed applies GateLastStep policy correctly.
func TestCompositeResult_ComputePassed_LastStep(t *testing.T) {
	tests := []struct {
		name     string
		steps    []StepResult
		expected bool
	}{
		{
			"all pass",
			[]StepResult{{Passed: true}, {Passed: true}},
			true,
		},
		{
			"first fails, last passes",
			[]StepResult{{Passed: false}, {Passed: true}},
			true,
		},
		{
			"first passes, last fails",
			[]StepResult{{Passed: true}, {Passed: false, StepName: "step2", FailureReason: "crash"}},
			false,
		},
		{
			"single step fails",
			[]StepResult{{Passed: false, StepName: "only", FailureReason: "error"}},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := CompositeResult{Steps: tt.steps, GatingPolicy: GateLastStep}
			r.ComputePassed()
			if r.Passed != tt.expected {
				t.Errorf("ComputePassed() Passed = %v, want %v", r.Passed, tt.expected)
			}
		})
	}
}

// Contract: StepRunDir returns a path under steps/<stepName>.
func TestStepRunDir(t *testing.T) {
	got := StepRunDir("/tmp/run", "crashtest")
	want := "/tmp/run/steps/crashtest"
	if got != want {
		t.Errorf("StepRunDir() = %q, want %q", got, want)
	}
}

// Contract: ResolveStepArgs substitutes all placeholders correctly.
func TestResolveStepArgs(t *testing.T) {
	args := []string{
		"-db", "<RUN_DIR>/db",
		"-seed", "<SEED>",
		"--dir", "<DB_DIR>",
	}
	got := ResolveStepArgs(args, "/tmp/run", 12345, "/tmp/run/db")
	want := []string{
		"-db", "/tmp/run/db",
		"-seed", "12345",
		"--dir", "/tmp/run/db",
	}

	if len(got) != len(want) {
		t.Fatalf("ResolveStepArgs() returned %d args, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("ResolveStepArgs()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// Contract: GatingPolicy constants are well-defined strings.
func TestGatingPolicy_Constants(t *testing.T) {
	if GateAllSteps != "all_steps" {
		t.Errorf("GateAllSteps = %q, want %q", GateAllSteps, "all_steps")
	}
	if GateLastStep != "last_step" {
		t.Errorf("GateLastStep = %q, want %q", GateLastStep, "last_step")
	}
}
