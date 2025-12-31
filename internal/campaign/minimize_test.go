package campaign

import (
	"slices"
	"testing"
	"time"
)

// Contract: DefaultMinBounds returns Red Team approved bounds (5s/4/500).
func TestDefaultMinBounds(t *testing.T) {
	bounds := DefaultMinBounds()

	if bounds.MinDuration != 5*time.Second {
		t.Errorf("expected MinDuration=5s, got %s", bounds.MinDuration)
	}
	if bounds.MinThreads != 4 {
		t.Errorf("expected MinThreads=4, got %d", bounds.MinThreads)
	}
	if bounds.MinKeys != 500 {
		t.Errorf("expected MinKeys=500, got %d", bounds.MinKeys)
	}
}

// Contract: DefaultMinimizeConfig returns config with minimization disabled.
func TestDefaultMinimizeConfig(t *testing.T) {
	cfg := DefaultMinimizeConfig()

	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if len(cfg.AllowedFailureKinds) == 0 {
		t.Error("expected non-empty AllowedFailureKinds")
	}

	// Check specific allowed kinds
	expected := []string{"verification_failure", "oracle_failure", "corruption", "exit_error"}
	for _, kind := range expected {
		if !cfg.AllowedFailureKinds[kind] {
			t.Errorf("expected %s in AllowedFailureKinds", kind)
		}
	}
}

// Contract: NewMinimizer creates a minimizer with the given config.
func TestNewMinimizer(t *testing.T) {
	runner := &Runner{}
	cfg := DefaultMinimizeConfig()

	m := NewMinimizer(runner, cfg)
	if m == nil {
		t.Fatal("expected non-nil minimizer")
	}
	if m.runner != runner {
		t.Error("runner not set correctly")
	}
}

// Contract: ShouldMinimize returns false when minimization is disabled.
func TestShouldMinimize_Disabled(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = false

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance:    &Instance{Tool: ToolStress},
		Passed:      false,
		IsDuplicate: false,
		FailureKind: "verification_failure",
	}

	if m.ShouldMinimize(result) {
		t.Error("expected false when disabled")
	}
}

// Contract: ShouldMinimize returns false for duplicate failures.
func TestShouldMinimize_Duplicate(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = true

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance:    &Instance{Tool: ToolStress},
		Passed:      false,
		IsDuplicate: true,
		FailureKind: "verification_failure",
	}

	if m.ShouldMinimize(result) {
		t.Error("expected false for duplicates")
	}
}

// Contract: ShouldMinimize returns false for passing runs.
func TestShouldMinimize_Passing(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = true

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance: &Instance{Tool: ToolStress},
		Passed:   true,
	}

	if m.ShouldMinimize(result) {
		t.Error("expected false for passing runs")
	}
}

// Contract: ShouldMinimize returns false for non-stresstest tools.
func TestShouldMinimize_NonStress(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = true

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance:    &Instance{Tool: ToolCrash},
		Passed:      false,
		IsDuplicate: false,
		FailureKind: "verification_failure",
	}

	if m.ShouldMinimize(result) {
		t.Error("expected false for non-stresstest")
	}
}

// Contract: ShouldMinimize returns false for failure kinds not in allowlist.
func TestShouldMinimize_NotAllowed(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = true

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance:    &Instance{Tool: ToolStress},
		Passed:      false,
		IsDuplicate: false,
		FailureKind: "timeout", // Not in allowlist
	}

	if m.ShouldMinimize(result) {
		t.Error("expected false for non-allowed failure kind")
	}
}

// Contract: ShouldMinimize returns true for eligible failures.
func TestShouldMinimize_Eligible(t *testing.T) {
	cfg := DefaultMinimizeConfig()
	cfg.Enabled = true

	m := NewMinimizer(&Runner{}, cfg)
	result := &RunResult{
		Instance:    &Instance{Tool: ToolStress},
		Passed:      false,
		IsDuplicate: false,
		FailureKind: "verification_failure",
	}

	if !m.ShouldMinimize(result) {
		t.Error("expected true for eligible failure")
	}
}

// Contract: parseDuration extracts duration from -duration flag.
func TestParseDuration_Separated(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-threads", "4", "-duration", "20s", "-keys", "5000"}

	d := m.parseDuration(args)
	if d != 20*time.Second {
		t.Errorf("expected 20s, got %s", d)
	}
}

// Contract: parseDuration handles -duration=X style.
func TestParseDuration_Equals(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-threads=4", "-duration=30s", "-keys=5000"}

	d := m.parseDuration(args)
	if d != 30*time.Second {
		t.Errorf("expected 30s, got %s", d)
	}
}

// Contract: parseThreads extracts thread count from -threads flag.
func TestParseThreads_Separated(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-duration", "20s", "-threads", "16", "-keys", "5000"}

	n := m.parseThreads(args)
	if n != 16 {
		t.Errorf("expected 16, got %d", n)
	}
}

// Contract: parseThreads handles -threads=X style.
func TestParseThreads_Equals(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-duration=20s", "-threads=8", "-keys=5000"}

	n := m.parseThreads(args)
	if n != 8 {
		t.Errorf("expected 8, got %d", n)
	}
}

// Contract: parseKeys extracts key count from -keys flag.
func TestParseKeys_Separated(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-duration", "20s", "-threads", "4", "-keys", "10000"}

	n := m.parseKeys(args)
	if n != 10000 {
		t.Errorf("expected 10000, got %d", n)
	}
}

// Contract: parseKeys handles -keys=X style.
func TestParseKeys_Equals(t *testing.T) {
	m := &Minimizer{}
	args := []string{"-duration=20s", "-threads=4", "-keys=2000"}

	n := m.parseKeys(args)
	if n != 2000 {
		t.Errorf("expected 2000, got %d", n)
	}
}

// Contract: buildArgs replaces parameter values while preserving other args.
func TestBuildArgs_ReplacesSeparated(t *testing.T) {
	m := &Minimizer{}
	original := []string{"-duration", "20s", "-threads", "32", "-keys", "5000", "-v"}

	result := m.buildArgs(original, 10*time.Second, 8, 1000)

	// Check expected flag pairs
	expected := map[string]string{
		"-duration": "10s",
		"-threads":  "8",
		"-keys":     "1000",
	}

	for i, arg := range result {
		if val, ok := expected[arg]; ok {
			if i+1 >= len(result) || result[i+1] != val {
				t.Errorf("expected %s %s, got different value", arg, val)
			}
		}
	}

	// Check -v is preserved
	if !slices.Contains(result, "-v") {
		t.Error("-v flag was not preserved")
	}
}

// Contract: buildArgs handles -flag=value style args.
func TestBuildArgs_ReplacesEquals(t *testing.T) {
	m := &Minimizer{}
	original := []string{"-duration=20s", "-threads=32", "-keys=5000", "-v"}

	result := m.buildArgs(original, 10*time.Second, 8, 1000)

	expectations := []string{"-duration=10s", "-threads=8", "-keys=1000", "-v"}
	for _, exp := range expectations {
		if !slices.Contains(result, exp) {
			t.Errorf("expected %s in result, got %v", exp, result)
		}
	}
}

// Contract: MinimizeResult tracks attempts and duration.
func TestMinimizeResult_Fields(t *testing.T) {
	result := MinimizeResult{
		Success:              true,
		OriginalArgs:         []string{"-duration", "20s"},
		MinimalArgs:          []string{"-duration", "5s"},
		FinalDuration:        "5s",
		FinalThreads:         4,
		FinalKeys:            500,
		PreservedFailureKind: "verification_failure",
		TotalAttempts:        5,
		TotalDurationMs:      1000,
	}

	if !result.Success {
		t.Error("expected Success=true")
	}
	if result.TotalAttempts != 5 {
		t.Errorf("expected 5 attempts, got %d", result.TotalAttempts)
	}
	if result.PreservedFailureKind != "verification_failure" {
		t.Errorf("expected PreservedFailureKind=verification_failure, got %s", result.PreservedFailureKind)
	}
}

// Contract: ReductionStep captures parameter reduction details.
func TestReductionStep_Fields(t *testing.T) {
	step := ReductionStep{
		Parameter:   "duration",
		OriginalVal: "20s",
		ReducedVal:  "10s",
		StillFails:  true,
		DurationMs:  500,
	}

	if step.Parameter != "duration" {
		t.Errorf("expected parameter=duration, got %s", step.Parameter)
	}
	if !step.StillFails {
		t.Error("expected StillFails=true")
	}
}
