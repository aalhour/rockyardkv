package campaign

import "testing"

func TestFaultModelString(t *testing.T) {
	tests := []struct {
		name     string
		fm       FaultModel
		expected string
	}{
		{
			name:     "none",
			fm:       FaultModel{Kind: FaultNone},
			expected: "none",
		},
		{
			name: "read/corruption/1in7/worker",
			fm: FaultModel{
				Kind:      FaultRead,
				ErrorType: ErrorTypeCorruption,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			expected: "read/corruption/1in7/worker",
		},
		{
			name: "write/status/1in100/flusher",
			fm: FaultModel{
				Kind:      FaultWrite,
				ErrorType: ErrorTypeStatus,
				OneIn:     100,
				Scope:     ScopeFlusher,
			},
			expected: "write/status/1in100/flusher",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fm.String()
			if got != tt.expected {
				t.Errorf("FaultModel.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTierConstants(t *testing.T) {
	if TierQuick != "quick" {
		t.Errorf("TierQuick = %q, want %q", TierQuick, "quick")
	}
	if TierNightly != "nightly" {
		t.Errorf("TierNightly = %q, want %q", TierNightly, "nightly")
	}
}

func TestToolConstants(t *testing.T) {
	if ToolStress != "stresstest" {
		t.Errorf("ToolStress = %q, want %q", ToolStress, "stresstest")
	}
	if ToolCrash != "crashtest" {
		t.Errorf("ToolCrash = %q, want %q", ToolCrash, "crashtest")
	}
	if ToolAdversarial != "adversarialtest" {
		t.Errorf("ToolAdversarial = %q, want %q", ToolAdversarial, "adversarialtest")
	}
	if ToolGolden != "goldentest" {
		t.Errorf("ToolGolden = %q, want %q", ToolGolden, "goldentest")
	}
}
