package campaign

import (
	"testing"
)

// Contract: SweepInstance.Expand returns base instance if no cases or params.
func TestSweepInstance_Expand_NoExpansion(t *testing.T) {
	s := SweepInstance{
		Base: Instance{Name: "test", Tool: ToolCrash},
	}
	got := s.Expand()
	if len(got) != 1 {
		t.Fatalf("Expand() returned %d instances, want 1", len(got))
	}
	if got[0].Name != "test" {
		t.Errorf("Expand()[0].Name = %q, want %q", got[0].Name, "test")
	}
}

// Contract: SweepInstance.Expand with Cases returns one instance per case.
func TestSweepInstance_Expand_WithCases(t *testing.T) {
	s := SweepInstance{
		Base: Instance{
			Name: "test",
			Tool: ToolCrash,
			Args: []string{"-cycles=<CYCLES>"},
		},
		Cases: []SweepCase{
			{ID: "case1", Params: map[string]string{"CYCLES": "4"}},
			{ID: "case2", Params: map[string]string{"CYCLES": "6"}},
		},
	}

	got := s.Expand()
	if len(got) != 2 {
		t.Fatalf("Expand() returned %d instances, want 2", len(got))
	}

	if got[0].Name != "test/case1" {
		t.Errorf("Expand()[0].Name = %q, want %q", got[0].Name, "test/case1")
	}
	if got[1].Name != "test/case2" {
		t.Errorf("Expand()[1].Name = %q, want %q", got[1].Name, "test/case2")
	}

	if got[0].Args[0] != "-cycles=4" {
		t.Errorf("Expand()[0].Args[0] = %q, want %q", got[0].Args[0], "-cycles=4")
	}
	if got[1].Args[0] != "-cycles=6" {
		t.Errorf("Expand()[1].Args[0] = %q, want %q", got[1].Args[0], "-cycles=6")
	}
}

// Contract: SweepInstance.Expand with Params generates cross-product.
func TestSweepInstance_Expand_CrossProduct(t *testing.T) {
	s := SweepInstance{
		Base: Instance{
			Name: "test",
			Tool: ToolCrash,
			Args: []string{"-a=<A>", "-b=<B>"},
		},
		Params: []SweepParam{
			{Name: "A", Values: []string{"1", "2"}},
			{Name: "B", Values: []string{"x", "y"}},
		},
	}

	got := s.Expand()
	if len(got) != 4 {
		t.Fatalf("Expand() returned %d instances, want 4", len(got))
	}

	// Check that all combinations are present
	names := make(map[string]bool)
	for _, inst := range got {
		names[inst.Name] = true
	}

	expected := []string{
		"test/A_1_B_x",
		"test/A_1_B_y",
		"test/A_2_B_x",
		"test/A_2_B_y",
	}
	for _, exp := range expected {
		if !names[exp] {
			t.Errorf("Missing expected instance: %q", exp)
		}
	}
}

// Contract: DisableWALFaultFSMinimizeCases returns 4 cases matching the script.
func TestDisableWALFaultFSMinimizeCases(t *testing.T) {
	cases := DisableWALFaultFSMinimizeCases()
	if len(cases) != 4 {
		t.Fatalf("DisableWALFaultFSMinimizeCases() returned %d cases, want 4", len(cases))
	}

	expectedIDs := []string{
		"drop_cycles_4",
		"delete_cycles_4",
		"drop_plus_delete_cycles_4",
		"drop_plus_delete_cycles_6",
	}

	for i, exp := range expectedIDs {
		if cases[i].ID != exp {
			t.Errorf("cases[%d].ID = %q, want %q", i, cases[i].ID, exp)
		}
	}
}

// Contract: SweepCase.Params substitution works in expandCase.
func TestSweepInstance_ExpandCase_ParamSubstitution(t *testing.T) {
	s := SweepInstance{
		Base: Instance{
			Name: "base",
			Tool: ToolCrash,
			Args: []string{"-flag=<FLAG_VALUE>", "-other"},
		},
	}

	c := SweepCase{
		ID:     "test_case",
		Params: map[string]string{"FLAG_VALUE": "replaced"},
	}

	got := s.expandCase(c)
	if got.Name != "base/test_case" {
		t.Errorf("Name = %q, want %q", got.Name, "base/test_case")
	}
	if got.Args[0] != "-flag=replaced" {
		t.Errorf("Args[0] = %q, want %q", got.Args[0], "-flag=replaced")
	}
	if got.Args[1] != "-other" {
		t.Errorf("Args[1] = %q, want %q", got.Args[1], "-other")
	}
}

// Contract: caseID generates stable IDs from params.
func TestSweepInstance_CaseID(t *testing.T) {
	s := SweepInstance{
		Params: []SweepParam{
			{Name: "A", Values: []string{"1"}},
			{Name: "B", Values: []string{"x"}},
		},
	}

	c := SweepCase{Params: map[string]string{"A": "1", "B": "x"}}
	got := s.caseID(c)
	want := "A_1_B_x"
	if got != want {
		t.Errorf("caseID() = %q, want %q", got, want)
	}
}

// Contract: caseID sanitizes special characters.
func TestSweepInstance_CaseID_Sanitization(t *testing.T) {
	s := SweepInstance{
		Params: []SweepParam{
			{Name: "mode", Values: []string{"drop+delete"}},
		},
	}

	c := SweepCase{Params: map[string]string{"mode": "drop+delete"}}
	got := s.caseID(c)
	want := "mode_drop_plus_delete"
	if got != want {
		t.Errorf("caseID() = %q, want %q", got, want)
	}
}

// Contract: caseID returns explicit ID if set.
func TestSweepInstance_CaseID_ExplicitID(t *testing.T) {
	s := SweepInstance{}
	c := SweepCase{ID: "explicit", Params: map[string]string{"A": "1"}}
	got := s.caseID(c)
	if got != "explicit" {
		t.Errorf("caseID() = %q, want %q", got, "explicit")
	}
}
