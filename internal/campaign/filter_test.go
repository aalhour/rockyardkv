package campaign

import "testing"

// Contract: ParseFilter returns empty filter for empty string.
func TestParseFilter_Empty(t *testing.T) {
	f, err := ParseFilter("")
	if err != nil {
		t.Fatalf("ParseFilter('') error = %v", err)
	}
	if len(f.Clauses) != 0 {
		t.Errorf("ParseFilter('') clauses = %d, want 0", len(f.Clauses))
	}
}

// Contract: ParseFilter parses single equality clause.
func TestParseFilter_SingleEqual(t *testing.T) {
	f, err := ParseFilter("tier=quick")
	if err != nil {
		t.Fatalf("ParseFilter error = %v", err)
	}
	if len(f.Clauses) != 1 {
		t.Fatalf("clauses = %d, want 1", len(f.Clauses))
	}
	c := f.Clauses[0]
	if c.Key != "tier" {
		t.Errorf("Key = %q, want %q", c.Key, "tier")
	}
	if c.Op != OpEqual {
		t.Errorf("Op = %v, want OpEqual", c.Op)
	}
	if len(c.Values) != 1 || c.Values[0] != "quick" {
		t.Errorf("Values = %v, want [quick]", c.Values)
	}
}

// Contract: ParseFilter parses not-equal clause.
func TestParseFilter_NotEqual(t *testing.T) {
	f, err := ParseFilter("tool!=goldentest")
	if err != nil {
		t.Fatalf("ParseFilter error = %v", err)
	}
	if len(f.Clauses) != 1 {
		t.Fatalf("clauses = %d, want 1", len(f.Clauses))
	}
	c := f.Clauses[0]
	if c.Key != "tool" {
		t.Errorf("Key = %q, want %q", c.Key, "tool")
	}
	if c.Op != OpNotEqual {
		t.Errorf("Op = %v, want OpNotEqual", c.Op)
	}
}

// Contract: ParseFilter handles pipe-separated OR values.
func TestParseFilter_OrValues(t *testing.T) {
	f, err := ParseFilter("tool=stresstest|crashtest")
	if err != nil {
		t.Fatalf("ParseFilter error = %v", err)
	}
	if len(f.Clauses) != 1 {
		t.Fatalf("clauses = %d, want 1", len(f.Clauses))
	}
	c := f.Clauses[0]
	if len(c.Values) != 2 {
		t.Fatalf("Values length = %d, want 2", len(c.Values))
	}
	if c.Values[0] != "stresstest" || c.Values[1] != "crashtest" {
		t.Errorf("Values = %v, want [stresstest crashtest]", c.Values)
	}
}

// Contract: ParseFilter handles comma-separated AND clauses.
func TestParseFilter_AndClauses(t *testing.T) {
	f, err := ParseFilter("tier=quick,oracle_required=true")
	if err != nil {
		t.Fatalf("ParseFilter error = %v", err)
	}
	if len(f.Clauses) != 2 {
		t.Fatalf("clauses = %d, want 2", len(f.Clauses))
	}
}

// Contract: ParseFilter rejects unknown keys.
func TestParseFilter_UnknownKey(t *testing.T) {
	_, err := ParseFilter("invalid_key=value")
	if err == nil {
		t.Error("ParseFilter should reject unknown keys")
	}
}

// Contract: ParseFilter rejects missing operator.
func TestParseFilter_MissingOperator(t *testing.T) {
	_, err := ParseFilter("tierquick")
	if err == nil {
		t.Error("ParseFilter should reject clause without operator")
	}
}

// Contract: ParseFilter rejects empty key.
func TestParseFilter_EmptyKey(t *testing.T) {
	_, err := ParseFilter("=value")
	if err == nil {
		t.Error("ParseFilter should reject empty key")
	}
}

// Contract: Filter.Match returns true for empty filter.
func TestFilter_Match_Empty(t *testing.T) {
	f := &Filter{}
	tags := Tags{Tier: "quick"}
	if !f.Match(tags) {
		t.Error("Empty filter should match all tags")
	}
}

// Contract: Filter.Match with equality matches correct values.
func TestFilter_Match_Equal(t *testing.T) {
	f, _ := ParseFilter("tier=quick")
	tags := Tags{Tier: "quick"}
	if !f.Match(tags) {
		t.Error("Filter tier=quick should match tags with Tier=quick")
	}
	tags.Tier = "nightly"
	if f.Match(tags) {
		t.Error("Filter tier=quick should not match tags with Tier=nightly")
	}
}

// Contract: Filter.Match with OR values matches any value.
func TestFilter_Match_OrValues(t *testing.T) {
	f, _ := ParseFilter("tool=stresstest|crashtest")
	tags := Tags{Tool: "stresstest"}
	if !f.Match(tags) {
		t.Error("Should match stresstest")
	}
	tags.Tool = "crashtest"
	if !f.Match(tags) {
		t.Error("Should match crashtest")
	}
	tags.Tool = "goldentest"
	if f.Match(tags) {
		t.Error("Should not match goldentest")
	}
}

// Contract: Filter.Match with not-equal excludes values.
func TestFilter_Match_NotEqual(t *testing.T) {
	f, _ := ParseFilter("kind!=golden")
	tags := Tags{Kind: "stress"}
	if !f.Match(tags) {
		t.Error("Should match non-golden kinds")
	}
	tags.Kind = "golden"
	if f.Match(tags) {
		t.Error("Should not match golden kind")
	}
}

// Contract: Filter.Match with AND clauses requires all to match.
func TestFilter_Match_And(t *testing.T) {
	f, _ := ParseFilter("tier=quick,oracle_required=false")
	tags := Tags{Tier: "quick", OracleRequired: false}
	if !f.Match(tags) {
		t.Error("Should match when both conditions are satisfied")
	}
	tags.OracleRequired = true
	if f.Match(tags) {
		t.Error("Should not match when one condition fails")
	}
}

// Contract: FilterInstances returns instances matching the filter.
func TestFilterInstances(t *testing.T) {
	instances := []Instance{
		{Name: "stress.1", Tier: TierQuick, Tool: ToolStress},
		{Name: "crash.1", Tier: TierQuick, Tool: ToolCrash},
		{Name: "golden.1", Tier: TierNightly, Tool: ToolGolden},
	}

	f, _ := ParseFilter("tool=stresstest")
	filtered := FilterInstances(instances, f)

	if len(filtered) != 1 {
		t.Fatalf("FilterInstances length = %d, want 1", len(filtered))
	}
	if filtered[0].Name != "stress.1" {
		t.Errorf("filtered[0].Name = %q, want %q", filtered[0].Name, "stress.1")
	}
}

// Contract: FilterInstances returns all instances with nil filter.
func TestFilterInstances_NilFilter(t *testing.T) {
	instances := []Instance{
		{Name: "stress.1"},
		{Name: "crash.1"},
	}

	filtered := FilterInstances(instances, nil)
	if len(filtered) != 2 {
		t.Errorf("FilterInstances(nil) length = %d, want 2", len(filtered))
	}
}

// Contract: Filter.String returns original filter string format.
func TestFilter_String(t *testing.T) {
	f, _ := ParseFilter("tier=quick,tool!=goldentest")
	s := f.String()
	// The order should be preserved
	if s != "tier=quick,tool!=goldentest" {
		t.Errorf("Filter.String() = %q, want %q", s, "tier=quick,tool!=goldentest")
	}
}

// Contract: FilterClause.String returns correct representation.
func TestFilterClause_String(t *testing.T) {
	c := FilterClause{Key: "tool", Op: OpEqual, Values: []string{"a", "b"}}
	if c.String() != "tool=a|b" {
		t.Errorf("FilterClause.String() = %q, want %q", c.String(), "tool=a|b")
	}

	c = FilterClause{Key: "kind", Op: OpNotEqual, Values: []string{"x"}}
	if c.String() != "kind!=x" {
		t.Errorf("FilterClause.String() = %q, want %q", c.String(), "kind!=x")
	}
}
