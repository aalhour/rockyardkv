package campaign

import (
	"fmt"
	"slices"
	"strings"
)

// Filter represents a parsed filter expression.
type Filter struct {
	Clauses []FilterClause
}

// FilterClause represents a single filter clause (key op values).
type FilterClause struct {
	Key    string
	Op     FilterOp
	Values []string // Multiple values for OR (pipe-separated)
}

// FilterOp is the filter operation type.
type FilterOp int

const (
	// OpEqual matches if tag value equals any of the values.
	OpEqual FilterOp = iota
	// OpNotEqual matches if tag value does not equal any of the values.
	OpNotEqual
)

// ParseFilter parses a filter string into a Filter.
// Format: "key=value,key!=value,key=val1|val2"
// - Comma separates clauses (AND semantics)
// - Pipe separates values within a clause (OR semantics)
// - "=" for equality, "!=" for inequality
func ParseFilter(filterStr string) (*Filter, error) {
	if filterStr == "" {
		return &Filter{}, nil
	}

	validKeys := make(map[string]bool)
	for _, k := range AllTagKeys() {
		validKeys[k] = true
	}

	clauses := strings.Split(filterStr, ",")
	result := &Filter{
		Clauses: make([]FilterClause, 0, len(clauses)),
	}

	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		parsed, err := parseClause(clause, validKeys)
		if err != nil {
			return nil, err
		}
		result.Clauses = append(result.Clauses, parsed)
	}

	return result, nil
}

// parseClause parses a single clause like "key=value" or "key!=val1|val2".
func parseClause(clause string, validKeys map[string]bool) (FilterClause, error) {
	var key, valueStr string
	var op FilterOp

	// Check for != first (longer operator)
	if idx := strings.Index(clause, "!="); idx != -1 {
		key = strings.TrimSpace(clause[:idx])
		valueStr = strings.TrimSpace(clause[idx+2:])
		op = OpNotEqual
	} else if idx := strings.Index(clause, "="); idx != -1 {
		key = strings.TrimSpace(clause[:idx])
		valueStr = strings.TrimSpace(clause[idx+1:])
		op = OpEqual
	} else {
		return FilterClause{}, fmt.Errorf("invalid filter clause %q: missing operator (= or !=)", clause)
	}

	if key == "" {
		return FilterClause{}, fmt.Errorf("invalid filter clause %q: empty key", clause)
	}

	if !validKeys[key] {
		return FilterClause{}, fmt.Errorf("unknown filter key %q; valid keys: %v", key, AllTagKeys())
	}

	// Parse pipe-separated values
	values := strings.Split(valueStr, "|")
	for i, v := range values {
		values[i] = strings.TrimSpace(v)
	}

	return FilterClause{
		Key:    key,
		Op:     op,
		Values: values,
	}, nil
}

// Match returns true if the tags match all filter clauses (AND semantics).
func (f *Filter) Match(tags Tags) bool {
	if len(f.Clauses) == 0 {
		return true
	}

	for _, clause := range f.Clauses {
		if !clause.Match(tags) {
			return false
		}
	}
	return true
}

// Match returns true if the tags match this clause.
func (c FilterClause) Match(tags Tags) bool {
	tagValue := tags.Get(c.Key)

	switch c.Op {
	case OpEqual:
		// Match if tag value equals any of the filter values (OR)
		return slices.Contains(c.Values, tagValue)

	case OpNotEqual:
		// Match if tag value does not equal any of the filter values
		return !slices.Contains(c.Values, tagValue)

	default:
		return false
	}
}

// FilterInstances returns instances that match the filter.
func FilterInstances(instances []Instance, filter *Filter) []Instance {
	if filter == nil || len(filter.Clauses) == 0 {
		return instances
	}

	result := make([]Instance, 0, len(instances))
	for _, inst := range instances {
		tags := inst.ComputeTags()
		if filter.Match(tags) {
			result = append(result, inst)
		}
	}
	return result
}

// String returns a string representation of the filter.
func (f *Filter) String() string {
	if len(f.Clauses) == 0 {
		return ""
	}

	parts := make([]string, len(f.Clauses))
	for i, c := range f.Clauses {
		parts[i] = c.String()
	}
	return strings.Join(parts, ",")
}

// String returns a string representation of the clause.
func (c FilterClause) String() string {
	op := "="
	if c.Op == OpNotEqual {
		op = "!="
	}
	return c.Key + op + strings.Join(c.Values, "|")
}
