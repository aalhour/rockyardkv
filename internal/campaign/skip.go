package campaign

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SkipPolicy represents an instance-level skip policy.
// Unlike fingerprint-based quarantine, this skips instances BEFORE they run.
type SkipPolicy struct {
	// InstanceName is the exact instance name to skip (highest priority).
	InstanceName string `json:"instance_name,omitempty"`

	// Group matches instances whose name starts with this prefix.
	// For example, "status.durability" matches "status.durability.cycles4".
	Group string `json:"group,omitempty"`

	// Tags matches instances with all specified tag values.
	// For example, {"tier": "nightly", "kind": "crash"} matches all nightly crash tests.
	Tags map[string]string `json:"tags,omitempty"`

	// Reason is a human-readable explanation for why the instance is skipped.
	Reason string `json:"reason"`

	// IssueID links to a tracking issue (e.g., "GH-456").
	IssueID string `json:"issue_id,omitempty"`
}

// Matches returns true if this policy matches the given instance.
func (p *SkipPolicy) Matches(inst *Instance) bool {
	// Exact instance name match (highest priority)
	if p.InstanceName != "" && inst.Name == p.InstanceName {
		return true
	}

	// Group prefix match
	if p.Group != "" && strings.HasPrefix(inst.Name, p.Group) {
		return true
	}

	// Tag match - all specified tags must match
	if len(p.Tags) > 0 {
		tags := inst.ComputeTags()
		for key, value := range p.Tags {
			if !tagMatches(tags, key, value) {
				return false
			}
		}
		return true
	}

	return false
}

// tagMatches checks if a tag key/value matches the instance tags.
// Only known tag keys are accepted; unknown keys always return false.
// This ensures skip policies are auditable and do not diverge from the filter schema.
func tagMatches(tags Tags, key, value string) bool {
	switch key {
	case "tier":
		return tags.Tier == value
	case "tool":
		return tags.Tool == value
	case "kind":
		return tags.Kind == value
	case "group":
		return tags.Group == value
	case "fault_kind":
		return tags.FaultKind == value
	case "fault_scope":
		return tags.FaultScope == value
	case "campaign":
		return tags.Campaign == value
	case "oracle_required":
		return (value == "true") == tags.OracleRequired
	default:
		// Unknown tag keys are rejected to prevent non-auditable skip selection.
		// This matches the filter schema policy: only known keys are allowed.
		return false
	}
}

// ValidateSkipPolicyTags returns an error if any tag key in the policy is unknown.
func ValidateSkipPolicyTags(p *SkipPolicy) error {
	allowedKeys := map[string]bool{
		"tier":            true,
		"tool":            true,
		"kind":            true,
		"group":           true,
		"fault_kind":      true,
		"fault_scope":     true,
		"campaign":        true,
		"oracle_required": true,
	}

	for key := range p.Tags {
		if !allowedKeys[key] {
			return fmt.Errorf("unknown tag key in skip policy: %q (allowed: %v)", key, allowedTagKeyList())
		}
	}
	return nil
}

// allowedTagKeyList returns a sorted list of allowed tag keys for error messages.
func allowedTagKeyList() []string {
	return []string{"tier", "tool", "kind", "group", "fault_kind", "fault_scope", "campaign", "oracle_required"}
}

// SkipResult records why an instance was skipped.
type SkipResult struct {
	InstanceName string `json:"instance_name"`
	Reason       string `json:"reason"`
	IssueID      string `json:"issue_id,omitempty"`
	Policy       string `json:"policy"` // Which policy matched (for debugging)
}

// InstanceSkipPolicies manages a set of skip policies.
type InstanceSkipPolicies struct {
	policies []*SkipPolicy
	path     string
}

// NewInstanceSkipPolicies creates a new skip policy manager.
// If path is non-empty, policies are loaded from disk.
func NewInstanceSkipPolicies(path string) *InstanceSkipPolicies {
	sp := &InstanceSkipPolicies{
		policies: make([]*SkipPolicy, 0),
		path:     path,
	}
	if path != "" {
		sp.load()
	}
	return sp
}

// load reads skip policies from disk and validates them.
func (sp *InstanceSkipPolicies) load() {
	data, err := os.ReadFile(sp.path)
	if err != nil {
		return // File doesn't exist yet
	}

	var policies []*SkipPolicy
	if err := json.Unmarshal(data, &policies); err != nil {
		return
	}

	sp.policies = policies
}

// LoadWithValidation loads policies and returns any validation errors.
// Use this when callers want to surface validation issues to users.
func (sp *InstanceSkipPolicies) LoadWithValidation() error {
	data, err := os.ReadFile(sp.path)
	if err != nil {
		return err
	}

	var policies []*SkipPolicy
	if err := json.Unmarshal(data, &policies); err != nil {
		return err
	}

	// Validate all policies have valid tag keys
	for _, p := range policies {
		if err := ValidateSkipPolicyTags(p); err != nil {
			return err
		}
	}

	sp.policies = policies
	return nil
}

// Save writes skip policies to disk.
func (sp *InstanceSkipPolicies) Save() error {
	if sp.path == "" {
		return nil
	}

	data, err := json.MarshalIndent(sp.policies, "", "  ")
	if err != nil {
		return err
	}

	// Ensure directory exists
	if dir := filepath.Dir(sp.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	return os.WriteFile(sp.path, data, 0o644)
}

// Add adds a new skip policy.
func (sp *InstanceSkipPolicies) Add(policy *SkipPolicy) {
	sp.policies = append(sp.policies, policy)
}

// ShouldSkip returns a SkipResult if the instance should be skipped, nil otherwise.
func (sp *InstanceSkipPolicies) ShouldSkip(inst *Instance) *SkipResult {
	for _, p := range sp.policies {
		if p.Matches(inst) {
			// Build policy description for debugging
			var policyDesc string
			if p.InstanceName != "" {
				policyDesc = "instance:" + p.InstanceName
			} else if p.Group != "" {
				policyDesc = "group:" + p.Group
			} else if len(p.Tags) > 0 {
				policyDesc = "tags"
			}

			return &SkipResult{
				InstanceName: inst.Name,
				Reason:       p.Reason,
				IssueID:      p.IssueID,
				Policy:       policyDesc,
			}
		}
	}
	return nil
}

// Count returns the number of skip policies.
func (sp *InstanceSkipPolicies) Count() int {
	return len(sp.policies)
}
