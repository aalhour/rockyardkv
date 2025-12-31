package campaign

import (
	"strings"
	"testing"
)

// Contract: SkipPolicy matches instances by exact name.
func TestSkipPolicy_MatchesByInstanceName(t *testing.T) {
	policy := &SkipPolicy{
		InstanceName: "stress.read.crash",
		Reason:       "known flaky",
	}

	inst := &Instance{Name: "stress.read.crash"}
	if !policy.Matches(inst) {
		t.Error("expected policy to match instance by exact name")
	}

	instOther := &Instance{Name: "stress.write.crash"}
	if policy.Matches(instOther) {
		t.Error("policy should not match different instance name")
	}
}

// Contract: SkipPolicy matches instances by group prefix.
func TestSkipPolicy_MatchesByGroup(t *testing.T) {
	policy := &SkipPolicy{
		Group:  "status.durability",
		Reason: "quarantined for investigation",
	}

	inst := &Instance{Name: "status.durability.cycles4"}
	if !policy.Matches(inst) {
		t.Error("expected policy to match instance by group prefix")
	}

	instOther := &Instance{Name: "stress.read.crash"}
	if policy.Matches(instOther) {
		t.Error("policy should not match instance outside group")
	}
}

// Contract: SkipPolicy matches instances by tag values.
func TestSkipPolicy_MatchesByTags(t *testing.T) {
	policy := &SkipPolicy{
		Tags: map[string]string{
			"tier": "nightly",
			"kind": "crash",
		},
		Reason: "nightly crash tests disabled",
	}

	inst := &Instance{
		Name: "crash.test.1",
		Tier: TierNightly,
		Tool: ToolCrash,
	}
	if !policy.Matches(inst) {
		t.Error("expected policy to match instance by tags")
	}

	instQuick := &Instance{
		Name: "crash.test.2",
		Tier: TierQuick,
		Tool: ToolCrash,
	}
	if policy.Matches(instQuick) {
		t.Error("policy should not match instance with different tier")
	}
}

// Contract: InstanceSkipPolicies.ShouldSkip returns SkipResult for matching instances.
func TestInstanceSkipPolicies_ShouldSkip(t *testing.T) {
	policies := NewInstanceSkipPolicies("")
	policies.Add(&SkipPolicy{
		InstanceName: "broken.test",
		Reason:       "test is broken",
		IssueID:      "GH-123",
	})

	inst := &Instance{Name: "broken.test"}
	skipResult := policies.ShouldSkip(inst)
	if skipResult == nil {
		t.Fatal("expected skip result for matching instance")
	}
	if skipResult.Reason != "test is broken" {
		t.Errorf("reason = %q, want %q", skipResult.Reason, "test is broken")
	}
	if skipResult.IssueID != "GH-123" {
		t.Errorf("issue_id = %q, want %q", skipResult.IssueID, "GH-123")
	}
}

// Contract: InstanceSkipPolicies.ShouldSkip returns nil for non-matching instances.
func TestInstanceSkipPolicies_ShouldSkip_NoMatch(t *testing.T) {
	policies := NewInstanceSkipPolicies("")
	policies.Add(&SkipPolicy{
		InstanceName: "broken.test",
		Reason:       "test is broken",
	})

	inst := &Instance{Name: "working.test"}
	skipResult := policies.ShouldSkip(inst)
	if skipResult != nil {
		t.Error("expected nil for non-matching instance")
	}
}

// Contract: Skipped instances are NOT counted as passed.
func TestCampaignSummary_SkippedNotCounted(t *testing.T) {
	summary := &CampaignSummary{
		TotalRuns:   2,
		PassedRuns:  2,
		SkippedRuns: 1,
		Skipped: []SkipSummary{
			{Instance: "skipped.test", Reason: "broken"},
		},
	}

	// SkippedRuns should be separate from TotalRuns/PassedRuns
	if summary.SkippedRuns == 0 {
		t.Error("SkippedRuns should be tracked")
	}

	// TotalRuns should only count actually executed runs
	if summary.TotalRuns != 2 {
		t.Errorf("TotalRuns = %d, want 2 (executed runs only)", summary.TotalRuns)
	}
}

// Contract: SkipSummary contains instance, reason, and issue_id.
func TestSkipSummary_Fields(t *testing.T) {
	ss := SkipSummary{
		Instance: "test.instance",
		Reason:   "known issue",
		IssueID:  "GH-456",
	}

	if ss.Instance != "test.instance" {
		t.Errorf("Instance = %q, want %q", ss.Instance, "test.instance")
	}
	if ss.Reason != "known issue" {
		t.Errorf("Reason = %q, want %q", ss.Reason, "known issue")
	}
	if ss.IssueID != "GH-456" {
		t.Errorf("IssueID = %q, want %q", ss.IssueID, "GH-456")
	}
}

// Contract: ValidateSkipPolicyTags returns nil for valid tag keys.
func TestValidateSkipPolicyTags_ValidKeys(t *testing.T) {
	policy := &SkipPolicy{
		Tags: map[string]string{
			"tier":            "nightly",
			"tool":            "crashtest",
			"oracle_required": "true",
		},
		Reason: "valid policy",
	}

	if err := ValidateSkipPolicyTags(policy); err != nil {
		t.Errorf("unexpected error for valid keys: %v", err)
	}
}

// Contract: ValidateSkipPolicyTags returns error for unknown tag keys.
func TestValidateSkipPolicyTags_UnknownKey(t *testing.T) {
	policy := &SkipPolicy{
		Tags: map[string]string{
			"tier":        "nightly",
			"unknown_key": "value",
		},
		Reason: "invalid policy",
	}

	err := ValidateSkipPolicyTags(policy)
	if err == nil {
		t.Fatal("expected error for unknown tag key")
	}

	if !strings.Contains(err.Error(), "unknown tag key") {
		t.Errorf("error should mention unknown tag key, got: %v", err)
	}
}

// Contract: tagMatches returns false for unknown tag keys.
func TestTagMatches_UnknownKey(t *testing.T) {
	tags := Tags{
		Tier: "quick",
		Tool: "stresstest",
		Extra: map[string]string{
			"custom_key": "custom_value",
		},
	}

	// Unknown keys should NOT match (even if present in Extra)
	if tagMatches(tags, "custom_key", "custom_value") {
		t.Error("tagMatches should return false for unknown keys")
	}

	// Known keys should still work
	if !tagMatches(tags, "tier", "quick") {
		t.Error("tagMatches should return true for known key tier=quick")
	}
}
