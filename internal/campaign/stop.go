// stop.go defines stop conditions for instance runs.
//
// Stop conditions specify what constitutes success vs failure for a run.
// They control termination requirements, verification passes, and oracle checks.
package campaign

// StopCondition defines when an instance run is considered complete and what
// constitutes success vs failure.
type StopCondition struct {
	// RequireTermination requires the process to terminate within the timeout.
	// If false, the runner will kill after timeout but not treat it as failure.
	RequireTermination bool

	// RequireFinalVerificationPass requires the tool's final verification to pass.
	// For stresstest, this means expected state verification.
	// For crashtest, this means recovery verification.
	RequireFinalVerificationPass bool

	// RequireOracleCheckConsistencyOK requires `ldb checkconsistency` to return OK.
	// Only applies to instances with RequiresOracle=true.
	RequireOracleCheckConsistencyOK bool

	// DedupeByFingerprint enables deduplication by failure fingerprint.
	// When true, repeated failures with the same fingerprint are marked as duplicates.
	DedupeByFingerprint bool
}

// DefaultStopCondition returns the default stop condition for most instances.
func DefaultStopCondition() StopCondition {
	return StopCondition{
		RequireTermination:              true,
		RequireFinalVerificationPass:    true,
		RequireOracleCheckConsistencyOK: true,
		DedupeByFingerprint:             true,
	}
}

// InstanceTimeout returns the default timeout for an instance based on tier.
func InstanceTimeout(tier Tier) int {
	switch tier {
	case TierQuick:
		return 5 * 60 // 5 minutes
	case TierNightly:
		return 30 * 60 // 30 minutes
	default:
		return 10 * 60 // 10 minutes
	}
}

// GlobalTimeout returns the global timeout for a campaign run based on tier.
func GlobalTimeout(tier Tier) int {
	switch tier {
	case TierQuick:
		return 30 * 60 // 30 minutes
	case TierNightly:
		return 8 * 60 * 60 // 8 hours
	default:
		return 60 * 60 // 1 hour
	}
}
