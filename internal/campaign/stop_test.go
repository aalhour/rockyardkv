package campaign

import "testing"

// Contract: DefaultStopCondition requires termination, verification, oracle check, and deduplication.
func TestDefaultStopCondition(t *testing.T) {
	stop := DefaultStopCondition()

	if !stop.RequireTermination {
		t.Error("DefaultStopCondition should require termination")
	}

	if !stop.RequireFinalVerificationPass {
		t.Error("DefaultStopCondition should require final verification")
	}

	if !stop.RequireOracleCheckConsistencyOK {
		t.Error("DefaultStopCondition should require oracle consistency check")
	}

	if !stop.DedupeByFingerprint {
		t.Error("DefaultStopCondition should dedupe by fingerprint")
	}
}

// Contract: InstanceTimeout returns a positive duration for the quick tier.
func TestInstanceTimeout_Quick(t *testing.T) {
	timeout := InstanceTimeout(TierQuick)
	if timeout <= 0 {
		t.Error("InstanceTimeout(TierQuick) should return positive duration")
	}
}

// Contract: InstanceTimeout for nightly tier is at least as long as quick tier.
func TestInstanceTimeout_Nightly(t *testing.T) {
	timeout := InstanceTimeout(TierNightly)
	if timeout <= 0 {
		t.Error("InstanceTimeout(TierNightly) should return positive duration")
	}

	quickTimeout := InstanceTimeout(TierQuick)
	if timeout < quickTimeout {
		t.Errorf("InstanceTimeout(TierNightly) = %v should be >= TierQuick = %v",
			timeout, quickTimeout)
	}
}

// Contract: GlobalTimeout returns a positive duration for the quick tier.
func TestGlobalTimeout_Quick(t *testing.T) {
	timeout := GlobalTimeout(TierQuick)
	if timeout <= 0 {
		t.Error("GlobalTimeout(TierQuick) should return positive duration")
	}
}

// Contract: GlobalTimeout for nightly tier is at least as long as quick tier.
func TestGlobalTimeout_Nightly(t *testing.T) {
	timeout := GlobalTimeout(TierNightly)
	if timeout <= 0 {
		t.Error("GlobalTimeout(TierNightly) should return positive duration")
	}

	quickTimeout := GlobalTimeout(TierQuick)
	if timeout < quickTimeout {
		t.Errorf("GlobalTimeout(TierNightly) = %v should be >= TierQuick = %v",
			timeout, quickTimeout)
	}
}

// Contract: GlobalTimeout returns a positive duration for unknown tiers.
func TestGlobalTimeout_UnknownTier(t *testing.T) {
	timeout := GlobalTimeout(Tier("unknown"))

	if timeout <= 0 {
		t.Error("GlobalTimeout(unknown) should return positive value")
	}
}
