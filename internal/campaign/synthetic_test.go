package campaign

import (
	"context"
	"os"
	"testing"
)

// Contract: SyntheticInstance returns nil when env var is not set.
func TestSyntheticInstance_ReturnsNilWithoutEnvVar(t *testing.T) {
	// Ensure env var is not set
	os.Unsetenv("ROCKYARDKV_SYNTHETIC_FAIL")

	inst := SyntheticInstance()
	if inst != nil {
		t.Error("SyntheticInstance() should return nil when ROCKYARDKV_SYNTHETIC_FAIL is not set")
	}
}

// Contract: SyntheticInstance returns instance when env var is "1".
func TestSyntheticInstance_ReturnsInstanceWithEnvVar(t *testing.T) {
	os.Setenv("ROCKYARDKV_SYNTHETIC_FAIL", "1")
	defer os.Unsetenv("ROCKYARDKV_SYNTHETIC_FAIL")

	inst := SyntheticInstance()
	if inst == nil {
		t.Fatal("SyntheticInstance() should return instance when ROCKYARDKV_SYNTHETIC_FAIL=1")
	}

	if inst.Name != "synthetic.deterministic_fail" {
		t.Errorf("Name = %q, want %q", inst.Name, "synthetic.deterministic_fail")
	}
}

// Contract: RunSyntheticFailure produces stable, classifiable failure.
func TestRunSyntheticFailure_ProducesStableFailure(t *testing.T) {
	runDir := t.TempDir()

	config := SyntheticFailConfig{
		Enabled:        true,
		FailAfterOps:   100,
		FailureKind:    "verification_failure",
		FailureMessage: "synthetic verification failed",
	}

	result := RunSyntheticFailure(context.Background(), config, runDir)

	if result.Passed {
		t.Error("synthetic failure should not pass")
	}

	if result.FailureKind != "verification_failure" {
		t.Errorf("FailureKind = %q, want %q", result.FailureKind, "verification_failure")
	}

	if result.FailureReason != "synthetic verification failed" {
		t.Errorf("FailureReason = %q, want %q", result.FailureReason, "synthetic verification failed")
	}

	if result.Fingerprint == "" {
		t.Error("Fingerprint should not be empty")
	}
}

// Contract: RunSyntheticFailure produces the same fingerprint for same config.
func TestRunSyntheticFailure_StableFingerprint(t *testing.T) {
	config := SyntheticFailConfig{
		Enabled:        true,
		FailAfterOps:   50,
		FailureKind:    "oracle_failure",
		FailureMessage: "oracle check failed",
	}

	result1 := RunSyntheticFailure(context.Background(), config, t.TempDir())
	result2 := RunSyntheticFailure(context.Background(), config, t.TempDir())

	if result1.Fingerprint != result2.Fingerprint {
		t.Errorf("Fingerprints differ: %q vs %q", result1.Fingerprint, result2.Fingerprint)
	}
}
