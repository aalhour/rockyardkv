//go:build crashtest

package testutil

import (
	"os"
	"os/exec"
	"testing"
)

func TestKillPoint_SetAndGet(t *testing.T) {
	// Clear any existing state
	ClearKillPoint()
	ResetKillPointCounts()

	// Initially no target
	if got := GetKillPointTarget(); got != "" {
		t.Errorf("GetKillPointTarget() = %q, want empty", got)
	}
	if IsKillPointArmed() {
		t.Error("IsKillPointArmed() = true, want false")
	}

	// Set a target
	SetKillPoint("test.point:0")
	if got := GetKillPointTarget(); got != "test.point:0" {
		t.Errorf("GetKillPointTarget() = %q, want %q", got, "test.point:0")
	}
	if !IsKillPointArmed() {
		t.Error("IsKillPointArmed() = false, want true")
	}

	// Clear target
	ClearKillPoint()
	if got := GetKillPointTarget(); got != "" {
		t.Errorf("GetKillPointTarget() = %q, want empty", got)
	}
	if IsKillPointArmed() {
		t.Error("IsKillPointArmed() = true, want false")
	}
}

func TestKillPoint_ArmDisarm(t *testing.T) {
	ClearKillPoint()

	SetKillPoint("test.point:0")
	if !IsKillPointArmed() {
		t.Fatal("expected armed after SetKillPoint")
	}

	DisarmKillPoint()
	if IsKillPointArmed() {
		t.Error("expected disarmed after DisarmKillPoint")
	}

	// Target should still be set
	if got := GetKillPointTarget(); got != "test.point:0" {
		t.Errorf("target cleared unexpectedly: got %q", got)
	}

	ArmKillPoint()
	if !IsKillPointArmed() {
		t.Error("expected armed after ArmKillPoint")
	}

	ClearKillPoint()
}

func TestKillPoint_HitCounts(t *testing.T) {
	ClearKillPoint()
	ResetKillPointCounts()

	// Set a different target so MaybeKill doesn't exit
	SetKillPoint("different.point")

	// Call MaybeKill several times
	MaybeKill("test.point:0")
	MaybeKill("test.point:0")
	MaybeKill("test.point:1")

	if got := GetKillPointHitCount("test.point:0"); got != 2 {
		t.Errorf("GetKillPointHitCount(test.point:0) = %d, want 2", got)
	}
	if got := GetKillPointHitCount("test.point:1"); got != 1 {
		t.Errorf("GetKillPointHitCount(test.point:1) = %d, want 1", got)
	}
	if got := GetKillPointHitCount("nonexistent"); got != 0 {
		t.Errorf("GetKillPointHitCount(nonexistent) = %d, want 0", got)
	}

	// Reset counts
	ResetKillPointCounts()
	if got := GetKillPointHitCount("test.point:0"); got != 0 {
		t.Errorf("after reset, GetKillPointHitCount(test.point:0) = %d, want 0", got)
	}

	ClearKillPoint()
}

func TestKillPoint_MaybeKillNoOpWhenDisarmed(t *testing.T) {
	ClearKillPoint()
	ResetKillPointCounts()

	// Even if target matches, should not exit when disarmed
	SetKillPoint("test.point:0")
	DisarmKillPoint()

	// This should not exit
	MaybeKill("test.point:0")

	// Count should still be 0 because we're disarmed
	if got := GetKillPointHitCount("test.point:0"); got != 0 {
		t.Errorf("expected 0 hits when disarmed, got %d", got)
	}

	ClearKillPoint()
}

func TestKillPoint_MaybeKillNoOpWhenMismatch(t *testing.T) {
	ClearKillPoint()
	ResetKillPointCounts()

	SetKillPoint("target.point:0")

	// Different point should not exit
	MaybeKill("other.point:0")

	// Should have counted the hit
	if got := GetKillPointHitCount("other.point:0"); got != 1 {
		t.Errorf("GetKillPointHitCount(other.point:0) = %d, want 1", got)
	}

	ClearKillPoint()
}

func TestKillPoint_Constants(t *testing.T) {
	// Verify constants are defined and follow naming convention
	tests := []struct {
		name string
		want string
	}{
		{"KPWALAppend0", "WAL.Append:0"},
		{"KPWALSync0", "WAL.Sync:0"},
		{"KPWALSync1", "WAL.Sync:1"},
		{"KPManifestWrite0", "Manifest.Write:0"},
		{"KPManifestSync0", "Manifest.Sync:0"},
		{"KPManifestSync1", "Manifest.Sync:1"},
		{"KPCurrentWrite0", "Current.Write:0"},
		{"KPCurrentWrite1", "Current.Write:1"},
	}

	actuals := []string{
		KPWALAppend0,
		KPWALSync0,
		KPWALSync1,
		KPManifestWrite0,
		KPManifestSync0,
		KPManifestSync1,
		KPCurrentWrite0,
		KPCurrentWrite1,
	}

	for i, tc := range tests {
		if actuals[i] != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, actuals[i], tc.want)
		}
	}
}

// TestKillPoint_ExitsAtTarget verifies that MaybeKill exits the process
// when the target matches. This runs a subprocess to avoid killing the test.
func TestKillPoint_ExitsAtTarget(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		// We're in the subprocess - set kill point and trigger it
		SetKillPoint("crash.now:0")
		MaybeKill("crash.now:0")
		// If we get here, the kill point didn't work
		os.Exit(1)
	}

	// Run this test as a subprocess
	cmd := exec.Command(os.Args[0], "-test.run=^TestKillPoint_ExitsAtTarget$")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")

	err := cmd.Run()

	// The subprocess should exit with code 0 (clean kill)
	if exitErr, ok := err.(*exec.ExitError); ok {
		t.Errorf("subprocess exited with code %d, want 0", exitErr.ExitCode())
	} else if err != nil {
		// Subprocess exited with code 0 (nil error means success)
		t.Errorf("unexpected error: %v", err)
	}
	// err == nil means exit code 0, which is what we want
}

// TestKillPoint_EnvVarSetsTarget verifies that ROCKYARDKV_KILL_POINT
// environment variable sets the kill point target on startup.
func TestKillPoint_EnvVarSetsTarget(t *testing.T) {
	if os.Getenv("CHECK_ENV_VAR") == "1" {
		// We're in the subprocess - check if env var was parsed
		target := GetKillPointTarget()
		if target != "env.test:0" {
			os.Exit(2) // Wrong target
		}
		if !IsKillPointArmed() {
			os.Exit(3) // Not armed
		}
		os.Exit(0) // Success
	}

	// Run this test as a subprocess with the env var set
	cmd := exec.Command(os.Args[0], "-test.run=^TestKillPoint_EnvVarSetsTarget$")
	cmd.Env = append(os.Environ(),
		"CHECK_ENV_VAR=1",
		KillPointEnvVar+"=env.test:0",
	)

	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			switch exitErr.ExitCode() {
			case 2:
				t.Error("subprocess: wrong target from env var")
			case 3:
				t.Error("subprocess: not armed from env var")
			default:
				t.Errorf("subprocess exited with code %d", exitErr.ExitCode())
			}
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}
}
