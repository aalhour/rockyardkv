package campaign

import "testing"

// Contract: classifyFailureKind must not classify exec/startup errors as timeouts.
func TestClassifyFailureKind_ExecError_IsExitError(t *testing.T) {
	got := classifyFailureKind("fork/exec ./bin/stresstest: no such file or directory", -1)
	if got != "exit_error" {
		t.Fatalf("classifyFailureKind(exec error, -1)=%q, want %q", got, "exit_error")
	}
}

// Contract: classifyFailureKind classifies explicit timeouts as timeouts.
func TestClassifyFailureKind_Timeout_IsTimeout(t *testing.T) {
	got := classifyFailureKind("timeout", -1)
	if got != "timeout" {
		t.Fatalf("classifyFailureKind(timeout, -1)=%q, want %q", got, "timeout")
	}
}
