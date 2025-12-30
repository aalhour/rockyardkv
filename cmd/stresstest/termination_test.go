// termination_test.go tests that stresstest terminates within expected time bounds.
//
// UC-15: Regression guard that `bin/stresstest -goroutine-faults` terminates (no hang).
// This test guards against the shutdown hang fixed in UC.T3.
package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestStresstest_GoroutineFaults_Terminates verifies that stresstest with
// goroutine-local fault injection terminates within expected time bounds.
//
// This is a regression guard for UC.T3 which fixed a hang where workers
// blocked indefinitely in MaybeStallWrite() or Flush() after fault injection
// caused the WriteController to remain in Stopped state.
//
// The test runs the stresstest binary (not the test binary) with a short
// duration and verifies it terminates within (duration + ceiling).
func TestStresstest_GoroutineFaults_Terminates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping termination test in short mode")
	}

	// Test parameters
	const (
		duration = 5 * time.Second
		ceiling  = 10 * time.Second // Extra time allowed for shutdown
		timeout  = duration + ceiling
	)

	// Create artifact directory
	runDir := filepath.Join(t.TempDir(), "termination-test")
	if err := os.MkdirAll(runDir, 0755); err != nil {
		t.Fatalf("create run dir: %v", err)
	}

	// Build stresstest binary if not already built
	binPath := filepath.Join("..", "..", "bin", "stresstest")
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		// Try to find it in workspace root
		binPath = filepath.Join("..", "..", "bin", "stresstest")
		if _, err := os.Stat(binPath); os.IsNotExist(err) {
			t.Skip("stresstest binary not found; run 'make build' first")
		}
	}

	// Make path absolute
	binPath, err := filepath.Abs(binPath)
	if err != nil {
		t.Fatalf("abs path: %v", err)
	}

	// Run stresstest with goroutine-faults
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, binPath,
		"-goroutine-faults",
		"-fault-writer-write=5",
		"-fault-error-type=status",
		"-seed=12345",
		"-duration=5s",
		"-flush=1s",
		"-reopen=1s",
		"-run-dir="+runDir,
		"-cleanup",
		"-v",
	)

	startTime := time.Now()

	// Run and capture output
	output, err := cmd.CombinedOutput()
	elapsed := time.Since(startTime)

	// Check if we hit the timeout
	if ctx.Err() == context.DeadlineExceeded {
		// Save output for debugging
		outputPath := filepath.Join(runDir, "timeout_output.log")
		_ = os.WriteFile(outputPath, output, 0644)

		t.Fatalf("HANG DETECTED: stresstest did not terminate within %v (elapsed: %v)\n"+
			"Output saved to: %s\n"+
			"Last output:\n%s",
			timeout, elapsed, outputPath, truncateOutput(output, 2000))
	}

	// The test may exit with code 1 due to injected faults causing errors,
	// which is expected. We only care about termination time, not exit code.
	t.Logf("stresstest terminated in %v (limit: %v)", elapsed, timeout)

	if err != nil {
		// Log error but don't fail - errors are expected with fault injection
		t.Logf("stresstest exited with error (expected with fault injection): %v", err)
	}

	// Verify termination time is reasonable
	if elapsed > timeout {
		t.Errorf("stresstest took too long: %v > %v", elapsed, timeout)
	}

	// Check that artifacts were collected on failure
	runJSON := filepath.Join(runDir, "run.json")
	if _, statErr := os.Stat(runJSON); statErr == nil {
		t.Logf("Artifacts collected at: %s", runDir)
	}

	t.Logf("✅ Termination guard passed: stresstest with -goroutine-faults terminates within bounds")
}

// truncateOutput returns the last n bytes of output for logging.
func truncateOutput(output []byte, n int) string {
	if len(output) <= n {
		return string(output)
	}
	return "...(truncated)...\n" + string(output[len(output)-n:])
}
