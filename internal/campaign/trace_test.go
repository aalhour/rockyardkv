package campaign

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Contract: DefaultTraceConfig returns a config with trace disabled by default.
func TestDefaultTraceConfig(t *testing.T) {
	cfg := DefaultTraceConfig()
	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if cfg.MaxSizeBytes != 256*1024*1024 {
		t.Errorf("expected MaxSizeBytes=256MB, got %d", cfg.MaxSizeBytes)
	}
	if cfg.TraceDir != "trace" {
		t.Errorf("expected TraceDir=trace, got %s", cfg.TraceDir)
	}
}

// Contract: TracePaths returns consistent paths for trace file and truncated marker.
func TestTracePaths(t *testing.T) {
	cfg := DefaultTraceConfig()
	runDir := "/tmp/test-run"

	traceFile, marker := TracePaths(runDir, cfg)

	expectedTrace := filepath.Join(runDir, "trace", "ops.bin")
	expectedMarker := filepath.Join(runDir, "trace", "truncated.txt")

	if traceFile != expectedTrace {
		t.Errorf("expected trace file %s, got %s", expectedTrace, traceFile)
	}
	if marker != expectedMarker {
		t.Errorf("expected marker %s, got %s", expectedMarker, marker)
	}
}

// Contract: EnsureTraceDir does nothing when trace is disabled.
func TestEnsureTraceDir_Disabled(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = false

	err := EnsureTraceDir("/nonexistent/path", cfg)
	if err != nil {
		t.Errorf("expected no error when disabled, got %v", err)
	}
}

// Contract: EnsureTraceDir creates the trace directory when enabled.
func TestEnsureTraceDir_Enabled(t *testing.T) {
	runDir := t.TempDir()
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	err := EnsureTraceDir(runDir, cfg)
	if err != nil {
		t.Fatalf("EnsureTraceDir failed: %v", err)
	}

	traceDir := filepath.Join(runDir, "trace")
	if _, err := os.Stat(traceDir); os.IsNotExist(err) {
		t.Error("trace directory was not created")
	}
}

// Contract: InjectTraceArgs does nothing when trace is disabled.
func TestInjectTraceArgs_Disabled(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = false

	original := []string{"-duration", "10s", "-threads", "4"}
	result, tracePath := InjectTraceArgs(original, "/tmp/run", cfg)

	if len(result) != len(original) {
		t.Error("args should not be modified when disabled")
	}
	if tracePath != "" {
		t.Errorf("tracePath should be empty when disabled, got %s", tracePath)
	}
}

// Contract: InjectTraceArgs adds -trace-out and -trace-max-size when enabled.
func TestInjectTraceArgs_Enabled(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	original := []string{"-duration", "10s", "-threads", "4"}
	result, tracePath := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 4 args: -trace-out, <path>, -trace-max-size, <size>
	if len(result) != len(original)+4 {
		t.Errorf("expected %d args, got %d: %v", len(original)+4, len(result), result)
	}

	foundTraceOut := false
	foundMaxSize := false
	for i, arg := range result {
		if arg == "-trace-out" {
			foundTraceOut = true
			if i+1 < len(result) && result[i+1] != tracePath {
				t.Errorf("trace path mismatch: %s vs %s", result[i+1], tracePath)
			}
		}
		if arg == "-trace-max-size" {
			foundMaxSize = true
			if i+1 < len(result) && result[i+1] != "268435456" { // 256MB
				t.Errorf("expected max size 268435456, got %s", result[i+1])
			}
		}
	}
	if !foundTraceOut {
		t.Error("-trace-out was not injected")
	}
	if !foundMaxSize {
		t.Error("-trace-max-size was not injected")
	}
}

// Contract: InjectTraceArgs returns existing path when -trace-out already present, but still injects -trace-max-size.
func TestInjectTraceArgs_AlreadyPresent(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	original := []string{"-duration", "10s", "-trace-out", "/existing/path"}
	result, tracePath := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 2 args: -trace-max-size, <size> (but not -trace-out)
	if len(result) != len(original)+2 {
		t.Errorf("expected %d args, got %d: %v", len(original)+2, len(result), result)
	}
	if tracePath != "/existing/path" {
		t.Errorf("tracePath should be the existing path, got %s", tracePath)
	}

	// Verify -trace-max-size was injected
	foundMaxSize := false
	for _, arg := range result {
		if arg == "-trace-max-size" {
			foundMaxSize = true
		}
	}
	if !foundMaxSize {
		t.Error("-trace-max-size should still be injected when -trace-out is already present")
	}
}

// Contract: InjectTraceArgs handles -trace-out= style but still injects -trace-max-size.
func TestInjectTraceArgs_EqualsStyle(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	original := []string{"-duration", "10s", "-trace-out=/other/path"}
	result, tracePath := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 2 args: -trace-max-size, <size> (but not -trace-out)
	if len(result) != len(original)+2 {
		t.Errorf("expected %d args, got %d: %v", len(original)+2, len(result), result)
	}
	if tracePath != "/other/path" {
		t.Errorf("tracePath should be the existing path, got %s", tracePath)
	}
}

// Contract: InjectTraceArgs does not inject -trace-max-size when MaxSizeBytes is 0.
func TestInjectTraceArgs_NoMaxSizeWhenZero(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true
	cfg.MaxSizeBytes = 0 // Unlimited

	original := []string{"-duration", "10s"}
	result, _ := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 2 args: -trace-out, <path> (but not -trace-max-size)
	if len(result) != len(original)+2 {
		t.Errorf("expected %d args, got %d: %v", len(original)+2, len(result), result)
	}

	for _, arg := range result {
		if arg == "-trace-max-size" {
			t.Error("-trace-max-size should not be injected when MaxSizeBytes is 0")
		}
	}
}

// Contract: InjectTraceArgs does not duplicate -trace-max-size when already present.
func TestInjectTraceArgs_MaxSizeAlreadyPresent(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	original := []string{"-duration", "10s", "-trace-max-size", "1024"}
	result, _ := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 2 args: -trace-out, <path> (but not -trace-max-size since already present)
	if len(result) != len(original)+2 {
		t.Errorf("expected %d args, got %d: %v", len(original)+2, len(result), result)
	}

	count := 0
	for _, arg := range result {
		if arg == "-trace-max-size" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 -trace-max-size, got %d", count)
	}
}

// Contract: InjectTraceArgs handles -trace-max-size= style without duplicating.
func TestInjectTraceArgs_MaxSizeEqualsStyle(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	original := []string{"-duration", "10s", "-trace-max-size=2048"}
	result, _ := InjectTraceArgs(original, "/tmp/run", cfg)

	// Should add 2 args: -trace-out, <path> (but not -trace-max-size since already present)
	if len(result) != len(original)+2 {
		t.Errorf("expected %d args, got %d: %v", len(original)+2, len(result), result)
	}

	count := 0
	for _, arg := range result {
		if arg == "-trace-max-size" || strings.HasPrefix(arg, "-trace-max-size=") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 -trace-max-size, got %d", count)
	}
}

// Contract: CheckTraceSize returns 0 and no error for non-existent file.
func TestCheckTraceSize_NonExistent(t *testing.T) {
	cfg := DefaultTraceConfig()
	size, exceeded, err := CheckTraceSize("/nonexistent/file", cfg)
	if err != nil {
		t.Errorf("expected no error for non-existent file, got %v", err)
	}
	if size != 0 {
		t.Errorf("expected size 0, got %d", size)
	}
	if exceeded {
		t.Error("exceeded should be false for non-existent file")
	}
}

// Contract: CheckTraceSize reports size and exceeded status correctly.
func TestCheckTraceSize_ExistsWithinLimit(t *testing.T) {
	f, err := os.CreateTemp("", "trace-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString("test data")
	_ = f.Close()

	cfg := DefaultTraceConfig()
	size, exceeded, err := CheckTraceSize(f.Name(), cfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if size != 9 { // "test data" is 9 bytes
		t.Errorf("expected size 9, got %d", size)
	}
	if exceeded {
		t.Error("exceeded should be false for small file")
	}
}

// Contract: WriteTruncatedMarker creates a marker file with limit info.
func TestWriteTruncatedMarker(t *testing.T) {
	runDir := t.TempDir()
	cfg := DefaultTraceConfig()

	// Create trace dir first
	traceDir := filepath.Join(runDir, cfg.TraceDir)
	if err := os.MkdirAll(traceDir, 0o755); err != nil {
		t.Fatal(err)
	}

	err := WriteTruncatedMarker(runDir, cfg, 100*1024*1024)
	if err != nil {
		t.Fatalf("WriteTruncatedMarker failed: %v", err)
	}

	_, markerPath := TracePaths(runDir, cfg)
	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("failed to read marker: %v", err)
	}

	content := string(data)
	if len(content) == 0 {
		t.Error("marker file is empty")
	}
}

// Contract: BuildReplayCommand constructs a valid traceanalyzer command with quoted paths.
func TestBuildReplayCommand(t *testing.T) {
	cmd := BuildReplayCommand("/tmp/trace.bin", "/tmp/db", "./bin")
	// filepath.Join normalizes "./bin" to "bin", paths are quoted for shell safety
	expected := `"bin/traceanalyzer" -db "/tmp/db" -create=true replay "/tmp/trace.bin"`
	if cmd != expected {
		t.Errorf("expected %q, got %q", expected, cmd)
	}
}

// Contract: BuildReplayCommand handles paths with spaces.
func TestBuildReplayCommand_PathsWithSpaces(t *testing.T) {
	cmd := BuildReplayCommand("/tmp/my trace.bin", "/tmp/my db", "./bin")
	expected := `"bin/traceanalyzer" -db "/tmp/my db" -create=true replay "/tmp/my trace.bin"`
	if cmd != expected {
		t.Errorf("expected %q, got %q", expected, cmd)
	}
}

// Contract: WriteReplayScript creates an executable replay.sh script.
func TestWriteReplayScript(t *testing.T) {
	runDir := t.TempDir()

	err := WriteReplayScript(runDir, "/tmp/trace.bin", "/tmp/db", "./bin")
	if err != nil {
		t.Fatalf("WriteReplayScript failed: %v", err)
	}

	scriptPath := filepath.Join(runDir, "replay.sh")
	info, err := os.Stat(scriptPath)
	if err != nil {
		t.Fatalf("replay.sh not found: %v", err)
	}

	// Check executable bit
	if info.Mode()&0o100 == 0 {
		t.Error("replay.sh is not executable")
	}

	// Check content
	data, _ := os.ReadFile(scriptPath)
	if len(data) == 0 {
		t.Error("replay.sh is empty")
	}
}

// Contract: CollectTraceResult returns nil when trace is disabled.
func TestCollectTraceResult_Disabled(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = false

	result := CollectTraceResult("/tmp/run", "/tmp/db", "./bin", cfg)
	if result != nil {
		t.Error("expected nil when trace disabled")
	}
}

// Contract: CollectTraceResult returns nil when trace file does not exist.
func TestCollectTraceResult_NoFile(t *testing.T) {
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	result := CollectTraceResult("/nonexistent/run", "/tmp/db", "./bin", cfg)
	if result != nil {
		t.Error("expected nil when trace file does not exist")
	}
}

// Contract: CollectTraceResult returns trace info when trace file exists.
func TestCollectTraceResult_WithFile(t *testing.T) {
	runDir := t.TempDir()
	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	// Create trace directory and file
	traceDir := filepath.Join(runDir, cfg.TraceDir)
	if err := os.MkdirAll(traceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	traceFile := filepath.Join(traceDir, "ops.bin")
	if err := os.WriteFile(traceFile, []byte("trace data"), 0o644); err != nil {
		t.Fatal(err)
	}

	result := CollectTraceResult(runDir, "/tmp/db", "./bin", cfg)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Path != traceFile {
		t.Errorf("expected path %s, got %s", traceFile, result.Path)
	}
	if result.BytesWritten != 10 { // "trace data" is 10 bytes
		t.Errorf("expected 10 bytes, got %d", result.BytesWritten)
	}
	if result.Truncated {
		t.Error("should not be truncated")
	}
}
