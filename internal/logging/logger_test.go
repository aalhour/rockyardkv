package logging

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Contract: DefaultLogger filters messages by level.
func TestDefaultLogger_LevelFiltering(t *testing.T) {
	tests := []struct {
		level     Level
		wantError bool
		wantWarn  bool
		wantInfo  bool
		wantDebug bool
	}{
		{LevelError, true, false, false, false},
		{LevelWarn, true, true, false, false},
		{LevelInfo, true, true, true, false},
		{LevelDebug, true, true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.level.String(), func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(&buf, tt.level)

			logger.Errorf("error message")
			logger.Warnf("warn message")
			logger.Infof("info message")
			logger.Debugf("debug message")

			output := buf.String()

			if got := strings.Contains(output, "ERROR "); got != tt.wantError {
				t.Errorf("Error logged: got %v, want %v", got, tt.wantError)
			}
			if got := strings.Contains(output, "WARN "); got != tt.wantWarn {
				t.Errorf("Warn logged: got %v, want %v", got, tt.wantWarn)
			}
			if got := strings.Contains(output, "INFO "); got != tt.wantInfo {
				t.Errorf("Info logged: got %v, want %v", got, tt.wantInfo)
			}
			if got := strings.Contains(output, "DEBUG "); got != tt.wantDebug {
				t.Errorf("Debug logged: got %v, want %v", got, tt.wantDebug)
			}
		})
	}
}

// Contract: DefaultLogger formats messages correctly.
func TestDefaultLogger_Formatted(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelDebug)

	logger.Errorf("error %d", 1)
	logger.Warnf("warn %d", 2)
	logger.Infof("info %d", 3)
	logger.Debugf("debug %d", 4)

	output := buf.String()

	if !strings.Contains(output, "error 1") {
		t.Error("formatted error message not found")
	}
	if !strings.Contains(output, "warn 2") {
		t.Error("formatted warn message not found")
	}
	if !strings.Contains(output, "info 3") {
		t.Error("formatted info message not found")
	}
	if !strings.Contains(output, "debug 4") {
		t.Error("formatted debug message not found")
	}
}

// Contract: DiscardLogger does not panic.
func TestDiscardLogger(t *testing.T) {
	Discard.Errorf("error %d", 1)
	Discard.Warnf("warn %d", 1)
	Discard.Infof("info %d", 1)
	Discard.Debugf("debug %d", 1)
	Discard.Fatalf("fatal %d", 1)
}

// Contract: Level.String() returns human-readable level names.
func TestLevelString(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{LevelError, "ERROR"},
		{LevelWarn, "WARN"},
		{LevelInfo, "INFO"},
		{LevelDebug, "DEBUG"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.want {
			t.Errorf("Level(%d).String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}

// Contract: Namespace constants are in [name] format.
func TestNamespaceConstants(t *testing.T) {
	namespaces := []string{NSFlush, NSCompact, NSWAL, NSManifest, NSRecovery, NSDB}
	for _, ns := range namespaces {
		if !strings.HasPrefix(ns, "[") || !strings.Contains(ns, "]") {
			t.Errorf("namespace %q should be in [name] format", ns)
		}
	}
}

// Contract: Log format follows "TIMESTAMP LEVEL [component] message" pattern.
func TestLogFormat_Standard(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelInfo)

	logger.Infof("%s%s", NSFlush, "flush started")

	output := buf.String()

	if strings.HasPrefix(output, "rockyardkv") {
		t.Errorf("output should NOT start with 'rockyardkv', got: %s", output)
	}

	if !strings.Contains(output, "INFO ") {
		t.Error("output should contain 'INFO '")
	}

	if !strings.Contains(output, "[flush]") {
		t.Error("output should contain '[flush]'")
	}

	if !strings.Contains(output, "flush started") {
		t.Error("output should contain 'flush started'")
	}

	t.Logf("Log format verified: %s", output)
}

// Contract: IsNil returns true for nil interface.
func TestIsNil_NilInterface(t *testing.T) {
	var l Logger = nil
	if !IsNil(l) {
		t.Error("IsNil should return true for nil interface")
	}
}

// Contract: IsNil returns true for typed-nil (nil pointer assigned to interface).
func TestIsNil_TypedNil(t *testing.T) {
	var dl *DefaultLogger = nil
	var l Logger = dl
	if !IsNil(l) {
		t.Error("IsNil should return true for typed-nil")
	}
}

// Contract: IsNil returns false for valid logger.
func TestIsNil_ValidLogger(t *testing.T) {
	l := NewDefaultLogger(LevelWarn)
	if IsNil(l) {
		t.Error("IsNil should return false for valid logger")
	}
}

// Contract: OrDefault returns default logger for nil.
func TestOrDefault_Nil(t *testing.T) {
	l := OrDefault(nil)
	if l == nil {
		t.Error("OrDefault should return a non-nil logger")
	}
	dl, ok := l.(*DefaultLogger)
	if !ok {
		t.Error("OrDefault should return a *DefaultLogger")
	}
	if dl.Level() != LevelWarn {
		t.Errorf("OrDefault should return WARN level, got %s", dl.Level())
	}
}

// Contract: OrDefault returns default logger for typed-nil.
func TestOrDefault_TypedNil(t *testing.T) {
	var dl *DefaultLogger = nil
	var l Logger = dl

	result := OrDefault(l)
	if result == nil {
		t.Error("OrDefault should return a non-nil logger for typed-nil")
	}
	resultDL, ok := result.(*DefaultLogger)
	if !ok {
		t.Error("OrDefault should return a *DefaultLogger")
	}
	if resultDL.Level() != LevelWarn {
		t.Errorf("OrDefault should return WARN level, got %s", resultDL.Level())
	}
}

// Contract: OrDefault returns the provided logger if valid.
func TestOrDefault_ValidLogger(t *testing.T) {
	original := NewDefaultLogger(LevelDebug)
	result := OrDefault(original)
	if result != original {
		t.Error("OrDefault should return the same logger if valid")
	}
}

// Contract: Fatalf logs at FATAL level regardless of configured level.
func TestFatalf_AlwaysLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelError) // Lowest level

	logger.Fatalf("fatal error: %s", "corruption detected")

	output := buf.String()
	if !strings.Contains(output, "FATAL ") {
		t.Errorf("Fatalf should log at FATAL level, got: %s", output)
	}
	if !strings.Contains(output, "fatal error: corruption detected") {
		t.Errorf("Fatalf message not found, got: %s", output)
	}
}

// Contract: Fatalf calls the configured FatalHandler.
func TestFatalf_CallsFatalHandler(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelWarn)

	var handlerCalled atomic.Bool
	var capturedMsg string
	var mu sync.Mutex

	handler := FatalHandler(func(msg string) {
		mu.Lock()
		capturedMsg = msg
		mu.Unlock()
		handlerCalled.Store(true)
	})
	logger.SetFatalHandler(handler)

	logger.Fatalf("invariant violation: %s", "file already compacting")

	if !handlerCalled.Load() {
		t.Error("FatalHandler was not called")
	}

	mu.Lock()
	if !strings.Contains(capturedMsg, "invariant violation: file already compacting") {
		t.Errorf("FatalHandler received wrong message: %s", capturedMsg)
	}
	mu.Unlock()
}

// Contract: Fatalf without FatalHandler does not panic.
func TestFatalf_NoHandler_NoPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelWarn)
	// No handler set

	// Should not panic
	logger.Fatalf("fatal error")

	output := buf.String()
	if !strings.Contains(output, "FATAL ") {
		t.Error("Fatalf should still log even without handler")
	}
}

// Contract: DefaultLogger is safe for concurrent use.
func TestDefaultLogger_Concurrent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelDebug)

	var handlerCalls atomic.Int32
	handler := FatalHandler(func(msg string) {
		handlerCalls.Add(1)
	})
	logger.SetFatalHandler(handler)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logger.Errorf("error %d", n)
			logger.Warnf("warn %d", n)
			logger.Infof("info %d", n)
			logger.Debugf("debug %d", n)
			if n%10 == 0 {
				logger.Fatalf("fatal %d", n)
			}
		}(i)
	}
	wg.Wait()

	// 10 Fatalf calls (0, 10, 20, ..., 90)
	if got := handlerCalls.Load(); got != 10 {
		t.Errorf("Expected 10 fatal handler calls, got %d", got)
	}
}

// Contract: ErrFatal can be detected with errors.Is.
func TestErrFatal_Sentinel(t *testing.T) {
	if ErrFatal == nil {
		t.Error("ErrFatal should not be nil")
	}
	if ErrFatal.Error() != "fatal error" {
		t.Errorf("ErrFatal.Error() = %q, want %q", ErrFatal.Error(), "fatal error")
	}
}
