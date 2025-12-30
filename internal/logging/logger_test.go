package logging

import (
	"bytes"
	"strings"
	"testing"
)

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

			logger.Error("error message")
			logger.Warn("warn message")
			logger.Info("info message")
			logger.Debug("debug message")

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

func TestDefaultLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelError)

	// Should not log info at error level
	logger.Info("should not appear")
	if strings.Contains(buf.String(), "should not appear") {
		t.Error("info logged at error level")
	}

	// Change to info level
	logger.SetLevel(LevelInfo)
	if logger.Level() != LevelInfo {
		t.Errorf("Level() = %v, want %v", logger.Level(), LevelInfo)
	}

	// Now should log info
	logger.Info("should appear")
	if !strings.Contains(buf.String(), "should appear") {
		t.Error("info not logged at info level")
	}
}

func TestDiscardLogger(t *testing.T) {
	// Just verify it doesn't panic
	Discard.Error("error")
	Discard.Errorf("error %d", 1)
	Discard.Warn("warn")
	Discard.Warnf("warn %d", 1)
	Discard.Info("info")
	Discard.Infof("info %d", 1)
	Discard.Debug("debug")
	Discard.Debugf("debug %d", 1)
}

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

func TestNamespaceConstants(t *testing.T) {
	// Verify namespace constants are defined with brackets
	namespaces := []string{NSFlush, NSCompact, NSWAL, NSManifest, NSRecovery, NSDB}
	for _, ns := range namespaces {
		if !strings.HasPrefix(ns, "[") || !strings.Contains(ns, "]") {
			t.Errorf("namespace %q should be in [name] format", ns)
		}
	}
}

func TestLogFormat_Standard(t *testing.T) {
	// Verify the log format follows standard: "TIMESTAMP LEVEL [component] message"
	// Example: 2025/12/30 18:45:13 INFO [flush] flush started
	var buf bytes.Buffer
	logger := NewLogger(&buf, LevelInfo)

	// Log a message with namespace prefix
	logger.Infof("%s%s", NSFlush, "flush started")

	output := buf.String()

	// Verify no global prefix (should start with timestamp)
	// Timestamp format: YYYY/MM/DD HH:MM:SS
	if strings.HasPrefix(output, "rockyardkv") {
		t.Errorf("output should NOT start with 'rockyardkv', got: %s", output)
	}

	// Verify level is present (without colon)
	if !strings.Contains(output, "INFO ") {
		t.Error("output should contain 'INFO '")
	}

	// Verify component namespace is present
	if !strings.Contains(output, "[flush]") {
		t.Error("output should contain '[flush]'")
	}

	// Verify message content
	if !strings.Contains(output, "flush started") {
		t.Error("output should contain 'flush started'")
	}

	t.Logf("Log format verified: %s", output)
}
