package logging

// DiscardLogger is a no-op logger that discards all log messages.
// Use this for benchmarks or when logging is not desired.
type DiscardLogger struct{}

// Discard is the singleton discard logger.
var Discard Logger = &DiscardLogger{}

// Error implements Logger.
func (l *DiscardLogger) Error(msg string) {}

// Errorf implements Logger.
func (l *DiscardLogger) Errorf(format string, args ...any) {}

// Warn implements Logger.
func (l *DiscardLogger) Warn(msg string) {}

// Warnf implements Logger.
func (l *DiscardLogger) Warnf(format string, args ...any) {}

// Info implements Logger.
func (l *DiscardLogger) Info(msg string) {}

// Infof implements Logger.
func (l *DiscardLogger) Infof(format string, args ...any) {}

// Debug implements Logger.
func (l *DiscardLogger) Debug(msg string) {}

// Debugf implements Logger.
func (l *DiscardLogger) Debugf(format string, args ...any) {}
