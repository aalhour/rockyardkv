package logging

// DiscardLogger is a no-op logger that discards all log messages.
// Use this for benchmarks or when logging is not desired.
//
// Note: Fatalf on DiscardLogger does nothing. This is intentional for testing.
// In production, use a real logger with a FatalHandler to catch fatal conditions.
type DiscardLogger struct{}

// Discard is the singleton discard logger.
var Discard Logger = &DiscardLogger{}

// Errorf implements Logger.
func (l *DiscardLogger) Errorf(format string, args ...any) {}

// Warnf implements Logger.
func (l *DiscardLogger) Warnf(format string, args ...any) {}

// Infof implements Logger.
func (l *DiscardLogger) Infof(format string, args ...any) {}

// Debugf implements Logger.
func (l *DiscardLogger) Debugf(format string, args ...any) {}

// Fatalf implements Logger.
// On DiscardLogger, this is a no-op. Use a real logger with FatalHandler in production.
func (l *DiscardLogger) Fatalf(format string, args ...any) {}
