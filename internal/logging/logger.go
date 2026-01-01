// Package logging provides the logging interface and default implementations for RockyardKV.
//
// Design: Five-level interface (Error, Warn, Info, Debug, Fatal) inspired by Badger, Pebble, and RocksDB.
// Users can wrap their own structured loggers (slog, zap) if needed.
//
// Fatalf behavior (RocksDB-style): Logs at FATAL level and calls the configured FatalHandler.
// The default FatalHandler is a no-op, but DB wires it to set background error (stop writes).
// Unlike Pebble, Fatalf does NOT call os.Exit(1) by default.
//
// Log format: YYYY/MM/DD HH:MM:SS LEVEL [component] message
//
// Example: 2025/12/30 18:45:13 INFO [flush] flush started
//
// Component namespace prefixes are used for filtering:
//   - [flush]    — flush operations
//   - [compact]  — compaction operations
//   - [wal]      — WAL operations
//   - [manifest] — MANIFEST operations
//   - [recovery] — recovery operations
//   - [db]       — general database operations
//
// Reference: RocksDB v10.7.5 include/rocksdb/env.h (Logger class)
package logging

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"sync/atomic"
)

// ErrFatal is the sentinel error wrapped by fatal conditions.
// Use errors.Is(err, ErrFatal) to detect fatal errors in returned errors.
var ErrFatal = errors.New("fatal error")

// FatalHandler is called when Fatalf is invoked.
// The handler receives the formatted fatal message and should transition
// the system to a stopped state (e.g., reject writes, set background error).
//
// Contract: FatalHandler must be safe for concurrent use.
// Contract: FatalHandler must not call Fatalf (avoid infinite recursion).
type FatalHandler func(msg string)

// Level represents the logging level.
type Level int

const (
	// LevelError logs only errors.
	LevelError Level = iota
	// LevelWarn logs warnings and errors.
	LevelWarn
	// LevelInfo logs info, warnings, and errors.
	LevelInfo
	// LevelDebug logs everything including debug messages.
	LevelDebug
)

// String returns the string representation of the level.
func (l Level) String() string {
	switch l {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// Logger defines the interface for database logging.
//
// Concurrency: DefaultLogger and Discard are safe for concurrent use.
// User-provided Logger implementations MUST be safe for concurrent use,
// as logging may occur from multiple goroutines simultaneously.
//
// Fatalf contract (RocksDB-style):
//   - Logs the message at FATAL level
//   - Calls the configured FatalHandler to transition the system to a stopped state
//   - Does NOT exit the process (unlike Pebble)
//   - After Fatalf, subsequent writes should be rejected by the DB
type Logger interface {
	// Errorf logs a formatted error message.
	Errorf(format string, args ...any)

	// Warnf logs a formatted warning message.
	Warnf(format string, args ...any)

	// Infof logs a formatted informational message.
	Infof(format string, args ...any)

	// Debugf logs a formatted debug message.
	Debugf(format string, args ...any)

	// Fatalf logs a fatal error and triggers the fatal handler.
	// After Fatalf is called, the DB transitions to a stopped state:
	// writes are rejected, reads may continue.
	Fatalf(format string, args ...any)
}

// DefaultLogger is the default logger that writes to a specified output.
// It is stateless and safe for concurrent use (log.Logger is thread-safe).
// Level is read-only after construction — create a new logger to change level.
type DefaultLogger struct {
	logger       *log.Logger
	level        Level
	fatalHandler atomic.Pointer[FatalHandler]
}

// NewDefaultLogger creates a new default logger with the specified level.
// It writes to stderr.
// Output format: YYYY/MM/DD HH:MM:SS LEVEL [component] message
func NewDefaultLogger(level Level) *DefaultLogger {
	return &DefaultLogger{
		logger: log.New(os.Stderr, "", log.LstdFlags),
		level:  level,
	}
}

// NewLogger creates a new logger with the specified output and level.
// Output format: YYYY/MM/DD HH:MM:SS LEVEL [component] message
func NewLogger(w io.Writer, level Level) *DefaultLogger {
	return &DefaultLogger{
		logger: log.New(w, "", log.LstdFlags),
		level:  level,
	}
}

// SetFatalHandler sets the handler called when Fatalf is invoked.
// The handler should transition the system to a stopped state.
// This is typically wired by the DB to set its background error.
func (l *DefaultLogger) SetFatalHandler(h FatalHandler) {
	l.fatalHandler.Store(&h)
}

// Level returns the logging level.
func (l *DefaultLogger) Level() Level {
	return l.level
}

// Errorf logs a formatted error message.
func (l *DefaultLogger) Errorf(format string, args ...any) {
	if l.level >= LevelError {
		_ = l.logger.Output(2, "ERROR "+fmt.Sprintf(format, args...))
	}
}

// Warnf logs a formatted warning message.
func (l *DefaultLogger) Warnf(format string, args ...any) {
	if l.level >= LevelWarn {
		_ = l.logger.Output(2, "WARN "+fmt.Sprintf(format, args...))
	}
}

// Infof logs a formatted informational message.
func (l *DefaultLogger) Infof(format string, args ...any) {
	if l.level >= LevelInfo {
		_ = l.logger.Output(2, "INFO "+fmt.Sprintf(format, args...))
	}
}

// Debugf logs a formatted debug message.
func (l *DefaultLogger) Debugf(format string, args ...any) {
	if l.level >= LevelDebug {
		_ = l.logger.Output(2, "DEBUG "+fmt.Sprintf(format, args...))
	}
}

// Fatalf logs a fatal error and triggers the fatal handler.
// After Fatalf is called, the DB transitions to a stopped state:
// writes are rejected, reads may continue.
func (l *DefaultLogger) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	// Always log fatal messages (no level filtering for fatal)
	_ = l.logger.Output(2, "FATAL "+msg)

	// Call the fatal handler if set
	if h := l.fatalHandler.Load(); h != nil {
		(*h)(msg)
	}
}

// Namespace prefixes for log messages.
// Use these with fmt.Sprintf to add namespace context.
const (
	// NSFlush is the namespace for flush operations.
	NSFlush = "[flush] "
	// NSCompact is the namespace for compaction operations.
	NSCompact = "[compact] "
	// NSWAL is the namespace for WAL operations.
	NSWAL = "[wal] "
	// NSManifest is the namespace for MANIFEST operations.
	NSManifest = "[manifest] "
	// NSRecovery is the namespace for recovery operations.
	NSRecovery = "[recovery] "
	// NSDB is the namespace for general database operations.
	NSDB = "[db] "
	// NSCheckpoint is the namespace for checkpoint operations.
	NSCheckpoint = "[checkpoint] "
	// NSBackup is the namespace for backup/restore operations.
	NSBackup = "[backup] "
	// NSIngest is the namespace for SST file ingestion operations.
	NSIngest = "[ingest] "
	// NSTxn is the namespace for transaction operations.
	NSTxn = "[txn] "
)

// IsNil returns true if the logger is nil or a typed-nil.
// A typed-nil occurs when a nil pointer is assigned to an interface:
//
//	var l *MyLogger = nil
//	opts.Logger = l  // Interface is not nil, but underlying pointer is
//
// Calling methods on a typed-nil panics, so this function detects both cases.
func IsNil(l Logger) bool {
	if l == nil {
		return true
	}
	v := reflect.ValueOf(l)
	// Check if the underlying value is a nil pointer
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// OrDefault returns the provided logger if it is valid (non-nil and not typed-nil),
// otherwise returns a default WARN-level logger.
// This ensures db.logger is never nil after Open().
func OrDefault(l Logger) Logger {
	if IsNil(l) {
		return NewDefaultLogger(LevelWarn)
	}
	return l
}
