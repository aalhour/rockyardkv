// Package logging provides the logging interface and default implementations for RockyardKV.
//
// Design: Simple four-level interface (Error, Warn, Info, Debug) inspired by Badger and Pebble.
// Users can wrap their own structured loggers (slog, zap) if needed.
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
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

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
// All methods are safe for concurrent use.
type Logger interface {
	// Error logs an error message.
	Error(msg string)
	// Errorf logs a formatted error message.
	Errorf(format string, args ...any)

	// Warn logs a warning message.
	Warn(msg string)
	// Warnf logs a formatted warning message.
	Warnf(format string, args ...any)

	// Info logs an informational message.
	Info(msg string)
	// Infof logs a formatted informational message.
	Infof(format string, args ...any)

	// Debug logs a debug message.
	Debug(msg string)
	// Debugf logs a formatted debug message.
	Debugf(format string, args ...any)
}

// DefaultLogger is the default logger that writes to a specified output.
type DefaultLogger struct {
	mu     sync.Mutex
	logger *log.Logger
	level  Level
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

// SetLevel sets the logging level.
func (l *DefaultLogger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// Level returns the current logging level.
func (l *DefaultLogger) Level() Level {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Error logs an error message.
func (l *DefaultLogger) Error(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelError {
		l.logger.Println("ERROR", msg)
	}
}

// Errorf logs a formatted error message.
func (l *DefaultLogger) Errorf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelError {
		l.logger.Println("ERROR", fmt.Sprintf(format, args...))
	}
}

// Warn logs a warning message.
func (l *DefaultLogger) Warn(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelWarn {
		l.logger.Println("WARN", msg)
	}
}

// Warnf logs a formatted warning message.
func (l *DefaultLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelWarn {
		l.logger.Println("WARN", fmt.Sprintf(format, args...))
	}
}

// Info logs an informational message.
func (l *DefaultLogger) Info(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelInfo {
		l.logger.Println("INFO", msg)
	}
}

// Infof logs a formatted informational message.
func (l *DefaultLogger) Infof(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelInfo {
		l.logger.Println("INFO", fmt.Sprintf(format, args...))
	}
}

// Debug logs a debug message.
func (l *DefaultLogger) Debug(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelDebug {
		l.logger.Println("DEBUG", msg)
	}
}

// Debugf logs a formatted debug message.
func (l *DefaultLogger) Debugf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.level >= LevelDebug {
		l.logger.Println("DEBUG", fmt.Sprintf(format, args...))
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
