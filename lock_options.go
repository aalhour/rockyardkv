package rockyardkv

// lock_options.go re-exports lock manager options from internal/txn.
// The lock manager implementation is internal; only configuration is public.

import "github.com/aalhour/rockyardkv/internal/txn"

// Lock Manager errors - re-exported from internal/txn for public API.
var (
	// ErrLockTimeout is returned when a lock request times out.
	ErrLockTimeout = txn.ErrLockTimeout

	// ErrDeadlock is returned when a deadlock is detected.
	ErrDeadlock = txn.ErrDeadlock

	// ErrLockNotHeld is returned when trying to unlock a key not held by the transaction.
	ErrLockNotHeld = txn.ErrLockNotHeld
)

// LockType represents the type of lock.
type LockType = txn.LockType

// Lock type constants.
const (
	LockTypeShared    = txn.LockTypeShared
	LockTypeExclusive = txn.LockTypeExclusive
)

// LockManagerOptions configures the lock manager.
type LockManagerOptions = txn.LockManagerOptions

// DefaultLockManagerOptions returns default options.
var DefaultLockManagerOptions = txn.DefaultLockManagerOptions
