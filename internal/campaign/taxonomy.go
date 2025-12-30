// Package campaign implements the Jepsen-style campaign runner for RockyardKV.
//
// This package provides:
//   - Taxonomy types for campaign configuration (tiers, tools, fault models)
//   - Instance definitions for the campaign matrix
//   - Oracle gating and tool execution
//   - Artifact bundle writing and failure fingerprinting
//   - Campaign execution and reporting
package campaign

// Tier represents the test intensity level.
// Each tier has different duration, concurrency, and thoroughness settings.
type Tier string

const (
	// TierQuick is for local development and CI on pull requests.
	// Duration: ~2-5 minutes per instance.
	TierQuick Tier = "quick"

	// TierNightly is for nightly pipelines that run for hours.
	// More thorough, longer duration, higher stress.
	TierNightly Tier = "nightly"
)

// Tool represents the test binary to execute.
type Tool string

const (
	// ToolStress runs the stresstest binary for concurrent workloads.
	ToolStress Tool = "stresstest"

	// ToolCrash runs the crashtest binary for crash recovery testing.
	ToolCrash Tool = "crashtest"

	// ToolAdversarial runs the adversarialtest binary for corruption attacks.
	ToolAdversarial Tool = "adversarialtest"

	// ToolGolden runs the goldentest suite for C++ compatibility.
	ToolGolden Tool = "goldentest"
)

// FaultKind represents the type of fault to inject.
type FaultKind string

const (
	// FaultNone means no fault injection.
	FaultNone FaultKind = "none"

	// FaultRead injects read errors.
	FaultRead FaultKind = "read"

	// FaultWrite injects write errors.
	FaultWrite FaultKind = "write"

	// FaultSync injects sync/fsync errors.
	FaultSync FaultKind = "sync"

	// FaultCrash injects process crashes.
	FaultCrash FaultKind = "crash"

	// FaultCorrupt injects data corruption.
	FaultCorrupt FaultKind = "corrupt"
)

// FaultScope represents where faults are injected.
type FaultScope string

const (
	// ScopeWorker injects faults in worker goroutines.
	ScopeWorker FaultScope = "worker"

	// ScopeFlusher injects faults in the flusher goroutine.
	ScopeFlusher FaultScope = "flusher"

	// ScopeReopener injects faults during DB reopen.
	ScopeReopener FaultScope = "reopener"

	// ScopeGlobal injects faults globally (all goroutines).
	ScopeGlobal FaultScope = "global"
)

// FaultErrorType represents the error type for fault injection.
type FaultErrorType string

const (
	// ErrorTypeStatus returns a status error (retryable).
	ErrorTypeStatus FaultErrorType = "status"

	// ErrorTypeCorruption returns a corruption error (fatal).
	ErrorTypeCorruption FaultErrorType = "corruption"

	// ErrorTypeTruncated returns a truncated error.
	ErrorTypeTruncated FaultErrorType = "truncated"
)

// FaultModel describes the fault injection configuration.
type FaultModel struct {
	// Kind is the type of fault to inject.
	Kind FaultKind

	// ErrorType is the error type for the fault (status, corruption, truncated).
	ErrorType FaultErrorType

	// OneIn is the probability denominator (e.g., 7 means 1/7 chance).
	OneIn int

	// Scope is where faults are injected.
	Scope FaultScope
}

// String returns a human-readable description of the fault model.
func (f FaultModel) String() string {
	if f.Kind == FaultNone {
		return "none"
	}
	return string(f.Kind) + "/" + string(f.ErrorType) + "/1in" + itoa(f.OneIn) + "/" + string(f.Scope)
}

// itoa is a simple int to string conversion without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	if i < 0 {
		return "-" + itoa(-i)
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
