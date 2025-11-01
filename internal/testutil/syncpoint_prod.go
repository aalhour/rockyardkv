// Package testutil provides test utilities for stress testing and verification.
//
// This file provides production-safe sync point hooks that have minimal overhead
// when sync points are not enabled.
//
// Sync points are named locations in the code where tests can:
// - Inject delays
// - Inject errors
// - Force specific orderings of concurrent operations
// - Verify that code paths are executed
//
// Reference: RocksDB v10.7.5
//   - test_util/sync_point.h
//   - test_util/sync_point_impl.cc
package testutil

// Common sync point names used throughout the codebase.
// These follow RocksDB's naming convention: "Component::Function:Location"
const (
	// Database lifecycle
	SPDBOpen               = "DBImpl::Open:Start"
	SPDBOpenComplete       = "DBImpl::Open:Complete"
	SPDBClose              = "DBImpl::Close:Start"
	SPDBCloseComplete      = "DBImpl::Close:Complete"
	SPDBRecoverStart       = "DBImpl::Recover:Start"
	SPDBRecoverComplete    = "DBImpl::Recover:Complete"
	SPDBRecoverWALStart    = "DBImpl::RecoverWAL:Start"
	SPDBRecoverWALComplete = "DBImpl::RecoverWAL:Complete"
	SPDBCreateStart        = "DBImpl::Create:Start"
	SPDBCreateComplete     = "DBImpl::Create:Complete"

	// Write path
	SPDBWrite                 = "DBImpl::Write:Start"
	SPDBWriteWAL              = "DBImpl::Write:BeforeWAL"
	SPDBWriteWALComplete      = "DBImpl::Write:AfterWAL"
	SPDBWriteMemtable         = "DBImpl::Write:BeforeMemtable"
	SPDBWriteMemtableComplete = "DBImpl::Write:AfterMemtable"
	SPDBWriteComplete         = "DBImpl::Write:Complete"

	// Read path
	SPDBGet         = "DBImpl::Get:Start"
	SPDBGetMemtable = "DBImpl::Get:SearchMemtable"
	SPDBGetSST      = "DBImpl::Get:SearchSST"
	SPDBGetComplete = "DBImpl::Get:Complete"

	// Flush path
	SPFlushStart            = "FlushJob::Run:Start"
	SPFlushWriteSST         = "FlushJob::Run:WriteSST"
	SPFlushSyncSST          = "FlushJob::Run:SyncSST"
	SPFlushComplete         = "FlushJob::Run:Complete"
	SPFlushApplyVersionEdit = "FlushJob::Run:ApplyVersionEdit"
	SPDoFlushStart          = "DBImpl::DoFlush:Start"
	SPDoFlushComplete       = "DBImpl::DoFlush:Complete"

	// Compaction path
	SPCompactionStart          = "CompactionJob::Run:Start"
	SPCompactionOpenInputs     = "CompactionJob::Run:OpenInputs"
	SPCompactionProcessing     = "CompactionJob::Run:Processing"
	SPCompactionWriteOutput    = "CompactionJob::Run:WriteOutput"
	SPCompactionFinishOutput   = "CompactionJob::Run:FinishOutput"
	SPCompactionComplete       = "CompactionJob::Run:Complete"
	SPBGCompactionStart        = "BackgroundWork::Compaction:Start"
	SPBGCompactionPickComplete = "BackgroundWork::Compaction:PickComplete"
	SPBGCompactionExecute      = "BackgroundWork::Compaction:Execute"
	SPBGCompactionComplete     = "BackgroundWork::Compaction:Complete"

	// Background work
	SPBGFlushStart    = "BackgroundWork::Flush:Start"
	SPBGFlushExecute  = "BackgroundWork::Flush:Execute"
	SPBGFlushComplete = "BackgroundWork::Flush:Complete"
	SPBGLoopIteration = "BackgroundWork::Loop:Iteration"

	// Version/Manifest
	SPVersionSetLogAndApply     = "VersionSet::LogAndApply:Start"
	SPVersionSetLogAndApplyDone = "VersionSet::LogAndApply:Complete"
	SPVersionSetRecover         = "VersionSet::Recover:Start"
	SPVersionSetRecoverDone     = "VersionSet::Recover:Complete"

	// WAL
	SPWALWrite         = "WAL::Write:Start"
	SPWALWriteComplete = "WAL::Write:Complete"
	SPWALSync          = "WAL::Sync:Start"
	SPWALSyncComplete  = "WAL::Sync:Complete"

	// Memtable
	SPMemtableAdd         = "Memtable::Add:Start"
	SPMemtableAddComplete = "Memtable::Add:Complete"
	SPMemtableGet         = "Memtable::Get:Start"
	SPMemtableGetComplete = "Memtable::Get:Complete"

	// Table/SST
	SPTableBuildStart   = "TableBuilder::Build:Start"
	SPTableBuildFinish  = "TableBuilder::Build:Finish"
	SPTableReadStart    = "TableReader::Open:Start"
	SPTableReadComplete = "TableReader::Open:Complete"

	// Iterator
	SPIteratorSeek = "Iterator::Seek:Start"
	SPIteratorNext = "Iterator::Next:Start"
	SPIteratorPrev = "Iterator::Prev:Start"

	// Transaction
	SPTxnBegin          = "Transaction::Begin:Start"
	SPTxnCommit         = "Transaction::Commit:Start"
	SPTxnCommitValidate = "Transaction::Commit:Validate"
	SPTxnCommitWrite    = "Transaction::Commit:Write"
	SPTxnCommitComplete = "Transaction::Commit:Complete"
	SPTxnRollback       = "Transaction::Rollback:Start"
)

// SyncPointEnabled controls whether sync points are processed.
// In production, this should be false for zero overhead.
// Tests set this to true and configure the global manager.
var SyncPointEnabled = false

// ProcessSyncPoint is the main entry point for sync point processing.
// It's designed to have minimal overhead when disabled.
//
// Usage in production code:
//
//	if testutil.SyncPointEnabled {
//	    testutil.ProcessSyncPoint("DBImpl::Write:Start")
//	}
//
// Or use the convenience function:
//
//	testutil.SP("DBImpl::Write:Start")
func ProcessSyncPoint(name string) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcess(name)
}

// SP is a convenience alias for ProcessSyncPoint.
// It's short to minimize code noise in production code.
func SP(name string) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcess(name)
}

// SPCallback processes a sync point with optional callback data.
func SPCallback(name string, data any) error {
	if !SyncPointEnabled {
		return nil
	}
	return SyncPointProcessWithData(name, data)
}

// EnableSyncPoints enables sync point processing globally.
// Call this at the start of tests that need sync points.
func EnableSyncPoints() *SyncPointManager {
	mgr := NewSyncPointManager()
	mgr.EnableProcessing()
	mgr.SetGlobal()
	SyncPointEnabled = true
	return mgr
}

// DisableSyncPoints disables sync point processing.
// Call this to restore normal operation after tests.
func DisableSyncPoints() {
	SyncPointEnabled = false
	ClearGlobal()
}
