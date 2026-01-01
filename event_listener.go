package rockyardkv

// event_listener.go implements the EventListener interface for receiving database events.
// Reference: RocksDB v10.7.5 include/rocksdb/listener.h


import (
	"sync"
	"time"
)

// FlushJobInfo contains information about a flush job.
type FlushJobInfo struct {
	// CFName is the column family name.
	CFName string
	// FilePath is the path to the output SST file.
	FilePath string
	// ThreadID is the ID of the thread that performed the flush.
	ThreadID uint64
	// JobID is the unique identifier for this flush job.
	JobID int
	// TriggeredWritesSlowdown indicates if flush was triggered by write slowdown.
	TriggeredWritesSlowdown bool
	// TriggeredWritesStop indicates if flush was triggered by write stop.
	TriggeredWritesStop bool
	// SmallestSeqno is the smallest sequence number in the flushed file.
	SmallestSeqno uint64
	// LargestSeqno is the largest sequence number in the flushed file.
	LargestSeqno uint64
	// TableProperties contains properties of the flushed SST file.
	TableProperties map[string]string
	// FlushReason is the reason why the flush was triggered.
	FlushReason FlushReason
}

// FlushReason describes why a flush was triggered.
type FlushReason int

const (
	// FlushReasonOthers is for unspecified reasons.
	FlushReasonOthers FlushReason = iota
	// FlushReasonGetLiveFiles is for GetLiveFiles().
	FlushReasonGetLiveFiles
	// FlushReasonShutDown is for database shutdown.
	FlushReasonShutDown
	// FlushReasonExternalFileIngestion is for external file ingestion.
	FlushReasonExternalFileIngestion
	// FlushReasonManualFlush is for manual flush via Flush().
	FlushReasonManualFlush
	// FlushReasonWriteBufferFull is when write buffer is full.
	FlushReasonWriteBufferFull
	// FlushReasonWriteBufferManager is for write buffer manager.
	FlushReasonWriteBufferManager
	// FlushReasonWALFileFull is when WAL file is full.
	FlushReasonWALFileFull
	// FlushReasonManualCompaction is for manual compaction.
	FlushReasonManualCompaction
	// FlushReasonAutoCompaction is for automatic compaction.
	FlushReasonAutoCompaction
)

// String returns the string representation of the flush reason.
func (r FlushReason) String() string {
	names := []string{
		"Others",
		"GetLiveFiles",
		"ShutDown",
		"ExternalFileIngestion",
		"ManualFlush",
		"WriteBufferFull",
		"WriteBufferManager",
		"WALFileFull",
		"ManualCompaction",
		"AutoCompaction",
	}
	if int(r) < len(names) {
		return names[r]
	}
	return "Unknown"
}

// CompactionJobInfo contains information about a compaction job.
type CompactionJobInfo struct {
	// CFName is the column family name.
	CFName string
	// Status is the status of the compaction (nil for success).
	Status error
	// ThreadID is the ID of the thread that performed the compaction.
	ThreadID uint64
	// JobID is the unique identifier for this compaction job.
	JobID int
	// BaseInputLevel is the lowest input level.
	BaseInputLevel int
	// OutputLevel is the output level.
	OutputLevel int
	// InputFiles is the list of input file paths.
	InputFiles []string
	// OutputFiles is the list of output file paths.
	OutputFiles []string
	// NumInputRecords is the number of records in input files.
	NumInputRecords uint64
	// NumOutputRecords is the number of records in output files.
	NumOutputRecords uint64
	// NumCorruptKeys is the number of corrupt keys encountered.
	NumCorruptKeys uint64
	// TotalInputBytes is the total bytes read.
	TotalInputBytes uint64
	// TotalOutputBytes is the total bytes written.
	TotalOutputBytes uint64
	// NumInputFiles is the number of input files.
	NumInputFiles int
	// NumOutputFiles is the number of output files.
	NumOutputFiles int
	// IsManualCompaction indicates if this was a manual compaction.
	IsManualCompaction bool
	// CompactionReason is the reason for the compaction.
	CompactionReason CompactionReason
}

// CompactionReason describes why a compaction was triggered.
type CompactionReason int

const (
	// CompactionReasonUnknown is for unknown reasons.
	CompactionReasonUnknown CompactionReason = iota
	// CompactionReasonLevelL0FilesNum is for L0 file count trigger.
	CompactionReasonLevelL0FilesNum
	// CompactionReasonLevelMaxLevelSize is for level size trigger.
	CompactionReasonLevelMaxLevelSize
	// CompactionReasonManualCompaction is for manual compaction.
	CompactionReasonManualCompaction
	// CompactionReasonFilesMarkedForCompaction is for marked files.
	CompactionReasonFilesMarkedForCompaction
	// CompactionReasonBottomMostLevel is for bottom-most level compaction.
	CompactionReasonBottomMostLevel
	// CompactionReasonTTL is for TTL-based compaction.
	CompactionReasonTTL
	// CompactionReasonFlush is for post-flush compaction.
	CompactionReasonFlush
	// CompactionReasonExternalSSTIngestion is for external SST ingestion.
	CompactionReasonExternalSSTIngestion
	// CompactionReasonPeriodicCompaction is for periodic compaction.
	CompactionReasonPeriodicCompaction
)

// String returns the string representation of the compaction reason.
func (r CompactionReason) String() string {
	names := []string{
		"Unknown",
		"LevelL0FilesNum",
		"LevelMaxLevelSize",
		"ManualCompaction",
		"FilesMarkedForCompaction",
		"BottomMostLevel",
		"TTL",
		"Flush",
		"ExternalSSTIngestion",
		"PeriodicCompaction",
	}
	if int(r) < len(names) {
		return names[r]
	}
	return "Unknown"
}

// TableFileCreationInfo contains information about a table file creation.
type TableFileCreationInfo struct {
	// DBName is the database name.
	DBName string
	// CFName is the column family name.
	CFName string
	// FilePath is the path to the created file.
	FilePath string
	// FileSize is the size of the file in bytes.
	FileSize uint64
	// JobID is the ID of the job that created the file.
	JobID int
	// Reason is why the file was created.
	Reason TableFileCreationReason
	// Status is the creation status (nil for success).
	Status error
	// TableProperties contains properties of the created SST file.
	TableProperties map[string]string
}

// TableFileCreationReason describes why a table file was created.
type TableFileCreationReason int

const (
	// TableFileCreationReasonFlush is for flush.
	TableFileCreationReasonFlush TableFileCreationReason = iota
	// TableFileCreationReasonCompaction is for compaction.
	TableFileCreationReasonCompaction
	// TableFileCreationReasonRecovery is for recovery.
	TableFileCreationReasonRecovery
	// TableFileCreationReasonMisc is for miscellaneous reasons.
	TableFileCreationReasonMisc
)

// String returns the string representation of the table file creation reason.
func (r TableFileCreationReason) String() string {
	names := []string{"Flush", "Compaction", "Recovery", "Misc"}
	if int(r) < len(names) {
		return names[r]
	}
	return "Unknown"
}

// TableFileDeletionInfo contains information about a table file deletion.
type TableFileDeletionInfo struct {
	// DBName is the database name.
	DBName string
	// FilePath is the path to the deleted file.
	FilePath string
	// JobID is the ID of the job that deleted the file.
	JobID int
	// Status is the deletion status (nil for success).
	Status error
}

// BackgroundErrorInfo contains information about a background error.
type BackgroundErrorInfo struct {
	// Reason is the reason for the error.
	Reason BackgroundErrorReason
	// Status is the error that occurred.
	Status error
}

// BackgroundErrorReason describes the reason for a background error.
type BackgroundErrorReason int

const (
	// BackgroundErrorReasonFlush is for flush errors.
	BackgroundErrorReasonFlush BackgroundErrorReason = iota
	// BackgroundErrorReasonCompaction is for compaction errors.
	BackgroundErrorReasonCompaction
	// BackgroundErrorReasonWriteCallback is for write callback errors.
	BackgroundErrorReasonWriteCallback
	// BackgroundErrorReasonMemTable is for memtable errors.
	BackgroundErrorReasonMemTable
	// BackgroundErrorReasonManifestWrite is for manifest write errors.
	BackgroundErrorReasonManifestWrite
	// BackgroundErrorReasonFlushNoWAL is for flush no-WAL errors.
	BackgroundErrorReasonFlushNoWAL
	// BackgroundErrorReasonManifestWriteNoWAL is for manifest write no-WAL errors.
	BackgroundErrorReasonManifestWriteNoWAL
)

// String returns the string representation of the background error reason.
func (r BackgroundErrorReason) String() string {
	names := []string{
		"Flush",
		"Compaction",
		"WriteCallback",
		"MemTable",
		"ManifestWrite",
		"FlushNoWAL",
		"ManifestWriteNoWAL",
	}
	if int(r) < len(names) {
		return names[r]
	}
	return "Unknown"
}

// WriteStallInfo contains information about a write stall.
type WriteStallInfo struct {
	// CFName is the column family name.
	CFName string
	// Condition is the stall condition.
	Condition WriteStallCondition
	// Prev is the previous stall condition.
	Prev WriteStallCondition
}

// WriteStallCondition describes the write stall condition.
type WriteStallCondition int

const (
	// WriteStallConditionNormal means no stall.
	WriteStallConditionNormal WriteStallCondition = iota
	// WriteStallConditionDelayed means writes are delayed.
	WriteStallConditionDelayed
	// WriteStallConditionStopped means writes are stopped.
	WriteStallConditionStopped
)

// String returns the string representation of the write stall condition.
func (c WriteStallCondition) String() string {
	names := []string{"Normal", "Delayed", "Stopped"}
	if int(c) < len(names) {
		return names[c]
	}
	return "Unknown"
}

// EventListener receives notifications about database events.
// All callbacks should be thread-safe and non-blocking.
type EventListener interface {
	// OnFlushCompleted is called when a flush job completes.
	OnFlushCompleted(info *FlushJobInfo)

	// OnFlushBegin is called when a flush job begins.
	OnFlushBegin(info *FlushJobInfo)

	// OnCompactionCompleted is called when a compaction job completes.
	OnCompactionCompleted(info *CompactionJobInfo)

	// OnCompactionBegin is called when a compaction job begins.
	OnCompactionBegin(info *CompactionJobInfo)

	// OnTableFileCreated is called when a table file is created.
	OnTableFileCreated(info *TableFileCreationInfo)

	// OnTableFileDeleted is called when a table file is deleted.
	OnTableFileDeleted(info *TableFileDeletionInfo)

	// OnBackgroundError is called when a background error occurs.
	OnBackgroundError(info *BackgroundErrorInfo)

	// OnStallConditionsChanged is called when stall conditions change.
	OnStallConditionsChanged(info *WriteStallInfo)
}

// NoOpEventListener is a default implementation that does nothing.
// Embed this in your listener if you only want to handle specific events.
type NoOpEventListener struct{}

func (l *NoOpEventListener) OnFlushCompleted(info *FlushJobInfo)            {}
func (l *NoOpEventListener) OnFlushBegin(info *FlushJobInfo)                {}
func (l *NoOpEventListener) OnCompactionCompleted(info *CompactionJobInfo)  {}
func (l *NoOpEventListener) OnCompactionBegin(info *CompactionJobInfo)      {}
func (l *NoOpEventListener) OnTableFileCreated(info *TableFileCreationInfo) {}
func (l *NoOpEventListener) OnTableFileDeleted(info *TableFileDeletionInfo) {}
func (l *NoOpEventListener) OnBackgroundError(info *BackgroundErrorInfo)    {}
func (l *NoOpEventListener) OnStallConditionsChanged(info *WriteStallInfo)  {}

// CountingEventListener counts events for testing purposes.
type CountingEventListener struct {
	NoOpEventListener
	FlushCount      int
	CompactionCount int
	FileCreateCount int
	FileDeleteCount int
	ErrorCount      int
	StallCount      int
	mu              sync.Mutex
}

func (l *CountingEventListener) OnFlushCompleted(info *FlushJobInfo) {
	l.mu.Lock()
	l.FlushCount++
	l.mu.Unlock()
}

func (l *CountingEventListener) OnCompactionCompleted(info *CompactionJobInfo) {
	l.mu.Lock()
	l.CompactionCount++
	l.mu.Unlock()
}

func (l *CountingEventListener) OnTableFileCreated(info *TableFileCreationInfo) {
	l.mu.Lock()
	l.FileCreateCount++
	l.mu.Unlock()
}

func (l *CountingEventListener) OnTableFileDeleted(info *TableFileDeletionInfo) {
	l.mu.Lock()
	l.FileDeleteCount++
	l.mu.Unlock()
}

func (l *CountingEventListener) OnBackgroundError(info *BackgroundErrorInfo) {
	l.mu.Lock()
	l.ErrorCount++
	l.mu.Unlock()
}

func (l *CountingEventListener) OnStallConditionsChanged(info *WriteStallInfo) {
	l.mu.Lock()
	l.StallCount++
	l.mu.Unlock()
}

// TimingEventListener records timing information for testing.
type TimingEventListener struct {
	NoOpEventListener
	FlushTimes      []time.Time
	CompactionTimes []time.Time
	mu              sync.Mutex
}

func (l *TimingEventListener) OnFlushCompleted(info *FlushJobInfo) {
	l.mu.Lock()
	l.FlushTimes = append(l.FlushTimes, time.Now())
	l.mu.Unlock()
}

func (l *TimingEventListener) OnCompactionCompleted(info *CompactionJobInfo) {
	l.mu.Lock()
	l.CompactionTimes = append(l.CompactionTimes, time.Now())
	l.mu.Unlock()
}
