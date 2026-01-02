package rockyardkv

// event_listener_test.go implements tests for event listener.

import (
	"testing"
)

func TestNoOpEventListener(t *testing.T) {
	listener := &NoOpEventListener{}

	// All methods should be callable without panic
	listener.OnFlushCompleted(&FlushJobInfo{})
	listener.OnFlushBegin(&FlushJobInfo{})
	listener.OnCompactionCompleted(&CompactionJobInfo{})
	listener.OnCompactionBegin(&CompactionJobInfo{})
	listener.OnTableFileCreated(&TableFileCreationInfo{})
	listener.OnTableFileDeleted(&TableFileDeletionInfo{})
	listener.OnBackgroundError(&BackgroundErrorInfo{})
	listener.OnStallConditionsChanged(&WriteStallInfo{})
}

func TestCountingEventListener(t *testing.T) {
	listener := &CountingEventListener{}

	// Fire some events
	listener.OnFlushCompleted(&FlushJobInfo{})
	listener.OnFlushCompleted(&FlushJobInfo{})
	listener.OnCompactionCompleted(&CompactionJobInfo{})
	listener.OnTableFileCreated(&TableFileCreationInfo{})
	listener.OnTableFileCreated(&TableFileCreationInfo{})
	listener.OnTableFileCreated(&TableFileCreationInfo{})
	listener.OnTableFileDeleted(&TableFileDeletionInfo{})

	if listener.FlushCount != 2 {
		t.Errorf("FlushCount = %d, want 2", listener.FlushCount)
	}
	if listener.CompactionCount != 1 {
		t.Errorf("CompactionCount = %d, want 1", listener.CompactionCount)
	}
	if listener.FileCreateCount != 3 {
		t.Errorf("FileCreateCount = %d, want 3", listener.FileCreateCount)
	}
	if listener.FileDeleteCount != 1 {
		t.Errorf("FileDeleteCount = %d, want 1", listener.FileDeleteCount)
	}
}

func TestTimingEventListener(t *testing.T) {
	listener := &TimingEventListener{}

	listener.OnFlushCompleted(&FlushJobInfo{})
	listener.OnFlushCompleted(&FlushJobInfo{})
	listener.OnCompactionCompleted(&CompactionJobInfo{})

	if len(listener.FlushTimes) != 2 {
		t.Errorf("FlushTimes len = %d, want 2", len(listener.FlushTimes))
	}
	if len(listener.CompactionTimes) != 1 {
		t.Errorf("CompactionTimes len = %d, want 1", len(listener.CompactionTimes))
	}
}

func TestFlushReasonString(t *testing.T) {
	tests := []struct {
		reason FlushReason
		want   string
	}{
		{FlushReasonOthers, "Others"},
		{FlushReasonManualFlush, "ManualFlush"},
		{FlushReasonWriteBufferFull, "WriteBufferFull"},
		{FlushReason(100), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.reason.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

func TestCompactionReasonString(t *testing.T) {
	tests := []struct {
		reason CompactionReason
		want   string
	}{
		{CompactionReasonUnknown, "Unknown"},
		{CompactionReasonLevelL0FilesNum, "LevelL0FilesNum"},
		{CompactionReasonManualCompaction, "ManualCompaction"},
		{CompactionReason(100), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.reason.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

func TestTableFileCreationReasonString(t *testing.T) {
	tests := []struct {
		reason TableFileCreationReason
		want   string
	}{
		{TableFileCreationReasonFlush, "Flush"},
		{TableFileCreationReasonCompaction, "Compaction"},
		{TableFileCreationReasonRecovery, "Recovery"},
		{TableFileCreationReason(100), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.reason.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

func TestBackgroundErrorReasonString(t *testing.T) {
	tests := []struct {
		reason BackgroundErrorReason
		want   string
	}{
		{BackgroundErrorReasonFlush, "Flush"},
		{BackgroundErrorReasonCompaction, "Compaction"},
		{BackgroundErrorReasonManifestWrite, "ManifestWrite"},
		{BackgroundErrorReason(100), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.reason.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.reason, got, tt.want)
		}
	}
}

func TestWriteStallConditionString(t *testing.T) {
	tests := []struct {
		cond WriteStallCondition
		want string
	}{
		{WriteStallConditionNormal, "Normal"},
		{WriteStallConditionDelayed, "Delayed"},
		{WriteStallConditionStopped, "Stopped"},
		{WriteStallCondition(100), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.cond.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.cond, got, tt.want)
		}
	}
}

func TestFlushJobInfo(t *testing.T) {
	info := &FlushJobInfo{
		CFName:      "default",
		FilePath:    "/tmp/test.sst",
		JobID:       42,
		FlushReason: FlushReasonManualFlush,
	}

	if info.CFName != "default" {
		t.Errorf("CFName = %q, want 'default'", info.CFName)
	}
	if info.JobID != 42 {
		t.Errorf("JobID = %d, want 42", info.JobID)
	}
	if info.FlushReason != FlushReasonManualFlush {
		t.Errorf("FlushReason = %v, want ManualFlush", info.FlushReason)
	}
}

func TestCompactionJobInfo(t *testing.T) {
	info := &CompactionJobInfo{
		CFName:             "default",
		JobID:              123,
		BaseInputLevel:     0,
		OutputLevel:        1,
		NumInputRecords:    1000,
		NumOutputRecords:   800,
		IsManualCompaction: true,
		CompactionReason:   CompactionReasonManualCompaction,
	}

	if info.BaseInputLevel != 0 {
		t.Errorf("BaseInputLevel = %d, want 0", info.BaseInputLevel)
	}
	if info.OutputLevel != 1 {
		t.Errorf("OutputLevel = %d, want 1", info.OutputLevel)
	}
	if !info.IsManualCompaction {
		t.Error("IsManualCompaction should be true")
	}
}

func TestTableFileCreationInfo(t *testing.T) {
	info := &TableFileCreationInfo{
		DBName:   "/tmp/db",
		CFName:   "default",
		FilePath: "/tmp/db/000001.sst",
		FileSize: 1024 * 1024,
		JobID:    1,
		Reason:   TableFileCreationReasonFlush,
	}

	if info.FileSize != 1024*1024 {
		t.Errorf("FileSize = %d, want %d", info.FileSize, 1024*1024)
	}
	if info.Reason != TableFileCreationReasonFlush {
		t.Errorf("Reason = %v, want Flush", info.Reason)
	}
}

func TestWriteStallInfo(t *testing.T) {
	info := &WriteStallInfo{
		CFName:    "default",
		Condition: WriteStallConditionDelayed,
		Prev:      WriteStallConditionNormal,
	}

	if info.Condition != WriteStallConditionDelayed {
		t.Errorf("Condition = %v, want Delayed", info.Condition)
	}
	if info.Prev != WriteStallConditionNormal {
		t.Errorf("Prev = %v, want Normal", info.Prev)
	}
}

// TestEventListenerInterface verifies that all listener types implement the interface
func TestEventListenerInterface(t *testing.T) {
	var _ EventListener = (*NoOpEventListener)(nil)
	var _ EventListener = (*CountingEventListener)(nil)
	var _ EventListener = (*TimingEventListener)(nil)
}
