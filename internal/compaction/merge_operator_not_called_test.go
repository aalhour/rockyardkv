package compaction

import (
	"testing"

	"github.com/aalhour/rockyardkv/internal/manifest"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// panicMergeOperator is a merge operator that panics if invoked.
// This is used to ensure callers don't accidentally enable compaction-time merges.
type panicMergeOperator struct{}

func (p *panicMergeOperator) FullMerge(_ []byte, _ []byte, _ [][]byte) ([]byte, bool) {
	panic("merge operator must not be called during this test") //nolint:forbidigo
}

// TestCompactionJob_DoesNotRequireMergeOperator asserts that compaction can run
// without invoking a merge operator. Compaction-time merge is an optimization,
// not a correctness requirement.
func TestCompactionJob_DoesNotRequireMergeOperator(t *testing.T) {
	dir := t.TempDir()
	fs := vfs.Default()
	cache := table.NewTableCache(fs, table.TableCacheOptions{MaxOpenFiles: 10})
	defer cache.Close()

	// Create two small SSTs with disjoint keys.
	createTestSST(t, dir, 1, []string{"a", "c", "e"})
	createTestSST(t, dir, 2, []string{"b", "d", "f"})

	meta1 := makeTestFileMetaData(1, 1000, makeInternalKey("a", 100, 1), makeInternalKey("e", 100, 1))
	meta2 := makeTestFileMetaData(2, 1000, makeInternalKey("b", 100, 1), makeInternalKey("f", 100, 1))

	c := NewCompaction([]*CompactionInputFiles{
		{Level: 0, Files: []*manifest.FileMetaData{meta1, meta2}},
	}, 1)

	fileNum := uint64(100)
	job := NewCompactionJob(c, dir, fs, cache, func() uint64 {
		fileNum++
		return fileNum
	})

	// Do NOT set a merge operator on the job. Compaction should still succeed.
	if _, err := job.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}


