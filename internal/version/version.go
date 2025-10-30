// Package version manages database versions and the LSM-tree structure.
//
// A Version represents a snapshot of the database state at a point in time.
// It contains the list of SST files at each level and provides methods
// for querying and iterating over the data.
//
// A VersionSet manages all versions and the MANIFEST file. It provides
// the interface for logging and applying VersionEdits to create new versions.
//
// Reference: RocksDB v10.7.5
//   - db/version_set.h (Version class)
//   - db/version_set.cc
package version

import (
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// MaxNumLevels is the maximum number of levels in the LSM-tree.
const MaxNumLevels = 7

// Version represents a snapshot of the database state at a point in time.
// Each Version keeps track of the set of SST files at each level.
//
// Versions are immutable once created. New versions are created by applying
// VersionEdits to an existing version via the VersionBuilder.
//
// Versions use reference counting to manage their lifetime. When a Version
// is no longer needed, call Unref() to decrement the reference count.
type Version struct {
	// Files at each level, sorted by smallest key
	files [MaxNumLevels][]*manifest.FileMetaData

	// Reference count for this version
	refs int32

	// The VersionSet this version belongs to
	vset *VersionSet

	// Version number (for debugging)
	versionNumber uint64

	// Linked list pointers (for VersionSet's version list)
	prev *Version
	next *Version

	// Compaction score for each level (computed after version is finalized)
	compactionScore []float64 //nolint:unused // Reserved for future compaction scheduling
	compactionLevel []int     //nolint:unused // Reserved for future compaction scheduling
}

// NewVersion creates a new empty Version.
func NewVersion(vset *VersionSet, versionNumber uint64) *Version {
	return &Version{
		vset:          vset,
		versionNumber: versionNumber,
		refs:          0,
	}
}

// Ref increments the reference count.
func (v *Version) Ref() {
	atomic.AddInt32(&v.refs, 1)
}

// Unref decrements the reference count and deletes the version if it reaches 0.
func (v *Version) Unref() {
	if atomic.AddInt32(&v.refs, -1) == 0 {
		// Must hold the VersionSet's list lock when modifying the linked list
		// to prevent races with other Unref() calls and appendVersion().
		// We use a separate listMu to avoid deadlock with the main mu.
		if v.vset != nil {
			v.vset.listMu.Lock()
			defer v.vset.listMu.Unlock()
		}
		// Remove from linked list
		if v.prev != nil {
			v.prev.next = v.next
		}
		if v.next != nil {
			v.next.prev = v.prev
		}
		// Clear pointers to help GC
		v.prev = nil
		v.next = nil
		// The version is now unreachable and can be garbage collected
	}
}

// NumLevels returns the number of levels in use.
func (v *Version) NumLevels() int {
	return MaxNumLevels
}

// NumFiles returns the number of files at the given level.
func (v *Version) NumFiles(level int) int {
	if level < 0 || level >= MaxNumLevels {
		return 0
	}
	return len(v.files[level])
}

// Files returns the files at the given level.
func (v *Version) Files(level int) []*manifest.FileMetaData {
	if level < 0 || level >= MaxNumLevels {
		return nil
	}
	return v.files[level]
}

// TotalFiles returns the total number of files across all levels.
func (v *Version) TotalFiles() int {
	total := 0
	for level := range MaxNumLevels {
		total += len(v.files[level])
	}
	return total
}

// NumLevelBytes returns the total size of files at the given level.
func (v *Version) NumLevelBytes(level int) uint64 {
	if level < 0 || level >= MaxNumLevels {
		return 0
	}
	var size uint64
	for _, f := range v.files[level] {
		size += f.FD.FileSize
	}
	return size
}

// VersionNumber returns the version number for debugging.
func (v *Version) VersionNumber() uint64 {
	return v.versionNumber
}

// OverlappingInputs returns the files at the given level that overlap with
// the key range [begin, end]. If begin or end is nil, it means "no bound".
func (v *Version) OverlappingInputs(level int, begin, end []byte) []*manifest.FileMetaData {
	if level < 0 || level >= MaxNumLevels {
		return nil
	}

	var result []*manifest.FileMetaData
	for _, f := range v.files[level] {
		// Check if file overlaps with [begin, end]
		if begin != nil && len(f.Largest) > 0 {
			// Skip if file.largest < begin
			if compareInternalKey(f.Largest, begin) < 0 {
				continue
			}
		}
		if end != nil && len(f.Smallest) > 0 {
			// Skip if file.smallest > end
			if compareInternalKey(f.Smallest, end) > 0 {
				continue
			}
		}
		result = append(result, f)
	}
	return result
}

// compareInternalKey compares two internal keys.
// Returns negative if a < b, positive if a > b, zero if a == b.
func compareInternalKey(a, b []byte) int {
	// Internal key format: user_key + 8-byte trailer (seq + type)
	// Compare user keys first, then sequence numbers (descending)
	if len(a) < 8 || len(b) < 8 {
		// Malformed keys, compare as bytes
		return bytesCompare(a, b)
	}

	userKeyA := a[:len(a)-8]
	userKeyB := b[:len(b)-8]

	cmp := bytesCompare(userKeyA, userKeyB)
	if cmp != 0 {
		return cmp
	}

	// User keys are equal, compare by sequence number (descending)
	// The trailer contains the sequence number in the upper 56 bits
	// Higher sequence numbers should come first
	trailerA := decodeFixed64(a[len(a)-8:])
	trailerB := decodeFixed64(b[len(b)-8:])

	if trailerA > trailerB {
		return -1 // Higher sequence = earlier in sort order
	} else if trailerA < trailerB {
		return 1
	}
	return 0
}

// bytesCompare compares two byte slices lexicographically.
func bytesCompare(a, b []byte) int {
	minLen := min(len(b), len(a))
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// decodeFixed64 decodes a little-endian uint64.
func decodeFixed64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
