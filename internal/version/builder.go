// builder.go implements VersionBuilder for applying edits to versions.
//
// VersionBuilder efficiently applies a sequence of edits to a version
// without creating intermediate versions with full copies of state.
//
// Reference: RocksDB v10.7.5
//   - db/version_builder.h
//   - db/version_builder.cc
package version

import (
	"sort"

	"github.com/aalhour/rockyardkv/internal/manifest"
)

// Builder accumulates changes to a Version and produces a new Version.
//
// Usage:
//
//	builder := NewBuilder(vset, baseVersion)
//	builder.Apply(edit1)
//	builder.Apply(edit2)
//	newVersion := builder.SaveTo(vset)
type Builder struct {
	vset *VersionSet
	base *Version

	// Files to add, keyed by level
	addedFiles [MaxNumLevels]map[uint64]*manifest.FileMetaData

	// Files to delete, keyed by level
	deletedFiles [MaxNumLevels]map[uint64]struct{}
}

// NewBuilder creates a new Builder based on the given Version.
func NewBuilder(vset *VersionSet, base *Version) *Builder {
	b := &Builder{
		vset: vset,
		base: base,
	}
	for i := range MaxNumLevels {
		b.addedFiles[i] = make(map[uint64]*manifest.FileMetaData)
		b.deletedFiles[i] = make(map[uint64]struct{})
	}
	return b
}

// Apply applies a VersionEdit to the builder.
func (b *Builder) Apply(edit *manifest.VersionEdit) error {
	// Process deleted files
	for _, df := range edit.DeletedFiles {
		if df.Level >= 0 && df.Level < MaxNumLevels {
			b.deletedFiles[df.Level][df.FileNumber] = struct{}{}
			// Remove from added files if present (file was added then deleted in same batch)
			delete(b.addedFiles[df.Level], df.FileNumber)
		}
	}

	// Process new files
	for _, nf := range edit.NewFiles {
		if nf.Level >= 0 && nf.Level < MaxNumLevels {
			fileNum := nf.Meta.FD.GetNumber()
			// Remove from deleted files if present (file was deleted then re-added)
			delete(b.deletedFiles[nf.Level], fileNum)
			// Add to added files
			b.addedFiles[nf.Level][fileNum] = nf.Meta
		}
	}

	return nil
}

// SaveTo creates a new Version with all the accumulated changes.
func (b *Builder) SaveTo(vset *VersionSet) *Version {
	v := NewVersion(vset, vset.NextVersionNumber())

	for level := range MaxNumLevels {
		// Start with files from base version (if any)
		var files []*manifest.FileMetaData
		if b.base != nil {
			for _, f := range b.base.files[level] {
				fileNum := f.FD.GetNumber()
				// Skip if deleted
				if _, deleted := b.deletedFiles[level][fileNum]; deleted {
					continue
				}
				files = append(files, f)
			}
		}

		// Add new files
		for _, f := range b.addedFiles[level] {
			files = append(files, f)
		}

		// Sort files at this level
		if level == 0 {
			// L0 files may overlap, so sort by file number (oldest first)
			// When searching, we iterate in reverse (newest first)
			sortL0FilesByFileNumber(files)
		} else {
			// L1+ files are non-overlapping, sort by smallest key
			sortFilesBySmallestKey(files)
		}

		v.files[level] = files
	}

	return v
}

// sortL0FilesByFileNumber sorts L0 files by file number (oldest first).
// This allows us to iterate in reverse (newest first) during Get operations.
func sortL0FilesByFileNumber(files []*manifest.FileMetaData) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].FD.GetNumber() < files[j].FD.GetNumber()
	})
}

// sortFilesBySmallestKey sorts files by their smallest key.
func sortFilesBySmallestKey(files []*manifest.FileMetaData) {
	sort.Slice(files, func(i, j int) bool {
		return compareInternalKey(files[i].Smallest, files[j].Smallest) < 0
	})
}
