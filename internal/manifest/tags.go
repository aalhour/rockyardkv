// Package manifest provides encoding and decoding for RocksDB MANIFEST files.
//
// The MANIFEST file contains a sequence of VersionEdit records that describe
// changes to the database state. Each record is prefixed with tags that
// identify the type of change being made.
//
// Reference: RocksDB v10.7.5
//   - db/version_edit.h (Tag enum)
//   - db/version_edit.cc
package manifest

// Tag represents a serialized VersionEdit field tag.
// These numbers are written to disk and MUST NOT change.
// The number should be forward compatible so users can down-grade RocksDB safely.
// A future Tag is ignored by doing '&' between Tag and kTagSafeIgnoreMask field.
type Tag uint32

const (
	// Core tags (from original LevelDB)
	TagComparator     Tag = 1
	TagLogNumber      Tag = 2
	TagNextFileNumber Tag = 3
	TagLastSequence   Tag = 4
	TagCompactCursor  Tag = 5
	TagDeletedFile    Tag = 6
	TagNewFile        Tag = 7
	// Tag 8 was used for large value refs (deprecated)
	TagPrevLogNumber      Tag = 9
	TagMinLogNumberToKeep Tag = 10

	// Extended tags (RocksDB specific)
	TagNewFile2         Tag = 100
	TagNewFile3         Tag = 102
	TagNewFile4         Tag = 103 // 4th (the latest) format version of adding files
	TagColumnFamily     Tag = 200 // specify column family for version edit
	TagColumnFamilyAdd  Tag = 201
	TagColumnFamilyDrop Tag = 202
	TagMaxColumnFamily  Tag = 203

	TagInAtomicGroup Tag = 300

	TagBlobFileAddition Tag = 400
	TagBlobFileGarbage  Tag = 401

	// Mask for an unidentified tag from the future which can be safely ignored.
	TagSafeIgnoreMask Tag = 1 << 13

	// Forward compatible (aka ignorable) records - these have bit 13 set
	TagDBID                         Tag = TagSafeIgnoreMask | 1
	TagBlobFileAdditionDeprecated   Tag = TagSafeIgnoreMask | 2
	TagBlobFileGarbageDeprecated    Tag = TagSafeIgnoreMask | 3
	TagWalAddition                  Tag = TagSafeIgnoreMask | 4
	TagWalDeletion                  Tag = TagSafeIgnoreMask | 5
	TagFullHistoryTSLow             Tag = TagSafeIgnoreMask | 6
	TagWalAddition2                 Tag = TagSafeIgnoreMask | 7
	TagWalDeletion2                 Tag = TagSafeIgnoreMask | 8
	TagPersistUserDefinedTimestamps Tag = TagSafeIgnoreMask | 9
	// NOTE: kSubcompactionProgress (TagSafeIgnoreMask | 10) was added AFTER v10.7.5
	// and is intentionally not included here. We are pinned to v10.7.5.
)

// IsSafeToIgnore returns true if the tag can be safely ignored when unknown.
func (t Tag) IsSafeToIgnore() bool {
	return t&TagSafeIgnoreMask != 0
}

// NewFileCustomTag represents custom tags within a NewFile4 record.
// These provide additional metadata about SST files.
type NewFileCustomTag uint32

const (
	// NewFileTagTerminate marks the end of custom fields.
	NewFileTagTerminate NewFileCustomTag = 1

	// NewFileTagNeedCompaction indicates the file needs compaction.
	NewFileTagNeedCompaction NewFileCustomTag = 2

	// NewFileTagMinLogNumberToKeepHack is a hack for backward compatibility.
	// Since Manifest is not entirely forward-compatible, we encode
	// kMinLogNumberToKeep as part of NewFile.
	NewFileTagMinLogNumberToKeepHack NewFileCustomTag = 3

	// NewFileTagOldestBlobFileNumber is the oldest blob file number referenced.
	NewFileTagOldestBlobFileNumber NewFileCustomTag = 4

	// NewFileTagOldestAncestorTime is the oldest ancestor's creation time.
	NewFileTagOldestAncestorTime NewFileCustomTag = 5

	// NewFileTagFileCreationTime is when the file was created.
	NewFileTagFileCreationTime NewFileCustomTag = 6

	// NewFileTagFileChecksum is the file's checksum.
	NewFileTagFileChecksum NewFileCustomTag = 7

	// NewFileTagFileChecksumFuncName is the checksum function name.
	NewFileTagFileChecksumFuncName NewFileCustomTag = 8

	// NewFileTagTemperature is the file's temperature tier.
	NewFileTagTemperature NewFileCustomTag = 9

	// NewFileTagMinTimestamp is the minimum user-defined timestamp.
	NewFileTagMinTimestamp NewFileCustomTag = 10

	// NewFileTagMaxTimestamp is the maximum user-defined timestamp.
	NewFileTagMaxTimestamp NewFileCustomTag = 11

	// NewFileTagUniqueID is the file's unique identifier.
	NewFileTagUniqueID NewFileCustomTag = 12

	// NewFileTagEpochNumber is the file's epoch number for ordering.
	NewFileTagEpochNumber NewFileCustomTag = 13

	// NewFileTagCompensatedRangeDeletionSize is the compensated range deletion size.
	NewFileTagCompensatedRangeDeletionSize NewFileCustomTag = 14

	// NewFileTagTailSize is the tail size for tiered storage.
	NewFileTagTailSize NewFileCustomTag = 15

	// NewFileTagUserDefinedTimestampsPersisted indicates if UDT are persisted.
	NewFileTagUserDefinedTimestampsPersisted NewFileCustomTag = 16

	// NewFileTagCustomNonSafeIgnoreMask - if this bit is set, opening DB
	// should fail if we don't know this field.
	NewFileTagCustomNonSafeIgnoreMask NewFileCustomTag = 1 << 6

	// NewFileTagPathID is the path ID (forward incompatible).
	NewFileTagPathID NewFileCustomTag = NewFileTagCustomNonSafeIgnoreMask | 1
)

// IsSafeToIgnore returns true if the custom tag can be safely ignored when unknown.
func (t NewFileCustomTag) IsSafeToIgnore() bool {
	return t&NewFileTagCustomNonSafeIgnoreMask == 0
}

// Constants for file handling.
const (
	// FileNumberMask is used to extract file number from packed value.
	FileNumberMask uint64 = 0x3FFFFFFFFFFFFFFF

	// UnknownOldestAncestorTime indicates the oldest ancestor time is unknown.
	UnknownOldestAncestorTime uint64 = 0

	// UnknownFileCreationTime indicates file creation time is unknown.
	UnknownFileCreationTime uint64 = 0

	// UnknownEpochNumber indicates the epoch number is unknown.
	UnknownEpochNumber uint64 = 0

	// ReservedEpochNumberForFileIngestedBehind is reserved for files ingested behind.
	ReservedEpochNumberForFileIngestedBehind uint64 = 1

	// InvalidBlobFileNumber indicates an invalid blob file number.
	InvalidBlobFileNumber uint64 = 0

	// UnknownFileChecksumFuncName is the default checksum function name.
	UnknownFileChecksumFuncName = "Unknown"
)

// PackFileNumberAndPathID packs a file number and path ID into a single uint64.
func PackFileNumberAndPathID(number uint64, pathID uint64) uint64 {
	if number > FileNumberMask {
		// Pre-condition violation: file number out of range
		panic("file number exceeds maximum") //nolint:forbidigo // intentional panic for precondition violation
	}
	return number | (pathID * (FileNumberMask + 1))
}

// UnpackFileNumberAndPathID unpacks a file number and path ID from a packed uint64.
func UnpackFileNumberAndPathID(packed uint64) (number uint64, pathID uint32) {
	number = packed & FileNumberMask
	pathID = uint32(packed / (FileNumberMask + 1))
	return
}
