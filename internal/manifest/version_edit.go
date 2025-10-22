// version_edit.go implements VersionEdit encoding and decoding.
//
// VersionEdit describes a set of changes to be applied to a Version.
// It is serialized to the MANIFEST file and replayed during recovery.
//
// Reference: RocksDB v10.7.5
//   - db/version_edit.h
//   - db/version_edit.cc
package manifest

import (
	"errors"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// Errors returned during VersionEdit encoding/decoding.
var (
	ErrInvalidTag           = errors.New("manifest: invalid tag")
	ErrUnexpectedEndOfInput = errors.New("manifest: unexpected end of input")
	ErrInvalidFileMetadata  = errors.New("manifest: invalid file metadata")
	ErrUnknownRequiredTag   = errors.New("manifest: unknown required tag")
)

// SequenceNumber represents a database sequence number.
type SequenceNumber uint64

// MaxSequenceNumber is the maximum valid sequence number.
const MaxSequenceNumber SequenceNumber = (1 << 56) - 1

// Temperature represents the temperature tier for a file.
type Temperature uint8

const (
	TemperatureUnknown Temperature = iota
	TemperatureHot
	TemperatureWarm
	TemperatureCold
)

// FileDescriptor contains the core file identification and size info.
type FileDescriptor struct {
	PackedNumberAndPathID uint64
	FileSize              uint64
	SmallestSeqno         SequenceNumber
	LargestSeqno          SequenceNumber
}

// NewFileDescriptor creates a new FileDescriptor.
func NewFileDescriptor(number uint64, pathID uint32, fileSize uint64) FileDescriptor {
	return FileDescriptor{
		PackedNumberAndPathID: PackFileNumberAndPathID(number, uint64(pathID)),
		FileSize:              fileSize,
		SmallestSeqno:         MaxSequenceNumber,
		LargestSeqno:          0,
	}
}

// GetNumber returns the file number.
func (fd *FileDescriptor) GetNumber() uint64 {
	return fd.PackedNumberAndPathID & FileNumberMask
}

// GetPathID returns the path ID.
func (fd *FileDescriptor) GetPathID() uint32 {
	return uint32(fd.PackedNumberAndPathID / (FileNumberMask + 1))
}

// FileMetaData contains complete metadata about an SST file.
type FileMetaData struct {
	FD       FileDescriptor
	Smallest []byte // Smallest internal key
	Largest  []byte // Largest internal key

	// Additional metadata
	OldestAncestorTime             uint64
	FileCreationTime               uint64
	EpochNumber                    uint64
	FileChecksum                   string
	FileChecksumFuncName           string
	Temperature                    Temperature
	MarkedForCompaction            bool
	OldestBlobFileNumber           uint64
	CompensatedRangeDeletionSize   uint64
	TailSize                       uint64
	UserDefinedTimestampsPersisted bool

	// Runtime state (not persisted)
	BeingCompacted bool // True if this file is currently being compacted
}

// NewFileMetaData creates a new FileMetaData with default values.
func NewFileMetaData() *FileMetaData {
	return &FileMetaData{
		OldestAncestorTime:             UnknownOldestAncestorTime,
		FileCreationTime:               UnknownFileCreationTime,
		EpochNumber:                    UnknownEpochNumber,
		FileChecksumFuncName:           UnknownFileChecksumFuncName,
		Temperature:                    TemperatureUnknown,
		OldestBlobFileNumber:           InvalidBlobFileNumber,
		UserDefinedTimestampsPersisted: true, // Default is true
	}
}

// DeletedFileEntry represents a file to be deleted.
type DeletedFileEntry struct {
	Level      int
	FileNumber uint64
}

// NewFileEntry represents a new file to be added.
type NewFileEntry struct {
	Level int
	Meta  *FileMetaData
}

// VersionEdit represents a single edit to the database version.
// It is encoded in the MANIFEST file to track database state changes.
type VersionEdit struct {
	// Database identification
	DBId    string
	HasDBId bool

	// Comparator name
	Comparator    string
	HasComparator bool

	// Log file numbers
	LogNumber             uint64
	HasLogNumber          bool
	PrevLogNumber         uint64
	HasPrevLogNumber      bool
	MinLogNumberToKeep    uint64
	HasMinLogNumberToKeep bool

	// File number allocation
	NextFileNumber    uint64
	HasNextFileNumber bool

	// Sequence numbers
	LastSequence    SequenceNumber
	HasLastSequence bool

	// Column family
	ColumnFamily       uint32
	HasColumnFamily    bool
	ColumnFamilyName   string
	IsColumnFamilyAdd  bool
	IsColumnFamilyDrop bool
	MaxColumnFamily    uint32
	HasMaxColumnFamily bool

	// Atomic group
	IsInAtomicGroup  bool
	RemainingEntries uint32

	// File changes
	DeletedFiles []DeletedFileEntry
	NewFiles     []NewFileEntry

	// Compact cursors (level -> key)
	CompactCursors []struct {
		Level int
		Key   []byte
	}

	// User-defined timestamps
	FullHistoryTSLow                []byte
	HasFullHistoryTSLow             bool
	PersistUserDefinedTimestamps    bool
	HasPersistUserDefinedTimestamps bool
}

// NewVersionEdit creates a new empty VersionEdit.
func NewVersionEdit() *VersionEdit {
	return &VersionEdit{}
}

// Clear resets the VersionEdit to its initial state.
func (ve *VersionEdit) Clear() {
	*ve = VersionEdit{}
}

// SetDBId sets the database ID.
func (ve *VersionEdit) SetDBId(dbID string) {
	ve.DBId = dbID
	ve.HasDBId = true
}

// SetComparatorName sets the comparator name.
func (ve *VersionEdit) SetComparatorName(name string) {
	ve.Comparator = name
	ve.HasComparator = true
}

// SetLogNumber sets the current log number.
func (ve *VersionEdit) SetLogNumber(num uint64) {
	ve.LogNumber = num
	ve.HasLogNumber = true
}

// SetPrevLogNumber sets the previous log number.
func (ve *VersionEdit) SetPrevLogNumber(num uint64) {
	ve.PrevLogNumber = num
	ve.HasPrevLogNumber = true
}

// SetNextFileNumber sets the next file number.
func (ve *VersionEdit) SetNextFileNumber(num uint64) {
	ve.NextFileNumber = num
	ve.HasNextFileNumber = true
}

// SetLastSequence sets the last sequence number.
func (ve *VersionEdit) SetLastSequence(seq SequenceNumber) {
	ve.LastSequence = seq
	ve.HasLastSequence = true
}

// SetMinLogNumberToKeep sets the minimum log number to keep.
func (ve *VersionEdit) SetMinLogNumberToKeep(num uint64) {
	ve.MinLogNumberToKeep = num
	ve.HasMinLogNumberToKeep = true
}

// SetMaxColumnFamily sets the maximum column family ID.
func (ve *VersionEdit) SetMaxColumnFamily(cf uint32) {
	ve.MaxColumnFamily = cf
	ve.HasMaxColumnFamily = true
}

// SetColumnFamily sets the column family for this edit.
func (ve *VersionEdit) SetColumnFamily(cf uint32) {
	ve.ColumnFamily = cf
	ve.HasColumnFamily = true
}

// AddColumnFamily marks this edit as adding a column family.
func (ve *VersionEdit) AddColumnFamily(name string) {
	ve.ColumnFamilyName = name
	ve.IsColumnFamilyAdd = true
}

// DropColumnFamily marks this edit as dropping a column family.
func (ve *VersionEdit) DropColumnFamily() {
	ve.IsColumnFamilyDrop = true
}

// DeleteFile adds a file deletion entry.
func (ve *VersionEdit) DeleteFile(level int, fileNumber uint64) {
	ve.DeletedFiles = append(ve.DeletedFiles, DeletedFileEntry{
		Level:      level,
		FileNumber: fileNumber,
	})
}

// AddFile adds a new file entry.
func (ve *VersionEdit) AddFile(level int, meta *FileMetaData) {
	ve.NewFiles = append(ve.NewFiles, NewFileEntry{
		Level: level,
		Meta:  meta,
	})
}

// SetAtomicGroup marks this edit as part of an atomic group.
func (ve *VersionEdit) SetAtomicGroup(remainingEntries uint32) {
	ve.IsInAtomicGroup = true
	ve.RemainingEntries = remainingEntries
}

// EncodeTo encodes the VersionEdit to a byte slice.
func (ve *VersionEdit) EncodeTo() []byte {
	var dst []byte

	if ve.HasDBId {
		dst = encoding.AppendVarint32(dst, uint32(TagDBID))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte(ve.DBId))
	}

	if ve.HasComparator {
		dst = encoding.AppendVarint32(dst, uint32(TagComparator))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte(ve.Comparator))
	}

	if ve.HasLogNumber {
		dst = encoding.AppendVarint32(dst, uint32(TagLogNumber))
		dst = encoding.AppendVarint64(dst, ve.LogNumber)
	}

	if ve.HasPrevLogNumber {
		dst = encoding.AppendVarint32(dst, uint32(TagPrevLogNumber))
		dst = encoding.AppendVarint64(dst, ve.PrevLogNumber)
	}

	if ve.HasNextFileNumber {
		dst = encoding.AppendVarint32(dst, uint32(TagNextFileNumber))
		dst = encoding.AppendVarint64(dst, ve.NextFileNumber)
	}

	if ve.HasMaxColumnFamily {
		dst = encoding.AppendVarint32(dst, uint32(TagMaxColumnFamily))
		dst = encoding.AppendVarint32(dst, ve.MaxColumnFamily)
	}

	if ve.HasMinLogNumberToKeep {
		dst = encoding.AppendVarint32(dst, uint32(TagMinLogNumberToKeep))
		dst = encoding.AppendVarint64(dst, ve.MinLogNumberToKeep)
	}

	if ve.HasLastSequence {
		dst = encoding.AppendVarint32(dst, uint32(TagLastSequence))
		dst = encoding.AppendVarint64(dst, uint64(ve.LastSequence))
	}

	// Compact cursors
	for _, cc := range ve.CompactCursors {
		dst = encoding.AppendVarint32(dst, uint32(TagCompactCursor))
		dst = encoding.AppendVarint32(dst, uint32(cc.Level))
		dst = encoding.AppendLengthPrefixedSlice(dst, cc.Key)
	}

	// Deleted files
	for _, df := range ve.DeletedFiles {
		dst = encoding.AppendVarint32(dst, uint32(TagDeletedFile))
		dst = encoding.AppendVarint32(dst, uint32(df.Level))
		dst = encoding.AppendVarint64(dst, df.FileNumber)
	}

	// New files (using NewFile4 format)
	for _, nf := range ve.NewFiles {
		dst = ve.encodeNewFile4(dst, nf)
	}

	// Column family (0 is default and doesn't need to be written)
	if ve.HasColumnFamily && ve.ColumnFamily != 0 {
		dst = encoding.AppendVarint32(dst, uint32(TagColumnFamily))
		dst = encoding.AppendVarint32(dst, ve.ColumnFamily)
	}

	if ve.IsColumnFamilyAdd {
		dst = encoding.AppendVarint32(dst, uint32(TagColumnFamilyAdd))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte(ve.ColumnFamilyName))
	}

	if ve.IsColumnFamilyDrop {
		dst = encoding.AppendVarint32(dst, uint32(TagColumnFamilyDrop))
	}

	if ve.IsInAtomicGroup {
		dst = encoding.AppendVarint32(dst, uint32(TagInAtomicGroup))
		dst = encoding.AppendVarint32(dst, ve.RemainingEntries)
	}

	if ve.HasFullHistoryTSLow {
		dst = encoding.AppendVarint32(dst, uint32(TagFullHistoryTSLow))
		dst = encoding.AppendLengthPrefixedSlice(dst, ve.FullHistoryTSLow)
	}

	if ve.HasPersistUserDefinedTimestamps && ve.HasComparator {
		dst = encoding.AppendVarint32(dst, uint32(TagPersistUserDefinedTimestamps))
		val := byte(0)
		if ve.PersistUserDefinedTimestamps {
			val = 1
		}
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte{val})
	}

	return dst
}

// encodeNewFile4 encodes a new file entry in NewFile4 format.
func (ve *VersionEdit) encodeNewFile4(dst []byte, nf NewFileEntry) []byte {
	f := nf.Meta

	dst = encoding.AppendVarint32(dst, uint32(TagNewFile4))
	dst = encoding.AppendVarint32(dst, uint32(nf.Level))
	dst = encoding.AppendVarint64(dst, f.FD.GetNumber())
	dst = encoding.AppendVarint64(dst, f.FD.FileSize)

	// Encode file boundaries
	dst = encoding.AppendLengthPrefixedSlice(dst, f.Smallest)
	dst = encoding.AppendLengthPrefixedSlice(dst, f.Largest)

	dst = encoding.AppendVarint64(dst, uint64(f.FD.SmallestSeqno))
	dst = encoding.AppendVarint64(dst, uint64(f.FD.LargestSeqno))

	// Custom fields
	// Oldest ancestor time
	dst = encoding.AppendVarint32(dst, uint32(NewFileTagOldestAncestorTime))
	var timeBytes []byte
	timeBytes = encoding.AppendVarint64(timeBytes, f.OldestAncestorTime)
	dst = encoding.AppendLengthPrefixedSlice(dst, timeBytes)

	// File creation time
	dst = encoding.AppendVarint32(dst, uint32(NewFileTagFileCreationTime))
	timeBytes = nil
	timeBytes = encoding.AppendVarint64(timeBytes, f.FileCreationTime)
	dst = encoding.AppendLengthPrefixedSlice(dst, timeBytes)

	// Epoch number
	dst = encoding.AppendVarint32(dst, uint32(NewFileTagEpochNumber))
	var epochBytes []byte
	epochBytes = encoding.AppendVarint64(epochBytes, f.EpochNumber)
	dst = encoding.AppendLengthPrefixedSlice(dst, epochBytes)

	// File checksum (if not unknown)
	if f.FileChecksumFuncName != UnknownFileChecksumFuncName {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagFileChecksum))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte(f.FileChecksum))

		dst = encoding.AppendVarint32(dst, uint32(NewFileTagFileChecksumFuncName))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte(f.FileChecksumFuncName))
	}

	// Path ID (if not 0)
	if f.FD.GetPathID() != 0 {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagPathID))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte{byte(f.FD.GetPathID())})
	}

	// Temperature (if not unknown)
	if f.Temperature != TemperatureUnknown {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagTemperature))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte{byte(f.Temperature)})
	}

	// Marked for compaction
	if f.MarkedForCompaction {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagNeedCompaction))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte{1})
	}

	// Oldest blob file number (if valid)
	if f.OldestBlobFileNumber != InvalidBlobFileNumber {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagOldestBlobFileNumber))
		var blobBytes []byte
		blobBytes = encoding.AppendVarint64(blobBytes, f.OldestBlobFileNumber)
		dst = encoding.AppendLengthPrefixedSlice(dst, blobBytes)
	}

	// Compensated range deletion size (if non-zero)
	if f.CompensatedRangeDeletionSize != 0 {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagCompensatedRangeDeletionSize))
		var sizeBytes []byte
		sizeBytes = encoding.AppendVarint64(sizeBytes, f.CompensatedRangeDeletionSize)
		dst = encoding.AppendLengthPrefixedSlice(dst, sizeBytes)
	}

	// Tail size (if non-zero)
	if f.TailSize != 0 {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagTailSize))
		var tailBytes []byte
		tailBytes = encoding.AppendVarint64(tailBytes, f.TailSize)
		dst = encoding.AppendLengthPrefixedSlice(dst, tailBytes)
	}

	// User-defined timestamps persisted (only if false, since true is default)
	if !f.UserDefinedTimestampsPersisted {
		dst = encoding.AppendVarint32(dst, uint32(NewFileTagUserDefinedTimestampsPersisted))
		dst = encoding.AppendLengthPrefixedSlice(dst, []byte{0})
	}

	// Terminating tag
	dst = encoding.AppendVarint32(dst, uint32(NewFileTagTerminate))

	return dst
}

// DecodeFrom decodes a VersionEdit from a byte slice.
func (ve *VersionEdit) DecodeFrom(data []byte) error {
	ve.Clear()

	for len(data) > 0 {
		tagVal, n, err := encoding.DecodeVarint32(data)
		if err != nil {
			return ErrUnexpectedEndOfInput
		}
		data = data[n:]
		tag := Tag(tagVal)

		switch tag {
		case TagDBID:
			val, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.DBId = string(val)
			ve.HasDBId = true
			data = data[n:]

		case TagComparator:
			val, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.Comparator = string(val)
			ve.HasComparator = true
			data = data[n:]

		case TagLogNumber:
			val, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.LogNumber = val
			ve.HasLogNumber = true
			data = data[n:]

		case TagPrevLogNumber:
			val, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.PrevLogNumber = val
			ve.HasPrevLogNumber = true
			data = data[n:]

		case TagNextFileNumber:
			val, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.NextFileNumber = val
			ve.HasNextFileNumber = true
			data = data[n:]

		case TagLastSequence:
			val, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.LastSequence = SequenceNumber(val)
			ve.HasLastSequence = true
			data = data[n:]

		case TagMaxColumnFamily:
			val, n, err := encoding.DecodeVarint32(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.MaxColumnFamily = val
			ve.HasMaxColumnFamily = true
			data = data[n:]

		case TagMinLogNumberToKeep:
			val, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.MinLogNumberToKeep = val
			ve.HasMinLogNumberToKeep = true
			data = data[n:]

		case TagCompactCursor:
			level, n, err := encoding.DecodeVarint32(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			data = data[n:]

			key, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			data = data[n:]

			ve.CompactCursors = append(ve.CompactCursors, struct {
				Level int
				Key   []byte
			}{Level: int(level), Key: key})

		case TagDeletedFile:
			level, n, err := encoding.DecodeVarint32(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			data = data[n:]

			fileNum, n, err := encoding.DecodeVarint64(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			data = data[n:]

			ve.DeleteFile(int(level), fileNum)

		case TagNewFile4:
			var err error
			data, err = ve.decodeNewFile4(data)
			if err != nil {
				return err
			}

		case TagColumnFamily:
			val, n, err := encoding.DecodeVarint32(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.ColumnFamily = val
			ve.HasColumnFamily = true
			data = data[n:]

		case TagColumnFamilyAdd:
			name, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.ColumnFamilyName = string(name)
			ve.IsColumnFamilyAdd = true
			data = data[n:]

		case TagColumnFamilyDrop:
			ve.IsColumnFamilyDrop = true

		case TagInAtomicGroup:
			val, n, err := encoding.DecodeVarint32(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.IsInAtomicGroup = true
			ve.RemainingEntries = val
			data = data[n:]

		case TagFullHistoryTSLow:
			val, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			ve.FullHistoryTSLow = val
			ve.HasFullHistoryTSLow = true
			data = data[n:]

		case TagPersistUserDefinedTimestamps:
			val, n, err := encoding.DecodeLengthPrefixedSlice(data)
			if err != nil {
				return ErrUnexpectedEndOfInput
			}
			if len(val) > 0 && val[0] != 0 {
				ve.PersistUserDefinedTimestamps = true
			}
			ve.HasPersistUserDefinedTimestamps = true
			data = data[n:]

		default:
			// Unknown tag
			if tag.IsSafeToIgnore() {
				// Skip length-prefixed value
				val, n, err := encoding.DecodeLengthPrefixedSlice(data)
				if err != nil {
					return ErrUnexpectedEndOfInput
				}
				_ = val // Ignore
				data = data[n:]
			} else {
				return ErrUnknownRequiredTag
			}
		}
	}

	return nil
}

// decodeNewFile4 decodes a NewFile4 entry.
func (ve *VersionEdit) decodeNewFile4(data []byte) ([]byte, error) {
	meta := NewFileMetaData()

	// Level
	level, n, err := encoding.DecodeVarint32(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	data = data[n:]

	// File number
	fileNum, n, err := encoding.DecodeVarint64(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	data = data[n:]

	// File size
	fileSize, n, err := encoding.DecodeVarint64(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	data = data[n:]

	meta.FD = NewFileDescriptor(fileNum, 0, fileSize)

	// Smallest key
	smallest, n, err := encoding.DecodeLengthPrefixedSlice(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	meta.Smallest = smallest
	data = data[n:]

	// Largest key
	largest, n, err := encoding.DecodeLengthPrefixedSlice(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	meta.Largest = largest
	data = data[n:]

	// Smallest seqno
	smallestSeqno, n, err := encoding.DecodeVarint64(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	meta.FD.SmallestSeqno = SequenceNumber(smallestSeqno)
	data = data[n:]

	// Largest seqno
	largestSeqno, n, err := encoding.DecodeVarint64(data)
	if err != nil {
		return nil, ErrUnexpectedEndOfInput
	}
	meta.FD.LargestSeqno = SequenceNumber(largestSeqno)
	data = data[n:]

	// Decode custom tags
	for {
		customTag, n, err := encoding.DecodeVarint32(data)
		if err != nil {
			return nil, ErrUnexpectedEndOfInput
		}
		data = data[n:]

		if NewFileCustomTag(customTag) == NewFileTagTerminate {
			break
		}

		// Read the value
		val, n, err := encoding.DecodeLengthPrefixedSlice(data)
		if err != nil {
			return nil, ErrUnexpectedEndOfInput
		}
		data = data[n:]

		switch NewFileCustomTag(customTag) {
		case NewFileTagNeedCompaction:
			if len(val) > 0 && val[0] == 1 {
				meta.MarkedForCompaction = true
			}

		case NewFileTagPathID:
			if len(val) > 0 {
				pathID := uint32(val[0])
				meta.FD.PackedNumberAndPathID = PackFileNumberAndPathID(meta.FD.GetNumber(), uint64(pathID))
			}

		case NewFileTagOldestBlobFileNumber:
			num, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.OldestBlobFileNumber = num
			}

		case NewFileTagOldestAncestorTime:
			t, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.OldestAncestorTime = t
			}

		case NewFileTagFileCreationTime:
			t, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.FileCreationTime = t
			}

		case NewFileTagFileChecksum:
			meta.FileChecksum = string(val)

		case NewFileTagFileChecksumFuncName:
			meta.FileChecksumFuncName = string(val)

		case NewFileTagTemperature:
			if len(val) > 0 {
				meta.Temperature = Temperature(val[0])
			}

		case NewFileTagEpochNumber:
			num, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.EpochNumber = num
			}

		case NewFileTagCompensatedRangeDeletionSize:
			num, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.CompensatedRangeDeletionSize = num
			}

		case NewFileTagTailSize:
			num, _, err := encoding.DecodeVarint64(val)
			if err == nil {
				meta.TailSize = num
			}

		case NewFileTagUserDefinedTimestampsPersisted:
			if len(val) > 0 && val[0] == 0 {
				meta.UserDefinedTimestampsPersisted = false
			}

		default:
			// Check if we must understand this tag
			if !NewFileCustomTag(customTag).IsSafeToIgnore() {
				return nil, ErrUnknownRequiredTag
			}
			// Otherwise ignore
		}
	}

	ve.AddFile(int(level), meta)
	return data, nil
}
