// Package table provides SST file reading and writing functionality.
// This file implements TableProperties parsing.
//
// Reference: RocksDB v10.7.5
//   - table/table_properties.cc
//   - table/meta_blocks.cc (ParsePropertiesBlock)
//   - include/rocksdb/table_properties.h

package table

import (
	"github.com/aalhour/rockyardkv/internal/block"
	"github.com/aalhour/rockyardkv/internal/encoding"
)

// Property name constants from RocksDB.
// Reference: include/rocksdb/table_properties.h
const (
	PropDBID                           = "rocksdb.creating.db.identity"
	PropDBSessionID                    = "rocksdb.creating.session.identity"
	PropDBHostID                       = "rocksdb.creating.host.identity"
	PropOriginalFileNumber             = "rocksdb.original.file.number"
	PropDataSize                       = "rocksdb.data.size"
	PropIndexSize                      = "rocksdb.index.size"
	PropIndexPartitions                = "rocksdb.index.partitions"
	PropTopLevelIndexSize              = "rocksdb.top-level.index.size"
	PropIndexKeyIsUserKey              = "rocksdb.index.key.is.user.key"
	PropIndexValueIsDeltaEncoded       = "rocksdb.index.value.is.delta.encoded"
	PropFilterSize                     = "rocksdb.filter.size"
	PropRawKeySize                     = "rocksdb.raw.key.size"
	PropRawValueSize                   = "rocksdb.raw.value.size"
	PropNumDataBlocks                  = "rocksdb.num.data.blocks"
	PropNumEntries                     = "rocksdb.num.entries"
	PropNumFilterEntries               = "rocksdb.num.filter.entries"
	PropDeletedKeys                    = "rocksdb.deleted.keys"
	PropMergeOperands                  = "rocksdb.merge.operands"
	PropNumRangeDeletions              = "rocksdb.num.range-deletions"
	PropFormatVersion                  = "rocksdb.format.version"
	PropFixedKeyLen                    = "rocksdb.fixed.key.length"
	PropFilterPolicy                   = "rocksdb.filter.policy"
	PropColumnFamilyName               = "rocksdb.column.family.name"
	PropColumnFamilyID                 = "rocksdb.column.family.id"
	PropComparator                     = "rocksdb.comparator"
	PropMergeOperator                  = "rocksdb.merge.operator"
	PropPrefixExtractorName            = "rocksdb.prefix.extractor.name"
	PropPropertyCollectors             = "rocksdb.property.collectors"
	PropCompression                    = "rocksdb.compression"
	PropCompressionOptions             = "rocksdb.compression_options"
	PropCreationTime                   = "rocksdb.creation.time"
	PropOldestKeyTime                  = "rocksdb.oldest.key.time"
	PropNewestKeyTime                  = "rocksdb.newest.key.time"
	PropFileCreationTime               = "rocksdb.file.creation.time"
	PropSlowCompressionEstimatedSize   = "rocksdb.sample_for_compression"
	PropFastCompressionEstimatedSize   = "rocksdb.sample_for_compression.2"
	PropTailStartOffset                = "rocksdb.tail.start.offset"
	PropUserDefinedTimestampsPersisted = "rocksdb.user.defined.timestamps.persisted"
	PropKeyLargestSeqno                = "rocksdb.key.largest.seqno"
	PropKeySmallestSeqno               = "rocksdb.key.smallest.seqno"
)

// TableProperties contains metadata about an SST file.
type TableProperties struct {
	// Basic statistics
	DataSize          uint64
	IndexSize         uint64
	IndexPartitions   uint64
	TopLevelIndexSize uint64
	FilterSize        uint64
	RawKeySize        uint64
	RawValueSize      uint64
	NumDataBlocks     uint64
	NumEntries        uint64
	NumFilterEntries  uint64
	NumDeletions      uint64
	NumMergeOperands  uint64
	NumRangeDeletions uint64
	FormatVersion     uint64
	FixedKeyLen       uint64
	ColumnFamilyID    uint64
	CreationTime      uint64
	OldestKeyTime     uint64
	NewestKeyTime     uint64
	FileCreationTime  uint64
	OrigFileNumber    uint64
	TailStartOffset   uint64
	KeyLargestSeqno   uint64
	KeySmallestSeqno  uint64

	// Boolean-like properties (stored as uint64)
	IndexKeyIsUserKey              uint64
	IndexValueIsDeltaEncoded       uint64
	UserDefinedTimestampsPersisted uint64
	SlowCompressionEstimatedSize   uint64
	FastCompressionEstimatedSize   uint64

	// String properties
	DBID                    string
	DBSessionID             string
	DBHostID                string
	FilterPolicyName        string
	ColumnFamilyName        string
	ComparatorName          string
	MergeOperatorName       string
	PrefixExtractorName     string
	PropertyCollectorsNames string
	CompressionName         string
	CompressionOptions      string

	// User-collected properties
	UserCollectedProperties map[string]string
}

// ParsePropertiesBlock parses a properties block into TableProperties.
func ParsePropertiesBlock(data []byte) (*TableProperties, error) {
	// The properties block is a regular block with key-value pairs
	blk, err := block.NewBlock(data)
	if err != nil {
		return nil, err
	}

	props := &TableProperties{
		UserCollectedProperties: make(map[string]string),
	}

	iter := blk.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := iter.Value()

		// Try to parse as uint64 property
		if parseUint64Property(props, key, value) {
			continue
		}

		// Try to parse as string property
		if parseStringProperty(props, key, value) {
			continue
		}

		// Unknown property - store in user-collected
		props.UserCollectedProperties[key] = string(value)
	}

	return props, nil
}

// parseUint64Property parses a uint64 property if the key matches.
func parseUint64Property(props *TableProperties, key string, value []byte) bool {
	var target *uint64

	switch key {
	case PropOriginalFileNumber:
		target = &props.OrigFileNumber
	case PropDataSize:
		target = &props.DataSize
	case PropIndexSize:
		target = &props.IndexSize
	case PropIndexPartitions:
		target = &props.IndexPartitions
	case PropTopLevelIndexSize:
		target = &props.TopLevelIndexSize
	case PropIndexKeyIsUserKey:
		target = &props.IndexKeyIsUserKey
	case PropIndexValueIsDeltaEncoded:
		target = &props.IndexValueIsDeltaEncoded
	case PropFilterSize:
		target = &props.FilterSize
	case PropRawKeySize:
		target = &props.RawKeySize
	case PropRawValueSize:
		target = &props.RawValueSize
	case PropNumDataBlocks:
		target = &props.NumDataBlocks
	case PropNumEntries:
		target = &props.NumEntries
	case PropNumFilterEntries:
		target = &props.NumFilterEntries
	case PropDeletedKeys:
		target = &props.NumDeletions
	case PropMergeOperands:
		target = &props.NumMergeOperands
	case PropNumRangeDeletions:
		target = &props.NumRangeDeletions
	case PropFormatVersion:
		target = &props.FormatVersion
	case PropFixedKeyLen:
		target = &props.FixedKeyLen
	case PropColumnFamilyID:
		target = &props.ColumnFamilyID
	case PropCreationTime:
		target = &props.CreationTime
	case PropOldestKeyTime:
		target = &props.OldestKeyTime
	case PropNewestKeyTime:
		target = &props.NewestKeyTime
	case PropFileCreationTime:
		target = &props.FileCreationTime
	case PropTailStartOffset:
		target = &props.TailStartOffset
	case PropUserDefinedTimestampsPersisted:
		target = &props.UserDefinedTimestampsPersisted
	case PropKeyLargestSeqno:
		target = &props.KeyLargestSeqno
	case PropKeySmallestSeqno:
		target = &props.KeySmallestSeqno
	case PropSlowCompressionEstimatedSize:
		target = &props.SlowCompressionEstimatedSize
	case PropFastCompressionEstimatedSize:
		target = &props.FastCompressionEstimatedSize
	default:
		return false
	}

	// Parse varint64
	v, _, err := encoding.DecodeVarint64(value)
	if err != nil {
		return false
	}
	*target = v
	return true
}

// parseStringProperty parses a string property if the key matches.
func parseStringProperty(props *TableProperties, key string, value []byte) bool {
	switch key {
	case PropDBID:
		props.DBID = string(value)
	case PropDBSessionID:
		props.DBSessionID = string(value)
	case PropDBHostID:
		props.DBHostID = string(value)
	case PropFilterPolicy:
		props.FilterPolicyName = string(value)
	case PropColumnFamilyName:
		props.ColumnFamilyName = string(value)
	case PropComparator:
		props.ComparatorName = string(value)
	case PropMergeOperator:
		props.MergeOperatorName = string(value)
	case PropPrefixExtractorName:
		props.PrefixExtractorName = string(value)
	case PropPropertyCollectors:
		props.PropertyCollectorsNames = string(value)
	case PropCompression:
		props.CompressionName = string(value)
	case PropCompressionOptions:
		props.CompressionOptions = string(value)
	default:
		return false
	}
	return true
}
