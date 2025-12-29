// replayer.go implements trace replay functionality.
//
// Replayer in RocksDB replays captured traces against a database,
// supporting timing preservation and operation statistics.
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/utilities/replayer.h
//   - trace_replay/trace_replay.h
//   - trace_replay/trace_replay.cc
package trace

import "time"

// Replayer replays trace records against a database.
type Replayer struct {
	reader         *Reader
	handler        ReplayHandler
	preserveTiming bool
}

// ReplayHandler handles replayed operations.
type ReplayHandler interface {
	// HandleWrite handles a write operation
	HandleWrite(cfID uint32, batchData []byte) error
	// HandleGet handles a get operation
	HandleGet(cfID uint32, key []byte) error
	// HandleIterSeek handles an iterator seek operation
	HandleIterSeek(cfID uint32, key []byte) error
	// HandleFlush handles a flush operation
	HandleFlush() error
	// HandleCompaction handles a compaction operation
	HandleCompaction() error
}

// ReplayHandlerV2 extends ReplayHandler with sequence number support.
// Used for seqno-prefix verification during crash recovery testing.
type ReplayHandlerV2 interface {
	ReplayHandler
	// HandleWriteWithSeqno handles a write operation with its assigned sequence number.
	// seqno is the sequence number assigned by the DB after the write completed.
	// For version 1 traces, seqno will be 0.
	HandleWriteWithSeqno(cfID uint32, seqno uint64, batchData []byte) error
}

// ReplayerOptions configures the replayer
type ReplayerOptions struct {
	// PreserveTiming delays operations to match original timing
	PreserveTiming bool

	// SpeedMultiplier speeds up or slows down replay (1.0 = original speed)
	SpeedMultiplier float64
}

// DefaultReplayerOptions returns default replayer options
func DefaultReplayerOptions() ReplayerOptions {
	return ReplayerOptions{
		PreserveTiming:  false,
		SpeedMultiplier: 1.0,
	}
}

// NewReplayer creates a new replayer
func NewReplayer(reader *Reader, handler ReplayHandler, opts ReplayerOptions) *Replayer {
	return &Replayer{
		reader:         reader,
		handler:        handler,
		preserveTiming: opts.PreserveTiming,
	}
}

// ReplayStats contains statistics about the replay
type ReplayStats struct {
	TotalRecords    uint64
	SuccessfulOps   uint64
	FailedOps       uint64
	Duration        time.Duration
	OperationCounts map[RecordType]uint64
}

// Replay replays all trace records
func (r *Replayer) Replay() (*ReplayStats, error) {
	stats := &ReplayStats{
		OperationCounts: make(map[RecordType]uint64),
	}

	startTime := time.Now()
	var firstRecordTime time.Time
	var lastTimestamp time.Time

	for {
		record, err := r.reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return stats, err
		}

		stats.TotalRecords++
		stats.OperationCounts[record.Type]++

		// Handle timing
		if r.preserveTiming && !lastTimestamp.IsZero() {
			delay := record.Timestamp.Sub(lastTimestamp)
			if delay > 0 {
				time.Sleep(delay)
			}
		}
		if firstRecordTime.IsZero() {
			firstRecordTime = record.Timestamp
		}
		lastTimestamp = record.Timestamp

		// Execute the operation
		err = r.executeRecord(record)
		if err != nil {
			stats.FailedOps++
			// Continue replaying despite errors
		} else {
			stats.SuccessfulOps++
		}
	}

	stats.Duration = time.Since(startTime)
	return stats, nil
}

// executeRecord executes a single trace record
func (r *Replayer) executeRecord(record *Record) error {
	switch record.Type {
	case TypeWrite:
		// Use version-aware decoder
		payload, err := r.reader.DecodeWritePayload(record.Payload)
		if err != nil {
			return err
		}
		// If handler supports V2 interface, call with seqno
		if v2Handler, ok := r.handler.(ReplayHandlerV2); ok {
			return v2Handler.HandleWriteWithSeqno(payload.ColumnFamilyID, payload.SequenceNumber, payload.Data)
		}
		return r.handler.HandleWrite(payload.ColumnFamilyID, payload.Data)

	case TypeGet:
		payload, err := DecodeGetPayload(record.Payload)
		if err != nil {
			return err
		}
		return r.handler.HandleGet(payload.ColumnFamilyID, payload.Key)

	case TypeIterSeek:
		payload, err := DecodeGetPayload(record.Payload)
		if err != nil {
			return err
		}
		return r.handler.HandleIterSeek(payload.ColumnFamilyID, payload.Key)

	case TypeFlush:
		return r.handler.HandleFlush()

	case TypeCompaction:
		return r.handler.HandleCompaction()

	default:
		// Unknown record type, skip
		return nil
	}
}
