package rockyardkv

// recovery_2pc.go implements 2PC (Two-Phase Commit) recovery for Write-Prepared transactions.
//
// During recovery, we scan the WAL for:
// - BeginPrepare...EndPrepare markers: Indicates a prepared transaction
// - Commit markers: Indicates the prepared transaction was committed
// - Rollback markers: Indicates the prepared transaction was rolled back
//
// Prepared transactions that have no Commit or Rollback marker are restored
// to the PrepareHeap so they can be committed or rolled back after recovery.
//
// Reference: RocksDB v10.7.5
//   - utilities/transactions/write_prepared_txn_db.cc (RecoverPreparedTransactions)
//   - db/db_impl/db_impl_open.cc (RecoverLogFiles)


import (
	"github.com/aalhour/rockyardkv/internal/batch"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/memtable"
)

// PreparedTransaction represents a recovered prepared transaction.
type PreparedTransaction struct {
	// Name is the transaction identifier (XID)
	Name string

	// PrepareSeq is the sequence number at prepare time
	PrepareSeq uint64

	// WriteBatch contains the prepared writes
	WriteBatch *batch.WriteBatch
}

// recovery2PCHandler extends the standard recovery handler with 2PC support.
type recovery2PCHandler struct {
	// Underlying memtable handler
	mem      *memtable.MemTable
	sequence uint64

	// 2PC state
	inPrepare      bool                            // Currently inside a BeginPrepare...EndPrepare block
	currentPrepare *batch.WriteBatch               // Accumulates writes during prepare
	preparedTxns   map[string]*PreparedTransaction // name -> prepared transaction
	committedTxns  map[string]bool                 // Transactions that have been committed
	rolledBackTxns map[string]bool                 // Transactions that have been rolled back
}

// newRecovery2PCHandler creates a new 2PC-aware recovery handler.
// Reserved for future full 2PC recovery integration.
func newRecovery2PCHandler(mem *memtable.MemTable, startSeq uint64) *recovery2PCHandler {
	return &recovery2PCHandler{
		mem:            mem,
		sequence:       startSeq,
		preparedTxns:   make(map[string]*PreparedTransaction),
		committedTxns:  make(map[string]bool),
		rolledBackTxns: make(map[string]bool),
	}
}

// Sequence returns the current sequence number.
func (h *recovery2PCHandler) Sequence() uint64 {
	return h.sequence
}

// GetPreparedTransactions returns transactions that were prepared but not committed/rolled back.
func (h *recovery2PCHandler) GetPreparedTransactions() []*PreparedTransaction {
	var result []*PreparedTransaction
	for name, txn := range h.preparedTxns {
		if !h.committedTxns[name] && !h.rolledBackTxns[name] {
			result = append(result, txn)
		}
	}
	return result
}

// Compile-time check that recovery2PCHandler implements batch.Handler2PC
var _ batch.Handler2PC = (*recovery2PCHandler)(nil)

// Put handles a Put operation during recovery.
func (h *recovery2PCHandler) Put(key, value []byte) error {
	if h.inPrepare {
		// Accumulate in the current prepare batch
		h.currentPrepare.Put(key, value)
		return nil
	}
	// Normal recovery - apply to memtable
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeValue, key, value)
	h.sequence++
	return nil
}

// Delete handles a Delete operation during recovery.
func (h *recovery2PCHandler) Delete(key []byte) error {
	if h.inPrepare {
		h.currentPrepare.Delete(key)
		return nil
	}
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeDeletion, key, nil)
	h.sequence++
	return nil
}

// SingleDelete handles a SingleDelete operation during recovery.
func (h *recovery2PCHandler) SingleDelete(key []byte) error {
	if h.inPrepare {
		h.currentPrepare.SingleDelete(key)
		return nil
	}
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeSingleDeletion, key, nil)
	h.sequence++
	return nil
}

// Merge handles a Merge operation during recovery.
func (h *recovery2PCHandler) Merge(key, value []byte) error {
	if h.inPrepare {
		h.currentPrepare.Merge(key, value)
		return nil
	}
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeMerge, key, value)
	h.sequence++
	return nil
}

// DeleteRange handles a DeleteRange operation during recovery.
func (h *recovery2PCHandler) DeleteRange(startKey, endKey []byte) error {
	if h.inPrepare {
		h.currentPrepare.DeleteRange(startKey, endKey)
		return nil
	}
	h.mem.AddRangeTombstone(dbformat.SequenceNumber(h.sequence), startKey, endKey)
	h.sequence++
	return nil
}

// LogData handles log data during recovery.
func (h *recovery2PCHandler) LogData(blob []byte) {
	// Ignored during recovery
}

// PutCF handles a column family Put during recovery.
func (h *recovery2PCHandler) PutCF(cfID uint32, key, value []byte) error {
	// For now, treat as default CF
	return h.Put(key, value)
}

// DeleteCF handles a column family Delete during recovery.
func (h *recovery2PCHandler) DeleteCF(cfID uint32, key []byte) error {
	return h.Delete(key)
}

// SingleDeleteCF handles a column family SingleDelete during recovery.
func (h *recovery2PCHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	return h.SingleDelete(key)
}

// MergeCF handles a column family Merge during recovery.
func (h *recovery2PCHandler) MergeCF(cfID uint32, key, value []byte) error {
	return h.Merge(key, value)
}

// DeleteRangeCF handles a column family DeleteRange during recovery.
func (h *recovery2PCHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	return h.DeleteRange(startKey, endKey)
}

// MarkBeginPrepare handles the start of a prepared transaction.
func (h *recovery2PCHandler) MarkBeginPrepare(unprepared bool) error {
	h.inPrepare = true
	h.currentPrepare = batch.New()
	return nil
}

// MarkEndPrepare handles the end of a prepared transaction.
func (h *recovery2PCHandler) MarkEndPrepare(xid []byte) error {
	if !h.inPrepare {
		return nil // Ignore if not in prepare
	}

	name := string(xid)
	h.preparedTxns[name] = &PreparedTransaction{
		Name:       name,
		PrepareSeq: h.sequence,
		WriteBatch: h.currentPrepare,
	}

	h.inPrepare = false
	h.currentPrepare = nil
	return nil
}

// MarkCommit handles a commit marker for a prepared transaction.
func (h *recovery2PCHandler) MarkCommit(xid []byte) error {
	name := string(xid)
	h.committedTxns[name] = true

	// If we have the prepared transaction, apply it to memtable
	if txn, ok := h.preparedTxns[name]; ok {
		// Apply the prepared writes to memtable
		applyHandler := &memtableApplyHandler{
			mem:      h.mem,
			sequence: txn.PrepareSeq,
		}
		if err := txn.WriteBatch.Iterate(applyHandler); err != nil {
			return err
		}
		// Update sequence to after the applied writes
		if applyHandler.sequence > h.sequence {
			h.sequence = applyHandler.sequence
		}
	}

	return nil
}

// MarkRollback handles a rollback marker for a prepared transaction.
func (h *recovery2PCHandler) MarkRollback(xid []byte) error {
	name := string(xid)
	h.rolledBackTxns[name] = true
	// Rolled back transactions are simply not applied
	return nil
}

// memtableApplyHandler applies batch operations to a memtable.
type memtableApplyHandler struct {
	mem      *memtable.MemTable
	sequence uint64
}

func (h *memtableApplyHandler) Put(key, value []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeValue, key, value)
	h.sequence++
	return nil
}

func (h *memtableApplyHandler) Delete(key []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeDeletion, key, nil)
	h.sequence++
	return nil
}

func (h *memtableApplyHandler) SingleDelete(key []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeSingleDeletion, key, nil)
	h.sequence++
	return nil
}

func (h *memtableApplyHandler) Merge(key, value []byte) error {
	h.mem.Add(dbformat.SequenceNumber(h.sequence), dbformat.TypeMerge, key, value)
	h.sequence++
	return nil
}

func (h *memtableApplyHandler) DeleteRange(startKey, endKey []byte) error {
	h.mem.AddRangeTombstone(dbformat.SequenceNumber(h.sequence), startKey, endKey)
	h.sequence++
	return nil
}

func (h *memtableApplyHandler) LogData(blob []byte) {}

func (h *memtableApplyHandler) PutCF(cfID uint32, key, value []byte) error {
	return h.Put(key, value)
}

func (h *memtableApplyHandler) DeleteCF(cfID uint32, key []byte) error {
	return h.Delete(key)
}

func (h *memtableApplyHandler) SingleDeleteCF(cfID uint32, key []byte) error {
	return h.SingleDelete(key)
}

func (h *memtableApplyHandler) MergeCF(cfID uint32, key, value []byte) error {
	return h.Merge(key, value)
}

func (h *memtableApplyHandler) DeleteRangeCF(cfID uint32, startKey, endKey []byte) error {
	return h.DeleteRange(startKey, endKey)
}
