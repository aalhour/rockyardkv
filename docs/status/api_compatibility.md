# API compatibility matrix

This document compares RockyardKV's public API with C++ RocksDB v10.7.5.

Status key:
- ✅ Implemented and compatible
- ⚠️ Partial implementation
- ❌ Not yet implemented
- 🔄 Different design (Go-idiomatic)

## Database operations

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::Open()` | `rockyardkv.Open()` | ✅ | |
| `DB::OpenForReadOnly()` | `rockyardkv.OpenForReadOnly()` | ✅ | |
| `DB::OpenAsSecondary()` | `rockyardkv.OpenAsSecondary()` | ✅ | |
| `DB::OpenWithColumnFamilies()` | — | ❌ | Use `Open()` then `CreateColumnFamily()` |
| `DB::OpenAsSecondaryWithColumnFamilies()` | — | ❌ | |
| `DB::ListColumnFamilies()` | `rockyardkv.ListColumnFamilies()` | ✅ | Static method in C++, instance in Go |
| `DB::Close()` | `database.Close()` | ✅ | |
| `DestroyDB()` | — | ❌ | Use `os.RemoveAll()` |
| `RepairDB()` | — | ❌ | |

## Read/write operations

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::Put()` | `database.Put()` | ✅ | |
| `DB::Get()` | `database.Get()` | ✅ | |
| `DB::Delete()` | `database.Delete()` | ✅ | |
| `DB::SingleDelete()` | `database.SingleDelete()` | ✅ | |
| `DB::DeleteRange()` | `database.DeleteRange()` | ✅ | |
| `DB::Merge()` | `database.Merge()` | ✅ | |
| `DB::Write()` | `database.Write()` | ✅ | |
| `DB::MultiGet()` | `database.MultiGet()` | ✅ | |
| `DB::KeyMayExist()` | — | ❌ | |
| `DB::GetApproximateSizes()` | — | ❌ | |
| `DB::GetApproximateMemTableStats()` | — | ❌ | |

## Column family operations

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::PutCF()` / `Put(cf, ...)` | `database.PutCF()` | ✅ | |
| `DB::GetCF()` / `Get(cf, ...)` | `database.GetCF()` | ✅ | |
| `DB::DeleteCF()` | `database.DeleteCF()` | ✅ | |
| `DB::DeleteRangeCF()` | `database.DeleteRangeCF()` | ✅ | |
| `DB::MergeCF()` | `database.MergeCF()` | ✅ | |
| `DB::CreateColumnFamily()` | `database.CreateColumnFamily()` | ✅ | |
| `DB::DropColumnFamily()` | `database.DropColumnFamily()` | ✅ | |
| `DB::CreateColumnFamilies()` | — | ❌ | Use multiple `CreateColumnFamily()` calls |
| `DB::DropColumnFamilies()` | — | ❌ | Use multiple `DropColumnFamily()` calls |

## WriteBatch

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `WriteBatch()` constructor | `rockyardkv.NewWriteBatch()` | ✅ | |
| `WriteBatch::Put()` | `wb.Put()` | ✅ | |
| `WriteBatch::Delete()` | `wb.Delete()` | ✅ | |
| `WriteBatch::SingleDelete()` | `wb.SingleDelete()` | ✅ | |
| `WriteBatch::DeleteRange()` | `wb.DeleteRange()` | ✅ | |
| `WriteBatch::Merge()` | `wb.Merge()` | ✅ | |
| `WriteBatch::Clear()` | `wb.Clear()` | ✅ | |
| `WriteBatch::Count()` | `wb.Count()` | ✅ | |
| `WriteBatch::Data()` | `wb.Data()` | ✅ | |
| `WriteBatch::PutCF()` | `wb.PutCF()` | ✅ | |
| `WriteBatch::DeleteCF()` | `wb.DeleteCF()` | ✅ | |
| `WriteBatch::SingleDeleteCF()` | `wb.SingleDeleteCF()` | ✅ | |
| `WriteBatch::DeleteRangeCF()` | `wb.DeleteRangeCF()` | ✅ | |
| `WriteBatch::MergeCF()` | `wb.MergeCF()` | ✅ | |
| `WriteBatch::PutLogData()` | — | ⚠️ | Internal only |
| `WriteBatchWithIndex` | — | ❌ | |

## Iterator

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::NewIterator()` | `database.NewIterator()` | ✅ | |
| `DB::NewIteratorCF()` | `database.NewIteratorCF()` | ✅ | |
| `Iterator::SeekToFirst()` | `iter.SeekToFirst()` | ✅ | |
| `Iterator::SeekToLast()` | `iter.SeekToLast()` | ✅ | |
| `Iterator::Seek()` | `iter.Seek()` | ✅ | |
| `Iterator::SeekForPrev()` | `iter.SeekForPrev()` | ✅ | |
| `Iterator::Next()` | `iter.Next()` | ✅ | |
| `Iterator::Prev()` | `iter.Prev()` | ⚠️ | Known issues crossing SST boundaries |
| `Iterator::Valid()` | `iter.Valid()` | ✅ | |
| `Iterator::Key()` | `iter.Key()` | ✅ | |
| `Iterator::Value()` | `iter.Value()` | ✅ | |
| `Iterator::Status()` | `iter.Error()` | 🔄 | Returns `error` instead of `Status` |
| `Iterator::Refresh()` | — | ❌ | |
| `DB::NewIterators()` | — | ❌ | Create iterators individually |

## Snapshots

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::GetSnapshot()` | `database.GetSnapshot()` | ✅ | |
| `DB::ReleaseSnapshot()` | `database.ReleaseSnapshot()` | ✅ | |

## Transactions

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `OptimisticTransactionDB::Open()` | `database.BeginTransaction()` | 🔄 | Optimistic by default |
| `TransactionDB::Open()` | `rockyardkv.OpenTransactionDB()` | ✅ | Pessimistic transactions |
| `Transaction::Get()` | `txn.Get()` | ✅ | |
| `Transaction::GetForUpdate()` | `txn.GetForUpdate()` | ✅ | |
| `Transaction::Put()` | `txn.Put()` | ✅ | |
| `Transaction::Delete()` | `txn.Delete()` | ✅ | |
| `Transaction::Commit()` | `txn.Commit()` | ✅ | |
| `Transaction::Rollback()` | `txn.Rollback()` | ✅ | |
| `Transaction::SetSavePoint()` | `txn.SetSavePoint()` | ✅ | |
| `Transaction::RollbackToSavePoint()` | `txn.RollbackToSavePoint()` | ✅ | |
| `Transaction::Prepare()` (2PC) | `txn.Prepare()` | ✅ | Write-prepared only |
| `Transaction::SetName()` | `txn.SetName()` | ✅ | |

## Compaction

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::CompactRange()` | `database.CompactRange()` | ✅ | |
| `DB::CompactFiles()` | — | ❌ | |
| `DB::SetOptions()` | — | ❌ | |
| `DB::EnableAutoCompaction()` | — | ❌ | Always enabled |
| `DB::DisableAutoCompaction()` | — | ❌ | |

## Flush

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::Flush()` | `database.Flush()` | ✅ | |
| `DB::FlushWAL()` | `database.FlushWAL()` | ✅ | |
| `DB::SyncWAL()` | `database.SyncWAL()` | ✅ | |

## Backup

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `BackupEngine::Open()` | `rockyardkv.NewBackupEngine()` | ✅ | |
| `BackupEngine::CreateNewBackup()` | `engine.CreateNewBackup()` | ✅ | |
| `BackupEngine::RestoreDBFromLatestBackup()` | `engine.RestoreDBFromLatestBackup()` | ✅ | |
| `BackupEngine::RestoreDBFromBackup()` | `engine.RestoreDBFromBackup()` | ✅ | |
| `BackupEngine::GetBackupInfo()` | `engine.GetBackupInfo()` | ✅ | |
| `BackupEngine::PurgeOldBackups()` | `engine.PurgeOldBackups()` | ✅ | |
| `BackupEngine::DeleteBackup()` | `engine.DeleteBackup()` | ✅ | |

## SST file operations

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `SstFileWriter()` | `rockyardkv.NewSstFileWriter()` | ✅ | |
| `SstFileWriter::Open()` | `writer.Open()` | ✅ | |
| `SstFileWriter::Put()` | `writer.Put()` | ✅ | |
| `SstFileWriter::Delete()` | `writer.Delete()` | ✅ | |
| `SstFileWriter::Finish()` | `writer.Finish()` | ✅ | |
| `DB::IngestExternalFile()` | `database.IngestExternalFile()` | ✅ | |

## Checkpoints

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `Checkpoint::Create()` | `db.NewCheckpoint()` | ✅ | |
| `Checkpoint::CreateCheckpoint()` | `checkpoint.CreateCheckpoint()` | ✅ | |

## Statistics and properties

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `DB::GetProperty()` | `database.GetProperty()` | ⚠️ | Limited properties |
| `DB::GetMapProperty()` | — | ❌ | |
| `DB::GetIntProperty()` | — | ❌ | |
| `Statistics` | `db.NewStatistics()` | ⚠️ | Basic counters |

## Utilities

| C++ RocksDB | RockyardKV | Status | Notes |
|-------------|------------|--------|-------|
| `ldb` CLI tool | `cmd/ldb` | ⚠️ | Subset of commands |
| `sst_dump` CLI tool | `cmd/sstdump` | ⚠️ | Basic functionality |
| `manifest_dump` | `cmd/ldb manifest_dump` | ✅ | |
| Rate limiter | `db.NewRateLimiter()` | ✅ | |
| Write buffer manager | `db.NewWriteBufferManager()` | ✅ | |

## Notable differences from C++ RocksDB

### Go-idiomatic patterns

1. **Error handling**: Returns `error` instead of `Status`.
   Go uses explicit error returns rather than status objects.

2. **Options**: Uses struct literals with zero-value defaults.
   `rockyardkv.DefaultOptions()` provides sensible defaults.

3. **Memory management**: Automatic via garbage collection.
   No need for `delete` or manual cleanup.

4. **Iterators**: Call `iter.Close()` when done.
   Unlike C++, Go iterators don't auto-cleanup.

### Not yet implemented

The following major features are planned but not yet implemented:

- `OpenWithColumnFamilies()` - open with existing column families
- `WriteBatchWithIndex` - indexed write batch for reads
- `KeyMayExist()` - probabilistic key existence check
- `CompactFiles()` - explicit file compaction
- Dynamic options (`SetOptions()`)
- `GetApproximateSizes()`

### Architectural differences

1. **Single WAL**: RockyardKV does not rotate WALs on memtable switch.
   This simplifies recovery but differs from C++ RocksDB.

2. **Background threads**: Uses goroutines instead of thread pools.
   Compaction and flush run as goroutines.

3. **Block cache**: Shared LRU cache implementation.
   API differs slightly from C++ `Cache` interface.

## Version compatibility

RockyardKV targets format compatibility with RocksDB v10.7.5:

- SST files: format version 3-5 supported
- WAL: compatible log record format
- MANIFEST: VersionEdit binary format compatible
- Compression: Snappy, LZ4, Zstd, Zlib supported

SST files created by RockyardKV can be read by C++ RocksDB and vice versa.

## See also

- [Configuration options](../configuration.md)
- [Durability report](durability_report.md)

