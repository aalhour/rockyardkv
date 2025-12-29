# Examples

Working examples demonstrating common RockyardKV use cases.

Each example is a standalone program you can run directly:

```bash
go run ./examples/basic/
```

## Examples

| Example | Description |
|---------|-------------|
| [basic](basic/) | Open a database, put, get, delete keys |
| [iteration](iteration/) | Iterate over key ranges with forward and reverse scans |
| [batch](batch/) | Atomic batch writes with WriteBatch |
| [snapshots](snapshots/) | Point-in-time consistent reads |
| [compression](compression/) | Enable LZ4/Snappy/ZSTD compression |
| [column_families](column_families/) | Organize data into separate column families |
| [transactions](transactions/) | Pessimistic transactions with isolation |
| [merge](merge/) | Custom merge operators for read-modify-write |
| [compaction_filter](compaction_filter/) | Filter or transform data during compaction |
| [backup](backup/) | Create and restore database backups |
| [sst_ingestion](sst_ingestion/) | Bulk load data via external SST files |

## Running all examples

```bash
for dir in examples/*/; do
    echo "=== Running $dir ==="
    go run "$dir"
done
```

## Cleanup

Examples create temporary databases in `/tmp/rockyardkv_*`.
Remove them with:

```bash
rm -rf /tmp/rockyardkv_*
```

## API

All examples use the stable public API from the `db` package.
See `db.NewWriteBatch()` for batch writes and the `db.DB` interface for database operations.
