# Status

This directory contains status documentation for RockyardKV.
Use these documents to understand what is verified, what is under investigation, and how to reproduce known issues.

## Documents

| Document | Description |
|----------|-------------|
| [API compatibility](api_compatibility.md) | Public API comparison with C++ RocksDB |
| [Compatibility](compatibility.md) | File format compatibility claims and verification commands |
| [Durability report](durability_report.md) | Crash durability status, known limitations, and reproduction commands |

## Evidence and artifacts

Each reproduction command writes logs and artifacts to the specified run directory.
Use those logs and artifacts as evidence when reviewing behavior.
