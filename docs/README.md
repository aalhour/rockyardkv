# Documentation

This directory contains detailed documentation for RockyardKV.

<!-- Ordering rationale: "need to know" → "want to know"
     Status/Config first (is it ready? what options?), then how-to guides,
     then deeper reference, then contributor docs.
-->

## Getting started

| Document | Description |
| -------- | ----------- |
| [Status](status/README.md) | Compatibility, limitations, and verification |
| [Configuration](configuration.md) | All database options, compression, checksums, and C++ compatibility |
| [Integration](integration.md) | How to integrate RockyardKV into your application |

## Reference

| Document | Description |
| -------- | ----------- |
| [Architecture](architecture.md) | Internal design and package structure |
| [Benchmarks](benchmarks.md) | Performance measurements and analysis |
| [Performance](performance.md) | Performance tuning and optimization |

## Guides

| Document | Description |
| -------- | ----------- |
| [Migration](migration.md) | Migrating from C++ RocksDB or CGo wrappers |

## Development

| Document | Description |
| -------- | ----------- |
| [Testing](testing/README.md) | Testing strategy, frameworks, and commands |
| [Release](release.md) | Release process and checklist |
