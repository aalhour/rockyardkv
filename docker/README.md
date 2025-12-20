# Docker Testing Environment

This directory contains Docker configuration for testing RockyardKV across multiple Go versions and platforms.

## Prerequisites

- Docker Desktop or Docker Engine
- docker-compose (usually included with Docker Desktop)

## Quick start

Run all tests with Go 1.25:

```bash
./docker/run-tests.sh
```

## Available commands

### Helper script

```bash
# Run all tests (unit + smoketest) on all Go versions
./docker/run-tests.sh

# Run specific test type
./docker/run-tests.sh test        # Unit tests only
./docker/run-tests.sh smoketest   # Smoke tests only
./docker/run-tests.sh stresstest  # Stress tests only

# Test specific Go version
./docker/run-tests.sh --go 1.25

# Configure stress tests
./docker/run-tests.sh stresstest --workers 16 --duration 120s
```

### Docker Compose directly

```bash
cd docker

# Build images
docker compose build

# Run specific services
docker compose run --rm go125-test        # Go 1.25 unit tests
docker compose run --rm go125-smoketest   # Go 1.25 smoke tests
docker compose run --rm go125-stresstest  # Go 1.25 stress tests

# Note: Go 1.25 is the minimum required version
```

### Manual Docker commands

```bash
# Build for specific Go version
docker build --build-arg GO_VERSION=1.25 \
  -t rockyardkv:go1.25 \
  -f docker/Dockerfile.test .

# Run tests
docker run --rm rockyardkv:go1.25 test
docker run --rm rockyardkv:go1.25 smoketest
docker run --rm rockyardkv:go1.25 stresstest

# Run with custom stress parameters
docker run --rm \
  -e STRESS_WORKERS=16 \
  -e STRESS_DURATION=300s \
  -v $(pwd)/docker/logs:/logs \
  -v $(pwd)/docker/data:/data \
  rockyardkv:go1.25 stresstest

# Interactive shell
docker run --rm -it rockyardkv:go1.25 shell
```

## Directory structure

```
docker/
├── Dockerfile.test      # Multi-stage build for test image
├── docker-compose.yml   # Service definitions for all test combinations
├── entrypoint.sh        # Container entrypoint with test commands
├── run-tests.sh         # Helper script for running tests
├── README.md            # This file
├── logs/                # Test output logs (git-ignored)
└── data/                # Test data directories (git-ignored)
```

## Log collection

All test output is saved to `docker/logs/` with timestamps:

```
docker/logs/
├── go125-test_20241226_143052.log
├── go125-smoketest_20241226_143105.log
├── go125-stresstest_20241226_143210.log
├── go124-test_20241226_143315.log
└── ...
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STRESS_WORKERS` | 8 | Number of concurrent stress test workers |
| `STRESS_DURATION` | 60s | Duration of stress test |
| `ROCKY_DATA_DIR` | /data | Directory for test databases |
| `ROCKY_LOG_DIR` | /logs | Directory for log output |

## Stress test investigation

After running stress tests, investigate issues:

```bash
# View stress test logs
tail -f docker/logs/go125-stresstest_*.log

# Check for errors
grep -i "error\|panic\|fail" docker/logs/*.log

# Inspect database state (if preserved)
ls -la docker/data/stresstest_*/

# Use ldb tool to inspect
docker run --rm -v $(pwd)/docker/data:/data \
  rockyardkv:go1.25 /bin/ldb \
  --db=/data/stresstest_20241226_143210 scan
```

## CI/CD integration

These Docker tests mirror what runs in GitHub Actions CI.
Use them locally to reproduce CI failures:

```bash
# Reproduce CI test failure
./docker/run-tests.sh test --go 1.24

# Run exactly what CI runs
docker compose run --rm go124-test
```

## Cleanup

```bash
# Remove test data
rm -rf docker/logs/* docker/data/*

# Remove Docker images
docker rmi rockyardkv:go1.25

# Full cleanup
docker compose down --rmi local --volumes
```
