#!/bin/sh
# RockyardKV Docker Entrypoint
#
# Commands:
#   test       - Run unit tests
#   smoketest  - Run smoke tests  
#   stresstest - Run stress tests
#   shell      - Drop into shell

set -e

LOG_DIR="${ROCKY_LOG_DIR:-/logs}"
DATA_DIR="${ROCKY_DATA_DIR:-/data}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

case "$1" in
    test)
        echo "=== RockyardKV Unit Tests ==="
        echo "Go version: $(go version)"
        echo "Timestamp: $TIMESTAMP"
        echo ""
        
        go test -v -race -timeout 10m ./... 2>&1 | tee "$LOG_DIR/test_${TIMESTAMP}.log"
        
        echo ""
        echo "=== Tests Complete ==="
        echo "Log: $LOG_DIR/test_${TIMESTAMP}.log"
        ;;
        
    smoketest)
        echo "=== RockyardKV Smoke Tests ==="
        echo "Go version: $(go version)"
        echo "Timestamp: $TIMESTAMP"
        echo ""
        
        /bin/smoketest -v 2>&1 | tee "$LOG_DIR/smoketest_${TIMESTAMP}.log"
        
        echo ""
        echo "=== Smoke Tests Complete ==="
        echo "Log: $LOG_DIR/smoketest_${TIMESTAMP}.log"
        ;;
        
    stresstest)
        WORKERS="${STRESS_WORKERS:-8}"
        DURATION="${STRESS_DURATION:-60s}"
        
        echo "=== RockyardKV Stress Tests ==="
        echo "Go version: $(go version)"
        echo "Workers: $WORKERS"
        echo "Duration: $DURATION"
        echo "Data dir: $DATA_DIR"
        echo "Timestamp: $TIMESTAMP"
        echo ""
        
        /bin/stresstest \
            -workers="$WORKERS" \
            -duration="$DURATION" \
            -dir="$DATA_DIR/stresstest_${TIMESTAMP}" \
            -v 2>&1 | tee "$LOG_DIR/stresstest_${TIMESTAMP}.log"
        
        echo ""
        echo "=== Stress Tests Complete ==="
        echo "Log: $LOG_DIR/stresstest_${TIMESTAMP}.log"
        echo "Data: $DATA_DIR/stresstest_${TIMESTAMP}"
        ;;
        
    bench)
        echo "=== RockyardKV Benchmarks ==="
        echo "Go version: $(go version)"
        echo "Timestamp: $TIMESTAMP"
        echo ""
        
        go test -bench=. -benchmem ./... 2>&1 | tee "$LOG_DIR/bench_${TIMESTAMP}.log"
        
        echo ""
        echo "=== Benchmarks Complete ==="
        echo "Log: $LOG_DIR/bench_${TIMESTAMP}.log"
        ;;
        
    shell)
        exec /bin/sh
        ;;
        
    *)
        echo "Usage: $0 {test|smoketest|stresstest|bench|shell}"
        echo ""
        echo "Environment variables:"
        echo "  STRESS_WORKERS   - Number of concurrent workers (default: 8)"
        echo "  STRESS_DURATION  - Test duration (default: 60s)"
        echo "  ROCKY_DATA_DIR   - Data directory (default: /data)"
        echo "  ROCKY_LOG_DIR    - Log directory (default: /logs)"
        exit 1
        ;;
esac
