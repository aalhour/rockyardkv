#!/bin/bash
# RockyardKV Docker Test Runner
#
# This script builds and runs tests across multiple Go versions,
# collects logs, and provides a summary report.
#
# Usage:
#   ./docker/run-tests.sh                # Run all tests
#   ./docker/run-tests.sh smoketest      # Run smoke tests only
#   ./docker/run-tests.sh stresstest     # Run stress tests only
#   ./docker/run-tests.sh --go 1.25      # Test specific Go version only

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$SCRIPT_DIR/logs"
DATA_DIR="$SCRIPT_DIR/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default settings
GO_VERSIONS=("1.25")  # Minimum required: Go 1.25
TEST_TYPE="all"  # all, test, smoketest, stresstest
STRESS_WORKERS=8
STRESS_DURATION="60s"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        test|smoketest|stresstest)
            TEST_TYPE="$1"
            shift
            ;;
        --go)
            GO_VERSIONS=("$2")
            shift 2
            ;;
        --workers)
            STRESS_WORKERS="$2"
            shift 2
            ;;
        --duration)
            STRESS_DURATION="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [test|smoketest|stresstest] [options]"
            echo ""
            echo "Commands:"
            echo "  test       Run unit tests"
            echo "  smoketest  Run smoke tests"
            echo "  stresstest Run stress tests"
            echo "  (none)     Run all tests"
            echo ""
            echo "Options:"
            echo "  --go VERSION      Test specific Go version (e.g., 1.25)"
            echo "  --workers N       Number of stress test workers (default: 8)"
            echo "  --duration TIME   Stress test duration (default: 60s)"
            echo "  --help            Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create directories
mkdir -p "$LOG_DIR" "$DATA_DIR"

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           RockyardKV Docker Test Runner                       ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Go versions: ${GO_VERSIONS[*]}"
echo "  Test type: $TEST_TYPE"
echo "  Log directory: $LOG_DIR"
echo "  Timestamp: $TIMESTAMP"
echo ""

# Track results
declare -A RESULTS

run_test() {
    local go_version=$1
    local test_type=$2
    local service="go${go_version//./}-${test_type}"
    
    echo -e "${BLUE}▶ Running ${test_type} tests with Go ${go_version}...${NC}"
    
    # Build image
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" build \
        --build-arg GO_VERSION="$go_version" \
        "go${go_version//./}-test" 2>&1 | tail -5
    
    # Run test
    local log_file="$LOG_DIR/${service}_${TIMESTAMP}.log"
    
    if docker compose -f "$SCRIPT_DIR/docker-compose.yml" run --rm \
        -e STRESS_WORKERS="$STRESS_WORKERS" \
        -e STRESS_DURATION="$STRESS_DURATION" \
        "$service" 2>&1 | tee "$log_file"; then
        RESULTS["$service"]="PASS"
        echo -e "${GREEN}✓ ${service} PASSED${NC}"
    else
        RESULTS["$service"]="FAIL"
        echo -e "${RED}✗ ${service} FAILED${NC}"
    fi
    echo ""
}

# Run tests
for go_version in "${GO_VERSIONS[@]}"; do
    case $TEST_TYPE in
        all)
            run_test "$go_version" "test"
            run_test "$go_version" "smoketest"
            ;;
        *)
            run_test "$go_version" "$TEST_TYPE"
            ;;
    esac
done

# Summary
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                         Test Summary                              ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

PASSED=0
FAILED=0

for service in "${!RESULTS[@]}"; do
    result="${RESULTS[$service]}"
    if [[ "$result" == "PASS" ]]; then
        echo -e "  ${GREEN}✓${NC} $service"
        ((PASSED++))
    else
        echo -e "  ${RED}✗${NC} $service"
        ((FAILED++))
    fi
done

echo ""
echo -e "Total: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "Logs: $LOG_DIR"
echo ""

# List log files
echo -e "${YELLOW}Log files:${NC}"
ls -la "$LOG_DIR"/*.log 2>/dev/null | tail -10 || echo "  (no logs yet)"

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo -e "${RED}Some tests failed. Check logs for details.${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}All tests passed!${NC}"

