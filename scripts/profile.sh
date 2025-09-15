#!/bin/bash
# Profiling script for RockyardKV
#
# Usage:
#   ./scripts/profile.sh cpu    # Generate CPU profile
#   ./scripts/profile.sh mem    # Generate memory profile
#   ./scripts/profile.sh bench  # Run benchmarks with profiles
#   ./scripts/profile.sh all    # Generate all profiles

set -e

PROFILE_DIR="profiles"
mkdir -p "$PROFILE_DIR"

case "$1" in
    cpu)
        echo "🔍 Running CPU profile..."
        go test ./db/... -bench=BenchmarkDBPut -run="^$" \
            -cpuprofile="$PROFILE_DIR/cpu.pprof" \
            -benchtime=10s
        echo ""
        echo "📊 CPU profile saved to $PROFILE_DIR/cpu.pprof"
        echo "View with: go tool pprof -http=:8080 $PROFILE_DIR/cpu.pprof"
        ;;
    
    mem)
        echo "🔍 Running memory profile..."
        go test ./db/... -bench=BenchmarkDBPut -run="^$" \
            -memprofile="$PROFILE_DIR/mem.pprof" \
            -memprofilerate=1 \
            -benchtime=5s
        echo ""
        echo "📊 Memory profile saved to $PROFILE_DIR/mem.pprof"
        echo "View with: go tool pprof -http=:8080 $PROFILE_DIR/mem.pprof"
        ;;
    
    alloc)
        echo "🔍 Running allocation profile..."
        go test ./db/... -bench=. -run="^$" \
            -memprofile="$PROFILE_DIR/alloc.pprof" \
            -memprofilerate=1 \
            -benchtime=2s \
            -benchmem 2>&1 | tee "$PROFILE_DIR/bench_alloc.txt"
        echo ""
        echo "📊 Allocation profile saved to $PROFILE_DIR/alloc.pprof"
        ;;
    
    block)
        echo "🔍 Running block (contention) profile..."
        go test ./db/... -bench=BenchmarkConcurrent -run="^$" \
            -blockprofile="$PROFILE_DIR/block.pprof" \
            -benchtime=5s
        echo ""
        echo "📊 Block profile saved to $PROFILE_DIR/block.pprof"
        ;;
    
    mutex)
        echo "🔍 Running mutex profile..."
        go test ./db/... -bench=BenchmarkConcurrent -run="^$" \
            -mutexprofile="$PROFILE_DIR/mutex.pprof" \
            -benchtime=5s
        echo ""
        echo "📊 Mutex profile saved to $PROFILE_DIR/mutex.pprof"
        ;;
    
    trace)
        echo "🔍 Running execution trace..."
        go test ./db/... -bench=BenchmarkMixed -run="^$" \
            -trace="$PROFILE_DIR/trace.out" \
            -benchtime=2s
        echo ""
        echo "📊 Trace saved to $PROFILE_DIR/trace.out"
        echo "View with: go tool trace $PROFILE_DIR/trace.out"
        ;;
    
    bench)
        echo "🏃 Running all benchmarks..."
        go test ./... -bench=. -run="^$" -benchmem 2>&1 | tee "$PROFILE_DIR/benchmarks.txt"
        echo ""
        echo "📊 Benchmark results saved to $PROFILE_DIR/benchmarks.txt"
        ;;
    
    all)
        echo "🔍 Running all profiles..."
        $0 cpu
        $0 mem
        $0 block
        $0 bench
        echo ""
        echo "✅ All profiles generated in $PROFILE_DIR/"
        ;;
    
    analyze)
        if [ -z "$2" ]; then
            echo "Usage: $0 analyze <profile.pprof>"
            exit 1
        fi
        echo "🔍 Analyzing profile: $2"
        go tool pprof -http=:8080 "$2"
        ;;
    
    top)
        if [ -z "$2" ]; then
            echo "Usage: $0 top <profile.pprof>"
            exit 1
        fi
        echo "🔍 Top functions in profile: $2"
        go tool pprof -top "$2"
        ;;
    
    *)
        echo "RockyardKV Profiling Script"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  cpu      - Generate CPU profile"
        echo "  mem      - Generate memory profile"
        echo "  alloc    - Generate allocation profile with benchmark"
        echo "  block    - Generate block (contention) profile"
        echo "  mutex    - Generate mutex profile"
        echo "  trace    - Generate execution trace"
        echo "  bench    - Run all benchmarks"
        echo "  all      - Generate all profiles"
        echo "  analyze  - Open profile in browser (pprof web UI)"
        echo "  top      - Show top functions in profile"
        echo ""
        echo "Example:"
        echo "  $0 cpu && $0 analyze profiles/cpu.pprof"
        ;;
esac

