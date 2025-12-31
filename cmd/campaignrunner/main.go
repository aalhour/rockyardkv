// Package main implements the campaign runner CLI.
//
// The campaign runner executes Jepsen-style test campaigns with:
//   - Tier-based configuration (quick for CI, nightly for thorough testing)
//   - Oracle gating (requires C++ tools for consistency verification)
//   - Failure fingerprinting and deduplication
//   - Artifact collection for debugging
//
// Usage:
//
//	campaignrunner -tier=quick -run-root=/tmp/campaign
//	campaignrunner -tier=nightly -fail-fast -run-root=/tmp/campaign
//	campaignrunner -group=status.durability -run-root=/tmp/status
//
// Groups:
//
//	stress, crash, golden - standard test campaigns
//	status.durability - durability scenarios (wal_sync, disablewal_faultfs, etc.)
//	status.adversarial - corruption attack suite
//
// Environment:
//
//	ROCKSDB_PATH: Path to RocksDB build directory containing ldb and sst_dump
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aalhour/rockyardkv/internal/campaign"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Parse flags
	tier := flag.String("tier", "quick", "Campaign tier: quick or nightly")
	group := flag.String("group", "", "Instance group to run (e.g., status.durability, stress)")
	runRoot := flag.String("run-root", "", "Root directory for run artifacts (required)")
	binDir := flag.String("bin-dir", "./bin", "Directory containing test binaries (stresstest, crashtest, etc.)")
	failFast := flag.Bool("fail-fast", false, "Stop on first failure")
	verbose := flag.Bool("v", false, "Verbose output")
	instanceTimeout := flag.Int("instance-timeout", 0, "Per-instance timeout in seconds (0 = default for tier)")
	globalTimeout := flag.Int("global-timeout", 0, "Global campaign timeout in seconds (0 = default for tier)")
	knownFailuresPath := flag.String("known-failures", "", "Path to known failures JSON file for deduplication")
	listGroups := flag.Bool("list-groups", false, "List available instance groups and exit")

	flag.Parse()

	// Handle -list-groups
	if *listGroups {
		fmt.Println("Available groups:")
		for _, g := range campaign.AllGroups() {
			fmt.Printf("  %s\n", g)
		}
		return nil
	}

	// Validate required flags
	if *runRoot == "" {
		// Default to timestamped directory under current dir
		*runRoot = filepath.Join(".", "campaign-runs", time.Now().Format("20060102-150405"))
	}

	// Parse tier
	var t campaign.Tier
	switch *tier {
	case "quick":
		t = campaign.TierQuick
	case "nightly":
		t = campaign.TierNightly
	default:
		return fmt.Errorf("unknown tier: %s (must be quick or nightly)", *tier)
	}

	// Set up oracle from environment
	oracle := campaign.NewOracleFromEnv()
	if oracle != nil && oracle.Available() {
		fmt.Printf("oracle: available at %s\n", os.Getenv("ROCKSDB_PATH"))
	} else {
		fmt.Println("oracle: not configured (set ROCKSDB_PATH)")
	}

	// Set up known failures tracker
	var kf *campaign.KnownFailures
	if *knownFailuresPath != "" {
		kf = campaign.NewKnownFailures(*knownFailuresPath)
		fmt.Printf("known-failures: loaded %d fingerprints from %s\n", kf.Count(), *knownFailuresPath)
	}

	// Build config
	config := campaign.RunnerConfig{
		Tier:            t,
		RunRoot:         *runRoot,
		BinDir:          *binDir,
		Oracle:          oracle,
		KnownFailures:   kf,
		FailFast:        *failFast,
		Verbose:         *verbose,
		Output:          os.Stdout,
		InstanceTimeout: *instanceTimeout,
		GlobalTimeout:   *globalTimeout,
	}

	runner := campaign.NewRunner(config)

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		fmt.Printf("\nreceived signal %v, cancelling campaign...\n", sig)
		cancel()
	}()

	// Run campaign
	fmt.Printf("campaign: tier=%s run-root=%s fail-fast=%v\n", t, *runRoot, *failFast)
	if *group != "" {
		fmt.Printf("group: %s\n", *group)
	}
	fmt.Println()

	var summary *campaign.CampaignSummary
	var err error

	if *group != "" {
		summary, err = runner.RunGroup(ctx, *group)
	} else {
		summary, err = runner.Run(ctx)
	}
	if err != nil {
		return err
	}

	// Print summary
	fmt.Println()
	fmt.Println("=== Campaign Summary ===")
	fmt.Printf("Tier:         %s\n", summary.Tier)
	fmt.Printf("Duration:     %s\n", time.Duration(summary.DurationMs)*time.Millisecond)
	fmt.Printf("Total Runs:   %d\n", summary.TotalRuns)
	fmt.Printf("Passed:       %d\n", summary.PassedRuns)
	fmt.Printf("Failed:       %d\n", summary.FailedRuns)
	fmt.Printf("Unique Errors: %d\n", summary.UniqueErrors)
	fmt.Printf("All Passed:   %v\n", summary.AllPassed)
	fmt.Printf("Artifacts:    %s\n", *runRoot)

	if !summary.AllPassed {
		fmt.Println("\nFailed runs:")
		for _, run := range summary.Runs {
			if !run.Passed {
				fmt.Printf("  - %s (seed %d): %s\n", run.Instance, run.Seed, run.Failure)
			}
		}
		return fmt.Errorf("campaign failed: %d/%d runs failed", summary.FailedRuns, summary.TotalRuns)
	}

	return nil
}
