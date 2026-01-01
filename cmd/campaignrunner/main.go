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
	"encoding/json"
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
	skipPoliciesPath := flag.String("skip-policies", "", "Path to instance-level skip policies JSON file (optional)")
	listGroups := flag.Bool("list-groups", false, "List available instance groups and exit")

	// Trace and minimization flags
	captureTrace := flag.Bool("capture-trace", false, "Enable trace capture for stresstest runs")
	minimize := flag.Bool("minimize", false, "Enable minimization for new failures (auto-enables trace)")
	traceMaxSize := flag.Int64("trace-max-size", 256*1024*1024, "Maximum trace file size in bytes (default 256MB)")

	// Introspection and recheck flags
	listInstances := flag.Bool("list-instances", false, "List available instances and exit")
	listJSON := flag.Bool("json", false, "Output in JSON format (with -list-instances)")
	filter := flag.String("filter", "", "Filter instances (e.g., tier=quick,oracle_required=true)")
	recheck := flag.String("recheck", "", "Recheck mode: re-evaluate existing artifacts at the given run root")

	// Governance flags
	requireQuarantine := flag.Bool("require-quarantine", false, "Fail if any unquarantined repeat failure is detected")

	flag.Parse()

	// Handle -list-groups
	if *listGroups {
		fmt.Println("Available groups:")
		for _, g := range campaign.AllGroups() {
			fmt.Printf("  %s\n", g)
		}
		return nil
	}

	// Handle -list-instances
	if *listInstances {
		return handleListInstances(*tier, *filter, *listJSON)
	}

	// Handle -recheck
	if *recheck != "" {
		return handleRecheck(*recheck)
	}

	// Validate required flags
	if *runRoot == "" {
		// Default to a timestamped directory under the OS temp directory to avoid
		// creating repo-local artifact directories when -run-root is omitted.
		*runRoot = filepath.Join(os.TempDir(), "rockyardkv-campaign-runs", time.Now().Format("20060102-150405"))
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

	// Build trace config
	traceConfig := campaign.DefaultTraceConfig()
	traceConfig.Enabled = *captureTrace || *minimize // Auto-enable with minimize
	traceConfig.MaxSizeBytes = *traceMaxSize

	// Build minimize config
	minimizeConfig := campaign.DefaultMinimizeConfig()
	minimizeConfig.Enabled = *minimize

	// Parse filter if provided
	var filterConfig *campaign.Filter
	if *filter != "" {
		var err error
		filterConfig, err = campaign.ParseFilter(*filter)
		if err != nil {
			return fmt.Errorf("invalid filter: %w", err)
		}
		fmt.Printf("filter: %s\n", *filter)
	}

	// Build config
	var skipPolicies *campaign.InstanceSkipPolicies
	if *skipPoliciesPath != "" {
		skipPolicies = campaign.NewInstanceSkipPolicies(*skipPoliciesPath)
		if err := skipPolicies.LoadWithValidation(); err != nil {
			return fmt.Errorf("invalid skip policies file: %w", err)
		}
		fmt.Printf("skip-policies: loaded %d policies from %s\n", skipPolicies.Count(), *skipPoliciesPath)
	}

	config := campaign.RunnerConfig{
		Tier:              t,
		RunRoot:           *runRoot,
		BinDir:            *binDir,
		Oracle:            oracle,
		KnownFailures:     kf,
		FailFast:          *failFast,
		Verbose:           *verbose,
		Output:            os.Stdout,
		InstanceTimeout:   *instanceTimeout,
		GlobalTimeout:     *globalTimeout,
		Trace:             traceConfig,
		Minimize:          minimizeConfig,
		Filter:            filterConfig,
		RequireQuarantine: *requireQuarantine,
		SkipPolicies:      skipPolicies,
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
	if traceConfig.Enabled {
		fmt.Printf("trace: enabled (max %d MB)\n", traceConfig.MaxSizeBytes/(1024*1024))
	}
	if minimizeConfig.Enabled {
		fmt.Printf("minimize: enabled (bounds: duration=%s threads=%d keys=%d)\n",
			minimizeConfig.Bounds.MinDuration, minimizeConfig.Bounds.MinThreads, minimizeConfig.Bounds.MinKeys)
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

	// Print failure classification stats for triage
	if summary.FailedRuns > 0 {
		fmt.Println("\n--- Failure Classification ---")
		fmt.Printf("New failures:    %d\n", summary.NewFailures)
		fmt.Printf("Known failures:  %d (quarantined)\n", summary.KnownFailures)
		fmt.Printf("Duplicates:      %d\n", summary.Duplicates)
		if summary.Unquarantined > 0 {
			fmt.Printf("⚠️  Unquarantined: %d (require quarantine mapping)\n", summary.Unquarantined)
		}
	}

	if !summary.AllPassed {
		fmt.Println("\nFailed runs:")
		for _, run := range summary.Runs {
			if !run.Passed {
				classStr := ""
				if run.FailureClass != "" {
					classStr = fmt.Sprintf(" [%s]", run.FailureClass)
				}
				fmt.Printf("  - %s (seed %d)%s: %s\n", run.Instance, run.Seed, classStr, run.Failure)
			}
		}

		// Check quarantine enforcement
		if *requireQuarantine && summary.Unquarantined > 0 {
			return fmt.Errorf("campaign failed: %d unquarantined repeat failures (require quarantine mapping)", summary.Unquarantined)
		}

		return fmt.Errorf("campaign failed: %d/%d runs failed", summary.FailedRuns, summary.TotalRuns)
	}

	return nil
}

// handleListInstances lists available instances with their tags.
func handleListInstances(tierStr, filterStr string, asJSON bool) error {
	// Get instances for the tier
	var t campaign.Tier
	switch tierStr {
	case "quick":
		t = campaign.TierQuick
	case "nightly":
		t = campaign.TierNightly
	default:
		t = campaign.TierQuick
	}

	instances := campaign.GetInstances(t)

	// Apply filter if provided
	if filterStr != "" {
		f, err := campaign.ParseFilter(filterStr)
		if err != nil {
			return fmt.Errorf("parse filter: %w", err)
		}
		instances = campaign.FilterInstances(instances, f)
	}

	if asJSON {
		// JSON output with full tags
		type instanceWithTags struct {
			Name string        `json:"name"`
			Tags campaign.Tags `json:"tags"`
		}

		output := make([]instanceWithTags, len(instances))
		for i, inst := range instances {
			output[i] = instanceWithTags{
				Name: inst.Name,
				Tags: inst.ComputeTags(),
			}
		}

		data, err := json.MarshalIndent(output, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
	} else {
		// Human-readable output
		fmt.Printf("Instances (%d):\n", len(instances))
		for _, inst := range instances {
			tags := inst.ComputeTags()
			fmt.Printf("  %s\n", inst.Name)
			fmt.Printf("    tier=%s tool=%s kind=%s oracle=%v\n",
				tags.Tier, tags.Tool, tags.Kind, tags.OracleRequired)
			if tags.FaultKind != "" && tags.FaultKind != "none" {
				fmt.Printf("    fault=%s/%s\n", tags.FaultKind, tags.FaultScope)
			}
		}
	}

	return nil
}

// handleRecheck re-evaluates existing run artifacts.
func handleRecheck(runRoot string) error {
	// Check if run root exists
	if _, err := os.Stat(runRoot); os.IsNotExist(err) {
		return fmt.Errorf("run root does not exist: %s", runRoot)
	}

	// Set up oracle
	oracle := campaign.NewOracleFromEnv()
	if oracle != nil && oracle.Available() {
		fmt.Printf("oracle: available at %s\n", os.Getenv("ROCKSDB_PATH"))
	} else {
		fmt.Println("oracle: not configured (results will be marked NOT VERIFIED for oracle-required runs)")
	}

	rechecker := campaign.NewRechecker(oracle)

	fmt.Printf("Rechecking artifacts at: %s\n", runRoot)
	results, err := rechecker.RecheckCampaign(runRoot)
	if err != nil {
		return fmt.Errorf("recheck campaign: %w", err)
	}

	// Print summary
	passed := 0
	failed := 0
	notVerified := 0

	for _, r := range results {
		if r.PolicyResult != nil {
			if !r.PolicyResult.Verified {
				notVerified++
			} else if r.PolicyResult.Passed {
				passed++
			} else {
				failed++
			}
		}
	}

	fmt.Println()
	fmt.Println("=== Recheck Summary ===")
	fmt.Printf("Total Runs:   %d\n", len(results))
	fmt.Printf("Passed:       %d\n", passed)
	fmt.Printf("Failed:       %d\n", failed)
	fmt.Printf("Not Verified: %d\n", notVerified)
	fmt.Printf("Schema:       %s\n", campaign.SchemaVersion)

	if failed > 0 {
		return fmt.Errorf("recheck found %d failures", failed)
	}

	return nil
}
