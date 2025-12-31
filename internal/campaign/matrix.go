package campaign

// QuickInstances returns the instance matrix for the quick tier.
// Quick tier is for local development and CI on pull requests.
func QuickInstances() []Instance {
	return []Instance{
		// Stress: goroutine faults with read/corruption
		{
			Name:           "stress.read.corruption.1in7",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-writer-read=7",
				"-fault-error-type=corruption",
				"-duration=20s",
				"-flush=1s",
				"-reopen=2s",
				"-keys=5000",
				"-threads=32",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347},
			FaultModel: FaultModel{
				Kind:      FaultRead,
				ErrorType: ErrorTypeCorruption,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			Stop: DefaultStopCondition(),
		},

		// Stress: goroutine faults with read/status
		{
			Name:           "stress.read.status.1in7",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-writer-read=7",
				"-fault-error-type=status",
				"-duration=20s",
				"-flush=1s",
				"-reopen=2s",
				"-keys=5000",
				"-threads=32",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347},
			FaultModel: FaultModel{
				Kind:      FaultRead,
				ErrorType: ErrorTypeStatus,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			Stop: DefaultStopCondition(),
		},

		// Stress: goroutine faults with write/status
		{
			Name:           "stress.write.status.1in7",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-writer-write=7",
				"-fault-error-type=status",
				"-duration=20s",
				"-flush=1s",
				"-reopen=2s",
				"-keys=5000",
				"-threads=32",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347},
			FaultModel: FaultModel{
				Kind:      FaultWrite,
				ErrorType: ErrorTypeStatus,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			Stop: DefaultStopCondition(),
		},

		// Stress: goroutine faults with sync/status
		{
			Name:           "stress.sync.status.1in7",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-flusher-sync=7",
				"-fault-error-type=status",
				"-duration=20s",
				"-flush=1s",
				"-reopen=2s",
				"-keys=5000",
				"-threads=32",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347},
			FaultModel: FaultModel{
				Kind:      FaultSync,
				ErrorType: ErrorTypeStatus,
				OneIn:     7,
				Scope:     ScopeFlusher,
			},
			Stop: DefaultStopCondition(),
		},

		// Crash: blackbox crash loop
		{
			Name:           "crash.blackbox",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-cycles=3",
				"-duration=2m",
				"-sync",
				"-kill-mode=random",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
			},
			Seeds: []int64{12345},
			FaultModel: FaultModel{
				Kind: FaultCrash,
			},
			Stop: DefaultStopCondition(),
		},

		// Golden: C++ compatibility
		{
			Name:           "golden.compat",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolGolden,
			Args: []string{
				"test", "-v", "-run", "Golden", "./...",
			},
			Seeds: []int64{0}, // No seed needed for golden tests
			FaultModel: FaultModel{
				Kind: FaultNone,
			},
			Stop: StopCondition{
				RequireTermination:              true,
				RequireFinalVerificationPass:    true,
				RequireOracleCheckConsistencyOK: false, // Golden tests verify differently
				DedupeByFingerprint:             true,
			},
		},
	}
}

// NightlyInstances returns the instance matrix for the nightly tier.
// Nightly tier is for thorough testing that can run for hours.
func NightlyInstances() []Instance {
	instances := QuickInstances()

	// Add longer-running stress instances
	nightlyStress := []Instance{
		// Stress: extended duration with read/corruption
		{
			Name:           "stress.read.corruption.1in7.extended",
			Tier:           TierNightly,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-writer-read=7",
				"-fault-error-type=corruption",
				"-duration=5m",
				"-flush=1s",
				"-reopen=5s",
				"-keys=50000",
				"-threads=64",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347, 12348, 12349},
			FaultModel: FaultModel{
				Kind:      FaultRead,
				ErrorType: ErrorTypeCorruption,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			Stop: DefaultStopCondition(),
		},

		// Stress: extended duration with write/status
		{
			Name:           "stress.write.status.1in7.extended",
			Tier:           TierNightly,
			RequiresOracle: true,
			Tool:           ToolStress,
			Args: []string{
				"-goroutine-faults",
				"-fault-writer-write=7",
				"-fault-error-type=status",
				"-duration=5m",
				"-flush=1s",
				"-reopen=5s",
				"-keys=50000",
				"-threads=64",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
				"-cleanup",
				"-v",
			},
			Seeds: []int64{12345, 12346, 12347, 12348, 12349},
			FaultModel: FaultModel{
				Kind:      FaultWrite,
				ErrorType: ErrorTypeStatus,
				OneIn:     7,
				Scope:     ScopeWorker,
			},
			Stop: DefaultStopCondition(),
		},

		// Crash: extended crash loop
		{
			Name:           "crash.blackbox.extended",
			Tier:           TierNightly,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-cycles=10",
				"-duration=5m",
				"-sync",
				"-kill-mode=random",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-seed", "<SEED>",
			},
			Seeds: []int64{12345, 12346, 12347},
			FaultModel: FaultModel{
				Kind: FaultCrash,
			},
			Stop: DefaultStopCondition(),
		},
	}

	return append(instances, nightlyStress...)
}

// GetInstances returns the instances for the specified tier.
// Includes both campaign instances (stress, crash, golden) and status instances (durability, adversarial).
func GetInstances(tier Tier) []Instance {
	var base []Instance
	switch tier {
	case TierQuick:
		base = QuickInstances()
	case TierNightly:
		base = NightlyInstances()
	default:
		base = QuickInstances()
	}

	// Include status instances that match the tier
	for _, inst := range StatusInstances() {
		if inst.Tier == tier || inst.Tier == TierQuick {
			base = append(base, inst)
		}
	}

	return base
}
