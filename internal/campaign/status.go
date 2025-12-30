package campaign

// StatusInstances returns the instance matrix for status/durability checks.
// These instances mirror the existing scripts/status/run_durability_repros.sh scenarios.
func StatusInstances() []Instance {
	return []Instance{
		// Golden compatibility (go test)
		{
			Name:           "status.golden",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolGolden,
			Args: []string{
				"test", "-v", "-run", "Golden", "./...",
			},
			Seeds: []int64{0},
			FaultModel: FaultModel{
				Kind: FaultNone,
			},
			Stop: StopCondition{
				RequireTermination:              true,
				RequireFinalVerificationPass:    true,
				RequireOracleCheckConsistencyOK: false,
				DedupeByFingerprint:             true,
			},
		},

		// WAL+sync crash durability
		{
			Name:           "status.durability.wal_sync",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-seed", "<SEED>",
				"-cycles=5",
				"-duration=6m",
				"-interval=10s",
				"-min-interval=2s",
				"-kill-mode=sigkill",
				"-sync",
				"-db", "<RUN_DIR>/db_sync",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-keep",
				"-v",
			},
			Seeds: []int64{9101},
			FaultModel: FaultModel{
				Kind: FaultCrash,
			},
			Stop: DefaultStopCondition(),
		},

		// WAL+sync sweep (multiple seeds)
		{
			Name:           "status.durability.wal_sync_sweep",
			Tier:           TierNightly,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-seed", "<SEED>",
				"-cycles=5",
				"-duration=6m",
				"-interval=10s",
				"-min-interval=2s",
				"-kill-mode=sigkill",
				"-sync",
				"-db", "<RUN_DIR>/db_sync",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-keep",
				"-v",
			},
			Seeds: []int64{9101, 9102, 9103, 9104, 9105, 9106, 9107, 9108},
			FaultModel: FaultModel{
				Kind: FaultCrash,
			},
			Stop: DefaultStopCondition(),
		},

		// DisableWAL + faultfs crash durability
		{
			Name:           "status.durability.disablewal_faultfs",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-seed", "<SEED>",
				"-cycles=25",
				"-duration=8m",
				"-interval=6s",
				"-min-interval=0.5s",
				"-kill-mode=sigterm",
				"-disable-wal",
				"-faultfs",
				"-faultfs-drop-unsynced",
				"-faultfs-delete-unsynced",
				"-db", "<RUN_DIR>/db_faultfs_disable_wal",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-keep",
				"-v",
			},
			Seeds: []int64{8201},
			FaultModel: FaultModel{
				Kind:  FaultCrash,
				Scope: ScopeGlobal,
			},
			Stop: DefaultStopCondition(),
		},

		// Adversarial corruption suite
		{
			Name:           "status.adversarial.corruption",
			Tier:           TierQuick,
			RequiresOracle: false,
			Tool:           ToolAdversarial,
			Args: []string{
				"-category=corruption",
				"-seed", "<SEED>",
				"-duration=30s",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-keep",
			},
			Seeds: []int64{777},
			FaultModel: FaultModel{
				Kind: FaultCorrupt,
			},
			Stop: StopCondition{
				RequireTermination:              true,
				RequireFinalVerificationPass:    true,
				RequireOracleCheckConsistencyOK: false,
				DedupeByFingerprint:             true,
			},
		},

		// Internal-key collision check
		{
			Name:           "status.durability.internal_key_collision",
			Tier:           TierQuick,
			RequiresOracle: true,
			Tool:           ToolCrash,
			Args: []string{
				"-seed", "<SEED>",
				"-cycles=4",
				"-duration=3m",
				"-interval=6s",
				"-min-interval=0.5s",
				"-kill-mode=sigterm",
				"-crash-schedule=5.327s,10.839s,10.547s,10.065s",
				"-disable-wal",
				"-faultfs",
				"-faultfs-drop-unsynced",
				"-faultfs-delete-unsynced",
				"-db", "<RUN_DIR>/db",
				"-run-dir", "<RUN_DIR>/artifacts",
				"-trace-dir", "<RUN_DIR>/traces",
				"-keep",
				"-v",
			},
			Seeds: []int64{8201},
			FaultModel: FaultModel{
				Kind:  FaultCrash,
				Scope: ScopeGlobal,
			},
			Stop: DefaultStopCondition(),
		},
	}
}

// GetStatusInstances returns status instances filtered by group prefix.
// If group is empty, returns all status instances.
func GetStatusInstances(group string) []Instance {
	all := StatusInstances()
	if group == "" {
		return all
	}

	var filtered []Instance
	for _, inst := range all {
		if matchesGroup(inst.Name, group) {
			filtered = append(filtered, inst)
		}
	}
	return filtered
}

// matchesGroup returns true if the instance name matches the group prefix.
func matchesGroup(name, group string) bool {
	if len(name) < len(group) {
		return false
	}
	return name[:len(group)] == group
}

// AllGroups returns all available instance groups.
func AllGroups() []string {
	return []string{
		"stress",
		"crash",
		"golden",
		"status.golden",
		"status.durability",
		"status.adversarial",
	}
}
