// status.go defines status/durability repro instances.
//
// These instances reproduce specific durability and consistency scenarios
// documented in docs/status/durability_report.md. They serve as regression
// tests for known failure modes and recovery behaviors.
package campaign

// StatusInstances returns the simple instance matrix for status/durability checks.
// For composite instances (multi-step), see StatusCompositeInstances().
// For sweep instances (parameter expansion), see StatusSweepInstances().
func StatusInstances() []Instance {
	return []Instance{
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
		"status.durability",
		"status.adversarial",
		"status.composite",
		"status.sweep",
	}
}

// StatusCompositeInstances returns composite (multi-step) instances.
// These instances execute multiple steps with a gating policy.
func StatusCompositeInstances() []CompositeInstance {
	return []CompositeInstance{
		// Internal-key collision check (both steps must pass)
		{
			Instance: Instance{
				Name:           "status.composite.internal_key_collision",
				Tier:           TierQuick,
				RequiresOracle: true,
				Seeds:          []int64{8201},
				FaultModel: FaultModel{
					Kind:  FaultCrash,
					Scope: ScopeGlobal,
				},
				Stop: DefaultStopCondition(),
			},
			Steps: []Step{
				{
					Name:           "crashtest",
					Tool:           ToolCrash,
					RequiresOracle: false,
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
					DiscoverDBPath: true,
				},
				{
					Name:           "collision-check",
					Tool:           ToolSSTDump,
					RequiresOracle: true,
					Args: []string{
						"--command=collision-check",
						"--dir", "<DB_DIR>",
						"--max-collisions=1",
					},
				},
			},
			GatingPolicy: GateAllSteps,
		},

		// Internal-key collision check (only collision-check gates)
		{
			Instance: Instance{
				Name:           "status.composite.internal_key_collision_only",
				Tier:           TierQuick,
				RequiresOracle: true,
				Seeds:          []int64{8201},
				FaultModel: FaultModel{
					Kind:  FaultCrash,
					Scope: ScopeGlobal,
				},
				Stop: DefaultStopCondition(),
			},
			Steps: []Step{
				{
					Name:           "crashtest",
					Tool:           ToolCrash,
					RequiresOracle: false,
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
					DiscoverDBPath: true,
				},
				{
					Name:           "collision-check",
					Tool:           ToolSSTDump,
					RequiresOracle: true,
					Args: []string{
						"--command=collision-check",
						"--dir", "<DB_DIR>",
						"--max-collisions=1",
					},
				},
			},
			GatingPolicy: GateLastStep,
		},
	}
}

// StatusSweepInstances returns sweep (parameter expansion) instances.
// These instances expand into multiple concrete runs.
func StatusSweepInstances() []SweepInstance {
	return []SweepInstance{
		// DisableWAL + faultfs minimization sweep
		{
			Base: Instance{
				Name:           "status.sweep.disablewal_faultfs_minimize",
				Tier:           TierNightly,
				RequiresOracle: true,
				Tool:           ToolCrash,
				Args: []string{
					"-seed", "<SEED>",
					"-cycles=<CYCLES>",
					"-duration=8m",
					"-interval=6s",
					"-min-interval=0.5s",
					"-kill-mode=sigterm",
					"-disable-wal",
					"-faultfs",
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
			Cases: disableWALFaultFSMinimizeCases(),
		},
	}
}

// disableWALFaultFSMinimizeCases returns the sweep cases for disablewal-faultfs-minimize.
func disableWALFaultFSMinimizeCases() []SweepCase {
	return []SweepCase{
		{
			ID: "drop_cycles_4",
			Params: map[string]string{
				"CYCLES":                  "4",
				"FAULTFS_DROP_UNSYNCED":   "-faultfs-drop-unsynced",
				"FAULTFS_DELETE_UNSYNCED": "",
			},
		},
		{
			ID: "delete_cycles_4",
			Params: map[string]string{
				"CYCLES":                  "4",
				"FAULTFS_DROP_UNSYNCED":   "",
				"FAULTFS_DELETE_UNSYNCED": "-faultfs-delete-unsynced",
			},
		},
		{
			ID: "drop_plus_delete_cycles_4",
			Params: map[string]string{
				"CYCLES":                  "4",
				"FAULTFS_DROP_UNSYNCED":   "-faultfs-drop-unsynced",
				"FAULTFS_DELETE_UNSYNCED": "-faultfs-delete-unsynced",
			},
		},
		{
			ID: "drop_plus_delete_cycles_6",
			Params: map[string]string{
				"CYCLES":                  "6",
				"FAULTFS_DROP_UNSYNCED":   "-faultfs-drop-unsynced",
				"FAULTFS_DELETE_UNSYNCED": "-faultfs-delete-unsynced",
			},
		},
	}
}
