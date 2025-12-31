package campaign

// Tags represents the structured tag set for an instance.
// Required tags are always present; optional tags use the Extra map.
type Tags struct {
	// Campaign is the campaign identifier (e.g., "C05").
	Campaign string `json:"campaign"`

	// Tier is the execution tier (quick/nightly).
	Tier string `json:"tier"`

	// Tool is the binary used (stresstest/crashtest/goldentest/adversarialtest/sstdump).
	Tool string `json:"tool"`

	// Kind is the high-level category (stress/crash/golden/status/adversarial).
	Kind string `json:"kind"`

	// OracleRequired indicates if the instance requires C++ oracle tools.
	OracleRequired bool `json:"oracle_required"`

	// Group is the group prefix (e.g., "status.durability").
	Group string `json:"group"`

	// FaultKind is the fault injection kind (none/read/write/sync/crash/corrupt).
	FaultKind string `json:"fault_kind"`

	// FaultScope is the fault injection scope (worker/flusher/reopener/global).
	FaultScope string `json:"fault_scope"`

	// Extra contains optional instance-specific metadata.
	Extra map[string]string `json:"extra,omitempty"`
}

// AllTagKeys returns all valid tag keys for filter validation.
func AllTagKeys() []string {
	return []string{
		"campaign",
		"tier",
		"tool",
		"kind",
		"oracle_required",
		"group",
		"fault_kind",
		"fault_scope",
	}
}

// ComputeTags derives the Tags from an Instance.
func (i *Instance) ComputeTags() Tags {
	// Derive kind from tool or name prefix
	kind := deriveKind(i.Tool, i.Name)

	// Derive group from name
	group := deriveGroup(i.Name)

	return Tags{
		Campaign:       "C05", // All current instances are C05
		Tier:           string(i.Tier),
		Tool:           string(i.Tool),
		Kind:           kind,
		OracleRequired: i.RequiresOracle,
		Group:          group,
		FaultKind:      string(i.FaultModel.Kind),
		FaultScope:     string(i.FaultModel.Scope),
		Extra:          nil,
	}
}

// deriveKind determines the high-level category from tool and name.
func deriveKind(tool Tool, name string) string {
	switch tool {
	case ToolStress:
		if len(name) >= 6 && name[:6] == "status" {
			return "status"
		}
		return "stress"
	case ToolCrash:
		if len(name) >= 6 && name[:6] == "status" {
			return "status"
		}
		return "crash"
	case ToolGolden:
		return "golden"
	case ToolAdversarial:
		if len(name) >= 6 && name[:6] == "status" {
			return "status"
		}
		return "adversarial"
	case ToolSSTDump:
		return "status" // sstdump is typically used in status composite steps
	default:
		return "unknown"
	}
}

// deriveGroup extracts the group prefix from an instance name.
func deriveGroup(name string) string {
	// Group is the first two dot-separated segments for status instances
	// e.g., "status.durability.wal_sync" -> "status.durability"
	// For non-status, use the first segment
	// e.g., "stress.read.corruption" -> "stress"

	dotCount := 0
	for i, c := range name {
		if c == '.' {
			dotCount++
			if dotCount == 2 {
				return name[:i]
			}
		}
	}

	// If less than 2 dots, find first dot
	for i, c := range name {
		if c == '.' {
			return name[:i]
		}
	}

	return name
}

// Get returns the value of a tag by key.
// Returns empty string for unknown keys or unset values.
func (t Tags) Get(key string) string {
	switch key {
	case "campaign":
		return t.Campaign
	case "tier":
		return t.Tier
	case "tool":
		return t.Tool
	case "kind":
		return t.Kind
	case "oracle_required":
		if t.OracleRequired {
			return "true"
		}
		return "false"
	case "group":
		return t.Group
	case "fault_kind":
		return t.FaultKind
	case "fault_scope":
		return t.FaultScope
	default:
		if t.Extra != nil {
			return t.Extra[key]
		}
		return ""
	}
}
