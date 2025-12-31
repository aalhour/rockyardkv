// sweep.go implements sweep instances for parameter matrix expansion.
//
// A sweep instance defines a base configuration with varying parameters.
// At run time, it expands into multiple concrete instances by taking the
// Cartesian product of all parameter values.
//
// Example: cycles=[1,2,3] × mode=[sync,async] → 6 concrete runs.
package campaign

import (
	"fmt"
	"maps"
	"strings"
)

// SweepParam represents a parameter that can be varied in a sweep.
type SweepParam struct {
	// Name is the parameter name (e.g., "cycles", "mode").
	Name string

	// Values are the values to sweep over.
	Values []string
}

// SweepCase represents a single concrete case in a sweep expansion.
type SweepCase struct {
	// ID is a stable identifier for this case (e.g., "cycles_4_mode_drop").
	ID string

	// Params maps parameter names to their values for this case.
	Params map[string]string
}

// SweepInstance defines a parameterized instance that expands into multiple runs.
type SweepInstance struct {
	// Base is the base instance (used as template).
	Base Instance

	// Params are the parameters to sweep over.
	Params []SweepParam

	// Cases are the explicit cases to run (if provided, Params is ignored).
	// This allows defining arbitrary combinations rather than full cross-product.
	Cases []SweepCase
}

// Expand returns the concrete instances for this sweep.
// Each returned instance has a unique Name derived from the sweep case.
func (s *SweepInstance) Expand() []Instance {
	cases := s.Cases
	if len(cases) == 0 && len(s.Params) > 0 {
		cases = s.crossProduct()
	}

	if len(cases) == 0 {
		// No expansion needed; return base as-is
		return []Instance{s.Base}
	}

	var instances []Instance
	for _, c := range cases {
		inst := s.expandCase(c)
		instances = append(instances, inst)
	}
	return instances
}

// crossProduct generates all combinations of parameter values.
func (s *SweepInstance) crossProduct() []SweepCase {
	if len(s.Params) == 0 {
		return nil
	}

	// Start with a single empty case
	cases := []SweepCase{{Params: make(map[string]string)}}

	for _, param := range s.Params {
		var newCases []SweepCase
		for _, c := range cases {
			for _, val := range param.Values {
				newCase := SweepCase{
					Params: make(map[string]string),
				}
				maps.Copy(newCase.Params, c.Params)
				newCase.Params[param.Name] = val
				newCases = append(newCases, newCase)
			}
		}
		cases = newCases
	}

	// Generate IDs
	for i := range cases {
		cases[i].ID = s.caseID(cases[i])
	}

	return cases
}

// caseID generates a stable ID for a sweep case.
func (s *SweepInstance) caseID(c SweepCase) string {
	if c.ID != "" {
		return c.ID
	}

	var parts []string
	for _, param := range s.Params {
		if val, ok := c.Params[param.Name]; ok {
			// Sanitize value for use in path
			sanitized := strings.ReplaceAll(val, "+", "_plus_")
			sanitized = strings.ReplaceAll(sanitized, " ", "_")
			parts = append(parts, fmt.Sprintf("%s_%s", param.Name, sanitized))
		}
	}
	return strings.Join(parts, "_")
}

// expandCase creates a concrete instance for a sweep case.
func (s *SweepInstance) expandCase(c SweepCase) Instance {
	inst := s.Base

	// Update name with case ID
	inst.Name = fmt.Sprintf("%s/%s", s.Base.Name, c.ID)

	// Substitute parameters in args
	inst.Args = make([]string, len(s.Base.Args))
	for i, arg := range s.Base.Args {
		for name, val := range c.Params {
			placeholder := fmt.Sprintf("<%s>", strings.ToUpper(name))
			arg = strings.ReplaceAll(arg, placeholder, val)
		}
		inst.Args[i] = arg
	}

	return inst
}

// DisableWALFaultFSMinimizeCases returns the sweep cases for disablewal-faultfs-minimize.
// These mirror the cases in scripts/status/run_durability_repros.sh.
func DisableWALFaultFSMinimizeCases() []SweepCase {
	return []SweepCase{
		{
			ID: "drop_cycles_4",
			Params: map[string]string{
				"cycles":                  "4",
				"faultfs-drop-unsynced":   "true",
				"faultfs-delete-unsynced": "false",
			},
		},
		{
			ID: "delete_cycles_4",
			Params: map[string]string{
				"cycles":                  "4",
				"faultfs-drop-unsynced":   "false",
				"faultfs-delete-unsynced": "true",
			},
		},
		{
			ID: "drop_plus_delete_cycles_4",
			Params: map[string]string{
				"cycles":                  "4",
				"faultfs-drop-unsynced":   "true",
				"faultfs-delete-unsynced": "true",
			},
		},
		{
			ID: "drop_plus_delete_cycles_6",
			Params: map[string]string{
				"cycles":                  "6",
				"faultfs-drop-unsynced":   "true",
				"faultfs-delete-unsynced": "true",
			},
		},
	}
}
