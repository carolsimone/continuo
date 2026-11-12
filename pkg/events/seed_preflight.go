package events

import (
	"slices"

	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

// SeedPreflight is the check a seed task's dispatch asks for: execution-controller
// runs it in the pod, before the seed's load, against Relation. Mode is check or
// report, EngineFamily the install's warehouse family, Allowed the relations of
// the views the run rebuilds and Acknowledged the objects an operator named. Both
// lists render as arrays, `[]` when empty.
type SeedPreflight struct {
	Mode         string   `json:"mode"`
	EngineFamily string   `json:"engine_family"`
	Relation     string   `json:"relation"`
	Allowed      []string `json:"allowed"`
	Acknowledged []string `json:"acknowledged"`
}

// SeedPreflightOf renders p for the wire; nil in, nil out.
func SeedPreflightOf(p *seedreload.Preflight) *SeedPreflight {
	if p == nil {
		return nil
	}
	return &SeedPreflight{
		Mode:         string(p.Mode),
		EngineFamily: string(p.Family),
		Relation:     p.Relation,
		Allowed:      nonNil(p.Allowed),
		Acknowledged: nonNil(p.Acknowledged),
	}
}

// ToDomain is the preflight spec the entry carries; nil in, nil out.
func (p *SeedPreflight) ToDomain() *seedreload.Preflight {
	if p == nil {
		return nil
	}
	return &seedreload.Preflight{
		Mode:         seedreload.PreflightMode(p.Mode),
		Family:       warehouse.Family(p.EngineFamily),
		Relation:     p.Relation,
		Allowed:      slices.Clone(p.Allowed),
		Acknowledged: slices.Clone(p.Acknowledged),
	}
}

// SeedPreflightFindings is what a seed's preflight found in the warehouse, as
// the seed task's final report carries it. Probes are the answers of the probe
// of each rebuilt view, left out when no probe ran.
type SeedPreflightFindings struct {
	DropSemantics string               `json:"drop_semantics"`
	Completeness  string               `json:"completeness"`
	Checked       bool                 `json:"checked"`
	Dependents    []PreflightDependent `json:"dependents"`
	Probes        []ViewProbe          `json:"probes,omitempty"`
}

// PreflightDependent is one object in the warehouse that depends on the seed's table.
type PreflightDependent struct {
	Schema  string `json:"schema"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Owner   string `json:"owner"`
	Depth   int    `json:"depth"`
	Allowed bool   `json:"allowed"`
}

// ViewProbe is the answer of one view's probe after a rebuild.
type ViewProbe struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

// SeedPreflightFindingsOf renders f for the wire; nil in, nil out. The
// dependents render as an array, `[]` when there are none.
func SeedPreflightFindingsOf(f *seedreload.Findings) *SeedPreflightFindings {
	if f == nil {
		return nil
	}
	out := &SeedPreflightFindings{
		DropSemantics: f.DropSemantics,
		Completeness:  f.Completeness,
		Checked:       f.Checked,
		Dependents:    make([]PreflightDependent, 0, len(f.Dependents)),
	}
	for _, d := range f.Dependents {
		out.Dependents = append(out.Dependents, PreflightDependent(d))
	}
	for _, p := range f.Probes {
		out.Probes = append(out.Probes, ViewProbe(p))
	}
	return out
}

// ToDomain is the findings the report carries; nil in, nil out.
func (f *SeedPreflightFindings) ToDomain() *seedreload.Findings {
	if f == nil {
		return nil
	}
	out := &seedreload.Findings{
		DropSemantics: f.DropSemantics,
		Completeness:  f.Completeness,
		Checked:       f.Checked,
	}
	for _, d := range f.Dependents {
		out.Dependents = append(out.Dependents, seedreload.Dependent(d))
	}
	for _, p := range f.Probes {
		out.Probes = append(out.Probes, seedreload.Probe(p))
	}
	return out
}

// nonNil returns s, or an empty slice for nil, so it renders as `[]`.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
