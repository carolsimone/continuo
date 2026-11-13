package release

import (
	"slices"
	"strings"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

const (
	noticeKindSeedReload = "seed_reload"

	predictionReloads = "reloads"
	predictionStops   = "stops"
	predictionRefused = "refused"

	// completenessNone is the completeness a preflight reports when the
	// warehouse cannot list the dependants of a table.
	completenessNone = "none"
)

// Notice says what promoting a release will do to one changed seed whose load
// drops its table: the load, the views the run rebuilds, the nodes that wait
// for their own schedule and, once the seed-build leg ran, what the warehouse
// holds. A notice predicts; the run's own preflight decides.
type Notice struct {
	Kind, NodeID, Service string
	EngineFamily          warehouse.Family
	Change                seedreload.Change
	Columns               ColumnChanges
	Load                  seedreload.Load
	Rebuilds              []seedreload.Rebuild
	RefreshOnSchedule     []seedreload.Boundary
	Warehouse             WarehouseFindings
	// Prediction is reloads, stops or refused.
	Prediction string
	// StopReason is the failure reason of a stop or a refusal; empty otherwise.
	StopReason string
	// Unvalidated is true when the release skipped validation and the seed's
	// columns changed.
	Unvalidated bool
}

// ColumnChanges are the seed's declared columns the candidate adds, removes or
// declares with another type, by name.
type ColumnChanges struct {
	Added, Removed, Retyped []string
}

// WarehouseFindings is what the seed-build leg's preflight found in the
// warehouse. Checked is false until a preflight ran to the end.
type WarehouseFindings struct {
	Checked         bool
	Completeness    string
	OutsideContinuo []OutsideObject
}

// OutsideObject is an object that reads the seed's table and that the run
// neither rebuilds nor has an operator's leave to drop.
type OutsideObject struct {
	Object, Kind, Owner string
}

// BuildNotices returns one notice per candidate seed that production holds
// with another content hash and whose load drops its table or is refused, in
// node-id order, and the report-mode preflight of each noticed seed whose load
// goes ahead, by node id. bootstrap is true for a release that skips
// validation.
func BuildNotices(candidate, prod Topology, family warehouse.Family, engine string, bootstrap bool) ([]Notice, map[string]*seedreload.Preflight) {
	prodByID := make(map[string]Node, len(prod))
	for _, n := range prod {
		prodByID[n.UniqueID] = n
	}
	graph := newSeedGraph(candidate)
	notices := []Notice{}
	preflights := map[string]*seedreload.Preflight{}
	for _, cand := range candidate {
		if cand.NodeType != string(pkg_model.NodeTypeDbtSeed) {
			continue
		}
		live, ok := prodByID[cand.UniqueID]
		if !ok || live.ContentHash == cand.ContentHash {
			continue
		}
		change := seedreload.ChangeOf(live.SeedColumnsHash, cand.SeedColumnsHash)
		seed, _ := graph.Node(cand.UniqueID)
		plan := seedreload.PlanSeed(seedreload.Request{
			Seed:        seed,
			Family:      family,
			Engine:      engine,
			Change:      change,
			Trigger:     seedreload.TriggerLoad,
			Unvalidated: bootstrap,
		}, graph)
		if !plan.Load.DropsTable && plan.Load.Refusal == "" {
			continue
		}
		columns := ColumnChanges{Added: []string{}, Removed: []string{}, Retyped: []string{}}
		if change == seedreload.ChangeColumns {
			columns = ColumnChangesOf(live.SeedColumns, cand.SeedColumns)
		}
		n := Notice{
			Kind:              noticeKindSeedReload,
			NodeID:            cand.UniqueID,
			Service:           cand.ServiceName,
			EngineFamily:      family,
			Change:            change,
			Columns:           columns,
			Load:              plan.Load,
			Rebuilds:          plan.Rebuilds,
			RefreshOnSchedule: plan.RefreshOnSchedule,
			Warehouse:         WarehouseFindings{OutsideContinuo: []OutsideObject{}},
			Prediction:        predictionReloads,
			Unvalidated:       bootstrap && change == seedreload.ChangeColumns,
		}
		if plan.Load.Refusal != "" {
			n.Prediction, n.StopReason = predictionRefused, string(plan.Load.Refusal)
		} else if p := plan.ReportPreflight(); p != nil {
			preflights[cand.UniqueID] = p
		}
		notices = append(notices, n)
	}
	slices.SortFunc(notices, func(a, b Notice) int { return strings.Compare(a.NodeID, b.NodeID) })
	return notices, preflights
}

// ColumnChangesOf compares a seed's declared columns in production with the
// candidate's: added in candidate order, removed in production order, and
// retyped (the same name with another type) in candidate order. No slice is nil.
func ColumnChangesOf(prod, candidate []SeedColumn) ColumnChanges {
	out := ColumnChanges{Added: []string{}, Removed: []string{}, Retyped: []string{}}
	prodType := make(map[string]string, len(prod))
	for _, c := range prod {
		prodType[c.Name] = c.Type
	}
	inCandidate := make(map[string]bool, len(candidate))
	for _, c := range candidate {
		inCandidate[c.Name] = true
		typ, ok := prodType[c.Name]
		switch {
		case !ok:
			out.Added = append(out.Added, c.Name)
		case typ != c.Type:
			out.Retyped = append(out.Retyped, c.Name)
		}
	}
	for _, c := range prod {
		if !inCandidate[c.Name] {
			out.Removed = append(out.Removed, c.Name)
		}
	}
	return out
}

// ApplyFindings records what the seed-build leg's preflight found and updates
// the prediction. A refused load stays refused. On a schema_bound engine a
// checked preflight that lists an object outside continuo, or that could not
// list the dependants at all, predicts that the load stops; anything else
// predicts that it reloads.
func (n *Notice) ApplyFindings(f seedreload.Findings) {
	outside := []OutsideObject{}
	for _, d := range f.Outside() {
		outside = append(outside, OutsideObject{Object: d.Object(), Kind: d.Kind, Owner: d.Owner})
	}
	n.Warehouse = WarehouseFindings{Checked: f.Checked, Completeness: f.Completeness, OutsideContinuo: outside}
	if n.Prediction == predictionRefused {
		return
	}
	if n.EngineFamily == warehouse.SchemaBound && f.Checked && (len(outside) > 0 || f.Completeness == completenessNone) {
		n.Prediction, n.StopReason = predictionStops, string(pkg_model.FailureReasonUnownedDependents)
		return
	}
	n.Prediction, n.StopReason = predictionReloads, ""
}

// seedGraph is a candidate topology as a seedreload.Graph. It leaves out
// dbt-test nodes, which are validation-only and never refresh on a schedule.
type seedGraph struct {
	nodes       map[string]seedreload.Node
	upstreams   map[string][]string
	downstreams map[string][]string
}

func newSeedGraph(topo Topology) *seedGraph {
	g := &seedGraph{
		nodes:       make(map[string]seedreload.Node, len(topo)),
		upstreams:   map[string][]string{},
		downstreams: map[string][]string{},
	}
	kept := make([]Node, 0, len(topo))
	for _, n := range topo {
		if n.NodeType == string(pkg_model.NodeTypeDbtTest) {
			continue
		}
		relation := n.ResolvedRelationID
		if relation == "" {
			relation = n.UniqueID
		}
		g.nodes[n.UniqueID] = seedreload.Node{
			Key:          n.UniqueID,
			NodeType:     n.NodeType,
			Materialized: n.Materialized,
			FullRefresh:  n.FullRefresh,
			Schedule:     n.Schedule,
			Relation:     relation,
		}
		kept = append(kept, n)
	}
	for _, n := range kept {
		for _, up := range n.UpstreamUniqueIDs {
			if _, ok := g.nodes[up]; !ok {
				continue
			}
			g.upstreams[n.UniqueID] = append(g.upstreams[n.UniqueID], up)
			g.downstreams[up] = append(g.downstreams[up], n.UniqueID)
		}
	}
	for _, m := range []map[string][]string{g.upstreams, g.downstreams} {
		for key, ids := range m {
			slices.Sort(ids)
			m[key] = slices.Compact(ids)
		}
	}
	return g
}

func (g *seedGraph) Node(key string) (seedreload.Node, bool) {
	n, ok := g.nodes[key]
	return n, ok
}

func (g *seedGraph) Upstreams(key string) []string { return g.upstreams[key] }

func (g *seedGraph) Downstreams(key string) []string { return g.downstreams[key] }
