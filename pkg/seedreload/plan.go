package seedreload

import (
	"slices"
	"strings"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/warehouse"
)

const (
	materializedView = "materialized_view"
	view             = "view"
	ephemeral        = "ephemeral"
)

// Request is one load of a seed to plan.
type Request struct {
	Seed   Node
	Family warehouse.Family
	// Engine is the WAREHOUSE_ENGINE name, used in reasons.
	Engine  string
	Change  Change
	Trigger Trigger
	// Unvalidated is true when the seed's release skipped validation.
	Unvalidated bool
}

// Load is what the seed's own task does.
type Load struct {
	// Operation is the operation of the seed's task; an empty one is the run's operation.
	Operation model.Operation
	// DropsTable is true when the load drops and re-creates the seed's table.
	DropsTable bool
	// Refusal is set when no command can apply the load: the seed never starts.
	Refusal model.FailureReason
	Reason  string
}

// Rebuild is a node the run rebuilds because the load drops the seed's table.
type Rebuild struct {
	Key, Materialized, Schedule, Relation string
	// Operation is the operation of the rebuild's task; an empty one is the run's operation.
	Operation model.Operation
	Reason    string
}

// Boundary is a node below the seed that no rebuild covers, so it refreshes on
// its own schedule. Materialized is the node type when the node has none.
type Boundary struct {
	Key, Materialized, Schedule string
}

// Plan is the decision for one load of a seed.
type Plan struct {
	Seed   Node
	Family warehouse.Family
	Load   Load
	// Rebuilds are the views to rebuild, each after the rebuilds it reads.
	Rebuilds []Rebuild
	// RefreshOnSchedule are the nodes the walk stopped at, sorted by key.
	RefreshOnSchedule []Boundary
}

// PlanSeed decides the load of req.Seed and, when it drops the seed's table,
// walks down the graph to find the views to rebuild.
func PlanSeed(req Request, g Graph) Plan {
	p := Plan{Seed: req.Seed, Family: req.Family, Load: planLoad(req)}
	if !p.Load.DropsTable || p.Load.Refusal != "" {
		return p
	}
	p.Rebuilds, p.RefreshOnSchedule = walk(req, g)
	return p
}

// planLoad applies the load rules: whether the table is dropped, which
// operation the seed's task runs and whether the load is refused.
func planLoad(req Request) Load {
	operator := req.Trigger == TriggerFullRefresh
	columns := req.Change == ChangeColumns
	var lead string
	switch {
	case operator:
		lead = "full refresh requested by an operator"
	case columns:
		lead = "columns changed"
	}
	join := func(parts ...string) string {
		var kept []string
		for _, s := range parts {
			if s != "" {
				kept = append(kept, s)
			}
		}
		return strings.Join(kept, "; ")
	}

	if req.Family == warehouse.LateBinding {
		return Load{
			Operation:  model.OperationRun,
			DropsTable: true,
			Reason:     join(lead, "on "+warehouse.DisplayName(req.Engine)+" every load re-creates the table", unvalidated(req)),
		}
	}

	const recreates = "+full_refresh: true re-creates the table on every load"
	wantsFull := operator || columns
	switch setting := req.Seed.FullRefresh; {
	case setting != nil && *setting && !wantsFull:
		return Load{Operation: model.OperationRun, DropsTable: true, Reason: recreates}
	case setting != nil && *setting:
		return Load{Operation: model.OperationRun, DropsTable: true, Reason: join(lead, recreates, unvalidated(req))}
	case setting != nil && !*setting && operator:
		return Load{Operation: model.OperationFullRefresh, Refusal: model.FailureReasonSeedFullRefreshDisabled,
			Reason: "+full_refresh: false keeps the table, so a full refresh cannot rebuild it"}
	case setting != nil && !*setting && columns:
		return Load{Operation: model.OperationFullRefresh, Refusal: model.FailureReasonSeedFullRefreshDisabled,
			Reason: join(lead, "+full_refresh: false keeps the table, so no command can apply the change")}
	case !wantsFull:
		return Load{Operation: model.OperationRun}
	default:
		return Load{Operation: model.OperationFullRefresh, DropsTable: true, Reason: join(lead, unvalidated(req))}
	}
}

// unvalidated is the notice that ends the reason of a column change whose
// release skipped validation.
func unvalidated(req Request) string {
	if req.Unvalidated && req.Change == ChangeColumns && req.Trigger == TriggerLoad {
		return "unvalidated: its release skipped validation"
	}
	return ""
}

// walk goes down from the seed. A view or materialized view is a rebuild and
// the walk goes on below it, an ephemeral node is walked through without a
// rebuild, and any other node is a boundary where the walk stops.
func walk(req Request, g Graph) ([]Rebuild, []Boundary) {
	rebuilds := map[string]Node{}
	boundaries := []Boundary{}
	visited := map[string]bool{req.Seed.Key: true}
	queue := slices.Clone(g.Downstreams(req.Seed.Key))
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		if visited[key] {
			continue
		}
		visited[key] = true
		n, ok := g.Node(key)
		if !ok {
			continue
		}
		switch n.Materialized {
		case view, materializedView:
			rebuilds[key] = n
		case ephemeral:
		default:
			materialized := n.Materialized
			if materialized == "" {
				materialized = n.NodeType
			}
			boundaries = append(boundaries, Boundary{Key: n.Key, Materialized: materialized, Schedule: n.Schedule})
			continue
		}
		queue = append(queue, g.Downstreams(key)...)
	}
	slices.SortFunc(boundaries, func(a, b Boundary) int { return strings.Compare(a.Key, b.Key) })

	ordered := orderRebuilds(rebuilds, g)
	out := make([]Rebuild, 0, len(ordered))
	for _, key := range ordered {
		n := rebuilds[key]
		out = append(out, Rebuild{
			Key:          n.Key,
			Materialized: n.Materialized,
			Schedule:     n.Schedule,
			Relation:     n.Relation,
			Operation:    rebuildOperation(n, req.Family),
			Reason:       rebuildReason(req, throughOf(key, rebuilds, g)),
		})
	}
	return out, boundaries
}

// rebuildOperation is run for a view and, for a materialized view, a full
// refresh where a plain run does not reload it (late_binding engines).
func rebuildOperation(n Node, family warehouse.Family) model.Operation {
	if n.Materialized == materializedView && family == warehouse.LateBinding {
		return model.OperationFullRefresh
	}
	return model.OperationRun
}

func rebuildReason(req Request, through string) string {
	var reason string
	if req.Family == warehouse.LateBinding {
		reason = "rebuild: may be stale on " + warehouse.DisplayName(req.Engine) + " after the load of " + req.Seed.Key
	} else {
		reason = "rebuild: dropped by the load of " + req.Seed.Key
	}
	if through != "" {
		reason += ", through " + through
	}
	return reason
}

// throughOf is the smallest-key upstream of key that is itself a rebuild.
func throughOf(key string, rebuilds map[string]Node, g Graph) string {
	for _, up := range g.Upstreams(key) {
		if _, ok := rebuilds[up]; ok {
			return up
		}
	}
	return ""
}

// orderRebuilds orders the rebuilds by Kahn's algorithm over the edges among
// them, taking the smallest key first.
func orderRebuilds(rebuilds map[string]Node, g Graph) []string {
	waiting := map[string]int{}
	for key := range rebuilds {
		for _, up := range g.Upstreams(key) {
			if _, ok := rebuilds[up]; ok {
				waiting[key]++
			}
		}
	}
	var ready []string
	for key := range rebuilds {
		if waiting[key] == 0 {
			ready = append(ready, key)
		}
	}
	ordered := make([]string, 0, len(rebuilds))
	for len(ready) > 0 {
		slices.Sort(ready)
		key := ready[0]
		ready = ready[1:]
		ordered = append(ordered, key)
		for _, down := range g.Downstreams(key) {
			if _, ok := rebuilds[down]; !ok {
				continue
			}
			if waiting[down]--; waiting[down] == 0 {
				ready = append(ready, down)
			}
		}
	}
	if len(ordered) < len(rebuilds) {
		// A cycle in the graph leaves its members waiting; keep them, by key.
		var rest []string
		for key := range rebuilds {
			if !slices.Contains(ordered, key) {
				rest = append(rest, key)
			}
		}
		slices.Sort(rest)
		ordered = append(ordered, rest...)
	}
	return ordered
}

// PreflightMode says whether a preflight stops the load or only reports.
type PreflightMode string

const (
	// PreflightCheck stops the load when a dependant is neither rebuilt nor named by an operator.
	PreflightCheck PreflightMode = "check"
	// PreflightReport lists the dependants and never stops the load.
	PreflightReport PreflightMode = "report"
)

// Preflight is the spec of the check a seed's pod runs before its load.
type Preflight struct {
	Mode   PreflightMode
	Family warehouse.Family
	// Relation is the seed's warehouse relation.
	Relation string
	// Allowed are the relations of the views the run rebuilds.
	Allowed []string
	// Acknowledged are the objects an operator named.
	Acknowledged []string
}

// Preflight returns the check for the load, in check mode on a schema_bound
// engine and report mode on a late_binding one, or nil when the load drops
// nothing or is refused.
func (p Plan) Preflight(acknowledged []string) *Preflight {
	mode := PreflightReport
	if p.Family == warehouse.SchemaBound {
		mode = PreflightCheck
	}
	return p.preflight(mode, acknowledged)
}

// ReportPreflight returns the report-mode preflight with nothing acknowledged,
// or nil when the load drops nothing or is refused.
func (p Plan) ReportPreflight() *Preflight {
	return p.preflight(PreflightReport, nil)
}

func (p Plan) preflight(mode PreflightMode, acknowledged []string) *Preflight {
	if !p.Load.DropsTable || p.Load.Refusal != "" {
		return nil
	}
	allowed := make([]string, 0, len(p.Rebuilds))
	for _, r := range p.Rebuilds {
		allowed = append(allowed, r.Relation)
	}
	return &Preflight{
		Mode:         mode,
		Family:       p.Family,
		Relation:     p.Seed.Relation,
		Allowed:      NormalizeObjects(allowed),
		Acknowledged: NormalizeObjects(acknowledged),
	}
}
