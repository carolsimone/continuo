package runplan

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	seed    topology.NodeKey = "analytics.seed_fx_rates_eur"
	current topology.NodeKey = "analytics.fx_rates_current"
	latest  topology.NodeKey = "analytics.fx_rates_latest_eur"
	table   topology.NodeKey = "analytics.fx_transactions_eur"
)

var pf = &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: string(seed), Allowed: []string{string(current)}}

// nodes returns one dbt-model node per key.
func nodes(keys ...topology.NodeKey) []topology.Node {
	out := make([]topology.Node, 0, len(keys))
	for _, k := range keys {
		out = append(out, topology.Node{Key: k, NodeType: "dbt-model"})
	}
	return out
}

// fxGraph is the seed, the view that reads it, the view that reads that view
// and a table that reads the seed.
func fxGraph() topology.Graph {
	return topology.NewGraph(nodes(seed, current, latest, table), []topology.Edge{
		{Upstream: seed, Downstream: current},
		{Upstream: current, Downstream: latest},
		{Upstream: seed, Downstream: table},
	})
}

func taskOf(p Plan, k topology.NodeKey) PlannedTask {
	for _, t := range p.Tasks {
		if t.Key == k {
			return t
		}
	}
	return PlannedTask{}
}

func TestApplySeedReloads_AddsTheRebuildsAfterTheSeed(t *testing.T) {
	g := fxGraph()
	full := model.OperationFullRefresh
	p := ApplySeedReloads(flatPlan([]topology.NodeKey{seed}), g, model.OperationRun, []SeedReload{{Seed: seed, Preflight: pf, Plan: seedreload.Plan{
		Load: seedreload.Load{Operation: full, DropsTable: true, Reason: "columns changed"},
		Rebuilds: []seedreload.Rebuild{
			{Key: string(current), Operation: model.OperationRun, Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur"},
			{Key: string(latest), Operation: model.OperationRun, Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current"},
		}}}})
	assert.Equal(t, []PlannedTask{
		{Key: current, Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur", RebuildOf: seed},
		{Key: latest, Reason: "rebuild: dropped by the load of analytics.seed_fx_rates_eur, through analytics.fx_rates_current", RebuildOf: seed},
		{Key: seed, Operation: &full, Reason: "columns changed", Preflight: pf},
	}, p.Tasks)
	assert.Equal(t, []topology.NodeKey{seed}, p.Ready)
}

func TestApplySeedReloads_SetsAnOperationOnlyWhereItDiffersFromTheRuns(t *testing.T) {
	g := fxGraph()
	p := ApplySeedReloads(flatPlan([]topology.NodeKey{seed}), g, model.OperationFullRefresh, []SeedReload{{Seed: seed, Plan: seedreload.Plan{
		Load:     seedreload.Load{Operation: model.OperationFullRefresh, DropsTable: true},
		Rebuilds: []seedreload.Rebuild{{Key: string(current), Operation: model.OperationRun}}}}})
	run := model.OperationRun
	assert.Nil(t, taskOf(p, seed).Operation)
	assert.Equal(t, &run, taskOf(p, current).Operation)
}

func TestApplySeedReloads_ARefusedSeedIsNotReadyAndNeitherIsAnythingBelowIt(t *testing.T) {
	g := fxGraph() // fx_transactions_eur reads the seed
	p := ApplySeedReloads(dagPlan(g, set(seed, table), nil), g, model.OperationRun, []SeedReload{{Seed: seed, Plan: seedreload.Plan{
		Load: seedreload.Load{Operation: model.OperationFullRefresh, Refusal: model.FailureReasonSeedFullRefreshDisabled, Reason: "columns changed; +full_refresh: false keeps the table, so no command can apply the change"}}}})
	assert.Equal(t, model.FailureReasonSeedFullRefreshDisabled, taskOf(p, seed).Refusal)
	assert.Empty(t, p.Ready)
}

// An ephemeral node between the seed and a view is no task, so nothing in the
// plan holds the view back; the refused seed still does.
func TestApplySeedReloads_ATaskBelowARefusedSeedThroughANodeWithoutATaskIsNotReady(t *testing.T) {
	g := topology.NewGraph(nodes("a.seed", "a.clean", "a.view", "a.other"), []topology.Edge{
		{Upstream: "a.seed", Downstream: "a.clean"}, {Upstream: "a.clean", Downstream: "a.view"},
	})
	p := ApplySeedReloads(flatPlan([]topology.NodeKey{"a.seed", "a.view", "a.other"}), g, model.OperationRun, []SeedReload{{Seed: "a.seed", Plan: seedreload.Plan{
		Load: seedreload.Load{Refusal: model.FailureReasonSeedFullRefreshDisabled}}}})
	assert.Equal(t, []topology.NodeKey{"a.other"}, p.Ready)
}

func TestApplySeedReloads_AnInheritedViewBelowAReloadedSeedRunsAgain(t *testing.T) {
	g := fxGraph()
	root := uuid.New()
	p := ApplySeedReloads(dagPlan(g, set(seed), map[topology.NodeKey]uuid.UUID{current: root}), g, model.OperationRun, []SeedReload{{Seed: seed, Plan: seedreload.Plan{
		Load:     seedreload.Load{Operation: model.OperationRun, DropsTable: true},
		Rebuilds: []seedreload.Rebuild{{Key: string(current), Operation: model.OperationRun, Reason: "r"}}}}})
	assert.False(t, taskOf(p, current).Inherited)
	assert.Equal(t, uuid.Nil, taskOf(p, current).InheritedFrom)
	assert.NotContains(t, p.Ready, current)
}

func TestApplySeedReloads_AViewOfTwoReloadedSeedsIsOneTaskWaitingForBoth(t *testing.T) {
	g := topology.NewGraph(nodes("a.seed_one", "a.seed_two", "a.both"), []topology.Edge{{Upstream: "a.seed_one", Downstream: "a.both"}, {Upstream: "a.seed_two", Downstream: "a.both"}})
	both := seedreload.Rebuild{Key: "a.both", Operation: model.OperationRun}
	one, two := both, both
	one.Reason, two.Reason = "rebuild: dropped by the load of a.seed_one", "rebuild: dropped by the load of a.seed_two"
	p := ApplySeedReloads(flatPlan([]topology.NodeKey{"a.seed_two", "a.seed_one"}), g, model.OperationRun, []SeedReload{
		{Seed: "a.seed_two", Plan: seedreload.Plan{Load: seedreload.Load{DropsTable: true}, Rebuilds: []seedreload.Rebuild{two}}},
		{Seed: "a.seed_one", Plan: seedreload.Plan{Load: seedreload.Load{DropsTable: true}, Rebuilds: []seedreload.Rebuild{one}}},
	})
	assert.Len(t, p.Tasks, 3)
	assert.Equal(t, topology.NodeKey("a.seed_one"), taskOf(p, "a.both").RebuildOf)
	assert.Equal(t, "rebuild: dropped by the load of a.seed_one", taskOf(p, "a.both").Reason)
	assert.Equal(t, []topology.NodeKey{"a.seed_one", "a.seed_two"}, p.Ready)
}

func TestApplySeedReloads_LeavesThePlanItWasGivenUntouched(t *testing.T) {
	g := fxGraph()
	in := flatPlan([]topology.NodeKey{seed})
	ApplySeedReloads(in, g, model.OperationRun, []SeedReload{{Seed: seed, Plan: seedreload.Plan{Load: seedreload.Load{Reason: "r"}}}})
	assert.Equal(t, flatPlan([]topology.NodeKey{seed}), in)
}

func TestSeedReloadGraph_AdaptsTheTopologyGraph(t *testing.T) {
	no := false
	g := topology.NewGraph([]topology.Node{
		{Key: seed, NodeType: "dbt-seed", Materialized: "seed", FullRefresh: &no, ScheduleName: "seed"},
		{Key: current, NodeType: "dbt-model", Materialized: "view", ResolvedRelationID: "analytics.fx_rates_now", ScheduleName: "hourly"},
		{Key: table, NodeType: "dbt-model", Materialized: "table"},
	}, []topology.Edge{{Upstream: seed, Downstream: current}, {Upstream: seed, Downstream: table}})
	sg := SeedReloadGraph(g)

	n, ok := sg.Node(string(current))
	assert.True(t, ok)
	assert.Equal(t, seedreload.Node{Key: string(current), NodeType: "dbt-model", Materialized: "view", Schedule: "hourly", Relation: "analytics.fx_rates_now"}, n)
	_, ok = sg.Node("analytics.absent")
	assert.False(t, ok)
	assert.Equal(t, []string{string(seed)}, sg.Upstreams(string(current)))
	assert.Equal(t, []string{string(current), string(table)}, sg.Downstreams(string(seed)))

	s, _ := sg.Node(string(seed))
	assert.Equal(t, &no, s.FullRefresh)
	assert.Equal(t, string(seed), s.Relation)
	assert.Equal(t, s, SeedNode(topology.Node{Key: seed, NodeType: "dbt-seed", Materialized: "seed", FullRefresh: &no, ScheduleName: "seed"}))
}
