package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// graphSpec is a node of schema stg in schedule, reading the stg tables in
// upstream.
func graphSpec(table, nodeType, schedule string, upstream ...string) topology.NodeSpec {
	one := 1
	ids := make([]string, 0, len(upstream))
	for _, u := range upstream {
		ids = append(ids, "stg."+u)
	}
	return topology.NodeSpec{
		UniqueID: "stg." + table, ServiceName: "svc-" + schedule, SchemaName: "stg", TableName: table,
		NodeType: nodeType, TestCount: &one, ContentHash: "h-" + table, ImageTag: "svc:1",
		SecretRef: "secret-" + table, ScheduleName: schedule, UpstreamUniqueIDs: ids,
	}
}

// graphRelease stores rel-1: the daily schedule (a, b) reads the seed seed_1
// and the model up of schedule other; other_x reads b and the seed seed_2.
// rel-2 adds c to daily.
func graphRelease(t *testing.T) (*postgres.TopologyRepository, string) {
	t.Helper()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	specs := []topology.NodeSpec{
		graphSpec("seed_1", "dbt-seed", "seed"),
		graphSpec("seed_2", "dbt-seed", "seed"),
		graphSpec("up", "dbt-model", "other"),
		graphSpec("a", "dbt-model", "daily", "seed_1", "up"),
		graphSpec("b", "dbt-model", "daily", "a"),
		graphSpec("other_x", "dbt-model", "other", "b", "seed_2"),
	}
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1", specs...))
	require.NoError(t, err)
	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "sha-2",
		append(specs, graphSpec("c", "dbt-model", "daily", "b"))...))
	require.NoError(t, err)
	return postgres.NewTopologyRepository(db, nil), tenant
}

func edgesOf(g topology.Graph) []topology.Edge {
	var out []topology.Edge
	for _, k := range g.Keys() {
		for _, d := range g.Downstreams(k) {
			out = append(out, topology.Edge{Upstream: k, Downstream: d})
		}
	}
	return out
}

func TestTopologyRepository_ScheduleGraphHoldsTheScheduleAndTheSeedsItReads(t *testing.T) {
	repo, tenant := graphRelease(t)
	g, err := repo.ScheduleGraph(context.Background(), tenant, "rel-1", "daily")
	require.NoError(t, err)

	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.b", "stg.seed_1"}, g.Keys())
	assert.Equal(t, []topology.Edge{
		{Upstream: "stg.a", Downstream: "stg.b"},
		{Upstream: "stg.seed_1", Downstream: "stg.a"},
	}, edgesOf(g))

	a, ok := g.Node("stg.a")
	require.True(t, ok)
	one := 1
	assert.Equal(t, topology.Node{
		Key: "stg.a", UniqueID: "stg.a", ServiceName: "svc-daily", SchemaName: "stg", TableName: "a",
		NodeType: "dbt-model", TestCount: &one, ContentHash: "h-a", ImageTag: "svc:1",
		SecretRef: "secret-a", ScheduleName: "daily",
	}, a)
}

// A seed schedule's own seeds are read once, and the graph is the release's:
// rel-2's extra node is not in rel-1's daily graph.
func TestTopologyRepository_ScheduleGraphReadsOneRelease(t *testing.T) {
	repo, tenant := graphRelease(t)
	ctx := context.Background()

	seeds, err := repo.ScheduleGraph(ctx, tenant, "rel-1", "seed")
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"stg.seed_1", "stg.seed_2"}, seeds.Keys())
	assert.Empty(t, edgesOf(seeds))

	daily2, err := repo.ScheduleGraph(ctx, tenant, "rel-2", "daily")
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.b", "stg.c", "stg.seed_1"}, daily2.Keys())

	none, err := repo.ScheduleGraph(ctx, tenant, "rel-1", "no-such-schedule")
	require.NoError(t, err)
	assert.Equal(t, 0, none.Len())
}

func TestTopologyRepository_GraphHoldsTheKeysAndTheEdgesAmongThem(t *testing.T) {
	repo, tenant := graphRelease(t)
	ctx := context.Background()

	g, err := repo.Graph(ctx, tenant, "rel-1", []topology.NodeKey{"stg.other_x", "stg.a", "stg.b", "raw.ghost"})
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.b", "stg.other_x"}, g.Keys())
	assert.Equal(t, []topology.Edge{
		{Upstream: "stg.a", Downstream: "stg.b"},
		{Upstream: "stg.b", Downstream: "stg.other_x"},
	}, edgesOf(g))

	empty, err := repo.Graph(ctx, tenant, "rel-1", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, empty.Len())
}

// A failed run was planned on rel-1, before any release held an unknown-external node.
// rel-2, the live release, adds the unknown-external table raw.x that the failed node
// a.m reads. The live schedule graph holds neither the unknown-external node nor its
// edge, so a rebase reads no new node and finds no change, and a whole-schedule
// run plans the tasks it planned before: neither is refused, and neither
// selects the unknown-external node.
func TestTopologyRepository_AnUnknownExternalUpstreamLeavesRebaseAndWholeScheduleRunsAsTheyWere(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	up := topoSpec("a", "up", "dbt-model", "h-up")
	failed := topoSpec("a", "m", "dbt-model", "h-m", "a.up")
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1", up, failed))
	require.NoError(t, err)
	failed.UpstreamUniqueIDs = []string{"a.up", "raw.x"}
	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "sha-2", up, failed, unknownExternalSpec("raw", "x")))
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)
	require.Equal(t, 1, countTopoRows(t, db, `
		SELECT count(*) FROM topology_edges
		WHERE tenant_id = $1 AND release_id = 'rel-2' AND upstream_key = 'raw.x' AND downstream_key = 'a.m'`, tenant),
		"the live release holds the unknown-external node and its edge")

	live, err := repo.ScheduleGraph(ctx, tenant, "rel-2", "daily")
	require.NoError(t, err)
	builtOn, err := repo.Graph(ctx, tenant, "rel-1", []topology.NodeKey{"a.up", "a.m"})
	require.NoError(t, err)
	source := runplan.Source{Operation: model.OperationRun, Tasks: []runplan.SourceTask{
		{Key: "a.m", TaskID: uuid.New(), BuiltOn: "rel-1"},
		{Key: "a.up", TaskID: uuid.New(), Succeeded: true, BuiltOn: "rel-1"},
	}}

	for _, widen := range []bool{false, true} {
		pv, err := runplan.Rebase{
			Source: source, SourceGraphs: map[string]topology.Graph{"rel-1": builtOn}, IncludeChangedUpstream: widen,
		}.Preview(live)
		require.NoError(t, err, "include changed upstream %v", widen)
		assert.Equal(t, runplan.Plan{
			Tasks: []runplan.PlannedTask{{Key: "a.m"}, {Key: "a.up", Inherited: true, InheritedFrom: source.Tasks[1].TaskID}},
			Ready: []topology.NodeKey{"a.m"},
		}, pv.Plan, "include changed upstream %v", widen)
		assert.Equal(t, 1, pv.FailedOrSkipped)
		assert.Zero(t, pv.New)
		assert.Equal(t, 1, pv.Inherited)
		assert.Empty(t, pv.Changed)
	}

	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(live)
	require.NoError(t, err)
	assert.Equal(t, runplan.Plan{
		Tasks: []runplan.PlannedTask{{Key: "a.m"}, {Key: "a.up"}},
		Ready: []topology.NodeKey{"a.up"},
	}, plan)
}

// A schedule's run graph holds the schedule's nodes and the seeds they read. A
// dormant node a schedule's node reads belongs to no schedule and is no seed,
// so neither it nor its edge is in the graph, and no run of the schedule, of
// any operation, executes it. The data tests of a dormant model are never
// stored, since no release stores a test.
func TestScheduleGraph_NeverHoldsADormantUpstream(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1",
		graphSpec("seed_1", "dbt-seed", "seed"),
		graphSpec("dormant_m", "dbt-model", "", "seed_1"),
		graphSpec("dormant_s", "dbt-snapshot", ""),
		topology.NodeSpec{
			UniqueID: "test.svc.not_null_dormant_m", ServiceName: "svc-", SchemaName: "stg", TableName: "not_null_dormant_m",
			NodeType: "dbt-test", UpstreamUniqueIDs: []string{"stg.dormant_m"},
		},
		graphSpec("a", "dbt-model", "daily", "dormant_m", "dormant_s", "seed_1"),
		graphSpec("b", "dbt-model", "daily", "a"),
	))
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)
	require.Equal(t, 2, countTopoRows(t, db, `
		SELECT count(*) FROM topology_edges
		WHERE tenant_id = $1 AND release_id = 'rel-1' AND downstream_key = 'stg.a' AND upstream_key IN ('stg.dormant_m', 'stg.dormant_s')`, tenant),
		"the release holds the edges from the dormant nodes to their reader")

	g, err := repo.ScheduleGraph(ctx, tenant, "rel-1", "daily")
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"stg.a", "stg.b", "stg.seed_1"}, g.Keys())
	assert.Equal(t, []topology.Edge{
		{Upstream: "stg.a", Downstream: "stg.b"},
		{Upstream: "stg.seed_1", Downstream: "stg.a"},
	}, edgesOf(g))
	for _, k := range g.Keys() {
		n, _ := g.Node(k)
		assert.False(t, n.IsDormant(), "%s", k)
	}

	for _, op := range []model.Operation{model.OperationRun, model.OperationBuild, model.OperationTest} {
		plan, err := runplan.WholeDAG{Operation: op}.Select(g)
		require.NoError(t, err, "operation %q", op)
		for _, task := range plan.Tasks {
			assert.NotContains(t, []topology.NodeKey{"stg.dormant_m", "stg.dormant_s"}, task.Key, "operation %q", op)
		}
	}
}
