package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
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
