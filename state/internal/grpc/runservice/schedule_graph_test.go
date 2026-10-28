package runservice_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/state/domain/topology"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
)

// dailyGraph is the e2e DAG as the live release views the schedule "daily":
// seed_table_1 -> table_a -> table_d <- table_b <- seed_table_2, where
// seed_table_2 belongs to the schedule "seeds".
func dailyGraph() topology.ScheduleGraph {
	node := func(service, table, nodeType, schedule string) topology.Node {
		return topology.Node{
			Key:         topology.KeyOf("analytics", table),
			ServiceName: service, SchemaName: "analytics", TableName: table,
			NodeType: nodeType, ScheduleName: schedule,
		}
	}
	return topology.ScheduleGraph{
		PromotionSeq: 7,
		Nodes: []topology.Node{
			node("svc-a", "seed_table_1", "dbt-seed", "daily"),
			node("svc-b", "seed_table_2", "dbt-seed", "seeds"),
			node("svc-a", "table_a", "dbt-model", "daily"),
			node("svc-a", "table_b", "dbt-model", "daily"),
			node("svc-a", "table_d", "dbt-model", "daily"),
		},
		Edges: []topology.Edge{
			{Upstream: "analytics.seed_table_1", Downstream: "analytics.table_a"},
			{Upstream: "analytics.seed_table_2", Downstream: "analytics.table_b"},
			{Upstream: "analytics.table_a", Downstream: "analytics.table_d"},
			{Upstream: "analytics.table_b", Downstream: "analytics.table_d"},
		},
	}
}

func TestGetScheduleGraph_MapsEveryField(t *testing.T) {
	store := newStore()
	store.putScheduleGraph(tenantA, "daily", dailyGraph())

	resp, err := newServer(store).GetScheduleGraph(ctxOf(tenantA), &runv1.GetScheduleGraphRequest{ScheduleName: "daily"})
	require.NoError(t, err)

	assert.Equal(t, int64(7), resp.GetPromotionSeq())
	require.Len(t, resp.GetNodes(), 5)
	seed2 := resp.GetNodes()[1]
	assert.Equal(t, "svc-b", seed2.GetServiceName())
	assert.Equal(t, "analytics", seed2.GetSchemaName())
	assert.Equal(t, "seed_table_2", seed2.GetTableName())
	assert.Equal(t, "dbt-seed", seed2.GetNodeType())
	assert.Equal(t, "seeds", seed2.GetScheduleName(), "an upstream node names its own schedule")
	assert.Equal(t, "daily", resp.GetNodes()[0].GetScheduleName())

	// An edge runs from the dependent node to the node it reads.
	type edge struct{ from, to string }
	var got []edge
	for _, e := range resp.GetEdges() {
		got = append(got, edge{e.GetFromNodeId(), e.GetToNodeId()})
	}
	assert.Equal(t, []edge{
		{"svc-a.analytics.table_a", "svc-a.analytics.seed_table_1"},
		{"svc-a.analytics.table_b", "svc-b.analytics.seed_table_2"},
		{"svc-a.analytics.table_d", "svc-a.analytics.table_a"},
		{"svc-a.analytics.table_d", "svc-a.analytics.table_b"},
	}, got)
}

func TestGetScheduleGraph_AScheduleTheReleaseDoesNotHoldIsEmpty(t *testing.T) {
	store := newStore()
	store.putScheduleGraph(tenantA, "daily", dailyGraph())

	resp, err := newServer(store).GetScheduleGraph(ctxOf(tenantA), &runv1.GetScheduleGraphRequest{ScheduleName: "weekly"})
	require.NoError(t, err)
	assert.Empty(t, resp.GetNodes())
	assert.Empty(t, resp.GetEdges())
}

func TestGetScheduleGraph_RefusesAnEmptyScheduleName(t *testing.T) {
	_, err := newServer(newStore()).GetScheduleGraph(ctxOf(tenantA), &runv1.GetScheduleGraphRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestScheduleTopologyReads_AreOfTheRequestsTenant(t *testing.T) {
	store := newStore()
	store.putScheduleGraph(tenantA, "daily", dailyGraph())
	store.summaries[tenantA] = []topology.ScheduleSummary{{ScheduleName: "daily", NodeCount: 4}}
	srv := newServer(store)
	graph := &runv1.GetScheduleGraphRequest{ScheduleName: "daily"}

	// The default tenant sees the rows, so an empty answer below is the
	// tenant's doing.
	own, err := srv.GetScheduleGraph(ctxOf(tenantA), graph)
	require.NoError(t, err)
	require.Len(t, own.GetNodes(), 5)
	ownTiles, err := srv.ListScheduleTopologies(ctxOf(tenantA), &runv1.ListScheduleTopologiesRequest{})
	require.NoError(t, err)
	require.Len(t, ownTiles.GetSchedules(), 1)

	other, err := srv.GetScheduleGraph(ctxOf(tenantB), graph)
	require.NoError(t, err)
	assert.Empty(t, other.GetNodes())
	assert.Empty(t, other.GetEdges())
	assert.Zero(t, other.GetPromotionSeq())
	tiles, err := srv.ListScheduleTopologies(ctxOf(tenantB), &runv1.ListScheduleTopologiesRequest{})
	require.NoError(t, err)
	assert.Empty(t, tiles.GetSchedules())
}

func TestListScheduleTopologies_MapsEveryField(t *testing.T) {
	store := newStore()
	loaded := time.Date(2026, 10, 27, 8, 30, 0, 0, time.UTC)
	store.summaries[tenantA] = []topology.ScheduleSummary{
		{ScheduleName: "daily", NodeCount: 4, LastUpdatedAt: loaded},
		{ScheduleName: "seeds", NodeCount: 1},
	}

	resp, err := newServer(store).ListScheduleTopologies(ctxOf(tenantA), &runv1.ListScheduleTopologiesRequest{})
	require.NoError(t, err)

	require.Len(t, resp.GetSchedules(), 2)
	daily, seeds := resp.GetSchedules()[0], resp.GetSchedules()[1]
	assert.Equal(t, "daily", daily.GetScheduleName())
	assert.Equal(t, int32(4), daily.GetNodeCount())
	assert.True(t, loaded.Equal(daily.GetLastUpdatedAt().AsTime()))
	assert.Equal(t, "seeds", seeds.GetScheduleName())
	assert.Nil(t, seeds.GetLastUpdatedAt(), "a zero time stays unset")
}

func TestListScheduleTopologies_NoLiveReleaseIsEmpty(t *testing.T) {
	resp, err := newServer(newStore()).ListScheduleTopologies(ctxOf(tenantA), &runv1.ListScheduleTopologiesRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.GetSchedules())
}
