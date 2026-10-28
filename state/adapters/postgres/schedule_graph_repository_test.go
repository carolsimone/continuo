package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/topology"
)

// liveScheduleRelease stores two releases of one tenant and makes the second
// live. The live release holds the e2e DAG
//
//	seed_table_1 -> table_a -> table_d <- table_b <- seed_table_2
//
// where the schedule "daily" holds every table but seed_table_2, which belongs
// to the schedule "seeds" and reads raw_src, another "seeds" node. other_y
// (schedule "other") reads table_d, and orphan belongs to no schedule. The
// first release holds table_old in "daily" and is not live.
func liveScheduleRelease(t *testing.T, db *sqlx.DB) (tenant string, loadedAt time.Time) {
	t.Helper()
	tenant = newTopoTenant(t, db)
	old := graphSpec("table_old", "dbt-model", "daily")
	specs := []topology.NodeSpec{
		graphSpec("raw_src", "dbt-seed", "seeds"),
		graphSpec("seed_table_1", "dbt-seed", "daily"),
		graphSpec("seed_table_2", "dbt-seed", "seeds", "raw_src"),
		graphSpec("table_a", "dbt-model", "daily", "seed_table_1"),
		graphSpec("table_b", "dbt-model", "daily", "seed_table_2"),
		graphSpec("table_d", "dbt-model", "daily", "table_a", "table_b"),
		graphSpec("other_y", "dbt-model", "other", "table_d"),
		graphSpec("orphan", "dbt-model", ""),
	}
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1", old))
	require.NoError(t, err)
	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "sha-2", specs...))
	require.NoError(t, err)
	makeTenantLive(t, db, tenant, "rel-2", 2)
	require.NoError(t, db.Get(&loadedAt, `SELECT loaded_at FROM topology_releases WHERE tenant_id = $1 AND release_id = 'rel-2'`, tenant))
	return tenant, loadedAt
}

func makeTenantLive(t *testing.T, db *sqlx.DB, tenant, releaseID string, seq int64) {
	t.Helper()
	require.NoError(t, postgres.NewTopologyRepository(db, nil).SetLivePointer(
		context.Background(), tenant, topology.Pointer{ReleaseID: releaseID, PromotionSeq: seq}, time.Now()))
}

func keysOf(nodes []topology.Node) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Key)
	}
	return out
}

func TestScheduleGraph_IsTheScheduleAndItsOneHopUpstreams(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)
	repo := postgres.NewScheduleGraphRepository(db)

	g, err := repo.Graph(context.Background(), tenant, "daily")
	require.NoError(t, err)

	assert.Equal(t, int64(2), g.PromotionSeq)
	assert.Equal(t, []topology.NodeKey{
		"stg.seed_table_1", "stg.seed_table_2", "stg.table_a", "stg.table_b", "stg.table_d",
	}, keysOf(g.Nodes), "the live release's nodes of the schedule plus seed_table_2, with no raw_src, other_y or table_old")
	assert.Equal(t, []topology.Edge{
		{Upstream: "stg.seed_table_1", Downstream: "stg.table_a"},
		{Upstream: "stg.seed_table_2", Downstream: "stg.table_b"},
		{Upstream: "stg.table_a", Downstream: "stg.table_d"},
		{Upstream: "stg.table_b", Downstream: "stg.table_d"},
	}, g.Edges, "the cross edge seed_table_2 -> table_b is kept, the edges of other schedules are not")

	seed2 := g.Nodes[1]
	one := 1
	assert.Equal(t, topology.Node{ //nolint:gosec // G101: secret-* is a Secret name, not a value
		Key: "stg.seed_table_2", UniqueID: "stg.seed_table_2", ServiceName: "svc-seeds", SchemaName: "stg",
		TableName: "seed_table_2", NodeType: "dbt-seed", TestCount: &one, ContentHash: "h-seed_table_2",
		ImageTag: "svc:1", SecretRef: "secret-seed_table_2", ScheduleName: "seeds",
	}, seed2)
}

func TestScheduleGraph_AScheduleWithNoUpstreamInAnotherScheduleHasNoCrossEdge(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)

	g, err := postgres.NewScheduleGraphRepository(db).Graph(context.Background(), tenant, "other")
	require.NoError(t, err)

	assert.Equal(t, []topology.NodeKey{"stg.other_y", "stg.table_d"}, keysOf(g.Nodes))
	assert.Equal(t, []topology.Edge{{Upstream: "stg.table_d", Downstream: "stg.other_y"}}, g.Edges)
}

func TestScheduleGraph_UnknownScheduleIsEmpty(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)

	g, err := postgres.NewScheduleGraphRepository(db).Graph(context.Background(), tenant, "weekly")
	require.NoError(t, err)

	assert.Empty(t, g.Nodes)
	assert.Empty(t, g.Edges)
	assert.Equal(t, int64(2), g.PromotionSeq, "the live promotion seq is reported whatever the schedule")
}

func TestScheduleGraph_NoLiveReleaseIsTheZeroGraph(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1", graphSpec("a", "dbt-model", "daily")))
	require.NoError(t, err)

	g, err := postgres.NewScheduleGraphRepository(db).Graph(context.Background(), tenant, "daily")
	require.NoError(t, err)

	assert.Equal(t, topology.ScheduleGraph{}, g, "a stored release that is not live is not read")
}

func TestScheduleGraph_AnotherTenantsReleaseIsInvisible(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)
	other := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, other, "rel-2", 9, "sha-x",
		graphSpec("theirs", "dbt-model", "daily")))
	require.NoError(t, err)
	makeTenantLive(t, db, other, "rel-2", 9)
	repo := postgres.NewScheduleGraphRepository(db)

	mine, err := repo.Graph(context.Background(), tenant, "daily")
	require.NoError(t, err)
	assert.NotContains(t, keysOf(mine.Nodes), topology.NodeKey("stg.theirs"))
	assert.Equal(t, int64(2), mine.PromotionSeq)

	theirs, err := repo.Graph(context.Background(), other, "daily")
	require.NoError(t, err)
	assert.Equal(t, []topology.NodeKey{"stg.theirs"}, keysOf(theirs.Nodes))
	assert.Equal(t, int64(9), theirs.PromotionSeq)

	nobody, err := repo.Graph(context.Background(), "t-no-release", "daily")
	require.NoError(t, err)
	assert.Equal(t, topology.ScheduleGraph{}, nobody)
}

func TestScheduleSummaries_CountTheLiveReleasesNodes(t *testing.T) {
	db := newTestDB(t)
	tenant, loadedAt := liveScheduleRelease(t, db)

	got, err := postgres.NewScheduleGraphRepository(db).Summaries(context.Background(), tenant)
	require.NoError(t, err)

	require.Len(t, got, 3, "the node of no schedule has no tile")
	assert.Equal(t, []string{"daily", "other", "seeds"}, []string{got[0].ScheduleName, got[1].ScheduleName, got[2].ScheduleName})
	assert.Equal(t, []int{4, 1, 2}, []int{got[0].NodeCount, got[1].NodeCount, got[2].NodeCount},
		"table_old of the release that is not live is not counted")
	for _, s := range got {
		assert.True(t, loadedAt.Equal(s.LastUpdatedAt), "%s: last_updated_at is when the live release was loaded", s.ScheduleName)
	}
}

func TestScheduleSummaries_AreOfTheTenantsLiveRelease(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)
	other := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, other, "rel-2", 9, "sha-x",
		graphSpec("theirs", "dbt-model", "daily"), graphSpec("theirs_2", "dbt-model", "daily")))
	require.NoError(t, err)
	makeTenantLive(t, db, other, "rel-2", 9)
	repo := postgres.NewScheduleGraphRepository(db)

	theirs, err := repo.Summaries(context.Background(), other)
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, 2, theirs[0].NodeCount)

	mine, err := repo.Summaries(context.Background(), tenant)
	require.NoError(t, err)
	assert.Equal(t, 4, mine[0].NodeCount)

	nobody, err := repo.Summaries(context.Background(), "t-no-release")
	require.NoError(t, err)
	assert.Empty(t, nobody)
}
