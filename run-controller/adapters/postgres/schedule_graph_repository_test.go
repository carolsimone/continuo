package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
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

func keysOf(nodes []topology.ScheduleGraphNode) []topology.NodeKey {
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
	}, seed2.Node)
}

func TestScheduleGraph_AScheduleWithNoUpstreamInAnotherScheduleHasNoCrossEdge(t *testing.T) {
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)

	g, err := postgres.NewScheduleGraphRepository(db).Graph(context.Background(), tenant, "other")
	require.NoError(t, err)

	assert.Equal(t, []topology.NodeKey{"stg.other_y", "stg.table_d"}, keysOf(g.Nodes))
	assert.Equal(t, []topology.Edge{{Upstream: "stg.table_d", Downstream: "stg.other_y"}}, g.Edges)
}

// The schedule graph the UI draws returns the upstreams of a schedule's nodes
// whatever their type, so an unknown-external node shows up with the edge to its
// reader, and with no service.
func TestGraph_ReturnsAnUnknownExternalUpstreamAndItsEdge(t *testing.T) {
	db := newTestDB(t)
	tenant := unknownExternalRelease(t, db)

	g, err := postgres.NewScheduleGraphRepository(db).Graph(context.Background(), tenant, "s")
	require.NoError(t, err)

	assert.Equal(t, int64(1), g.PromotionSeq)
	require.Equal(t, []topology.NodeKey{"a.m", "raw.x"}, keysOf(g.Nodes))
	zero := 0
	assert.Equal(t, topology.Node{
		Key: "raw.x", UniqueID: "raw.x", SchemaName: "raw", TableName: "x", NodeType: "unknown-external", TestCount: &zero,
	}, g.Nodes[1].Node)
	assert.Equal(t, "", g.Nodes[1].ServiceName)
	assert.Equal(t, []topology.Edge{{Upstream: "raw.x", Downstream: "a.m"}}, g.Edges)
}

// An unknown-external node has an empty schedule name, which belongs to no schedule:
// it adds no schedule to the list, no tile and no model to any count.
func TestScheduleSummaries_AnUnknownExternalNodeIsNoSchedule(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := unknownExternalRelease(t, db)

	summaries, err := postgres.NewScheduleGraphRepository(db).Summaries(ctx, tenant)
	require.NoError(t, err)
	got := summaries.Schedules
	require.Len(t, got, 1)
	assert.Equal(t, "s", got[0].ScheduleName)
	assert.Equal(t, 1, got[0].NodeCount)
	assert.Zero(t, summaries.DormantNodeCount, "an unknown-external node is not dormant")

	models := postgres.NewLiveScheduleModelRepository(db)
	counts, err := models.CountByScheduleName(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"s": 1}, counts)
	for name, want := range map[string]int{"s": 1, "": 0} {
		n, err := models.CountOf(ctx, tenant, name)
		require.NoError(t, err, name)
		assert.Equal(t, want, n, "models of %q", name)
	}
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

	summaries, err := postgres.NewScheduleGraphRepository(db).Summaries(context.Background(), tenant)
	require.NoError(t, err)
	got := summaries.Schedules

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
	require.Len(t, theirs.Schedules, 1)
	assert.Equal(t, 2, theirs.Schedules[0].NodeCount)
	assert.Zero(t, theirs.DormantNodeCount)

	mine, err := repo.Summaries(context.Background(), tenant)
	require.NoError(t, err)
	assert.Equal(t, 4, mine.Schedules[0].NodeCount)
	assert.Equal(t, 1, mine.DormantNodeCount, "orphan, the model of no schedule, is the tenant's one dormant node")

	nobody, err := repo.Summaries(context.Background(), "t-no-release")
	require.NoError(t, err)
	assert.Equal(t, topology.ScheduleSummaries{}, nobody)
}

// dormantRelease stores two releases of a new tenant and makes rel-2 live.
// rel-2 holds the model a of schedule "daily", which reads the dormant model d,
// the dormant snapshot never, the model up of schedule "other" and the
// unknown-external table raw.x, plus every node of extra. rel-1, which is not
// live, holds the dormant model old_dormant. It returns the tenant.
func dormantRelease(t *testing.T, db *sqlx.DB, extra ...topology.NodeSpec) string {
	t.Helper()
	tenant := newTopoTenant(t, db)
	reader := graphSpec("a", "dbt-model", "daily", "d", "never", "up")
	reader.UpstreamUniqueIDs = append(reader.UpstreamUniqueIDs, "raw.x")
	specs := append([]topology.NodeSpec{
		reader,
		graphSpec("d", "dbt-model", ""),
		graphSpec("never", "dbt-snapshot", ""),
		graphSpec("up", "dbt-model", "other"),
		unknownExternalSpec("raw", "x"),
	}, extra...)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1", graphSpec("old_dormant", "dbt-model", "")))
	require.NoError(t, err)
	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "sha-2", specs...))
	require.NoError(t, err)
	makeTenantLive(t, db, tenant, "rel-2", 2)
	return tenant
}

// insertNodeRun stores a run of tenant created at createdAt holding one run
// task, created with the run, of each node key, of service svc, and deletes
// the run and its tasks when the test ends.
func insertNodeRun(t *testing.T, db *sqlx.DB, tenant string, createdAt time.Time, nodeKeys ...string) {
	t.Helper()
	insertNodeRunOf(t, db, tenant, "svc", "run", createdAt, nodeKeys...)
}

// insertNodeRunOf stores a run of operation op of tenant created at createdAt
// holding one task of op, created with the run, of each node key, of service,
// and deletes the run and its tasks when the test ends.
func insertNodeRunOf(t *testing.T, db *sqlx.DB, tenant, service, op string, createdAt time.Time, nodeKeys ...string) {
	t.Helper()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, operation)
		VALUES ($1, $2, $3, 'succeeded', $4, $5)`, tenant, runID, "node-"+runID.String()[:8], createdAt, op)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, runID) })
	for _, k := range nodeKeys {
		schema, table, _ := strings.Cut(k, ".")
		_, err := db.Exec(`
			INSERT INTO tasks (tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, node_key, operation)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'succeeded', 2, $9, $10)`,
			tenant, uuid.New(), runID, createdAt, service, schema, table, "job-"+uuid.NewString()[:8], k, op)
		require.NoError(t, err)
	}
}

// The schedule graph returns a dormant node a schedule's node reads, marked
// dormant, with the creation time of the run of its newest run or full_refresh
// task; a dormant node that never ran has no last run. A node of a schedule is not dormant and
// carries no last run, whatever it ran, and a run of another tenant is not
// read.
func TestGraph_ReturnsADormantUpstreamWithItsLastRun(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := dormantRelease(t, db)
	older := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)
	newer := older.Add(26 * time.Hour)
	insertNodeRun(t, db, tenant, older, "stg.d", "stg.a")
	insertNodeRun(t, db, tenant, newer, "stg.d")
	insertNodeRun(t, db, tenant, newer.Add(time.Hour), "stg.a", "stg.up")
	insertNodeRun(t, db, newTopoTenant(t, db), newer.Add(48*time.Hour), "stg.d", "stg.never")

	g, err := postgres.NewScheduleGraphRepository(db).Graph(ctx, tenant, "daily")
	require.NoError(t, err)

	require.Equal(t, []topology.NodeKey{"raw.x", "stg.a", "stg.d", "stg.never", "stg.up"}, keysOf(g.Nodes))
	external, a, d, never, up := g.Nodes[0], g.Nodes[1], g.Nodes[2], g.Nodes[3], g.Nodes[4]
	assert.True(t, d.IsDormant())
	require.NotNil(t, d.LastRunAt)
	assert.True(t, newer.Equal(*d.LastRunAt), "the last run of d is its newest task's run: got %s", d.LastRunAt)
	assert.True(t, never.IsDormant())
	assert.Nil(t, never.LastRunAt, "a dormant node that never ran in its tenant has no last run")
	for _, n := range []topology.ScheduleGraphNode{external, a, up} {
		assert.False(t, n.IsDormant(), "%s", n.Key)
		assert.Nil(t, n.LastRunAt, "%s is not dormant, so it carries no last run", n.Key)
	}
}

// A dormant node's last run is its newest run or full refresh, on the schedule
// graph and on the node list alike: a test of the node is no run of it. A node
// whose only task is a test reads as never run on both, and a test newer than
// a node's run leaves the run its last run.
func TestLastRunOfADormantNode_IsItsNewestRunOnTheGraphAndTheNodeList(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1",
		graphSpec("a", "dbt-model", "daily", "test_only", "run_then_test", "refreshed"),
		graphSpec("test_only", "dbt-model", ""),
		graphSpec("run_then_test", "dbt-model", ""),
		graphSpec("refreshed", "dbt-model", "")))
	require.NoError(t, err)
	makeTenantLive(t, db, tenant, "rel-1", 1)
	// graphSpec gives a node of no schedule the service "svc-".
	ran := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC)
	refreshed := ran.Add(time.Hour)
	insertNodeRunOf(t, db, tenant, "svc-", "test", ran.Add(-time.Hour), "stg.test_only")
	insertNodeRunOf(t, db, tenant, "svc-", "run", ran, "stg.run_then_test")
	insertNodeRunOf(t, db, tenant, "svc-", "test", ran.Add(2*time.Hour), "stg.run_then_test")
	insertNodeRunOf(t, db, tenant, "svc-", "full_refresh", refreshed, "stg.refreshed")

	g, err := postgres.NewScheduleGraphRepository(db).Graph(ctx, tenant, "daily")
	require.NoError(t, err)
	onGraph := map[string]*time.Time{}
	for _, n := range g.Nodes {
		onGraph[n.TableName] = n.LastRunAt
	}
	listed, _, err := postgres.NewNodeRunRepository(db, tenant, discardLogger()).
		ListNodes(ctx, "", "", model.OperationRun, true, 50, 0)
	require.NoError(t, err)
	onList := map[string]*time.Time{}
	for _, n := range listed {
		onList[n.TableName] = n.LastRunAt
	}

	for name, lastRuns := range map[string]map[string]*time.Time{"graph": onGraph, "node list": onList} {
		require.Contains(t, lastRuns, "test_only", name)
		assert.Nil(t, lastRuns["test_only"], "%s: a test is no run of the node", name)
		require.NotNil(t, lastRuns["run_then_test"], name)
		assert.True(t, ran.Equal(*lastRuns["run_then_test"]), "%s: the run, not the newer test: got %s", name, lastRuns["run_then_test"])
		require.NotNil(t, lastRuns["refreshed"], name)
		assert.True(t, refreshed.Equal(*lastRuns["refreshed"]), "%s: a full refresh is a run: got %s", name, lastRuns["refreshed"])
	}
}

// The topology tiles count the live release's dormant nodes: the nodes
// continuo builds that belong to no schedule. An unknown-external node, which
// has no schedule either, is not one, and neither is a node of a release that
// is not live or of another tenant. The schedules keep their tiles and counts:
// a node of no schedule adds no tile and no count to any schedule.
func TestSummaries_CountsDormantNodes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := dormantRelease(t, db)
	other := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, other, "rel-9", 9, "sha-x",
		graphSpec("theirs", "dbt-model", ""), graphSpec("theirs_2", "dbt-model", ""), graphSpec("theirs_3", "dbt-model", "")))
	require.NoError(t, err)
	makeTenantLive(t, db, other, "rel-9", 9)

	got, err := postgres.NewScheduleGraphRepository(db).Summaries(ctx, tenant)
	require.NoError(t, err)

	assert.Equal(t, 2, got.DormantNodeCount, "d and never; not raw.x, old_dormant or the other tenant's")
	require.Len(t, got.Schedules, 2)
	assert.Equal(t, []string{"daily", "other"}, []string{got.Schedules[0].ScheduleName, got.Schedules[1].ScheduleName})
	assert.Equal(t, []int{1, 1}, []int{got.Schedules[0].NodeCount, got.Schedules[1].NodeCount})
	counts, err := postgres.NewLiveScheduleModelRepository(db).CountByScheduleName(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"daily": 1, "other": 1}, counts, "the models of a schedule leave out the dormant nodes")
}

// A stored release can hold an unknown-external node under the legacy type
// name "ingested", with an empty schedule. It is not a node continuo builds,
// so the tiles do not count it dormant.
func TestSummaries_DoesNotCountALegacyIngestedRowAsDormant(t *testing.T) {
	legacy := unknownExternalSpec("raw", "legacy")
	legacy.NodeType = "ingested"
	db := newTestDB(t)
	tenant := dormantRelease(t, db, legacy)
	require.Equal(t, 1, countTopoRows(t, db, `
		SELECT count(*) FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = 'rel-2' AND node_type = 'ingested' AND schedule_name = ''`, tenant))

	got, err := postgres.NewScheduleGraphRepository(db).Summaries(context.Background(), tenant)
	require.NoError(t, err)
	assert.Equal(t, 2, got.DormantNodeCount)
}

// A live release whose every node continuo builds is dormant has no schedule
// and no tile, and still reports its dormant nodes.
func TestSummaries_ALiveReleaseOfDormantNodesOnlyHasNoScheduleAndCountsThem(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "sha-1",
		graphSpec("d1", "dbt-model", ""), graphSpec("d2", "dbt-model", "", "d1"), unknownExternalSpec("raw", "x")))
	require.NoError(t, err)
	makeTenantLive(t, db, tenant, "rel-1", 1)

	got, err := postgres.NewScheduleGraphRepository(db).Summaries(context.Background(), tenant)
	require.NoError(t, err)
	assert.Empty(t, got.Schedules)
	assert.Equal(t, 2, got.DormantNodeCount)
}

// The dormant count is the SQL form of model.IsDormant. A live release holds
// one node of no schedule of every declared node type, and one unknown-external
// row under the legacy name "ingested": the count is the number of declared
// types IsDormant calls dormant. NewRelease drops a dbt test, so its row is
// added to the stored nodes directly, and the query reads a row of every type.
func TestSummaries_CountsEveryNodeTypeModelIsDormantCallsDormant(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	var specs []topology.NodeSpec
	want := 0
	for _, nt := range model.NodeTypes() {
		specs = append(specs, graphSpec("n_"+strings.ReplaceAll(string(nt), "-", "_"), string(nt), ""))
		if model.IsDormant(nt, "") {
			want++
		}
	}
	legacy := unknownExternalSpec("raw", "legacy")
	legacy.NodeType = "ingested"
	specs = append(specs, legacy)
	rel := topoRelease(t, tenant, "rel-1", 1, "sha-1", specs...)
	kept := make(map[string]bool, len(rel.Nodes))
	for _, n := range rel.Nodes {
		kept[n.UniqueID] = true
	}
	for _, s := range specs {
		if !kept[s.UniqueID] {
			rel.Nodes = append(rel.Nodes, topology.Node{
				Key: topology.KeyOf(s.SchemaName, s.TableName), UniqueID: s.UniqueID, ServiceName: s.ServiceName,
				SchemaName: s.SchemaName, TableName: s.TableName, NodeType: s.NodeType, ContentHash: s.ContentHash,
			})
		}
	}
	_, err := saveTopoRelease(t, db, rel)
	require.NoError(t, err)
	makeTenantLive(t, db, tenant, "rel-1", 1)
	require.Equal(t, len(model.NodeTypes())+1, countTopoRows(t, db, `
		SELECT count(DISTINCT node_type) FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = 'rel-1' AND schedule_name = ''`, tenant),
		"the release stores a node of no schedule of every declared type and of the legacy name")
	require.Positive(t, want)

	got, err := postgres.NewScheduleGraphRepository(db).Summaries(context.Background(), tenant)
	require.NoError(t, err)
	assert.Equal(t, want, got.DormantNodeCount)
	assert.Empty(t, got.Schedules)
}
