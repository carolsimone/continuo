package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTopoTenant returns a tenant id no other test uses and deletes every
// topology and materialization row of that tenant when the test ends.
func newTopoTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "t-" + uuid.NewString()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM live_release WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(ctx, `DELETE FROM topology_releases WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(ctx, `DELETE FROM node_materializations WHERE tenant_id = $1`, tenant)
	})
	return tenant
}

func topoSpec(schema, table, nodeType, hash string, upstream ...string) topology.NodeSpec {
	zero := 0
	return topology.NodeSpec{
		UniqueID: schema + "." + table, ServiceName: "svc", SchemaName: schema, TableName: table,
		NodeType: nodeType, TestCount: &zero, ContentHash: hash, ImageTag: "svc:1", ScheduleName: "daily",
		UpstreamUniqueIDs: upstream,
	}
}

func topoRelease(t *testing.T, tenant, releaseID string, seq int64, sha string, specs ...topology.NodeSpec) topology.Release {
	t.Helper()
	rel, err := topology.NewRelease(tenant, releaseID, seq,
		"s3://continuo/tenants/"+tenant+"/topologies/"+releaseID+"/topology.json.gz", sha, specs)
	require.NoError(t, err)
	return rel
}

// saveTopoRelease runs SaveRelease in a transaction of its own and commits it
// when the save succeeds.
func saveTopoRelease(t *testing.T, db *sqlx.DB, rel topology.Release) (bool, error) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	loaded, err := postgres.NewTopologyRepository(db, tx).SaveRelease(ctx, rel)
	if err != nil {
		return false, err
	}
	require.NoError(t, tx.Commit())
	return loaded, nil
}

func countTopoRows(t *testing.T, db *sqlx.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.Get(&n, query, args...))
	return n
}

func storedSeq(t *testing.T, db *sqlx.DB, tenant, releaseID string) int64 {
	t.Helper()
	var seq int64
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM topology_releases WHERE tenant_id = $1 AND release_id = $2`, tenant, releaseID))
	return seq
}

func TestTopologyRepository_SaveReleaseLoadsOnce(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	rel := topoRelease(t, tenant, "rel-1", 1, "aa",
		topoSpec("raw", "orders", "dbt-seed", "h1"),
		topoSpec("stg", "orders", "dbt-model", "h2", "raw.orders"))

	loaded, err := saveTopoRelease(t, db, rel)
	require.NoError(t, err)
	assert.True(t, loaded)
	loaded, err = saveTopoRelease(t, db, rel)
	require.NoError(t, err)
	assert.False(t, loaded, "a stored release is never loaded twice")

	assert.Equal(t, 2, countTopoRows(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1 AND release_id = 'rel-1'`, tenant))
	assert.Equal(t, 1, countTopoRows(t, db, `
		SELECT count(*) FROM topology_edges
		WHERE tenant_id = $1 AND release_id = 'rel-1' AND upstream_key = 'raw.orders' AND downstream_key = 'stg.orders'`, tenant))
}

// COPY loads a large release in one round trip per table, with no
// bind-parameter ceiling.
func TestTopologyRepository_CopiesTwoThousandNodesAndTheirEdges(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	const n = 2000
	specs := make([]topology.NodeSpec, 0, n)
	for i := 0; i < n; i++ {
		var upstream []string
		if i > 0 {
			upstream = []string{fmt.Sprintf("bulk.n%04d", i-1)}
		}
		specs = append(specs, topoSpec("bulk", fmt.Sprintf("n%04d", i), "dbt-model", fmt.Sprintf("h%d", i), upstream...))
	}
	loaded, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-bulk", 1, "aa", specs...))
	require.NoError(t, err)
	require.True(t, loaded)

	assert.Equal(t, n, countTopoRows(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1`, tenant))
	assert.Equal(t, n-1, countTopoRows(t, db, `SELECT count(*) FROM topology_edges WHERE tenant_id = $1`, tenant))
	nodes, err := postgres.NewTopologyRepository(db, nil).ReleaseNodes(context.Background(), tenant, "rel-bulk", "")
	require.NoError(t, err)
	require.Len(t, nodes, n)
	assert.Equal(t, topology.NodeKey("bulk.n0000"), nodes[0].Key)
	assert.Equal(t, topology.NodeKey("bulk.n1999"), nodes[n-1].Key)
}

// Every column round-trips, an unknown test count stays NULL, and nodes come
// back in byte order of the key ("raw.a_b" before "raw.aa"), whatever the
// database's collation.
func TestTopologyRepository_ReleaseNodesRoundTripsEveryColumnInKeyOrder(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	three := 3
	rich := topology.NodeSpec{ //nolint:gosec // G101: continuo-api-* is a Secret name, not a value
		UniqueID: "raw.ab", ServiceName: "svc-2", SchemaName: "raw", TableName: "ab", NodeType: "python-api",
		TestCount: &three, ContentHash: "h-ab", ImageTag: "svc-2:7", SecretRef: "continuo-api-weather", ScheduleName: "hourly",
	}
	eightHours := 28800
	rich.TimeoutSeconds = &eightHours
	unknown := topology.NodeSpec{
		UniqueID: "raw.a_b", ServiceName: "svc", SchemaName: "raw", TableName: "a_b", NodeType: "dbt-seed", ContentHash: "h-a_b",
	}
	rel := topoRelease(t, tenant, "rel-1", 1, "aa", rich, unknown,
		topoSpec("raw", "aa", "dbt-seed", "h-aa"), topoSpec("stg", "x", "dbt-model", "h-x"))
	_, err := saveTopoRelease(t, db, rel)
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)

	all, err := repo.ReleaseNodes(ctx, tenant, "rel-1", "")
	require.NoError(t, err)
	assert.Equal(t, rel.Nodes, all)

	seedsOnly, err := repo.ReleaseNodes(ctx, tenant, "rel-1", "dbt-seed")
	require.NoError(t, err)
	require.Len(t, seedsOnly, 2)
	assert.Equal(t, topology.NodeKey("raw.a_b"), seedsOnly[0].Key)
	assert.Nil(t, seedsOnly[0].TestCount)
	assert.Nil(t, seedsOnly[0].TimeoutSeconds)
	assert.Equal(t, topology.NodeKey("raw.aa"), seedsOnly[1].Key)

	none, err := repo.ReleaseNodes(ctx, tenant, "rel-unknown", "")
	require.NoError(t, err)
	assert.Empty(t, none)
}

// A stored release is immutable: a later release that declares another timeout
// for the same node leaves the earlier release's timeout as it was.
func TestTopologyRepository_AReleaseKeepsItsOwnTimeouts(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	withTimeout := func(seconds int) topology.NodeSpec {
		s := topoSpec("mart", "orders_full", "dbt-model", "h")
		s.TimeoutSeconds = &seconds
		return s
	}
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "aa", withTimeout(28800)))
	require.NoError(t, err)
	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "bb", withTimeout(7200)))
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)

	for releaseID, want := range map[string]int{"rel-1": 28800, "rel-2": 7200} {
		nodes, err := repo.ReleaseNodes(ctx, tenant, releaseID, "")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.NotNil(t, nodes[0].TimeoutSeconds, releaseID)
		assert.Equal(t, want, *nodes[0].TimeoutSeconds, releaseID)
	}
}

func TestTopologyRepository_SaveReleaseRejectsAnotherChecksum(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "aa", topoSpec("raw", "orders", "dbt-seed", "h1")))
	require.NoError(t, err)

	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 2, "bb", topoSpec("raw", "orders", "dbt-seed", "h1")))
	require.Error(t, err)
	assert.ErrorIs(t, err, repository.ErrReleaseChecksumConflict)
	assert.Equal(t, int64(1), storedSeq(t, db, tenant, "rel-1"))
}

// A stored release keeps the highest promotion seq it was announced under.
func TestTopologyRepository_SaveReleaseRaisesTheSeqOnly(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	spec := topoSpec("raw", "orders", "dbt-seed", "h1")
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 3, "aa", spec))
	require.NoError(t, err)

	loaded, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 5, "AA", spec))
	require.NoError(t, err, "the checksum is compared lowercased")
	assert.False(t, loaded)
	assert.Equal(t, int64(5), storedSeq(t, db, tenant, "rel-1"))

	_, err = saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 4, "aa", spec))
	require.NoError(t, err)
	assert.Equal(t, int64(5), storedSeq(t, db, tenant, "rel-1"))
}

// Two loaders racing on one release: the second waits for the first
// transaction, then finds the release stored and loads nothing.
func TestTopologyRepository_ConcurrentSavesLoadTheReleaseOnce(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	rel := topoRelease(t, tenant, "rel-race", 1, "aa", topoSpec("raw", "orders", "dbt-seed", "h1"))

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx1.Rollback()
	loaded, err := postgres.NewTopologyRepository(db, tx1).SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.True(t, loaded)

	type result struct {
		loaded bool
		err    error
	}
	done := make(chan result, 1)
	go func() {
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer tx2.Rollback()
		loaded, err := postgres.NewTopologyRepository(db, tx2).SaveRelease(ctx, rel)
		if err == nil {
			err = tx2.Commit()
		}
		done <- result{loaded: loaded, err: err}
	}()

	select {
	case r := <-done:
		t.Fatalf("the second load must wait for the first transaction, returned loaded=%v err=%v", r.loaded, r.err)
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx1.Commit())

	select {
	case r := <-done:
		require.NoError(t, r.err)
		assert.False(t, r.loaded, "the first transaction loaded the release")
	case <-time.After(5 * time.Second):
		t.Fatal("the second load did not proceed after the first committed")
	}
	assert.Equal(t, 1, countTopoRows(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1`, tenant))
}

func TestTopologyRepository_LivePointerUpserts(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	for i, id := range []string{"rel-1", "rel-2"} {
		_, err := saveTopoRelease(t, db, topoRelease(t, tenant, id, int64(i+1), "aa", topoSpec("raw", "orders", "dbt-seed", "h1")))
		require.NoError(t, err)
	}
	repo := postgres.NewTopologyRepository(db, nil)

	live, err := repo.LivePointer(ctx, tenant)
	require.NoError(t, err)
	assert.True(t, live.IsZero(), "no release is live yet")

	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	require.NoError(t, repo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, at))
	live, err = repo.LivePointer(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, live)

	require.NoError(t, repo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 2}, at.Add(time.Minute)))
	live, err = repo.LivePointer(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 2}, live)
	assert.Equal(t, 1, countTopoRows(t, db, `SELECT count(*) FROM live_release WHERE tenant_id = $1`, tenant))
}

func TestTopologyRepository_NodeContentHash(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "aa", topoSpec("raw", "orders", "dbt-seed", "h1")))
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)

	hash, found, err := repo.NodeContentHash(ctx, tenant, "rel-1", "raw.orders")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "h1", hash)

	_, found, err = repo.NodeContentHash(ctx, tenant, "rel-1", "raw.missing")
	require.NoError(t, err)
	assert.False(t, found)
}

// The topology lock holds a second transaction back until the first ends.
func TestTopologyRepository_LockTopologySerialisesTransactions(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx1.Rollback()
	require.NoError(t, postgres.NewTopologyRepository(db, tx1).LockTopology(ctx))

	done := make(chan error, 1)
	go func() {
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer tx2.Rollback()
		done <- postgres.NewTopologyRepository(db, tx2).LockTopology(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("the second LockTopology must wait while the first transaction holds the lock, returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx1.Rollback())

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the second LockTopology did not proceed after the first transaction ended")
	}
}

func TestTopologyRepository_LockAndSaveNeedATransaction(t *testing.T) {
	ctx := context.Background()
	repo := postgres.NewTopologyRepository(newTestDB(t), nil)
	assert.Error(t, repo.LockTopology(ctx))
	_, err := repo.SaveRelease(ctx, topology.Release{TenantID: "t", ReleaseID: "r"})
	assert.Error(t, err)
}

// Retention deletes a release loaded before the cutoff unless it is live or
// any run names it, finished or not; its nodes and edges go with it.
func TestTopologyRepository_PruneReleases(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	id := func(name string) string { return tenant + "-" + name }
	for _, name := range []string{"live", "running-run", "finished-run", "old", "recent"} {
		_, err := saveTopoRelease(t, db, topoRelease(t, tenant, id(name), 1, "aa",
			topoSpec("raw", "a", "dbt-seed", "h1"), topoSpec("raw", "b", "dbt-model", "h2", "raw.a")))
		require.NoError(t, err)
	}
	_, err := db.Exec(`UPDATE topology_releases SET loaded_at = '2000-01-01T00:00:00Z' WHERE tenant_id = $1 AND release_id <> $2`,
		tenant, id("recent"))
	require.NoError(t, err)
	repo := postgres.NewTopologyRepository(db, nil)
	require.NoError(t, repo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: id("live"), PromotionSeq: 1}, time.Now()))
	for name, status := range map[string]string{"running-run": "running", "finished-run": "succeeded"} {
		runID := uuid.New()
		_, err := db.Exec(`
			INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, initialization_status, release_id)
			VALUES ($5, $1, $2, $3, now(), 'completed', $4)`, runID, "prune-"+runID.String()[:8], status, id(name), tenant)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runID) })
	}

	pruned, err := repo.PruneReleases(ctx, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, pruned, int64(1))

	var kept []string
	require.NoError(t, db.Select(&kept, `
		SELECT release_id FROM topology_releases WHERE tenant_id = $1 ORDER BY release_id COLLATE "C"`, tenant))
	assert.Equal(t, []string{id("finished-run"), id("live"), id("recent"), id("running-run")}, kept)
	assert.Zero(t, countTopoRows(t, db, `
		SELECT count(*) FROM topology_nodes WHERE tenant_id = $1 AND release_id = $2`, tenant, id("old")))
	assert.Zero(t, countTopoRows(t, db, `
		SELECT count(*) FROM topology_edges WHERE tenant_id = $1 AND release_id = $2`, tenant, id("old")))
}

// The unit of work's Topology() writes inside its transaction.
func TestPostgresUnitOfWork_TopologyWritesRollBackWithTheTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	logger := discardLogger()
	u := postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, logger)

	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.Topology().LockTopology(ctx))
	loaded, err := u.Topology().SaveRelease(ctx, topoRelease(t, tenant, "rel-1", 1, "aa", topoSpec("raw", "orders", "dbt-seed", "h1")))
	require.NoError(t, err)
	require.True(t, loaded)
	require.NoError(t, u.Rollback())

	assert.Zero(t, countTopoRows(t, db, `SELECT count(*) FROM topology_releases WHERE tenant_id = $1`, tenant))
}

// A stored release is in the planner's statistics once its transaction
// commits: the planner estimates its edges near their number, not as the one
// row it assumes for a release the statistics do not list.
func TestTopologyRepository_SaveReleaseRefreshesThePlannerStatistics(t *testing.T) {
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	rel := dag2000.release(t, tenant, "rel-stats")
	loaded, err := saveTopoRelease(t, db, rel)
	require.NoError(t, err)
	require.True(t, loaded)

	var raw string
	require.NoError(t, db.Get(&raw, `EXPLAIN (FORMAT JSON) SELECT * FROM topology_edges WHERE tenant_id = $1 AND release_id = $2`, tenant, "rel-stats"))
	var plans []struct {
		Plan struct {
			Rows float64 `json:"Plan Rows"`
		} `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &plans))
	require.Len(t, plans, 1)
	assert.Greater(t, plans[0].Plan.Rows, float64(len(rel.Edges))/2,
		"estimated %v of the release's %d edges", plans[0].Plan.Rows, len(rel.Edges))
}
