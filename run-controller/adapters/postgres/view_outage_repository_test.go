//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertOutageRun inserts a running run of tenant and one task per entry of
// rebuildOf, keyed by node key and naming the seed whose load it rebuilds
// after ("" for none). The run, its tasks and the tenant's outages go when
// the test ends.
func insertOutageRun(t *testing.T, db *sqlx.DB, tenant string, rebuildOf map[topology.NodeKey]topology.NodeKey) uuid.UUID {
	t.Helper()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status)
		VALUES ($1, $2, $3, 'running')`, tenant, runID, "outages-"+runID.String()[:8])
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, runID) })
	for key, seed := range rebuildOf {
		_, err := db.Exec(`
			INSERT INTO tasks (tenant_id, task_id, run_id, service_name, schema_name, table_name, job_name, status,
			                   max_retries, node_key, rebuild_seed_key)
			VALUES ($1, $2, $3, 'svc', 'stg', $4, 'job-0', 'pending', 2, $5, $6)`,
			tenant, uuid.New(), runID, strings.TrimPrefix(string(key), "stg."), string(key), string(seed))
		require.NoError(t, err)
	}
	return runID
}

// A seed load opens one outage per task of its run that rebuilds after it,
// and nothing for its other tasks or another run's rebuilds. A second open
// keeps the time the outage opened; a close returns that time once.
func TestViewOutageRepository_OpensForTheSeedsRebuildsAndClosesOnSuccess(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	outages := postgres.NewViewOutageRepository(db, nil)
	runR := insertOutageRun(t, db, tenant, map[topology.NodeKey]topology.NodeKey{
		"stg.s": "", "stg.v1": "stg.s", "stg.v2": "stg.s", "stg.t": "",
	})
	insertOutageRun(t, db, tenant, map[topology.NodeKey]topology.NodeKey{"stg.v3": "stg.s"})
	t0 := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	before, err := outages.CountByState(ctx)
	require.NoError(t, err)

	n, err := outages.OpenForRebuilds(ctx, tenant, runR, "stg.s", seeds.OutageMissing, t0)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	_, err = outages.OpenForRebuilds(ctx, tenant, runR, "stg.s", seeds.OutageMissing, t0.Add(time.Minute))
	require.NoError(t, err)

	got, found, err := outages.Get(ctx, tenant, "stg.v2")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, seeds.ViewOutage{Key: "stg.v2", State: seeds.OutageMissing, Since: got.Since, SeedKey: "stg.s", RunID: runR}, got)
	assert.True(t, got.Since.Equal(t0))
	_, found, err = outages.Get(ctx, tenant, "stg.v3")
	require.NoError(t, err)
	assert.False(t, found, "another run's rebuild opens nothing")

	since, found, err := outages.Close(ctx, tenant, "stg.v1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, since.Equal(t0), "an open outage keeps the time it opened")
	_, found, _ = outages.Close(ctx, tenant, "stg.v1")
	assert.False(t, found)

	counts, err := outages.CountByState(ctx)
	require.NoError(t, err)
	assert.Equal(t, before[seeds.OutageMissing]+1, counts[seeds.OutageMissing])
	assert.Equal(t, before[seeds.OutageStale], counts[seeds.OutageStale])
}

// A promotion deletes the outages of the nodes its release does not hold, of
// its own tenant only.
func TestViewOutageRepository_DeletesTheOutagesOfNodesTheReleaseDropped(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant, other := newTopoTenant(t, db), newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-2", 2, "aa",
		topoSpec("stg", "s", "dbt-seed", "h1"), topoSpec("stg", "v1", "dbt-model", "h2", "stg.s")))
	require.NoError(t, err)
	outages := postgres.NewViewOutageRepository(db, nil)
	at := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)
	for _, tn := range []string{tenant, other} {
		runID := insertOutageRun(t, db, tn, map[topology.NodeKey]topology.NodeKey{"stg.v1": "stg.s", "stg.v2": "stg.s"})
		_, err := outages.OpenForRebuilds(ctx, tn, runID, "stg.s", seeds.OutageStale, at)
		require.NoError(t, err)
	}

	removed, err := outages.DeleteAbsentFrom(ctx, tenant, "rel-2")
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)

	_, found, err := outages.Get(ctx, tenant, "stg.v2")
	require.NoError(t, err)
	assert.False(t, found, "a node the release dropped loses its outage")
	_, found, err = outages.Get(ctx, tenant, "stg.v1")
	require.NoError(t, err)
	assert.True(t, found, "a node the release holds keeps its outage")
	_, found, err = outages.Get(ctx, other, "stg.v2")
	require.NoError(t, err)
	assert.True(t, found, "another tenant's outage stays")
}

// materializedSpec is a dbt node of stg with the materialization, reading
// upstream.
func materializedSpec(table, nodeType, materialized string, upstream ...string) topology.NodeSpec {
	spec := topoSpec("stg", table, nodeType, "h-"+table, upstream...)
	spec.Materialized = materialized
	return spec
}

// A seed load opens an outage for every rebuild of its run it reaches through
// views, materialized views and ephemeral nodes, whichever seed the rebuild is
// stamped for, and nothing past a table. Release rel-1 holds two seeds and
// their readers:
//
//	stg.sa ─┬─> stg.v (view, stamped for stg.sa)
//	stg.sb ─┤
//	        ├─> stg.e (ephemeral) ─> stg.w (view, stamped for stg.sb)
//	        └─> stg.t (table) ─┬─> stg.x (view, stamped for stg.so)
//	stg.so ────────────────────┘
//
// stg.sb loads first: stg.v and stg.w open with stg.sb as their seed, stg.x
// stays closed. stg.sa then fails, which records nothing, and stg.v's outage
// stays. A later load of stg.sa keeps the time stg.v's outage opened.
func TestViewOutageRepository_ASeedLoadOpensEveryRebuildItReaches(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, tenant, "rel-1", 1, "aa",
		materializedSpec("sa", "dbt-seed", "seed"),
		materializedSpec("sb", "dbt-seed", "seed"),
		materializedSpec("so", "dbt-seed", "seed"),
		materializedSpec("v", "dbt-model", "view", "stg.sa", "stg.sb"),
		materializedSpec("e", "dbt-model", "ephemeral", "stg.sb"),
		materializedSpec("w", "dbt-model", "view", "stg.e"),
		materializedSpec("t", "dbt-model", "table", "stg.sb"),
		materializedSpec("x", "dbt-model", "view", "stg.t", "stg.so")))
	require.NoError(t, err)
	runID := insertOutageRun(t, db, tenant, map[topology.NodeKey]topology.NodeKey{
		"stg.sa": "", "stg.sb": "", "stg.v": "stg.sa", "stg.w": "stg.sb", "stg.t": "", "stg.x": "stg.so",
	})
	_, err = db.Exec(`UPDATE runs SET release_id = 'rel-1', promotion_seq = 1 WHERE tenant_id = $1 AND run_id = $2`, tenant, runID)
	require.NoError(t, err)
	outages := postgres.NewViewOutageRepository(db, nil)
	t0 := time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

	n, err := outages.OpenForRebuilds(ctx, tenant, runID, "stg.sb", seeds.OutageMissing, t0)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	for _, key := range []topology.NodeKey{"stg.v", "stg.w"} {
		got, found, err := outages.Get(ctx, tenant, key)
		require.NoError(t, err)
		require.True(t, found, "%s is below stg.sb", key)
		assert.Equal(t, seeds.ViewOutage{Key: key, State: seeds.OutageMissing, Since: got.Since, SeedKey: "stg.sb", RunID: runID}, got)
		assert.True(t, got.Since.Equal(t0))
	}
	for _, key := range []topology.NodeKey{"stg.t", "stg.x", "stg.sa"} {
		_, found, err := outages.Get(ctx, tenant, key)
		require.NoError(t, err)
		assert.False(t, found, "%s is not a rebuild stg.sb reaches", key)
	}

	got, found, err := outages.Get(ctx, tenant, "stg.v")
	require.NoError(t, err)
	require.True(t, found, "stg.sa's failure leaves stg.v's outage open")
	assert.True(t, got.Since.Equal(t0))

	_, err = outages.OpenForRebuilds(ctx, tenant, runID, "stg.sa", seeds.OutageMissing, t0.Add(time.Hour))
	require.NoError(t, err)
	got, found, err = outages.Get(ctx, tenant, "stg.v")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, topology.NodeKey("stg.sa"), got.SeedKey)
	assert.True(t, got.Since.Equal(t0), "an open outage keeps the time it opened")
}
