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
