package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeEdges stores release rel-1 of a new tenant: one dbt model of schema
// e2e_schema and schedule schedule per table, and the edges (upstream,
// downstream) among them. It returns the tenant.
func storeEdges(t *testing.T, db *sqlx.DB, schedule string, edges [][2]string, tables ...string) string {
	t.Helper()
	upstream := map[string][]string{}
	for _, e := range edges {
		upstream[e[1]] = append(upstream[e[1]], "e2e_schema."+e[0])
	}
	specs := make([]topology.NodeSpec, 0, len(tables))
	for _, table := range tables {
		specs = append(specs, topology.NodeSpec{
			UniqueID: "e2e_schema." + table, ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table,
			NodeType: "dbt-model", ContentHash: "h-" + table, ImageTag: "service-1:5", ScheduleName: schedule,
			UpstreamUniqueIDs: upstream[table],
		})
	}
	tenant := planningTenant(t, db)
	rel, err := topology.NewRelease(tenant, "rel-1", 1, "s3://continuo/k", "sha", specs)
	require.NoError(t, err)
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = postgres.NewTopologyRepository(db, tx).SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return tenant
}

// storeFailureSchedule stores e2e-schedule-failure as release rel-1.
func storeFailureSchedule(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	return storeEdges(t, db, "e2e-schedule-failure", [][2]string{
		{"ftable_a", "ftable_c"}, {"ftable_b", "ftable_c"}, {"ftable_c", "ftable_d"}, {"ftable_c", "ftable_e"},
		{"ftable_d", "ftable_f"}, {"ftable_e", "ftable_f"}, {"ftable_a", "ftable_g"}, {"ftable_g", "ftable_h"},
	}, "ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h")
}

// insertTasks stores a run on rel-1 holding one undispatched task per entry of
// statuses, keyed by e2e_schema table, and returns the run id and each task's
// id. The run and its tasks are deleted when the test ends.
func insertTasks(t *testing.T, db *sqlx.DB, statuses map[string]run.TaskStatus) (uuid.UUID, map[string]uuid.UUID) {
	t.Helper()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, release_id, promotion_seq)
		VALUES ($1, $2, 'running', 'rel-1', 1)`, runID, "graph-"+runID.String()[:8])
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runID)
	})
	ids := make(map[string]uuid.UUID, len(statuses))
	for table, status := range statuses {
		ids[table] = uuid.New()
		_, err := db.Exec(`
			INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
			VALUES ($1, $2, 'service-1', 'e2e_schema', $3, $3, $4, 2, $5)`,
			ids[table], runID, table, string(status), "e2e_schema."+table)
		require.NoError(t, err)
	}
	return runID, ids
}

// inGraphTx runs fn with a TaskGraph of tenant inside a transaction it rolls
// back.
func inGraphTx(t *testing.T, db *sqlx.DB, tenant string, fn func(g *postgres.TaskGraph)) {
	t.Helper()
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	fn(postgres.NewTaskGraph(tx, tenant))
}

// The e2e-schedule-failure run: when ftable_a succeeds, ftable_c still waits for
// ftable_b and ftable_g is ready; when ftable_b succeeds, ftable_c is ready.
func TestTaskGraph_ReadyCandidatesOfTheExampleRun(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	setTaskStatus(t, db, r, "e2e_schema.ftable_a", run.TaskStatusSucceeded)

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.ReadyCandidates(ctx, r.ScheduleID(), "rel-1", "e2e_schema.ftable_a")
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, topology.NodeKey("e2e_schema.ftable_c"), got[0].Node.Key)
		assert.Equal(t, []run.TaskStatus{run.TaskStatusSucceeded, run.TaskStatusPending}, got[0].UpstreamStatuses)
		assert.False(t, got[0].Ready())
		assert.Equal(t, run.TaskIDFor(tenant, r.ScheduleID(), "e2e_schema.ftable_g"), got[1].TaskID)
		assert.Equal(t, topology.Node{
			Key: "e2e_schema.ftable_g", UniqueID: "e2e_schema.ftable_g", ServiceName: "service-1", SchemaName: "e2e_schema",
			TableName: "ftable_g", NodeType: "dbt-model", ContentHash: "h-ftable_g", ImageTag: "service-1:5",
			ScheduleName: "e2e-schedule-failure",
		}, got[1].Node)
		assert.Equal(t, []run.TaskStatus{run.TaskStatusSucceeded}, got[1].UpstreamStatuses)
		assert.True(t, got[1].Ready())
	})

	setTaskStatus(t, db, r, "e2e_schema.ftable_b", run.TaskStatusSucceeded)
	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.ReadyCandidates(ctx, r.ScheduleID(), "rel-1", "e2e_schema.ftable_b")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, topology.NodeKey("e2e_schema.ftable_c"), got[0].Node.Key)
		assert.True(t, got[0].Ready())
	})
}

// A downstream task already dispatched, or no longer pending, is not a
// candidate, so a redelivered success dispatches nothing twice.
func TestTaskGraph_ReadyCandidatesLeaveOutDispatchedAndFinishedTasks(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	setTaskStatus(t, db, r, "e2e_schema.ftable_a", run.TaskStatusSucceeded)
	_, err := db.Exec(`UPDATE task_tracker SET dispatched_at = $3 WHERE schedule_id = $1 AND node_key = $2`,
		r.ScheduleID(), "e2e_schema.ftable_g", time.Now())
	require.NoError(t, err)
	setTaskStatus(t, db, r, "e2e_schema.ftable_c", run.TaskStatusSkipped)

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.ReadyCandidates(ctx, r.ScheduleID(), "rel-1", "e2e_schema.ftable_a")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// An edge counts only when both its ends are tasks of the run: ftable_f reads
// ftable_d, which this run does not hold, so ftable_e's success makes it
// ready, and ftable_c's failure reaches it through ftable_e alone.
func TestTaskGraph_AnEdgeCountsOnlyBetweenTasksOfTheRun(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)

	succeeded, ids := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_c": run.TaskStatusSucceeded, "ftable_e": run.TaskStatusSucceeded, "ftable_f": run.TaskStatusPending,
	})
	failed, failedIDs := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_c": run.TaskStatusFailed, "ftable_e": run.TaskStatusPending, "ftable_f": run.TaskStatusPending,
	})

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.ReadyCandidates(ctx, succeeded, "rel-1", "e2e_schema.ftable_e")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, ids["ftable_f"], got[0].TaskID)
		assert.Equal(t, []run.TaskStatus{run.TaskStatusSucceeded}, got[0].UpstreamStatuses)
		assert.True(t, got[0].Ready())

		skipped, err := g.PendingDescendants(ctx, failed, "rel-1", "e2e_schema.ftable_c")
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{failedIDs["ftable_e"], failedIDs["ftable_f"]}, skipped)
	})
}

// The e2e-schedule-failure run: ftable_e's failure leaves ftable_f unable to run,
// ftable_g's leaves ftable_h; a failure of ftable_c reaches ftable_f by two
// paths and lists it once.
func TestTaskGraph_PendingDescendantsOfTheExampleRunsFailures(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	late, lateIDs := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusSucceeded, "ftable_b": run.TaskStatusSucceeded, "ftable_c": run.TaskStatusSucceeded,
		"ftable_d": run.TaskStatusSucceeded, "ftable_e": run.TaskStatusFailed, "ftable_f": run.TaskStatusPending,
		"ftable_g": run.TaskStatusFailed, "ftable_h": run.TaskStatusPending,
	})
	early, earlyIDs := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusSucceeded, "ftable_b": run.TaskStatusSucceeded, "ftable_c": run.TaskStatusFailed,
		"ftable_d": run.TaskStatusPending, "ftable_e": run.TaskStatusPending, "ftable_f": run.TaskStatusPending,
		"ftable_g": run.TaskStatusSucceeded, "ftable_h": run.TaskStatusSucceeded,
	})

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.PendingDescendants(ctx, late, "rel-1", "e2e_schema.ftable_e")
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{lateIDs["ftable_f"]}, got)
		got, err = g.PendingDescendants(ctx, late, "rel-1", "e2e_schema.ftable_g")
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{lateIDs["ftable_h"]}, got)
		got, err = g.PendingDescendants(ctx, early, "rel-1", "e2e_schema.ftable_c")
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{earlyIDs["ftable_d"], earlyIDs["ftable_e"], earlyIDs["ftable_f"]}, got)
	})
}

// A task that has succeeded, an inherited one included, stops the walk: a new
// node x feeding an inherited ftable_d does not take ftable_f down with it,
// since ftable_f reads the succeeded ftable_d, not x.
func TestTaskGraph_PendingDescendantsStopAtASucceededTask(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeEdges(t, db, "rebase", [][2]string{
		{"x", "ftable_d"}, {"ftable_d", "ftable_f"}, {"ftable_e", "ftable_f"},
	}, "x", "ftable_d", "ftable_e", "ftable_f")
	runID, ids := insertTasks(t, db, map[string]run.TaskStatus{
		"x": run.TaskStatusFailed, "ftable_d": run.TaskStatusSucceeded,
		"ftable_e": run.TaskStatusFailed, "ftable_f": run.TaskStatusPending,
	})

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		got, err := g.PendingDescendants(ctx, runID, "rel-1", "e2e_schema.x")
		require.NoError(t, err)
		assert.Empty(t, got)
		got, err = g.PendingDescendants(ctx, runID, "rel-1", "e2e_schema.ftable_e")
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{ids["ftable_f"]}, got)
	})
}

// cascade-2000: the root's 1,999 descendants, joined by about 5.5 × 10^7
// paths, come back in one query, each once.
func TestTaskGraph_PendingDescendantsOfTheCascadeRoot(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeShape(t, db, cascade2000)
	r := startScheduleRun(t, db, tenant, cascade2000.schedule(), model.OperationRun)

	inGraphTx(t, db, tenant, func(g *postgres.TaskGraph) {
		start := time.Now()
		got, err := g.PendingDescendants(ctx, r.ScheduleID(), "rel-1", "bench.fail_root")
		require.NoError(t, err)
		t.Logf("PendingDescendants of the cascade root: %d tasks in %s", len(got), time.Since(start))
		assert.Len(t, got, 1999)
		distinct := make(map[uuid.UUID]bool, len(got))
		for _, id := range got {
			distinct[id] = true
		}
		assert.Len(t, distinct, 1999)
		assert.NotContains(t, distinct, run.TaskIDFor(tenant, r.ScheduleID(), "bench.fail_root"))
	})
}
