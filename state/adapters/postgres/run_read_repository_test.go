package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
)

// readRun inserts a run of scheduleName on releaseID (nil when it names none)
// and deletes it, with its tasks, when the test ends. A completed run gets a
// completion time.
func readRun(t *testing.T, db *sqlx.DB, scheduleName string, releaseID *string, promotionSeq *int64, status string, completed bool) uuid.UUID {
	t.Helper()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, release_id, promotion_seq, completed_at)
		VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END)`,
		runID, scheduleName, status, releaseID, promotionSeq, completed)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, runID)
	})
	return runID
}

// readTask inserts a task of runID for the node "raw.<table>" of service svc.
func readTask(t *testing.T, db *sqlx.DB, runID uuid.UUID, table, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
		VALUES ($1, $2, 'svc', 'raw', $3::text, $4, $5, 2, 'raw.' || $3::text)`,
		id, runID, table, "job-"+id.String()[:8], status)
	require.NoError(t, err)
	return id
}

func TestRunReadRepository_GraphComesFromTheRunsOwnRelease(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	// rel-5: a -> b -> c. rel-6 (live) drops the edge a -> b.
	insertSchemaRelease(t, db, tenant, "rel-5", "raw.a", "raw.b", "raw.c")
	insertSchemaRelease(t, db, tenant, "rel-6", "raw.a", "raw.b", "raw.c")
	_, err := db.Exec(`
		INSERT INTO topology_edges (tenant_id, release_id, upstream_key, downstream_key) VALUES
		($1, 'rel-5', 'raw.a', 'raw.b'), ($1, 'rel-5', 'raw.b', 'raw.c'), ($1, 'rel-6', 'raw.b', 'raw.c')`, tenant)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, 'rel-6', 6, now())`, tenant)
	require.NoError(t, err)
	rel, seq := "rel-5", int64(5)
	runID := readRun(t, db, "graph-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	readTask(t, db, runID, "a", "succeeded")
	readTask(t, db, runID, "b", "running")
	readTask(t, db, runID, "c", "pending")

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(ctx, tenant, runID)
	require.NoError(t, err)

	assert.Equal(t, int64(5), g.RunPromotionSeq)
	assert.Equal(t, int64(6), g.LatestPromotionSeq)
	require.Len(t, g.Nodes, 3)
	assert.Equal(t, "dbt-seed", g.Nodes[0].NodeType)
	assert.Equal(t, run.TaskStatusSucceeded, g.Nodes[0].Status)
	assert.Equal(t, []projection.RunGraphEdge{
		{FromNodeID: "svc.raw.b", ToNodeID: "svc.raw.a"},
		{FromNodeID: "svc.raw.c", ToNodeID: "svc.raw.b"},
	}, g.Edges)
}

func TestRunReadRepository_RunWithoutAReleaseHasNoEdges(t *testing.T) {
	db := newTestDB(t)
	runID := readRun(t, db, "legacy-"+uuid.NewString()[:8], nil, nil, "running", false)
	readTask(t, db, runID, "a", "running")

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(context.Background(), "default", runID)
	require.NoError(t, err)
	require.Len(t, g.Nodes, 1)
	assert.Empty(t, g.Nodes[0].NodeType)
	assert.Empty(t, g.Edges)
	assert.Equal(t, int64(0), g.RunPromotionSeq)
}

func TestRunReadRepository_UnknownRunHasAnEmptyGraph(t *testing.T) {
	db := newTestDB(t)

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(context.Background(), "default", uuid.New())
	require.NoError(t, err)
	assert.Empty(t, g.Nodes)
	assert.Empty(t, g.Edges)
	assert.Equal(t, int64(0), g.RunPromotionSeq)
}

func TestRunReadRepository_ListsCompletedRunsNewestFirst(t *testing.T) {
	db := newTestDB(t)
	schedule := "list-" + uuid.NewString()[:8]
	first := readRun(t, db, schedule, nil, nil, "succeeded", true)
	_, err := db.Exec(`UPDATE scheduler_tracker SET created_at = created_at - interval '1 hour' WHERE schedule_id = $1`, first)
	require.NoError(t, err)
	second := readRun(t, db, schedule, nil, nil, "failed", true)
	readRun(t, db, schedule, nil, nil, "running", false)

	runs, total, err := postgres.NewRunReadRepository(db).ListCompletedRuns(context.Background(), schedule, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, runs, 1)
	assert.Equal(t, second, runs[0].RunID)
	assert.Equal(t, run.SchedulerStatusFailed, runs[0].TerminalStatus)
}

func TestRunReadRepository_DriftsAreTheNewestActiveRunPerSchedule(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-9", "raw.a")
	_, err := db.Exec(`INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, 'rel-9', 9, now())`, tenant)
	require.NoError(t, err)
	schedule := "drift-" + uuid.NewString()[:8]
	rel, seq := "rel-9", int64(7)
	newest := readRun(t, db, schedule, &rel, &seq, "running", false)
	readRun(t, db, schedule, nil, nil, "succeeded", true)

	latest, drifts, err := postgres.NewRunReadRepository(db).ListActiveRunDrifts(context.Background(), tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(9), latest)
	assert.Contains(t, drifts, projection.ActiveRunDrift{ScheduleName: schedule, RunID: newest, RunPromotionSeq: 7})
}
