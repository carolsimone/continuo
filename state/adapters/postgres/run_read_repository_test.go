package postgres_test

import (
	"context"
	"testing"
	"time"

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
		INSERT INTO runs (run_id, schedule_name, status, release_id, promotion_seq, completed_at)
		VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() END)`,
		runID, scheduleName, status, releaseID, promotionSeq, completed)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runID)
	})
	return runID
}

// readTask inserts a task of runID for the node "raw.<table>" of service svc.
func readTask(t *testing.T, db *sqlx.DB, runID uuid.UUID, table, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO tasks (task_id, run_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
		VALUES ($1, $2, 'svc', 'raw', $3::text, $4, $5, 2, 'raw.' || $3::text)`,
		id, runID, table, "job-"+id.String()[:8], status)
	require.NoError(t, err)
	return id
}

// waitForLease makes a pending task wait for the write lease on its node since
// the given instant, as a dispatch that finds its node held stores it.
func waitForLease(t *testing.T, db *sqlx.DB, taskID uuid.UUID, since time.Time) {
	t.Helper()
	_, err := db.Exec(`UPDATE tasks SET wait_reason = 'node_lease', lease_wait_since = $2 WHERE task_id = $1`, taskID, since)
	require.NoError(t, err)
}

// A pending task waiting for its node's lease carries the reason, when it began
// waiting and the run whose task holds the node; the holder itself, a task
// waiting for its upstreams and a waiter whose node no task holds name no
// holder run.
func TestGetRunGraph_ShowsALeaseWaitAndItsHolder(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-w", "raw.a", "raw.b", "raw.c")
	rel, seq := "rel-w", int64(1)
	holderRun := readRun(t, db, "cron-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	holder := readTask(t, db, holderRun, "a", "running")
	require.NoError(t, insertLease(db, tenant, "raw.a", holder, holderRun))

	waitingRun := readRun(t, db, "single-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	waiter := readTask(t, db, waitingRun, "a", "pending")
	since := time.Date(2026, 10, 24, 10, 5, 0, 0, time.UTC)
	waitForLease(t, db, waiter, since)
	readTask(t, db, waitingRun, "b", "pending")
	freedWaiter := readTask(t, db, waitingRun, "c", "pending")
	waitForLease(t, db, freedWaiter, since)

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(ctx, tenant, waitingRun)
	require.NoError(t, err)
	require.Len(t, g.Nodes, 3)
	node := g.Nodes[0]
	assert.Equal(t, "a", node.TableName)
	assert.Equal(t, run.WaitReasonNodeLease, node.WaitReason)
	require.NotNil(t, node.WaitingOnRunID)
	assert.Equal(t, holderRun, *node.WaitingOnRunID)
	require.NotNil(t, node.WaitingSince)
	assert.True(t, since.Equal(*node.WaitingSince), "waiting since %v, want %v", *node.WaitingSince, since)

	upstreamWait := g.Nodes[1]
	assert.Empty(t, upstreamWait.WaitReason, "a task waiting for its upstreams has no wait reason")
	assert.Nil(t, upstreamWait.WaitingOnRunID)
	assert.Nil(t, upstreamWait.WaitingSince)

	freed := g.Nodes[2]
	assert.Equal(t, run.WaitReasonNodeLease, freed.WaitReason, "a waiter whose node was freed still waits until it is granted")
	assert.Nil(t, freed.WaitingOnRunID, "no task holds its node")
	require.NotNil(t, freed.WaitingSince)

	held, err := postgres.NewRunReadRepository(db).GetRunGraph(ctx, tenant, holderRun)
	require.NoError(t, err)
	require.Len(t, held.Nodes, 1)
	assert.Empty(t, held.Nodes[0].WaitReason)
	assert.Nil(t, held.Nodes[0].WaitingOnRunID, "the holder waits on no run")
}

// A task never reads its own lease as the lease it waits for, and a lease of
// another tenant on the same node key is not the tenant's holder.
func TestGetRunGraph_AWaiterIsNeverItsOwnHolder(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-s", "raw.a", "raw.b")
	rel, seq := "rel-s", int64(1)
	runID := readRun(t, db, "self-"+uuid.NewString()[:8], &rel, &seq, "running", false)
	self := readTask(t, db, runID, "a", "pending")
	waitForLease(t, db, self, time.Now())
	require.NoError(t, insertLease(db, tenant, "raw.a", self, runID))

	otherTenantRun := readRun(t, db, "other-"+uuid.NewString()[:8], nil, nil, "running", false)
	otherTenantTask := readTask(t, db, otherTenantRun, "b", "running")
	require.NoError(t, insertLease(db, "other-"+uuid.NewString(), "raw.b", otherTenantTask, otherTenantRun))
	waiter := readTask(t, db, runID, "b", "pending")
	waitForLease(t, db, waiter, time.Now())

	g, err := postgres.NewRunReadRepository(db).GetRunGraph(ctx, tenant, runID)
	require.NoError(t, err)
	require.Len(t, g.Nodes, 2)
	assert.Nil(t, g.Nodes[0].WaitingOnRunID, "the lease on its node is its own")
	assert.Nil(t, g.Nodes[1].WaitingOnRunID, "the lease on its node key belongs to another tenant")
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
	_, err := db.Exec(`UPDATE runs SET created_at = created_at - interval '1 hour' WHERE run_id = $1`, first)
	require.NoError(t, err)
	second := readRun(t, db, schedule, nil, nil, "failed", true)
	_, err = db.Exec(`UPDATE runs SET failure_reason = 'stalled' WHERE run_id = $1`, second)
	require.NoError(t, err)
	readRun(t, db, schedule, nil, nil, "running", false)

	runs, total, err := postgres.NewRunReadRepository(db).ListCompletedRuns(context.Background(), schedule, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, runs, 2)
	assert.Equal(t, second, runs[0].RunID)
	assert.Equal(t, run.RunStatusFailed, runs[0].TerminalStatus)
	assert.Equal(t, run.RunFailureStalled, runs[0].FailureReason, "a run that failed without a failed task says why")
	assert.Equal(t, first, runs[1].RunID)
	assert.Empty(t, runs[1].FailureReason)
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
