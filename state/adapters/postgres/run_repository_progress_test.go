package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunRepository_SaveRun_PersistsProgressClock walks a run through its start
// and its first applied task status change, saving after each, and asserts
// runs.last_progress_at holds the instant of the latest one.
func TestRunRepository_SaveRun_PersistsProgressClock(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "progress-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now().Add(-time.Hour),
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE run_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", scheduleID)

	// saveIn loads the run FOR UPDATE, applies mutate, saves it and commits.
	saveIn := func(mutate func(rn *run.Run, tasks run.TaskCollection)) {
		t.Helper()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		runRepo := postgres.NewRunRepository(tx, schedRepo)
		rn, err := runRepo.LoadRunForUpdate(ctx, scheduleID)
		require.NoError(t, err)
		mutate(rn, postgres.NewTaskCollectionAdapter(taskRepo, tx, "default"))
		require.NoError(t, runRepo.SaveRun(ctx, rn))
		require.NoError(t, tx.Commit())
	}
	heartbeat := func() time.Time {
		t.Helper()
		got, err := schedRepo.GetByID(ctx, scheduleID)
		require.NoError(t, err)
		require.NotNil(t, got.LastHeartbeatAt, "last_progress_at must be persisted")
		return *got.LastHeartbeatAt
	}

	dispatchedAt := time.Now().Add(-30 * time.Minute).UTC().Truncate(time.Microsecond)
	g := topology.NewGraph([]topology.Node{{
		Key: "public.tbl", ServiceName: "svc", SchemaName: "public", TableName: "tbl", NodeType: "dbt-model",
	}}, nil)
	plan, err := runplan.SingleNode{Key: "public.tbl", ServiceName: "svc"}.Select(g)
	require.NoError(t, err)
	saveIn(func(rn *run.Run, tasks run.TaskCollection) {
		_, err := rn.Start(ctx, tasks, "default", plan, g, testTaskDefaults, dispatchedAt)
		require.NoError(t, err)
	})
	taskID := run.TaskIDFor("default", scheduleID, "public.tbl")
	got := heartbeat()
	assert.True(t, got.Equal(dispatchedAt), "after the start: last_progress_at = %v, want %v", got, dispatchedAt)

	runningAt := time.Now().UTC().Truncate(time.Microsecond)
	saveIn(func(rn *run.Run, tasks run.TaskCollection) {
		_, _, err := rn.RecordAttemptReport(ctx, tasks, nil,
			run.AttemptReport{TaskID: taskID, Attempt: 0, Status: run.AttemptRunning}, testTaskDefaults, runningAt)
		require.NoError(t, err)
	})
	got = heartbeat()
	assert.True(t, got.Equal(runningAt), "after the task started: last_progress_at = %v, want %v", got, runningAt)
}
