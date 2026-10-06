package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunRepository_SaveRun_PersistsProgressClock walks a run through dispatch
// and its first applied task status change, saving after each, and asserts
// scheduler_tracker.last_heartbeat_at holds the instant of the latest one.
func TestRunRepository_SaveRun_PersistsProgressClock(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "progress-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now().Add(-time.Hour),
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	// saveIn loads the run FOR UPDATE, applies mutate, saves it and commits.
	saveIn := func(mutate func(rn *run.Run, tasks run.TaskCollection)) {
		t.Helper()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		runRepo := postgres.NewRunRepository(tx, schedRepo)
		rn, err := runRepo.LoadRunForUpdate(ctx, scheduleID)
		require.NoError(t, err)
		mutate(rn, postgres.NewTaskCollectionAdapter(taskRepo, tx))
		require.NoError(t, runRepo.SaveRun(ctx, rn))
		require.NoError(t, tx.Commit())
	}
	heartbeat := func() time.Time {
		t.Helper()
		got, err := schedRepo.GetByID(ctx, scheduleID)
		require.NoError(t, err)
		require.NotNil(t, got.LastHeartbeatAt, "last_heartbeat_at must be persisted")
		return *got.LastHeartbeatAt
	}

	taskID := uuid.New()
	dispatchedAt := time.Now().Add(-30 * time.Minute).UTC().Truncate(time.Microsecond)
	saveIn(func(rn *run.Run, tasks run.TaskCollection) {
		_, err := rn.AcceptDispatch(ctx, tasks, []run.DispatchedTask{
			{TaskID: taskID, ServiceName: "svc", SchemaName: "public", TableName: "tbl", Status: run.TaskStatusPending, MaxRetries: 3},
		}, dispatchedAt)
		require.NoError(t, err)
	})
	got := heartbeat()
	assert.True(t, got.Equal(dispatchedAt), "after dispatch: last_heartbeat_at = %v, want %v", got, dispatchedAt)

	runningAt := time.Now().UTC().Truncate(time.Microsecond)
	saveIn(func(rn *run.Run, tasks run.TaskCollection) {
		_, err := rn.RecordTaskStatus(ctx, tasks, taskID, run.TaskStatusRunning, 0, runningAt)
		require.NoError(t, err)
	})
	got = heartbeat()
	assert.True(t, got.Equal(runningAt), "after the task started: last_heartbeat_at = %v, want %v", got, runningAt)
}
