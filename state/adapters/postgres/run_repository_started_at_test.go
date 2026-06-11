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

// TestRunRepository_SaveRun_PersistsStartedAtOnAcceptDispatch walks the real
// PENDING→RUNNING production path: a PENDING run is created, loaded FOR UPDATE,
// then AcceptDispatch receives a non-terminal task projection (so the run
// transitions to RUNNING and stamps started_at). SaveRun must persist that
// started_at to scheduler_tracker. Before the fix the column stayed NULL forever
// because SaveRun never consulted IsStartedDirty().
func TestRunRepository_SaveRun_PersistsStartedAtOnAcceptDispatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	createdAt := time.Now().Add(-time.Minute)
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "started-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            createdAt,
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	// Before: started_at is NULL on a freshly created PENDING row.
	before, err := schedRepo.GetByID(ctx, scheduleID)
	require.NoError(t, err)
	require.Nil(t, before.StartedAt, "freshly created run has no started_at")

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := postgres.NewRunRepository(tx, schedRepo)
	tasks := postgres.NewTaskCollectionAdapter(taskRepo, tx)

	rn, err := runRepo.LoadRunForUpdate(ctx, scheduleID)
	require.NoError(t, err)

	// A single non-terminal task keeps the run RUNNING (not auto-finalized),
	// so AcceptDispatch stamps started_at and sets startedDirty.
	projection := []run.DispatchedTask{
		{
			TaskID:      uuid.New(),
			ServiceName: "svc",
			SchemaName:  "public",
			TableName:   "tbl",
			Status:      run.TaskStatusPending,
			MaxRetries:  3,
		},
	}
	_, err = rn.AcceptDispatch(ctx, tasks, projection, time.Now())
	require.NoError(t, err)
	require.Equal(t, run.SchedulerStatusRunning, rn.Status(), "run must be RUNNING after a non-terminal dispatch")

	require.NoError(t, runRepo.SaveRun(ctx, rn))
	require.NoError(t, tx.Commit())

	got, err := schedRepo.GetByID(ctx, scheduleID)
	require.NoError(t, err)
	require.NotNil(t, got.StartedAt, "started_at must be persisted on PENDING→RUNNING")
	assert.False(t, got.StartedAt.Before(got.CreatedAt),
		"started_at must be at or after created_at (valid_timestamps constraint)")
	assert.Equal(t, run.SchedulerStatusRunning, got.Status)
}

// TestRunRepository_SaveRun_SingleUpdateForDirtyColumns asserts that a SaveRun
// touching several dirty columns at once (status, init_status, total/terminal
// task counts, started_at) lands every value via the consolidated single
// UPDATE. The verification is behavioural: all five columns reflect the
// in-memory aggregate state after one SaveRun commit.
func TestRunRepository_SaveRun_SingleUpdateForDirtyColumns(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "multi-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now().Add(-time.Minute),
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE schedule_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := postgres.NewRunRepository(tx, schedRepo)
	tasks := postgres.NewTaskCollectionAdapter(taskRepo, tx)

	rn, err := runRepo.LoadRunForUpdate(ctx, scheduleID)
	require.NoError(t, err)

	// Two tasks, neither terminal: AcceptDispatch sets total_task_count=2,
	// terminal_task_count=0, init_status=completed, status=running, started_at.
	projection := []run.DispatchedTask{
		{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "public", TableName: "a", Status: run.TaskStatusPending, MaxRetries: 3},
		{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "public", TableName: "b", Status: run.TaskStatusPending, MaxRetries: 3},
	}
	_, err = rn.AcceptDispatch(ctx, tasks, projection, time.Now())
	require.NoError(t, err)

	require.NoError(t, runRepo.SaveRun(ctx, rn))
	require.NoError(t, tx.Commit())

	got, err := schedRepo.GetByID(ctx, scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.SchedulerStatusRunning, got.Status)
	assert.Equal(t, "completed", got.InitializationStatus)
	require.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(2), got.TotalTaskCount.Int32)
	assert.Equal(t, int32(0), got.TerminalTaskCount)
	require.NotNil(t, got.StartedAt)
}

// TestSchedulerRepository_UpdateRunRowTx_StartedAt verifies the consolidated
// update writes only the named columns and leaves others untouched, exercising
// the started_at column directly.
func TestSchedulerRepository_UpdateRunRowTx_StartedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	createdAt := time.Now().Add(-time.Minute)
	require.NoError(t, repo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "urr-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            createdAt,
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	started := time.Now()
	status := string(run.SchedulerStatusRunning)
	require.NoError(t, repo.UpdateRunRowTx(ctx, tx, scheduleID, postgres.RunRowUpdate{
		Status:    &status,
		StartedAt: &started,
	}))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.SchedulerStatusRunning, got.Status)
	require.NotNil(t, got.StartedAt)
	// init_status was not in the update set — it must be unchanged.
	assert.Equal(t, "in_progress", got.InitializationStatus)
}

// TestSchedulerRepository_UpdateRunRowTx_NoFieldsIsNoop asserts that an empty
// dirty set issues no statement and returns nil (SaveRun on a run with no
// dirty columns must not error or touch the row).
func TestSchedulerRepository_UpdateRunRowTx_NoFieldsIsNoop(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "noop-" + scheduleID.String()[:8],
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "in_progress",
		Kind:                 "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// A non-existent id with no fields must still be a no-op (no row check runs).
	require.NoError(t, repo.UpdateRunRowTx(ctx, tx, uuid.New(), postgres.RunRowUpdate{}))
	require.NoError(t, tx.Commit())
}
