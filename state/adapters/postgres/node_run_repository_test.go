package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeRunRepository_List seeds two scheduler runs (cron + rerun) each with a
// task on (svc, sch, tbl), one extra task on (svc, sch, other) that must be
// excluded, and one execution per task.  It verifies ordering (rerun first),
// field projection, and that the unrelated task is filtered out.
func TestNodeRunRepository_List(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	execRepo := postgres.NewTaskExecutionRepository(db, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-nr-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]
	other := "other-" + uuid.New().String()[:8]

	// --- scheduler 1: cron, succeeded ---
	sched1ID := uuid.New()
	sched1 := &model.SchedulerTracker{
		ScheduleID:           sched1ID,
		ScheduleName:         sch,
		Status:               model.SchedulerStatusSucceeded,
		Kind:                 "cron",
		CreatedAt:            time.Now().Add(-2 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched1))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched1ID)

	// task1: on the target node
	task1ID := uuid.New()
	task1 := &model.TaskTracker{
		TaskID:          task1ID,
		ScheduleID:      sched1ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-1",
		Status:          model.TaskStatusSucceeded,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "m1",
		ImageTag:        "v1",
		CreatedAt:       time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task1))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task1ID)

	// execution for task1
	exec1 := &model.TaskExecution{
		ID:        uuid.New(),
		TaskID:    task1ID,
		CreatedAt: time.Now().Add(-2 * time.Minute),
		StartedAt: ptrTime(time.Now().Add(-90 * time.Second)),
		CompletedAt: ptrTime(time.Now().Add(-60 * time.Second)),
	}
	require.NoError(t, execRepo.Create(ctx, exec1))
	defer db.ExecContext(ctx, "DELETE FROM task_execution WHERE id = $1", exec1.ID)

	// extra task on (svc, sch, other) — must NOT appear in results
	taskExtraID := uuid.New()
	taskExtra := &model.TaskTracker{
		TaskID:          taskExtraID,
		ScheduleID:      sched1ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       other,
		JobName:         "job-extra",
		Status:          model.TaskStatusSucceeded,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "mx",
		ImageTag:        "vx",
		CreatedAt:       time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, taskExtra))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskExtraID)

	// --- scheduler 2: rerun (source = sched1), failed — created more recently ---
	sched2ID := uuid.New()
	sched2 := &model.SchedulerTracker{
		ScheduleID:           sched2ID,
		ScheduleName:         sch,
		Status:               model.SchedulerStatusFailed,
		Kind:                 "rerun",
		SourceRunID:          &sched1ID,
		CreatedAt:            time.Now().Add(-1 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched2))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched2ID)

	// task2: on the target node, rerun
	task2ID := uuid.New()
	errMsg := "boom"
	task2 := &model.TaskTracker{
		TaskID:          task2ID,
		ScheduleID:      sched2ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-2",
		Status:          model.TaskStatusFailed,
		RetryCount:      1,
		MaxRetries:      3,
		ManifestVersion: "m2",
		ImageTag:        "v2",
		CreatedAt:       time.Now().Add(-1 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task2))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task2ID)

	// execution for task2 (with error_message)
	exec2 := &model.TaskExecution{
		ID:           uuid.New(),
		TaskID:       task2ID,
		CreatedAt:    time.Now().Add(-1 * time.Minute),
		StartedAt:    ptrTime(time.Now().Add(-50 * time.Second)),
		CompletedAt:  ptrTime(time.Now().Add(-30 * time.Second)),
		ErrorMessage: &errMsg,
	}
	require.NoError(t, execRepo.Create(ctx, exec2))
	defer db.ExecContext(ctx, "DELETE FROM task_execution WHERE id = $1", exec2.ID)

	// --- query ---
	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, 50)
	require.NoError(t, err)
	require.Len(t, rows, 2, "expected exactly 2 rows for (svc, sch, tbl)")

	// ordered by scheduler_tracker.created_at DESC: sched2 (rerun) first
	assert.Equal(t, sched2ID, rows[0].ScheduleID)
	assert.Equal(t, "rerun", rows[0].Kind)
	assert.Equal(t, "v2", rows[0].ImageTag)
	assert.Equal(t, "m2", rows[0].ManifestVersion)
	assert.NotNil(t, rows[0].StartedAt)
	assert.NotNil(t, rows[0].CompletedAt)
	require.NotNil(t, rows[0].ErrorMessage)
	assert.Equal(t, "boom", *rows[0].ErrorMessage)

	assert.Equal(t, sched1ID, rows[1].ScheduleID)
	assert.Equal(t, "cron", rows[1].Kind)
}

// TestNodeRunRepository_List_TaskWithoutExecution verifies that a task with no
// corresponding task_execution row produces a NodeRun with nil StartedAt and
// nil CompletedAt (LEFT JOIN semantics).
func TestNodeRunRepository_List_TaskWithoutExecution(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-noe-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]

	schedID := uuid.New()
	sched := &model.SchedulerTracker{
		ScheduleID:           schedID,
		ScheduleName:         sch,
		Status:               model.SchedulerStatusRunning,
		Kind:                 "cron",
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", schedID)

	taskID := uuid.New()
	task := &model.TaskTracker{
		TaskID:          taskID,
		ScheduleID:      schedID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-pending",
		Status:          model.TaskStatusPending,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "m1",
		ImageTag:        "v1",
		CreatedAt:       time.Now(),
	}
	require.NoError(t, taskRepo.Create(ctx, task))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskID)

	// No task_execution row inserted.

	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, 50)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Nil(t, rows[0].StartedAt)
	assert.Nil(t, rows[0].CompletedAt)
}

// TestNodeRunRepository_List_NoMatch verifies that querying for a node identity
// with no data returns an empty slice and no error.
func TestNodeRunRepository_List_NoMatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	rows, err := nodeRunRepo.List(ctx, "nonexistent-svc", "nonexistent-sch", "nonexistent-tbl", 50)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func ptrTime(t time.Time) *time.Time { return &t }
