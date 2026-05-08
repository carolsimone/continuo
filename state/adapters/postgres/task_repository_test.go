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

func createTask(t *testing.T, repo postgres.TaskTrackerRepository, scheduleID uuid.UUID) *model.TaskTracker {
	t.Helper()
	task := &model.TaskTracker{
		TaskID:      uuid.New(),
		ScheduleID:  scheduleID,
		ServiceName: "svc",
		SchemaName:  "schema",
		TableName:   "tbl_" + uuid.New().String()[:8],
		JobName:     "job",
		Status:      model.TaskStatusFailed,
		RetryCount:  3,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
	}
	err := repo.Create(context.Background(), task)
	require.NoError(t, err)
	return task
}

func TestTaskRepository_UpdateTx_UpdatesStatusAndRetryCount(t *testing.T) {
	db := newTestDB(t)
	// Need a scheduler record first for FK constraint
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	scheduler := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduler.ScheduleID)

	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	task := createTask(t, taskRepo, scheduler.ScheduleID)
	defer db.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE task_id = $1", task.TaskID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	task.Status = model.TaskStatusPending
	task.RetryCount = 0

	err = taskRepo.UpdateTx(context.Background(), tx, task)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	updated, err := taskRepo.GetByID(context.Background(), task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusPending, updated.Status)
	assert.Equal(t, 0, updated.RetryCount)
}

func TestTaskRepository_HasRetryableFailedTaskTx(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	ctx := context.Background()

	newScheduler := func(t *testing.T) *model.SchedulerTracker {
		t.Helper()
		s := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
		t.Cleanup(func() {
			db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", s.ScheduleID)
		})
		return s
	}

	insertTask := func(t *testing.T, scheduleID uuid.UUID, status model.TaskStatus, retryCount, maxRetries int) *model.TaskTracker {
		t.Helper()
		task := &model.TaskTracker{
			TaskID:      uuid.New(),
			ScheduleID:  scheduleID,
			ServiceName: "svc",
			SchemaName:  "schema",
			TableName:   "tbl_" + uuid.New().String()[:8],
			JobName:     "job",
			Status:      status,
			RetryCount:  retryCount,
			MaxRetries:  maxRetries,
			CreatedAt:   time.Now(),
		}
		require.NoError(t, taskRepo.Create(ctx, task))
		t.Cleanup(func() {
			db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task.TaskID)
		})
		return task
	}

	runCheck := func(t *testing.T, scheduleID uuid.UUID) bool {
		t.Helper()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		result, err := taskRepo.HasRetryableFailedTaskTx(ctx, tx, scheduleID)
		require.NoError(t, err)
		return result
	}

	t.Run("no failed tasks returns false", func(t *testing.T) {
		s := newScheduler(t)
		// Insert a pending task (not failed)
		insertTask(t, s.ScheduleID, model.TaskStatusPending, 0, 3)
		assert.False(t, runCheck(t, s.ScheduleID))
	})

	t.Run("failed task with retry_count < max_retries returns true", func(t *testing.T) {
		s := newScheduler(t)
		insertTask(t, s.ScheduleID, model.TaskStatusFailed, 1, 3)
		assert.True(t, runCheck(t, s.ScheduleID))
	})

	t.Run("failed task with retry_count equal to max_retries returns false", func(t *testing.T) {
		s := newScheduler(t)
		insertTask(t, s.ScheduleID, model.TaskStatusFailed, 3, 3)
		assert.False(t, runCheck(t, s.ScheduleID))
	})

	t.Run("failed task with retry_count greater than max_retries returns false", func(t *testing.T) {
		s := newScheduler(t)
		insertTask(t, s.ScheduleID, model.TaskStatusFailed, 5, 3)
		assert.False(t, runCheck(t, s.ScheduleID))
	})

	t.Run("empty schedule returns false", func(t *testing.T) {
		s := newScheduler(t)
		assert.False(t, runCheck(t, s.ScheduleID))
	})
}

func TestTaskTrackerRepository_BulkCancelByScheduleIDTx(t *testing.T) {
	db := newTestDB(t)
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	ctx := context.Background()

	schedID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &model.SchedulerTracker{
		ScheduleID:           schedID,
		ScheduleName:         "bulk-cancel-" + schedID.String(),
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", schedID)

	// Create tasks in different states
	pendingID := uuid.New()
	runningID := uuid.New()
	succeededID := uuid.New()
	for _, tt := range []struct {
		id     uuid.UUID
		status model.TaskStatus
	}{
		{pendingID, model.TaskStatusPending},
		{runningID, model.TaskStatusRunning},
		{succeededID, model.TaskStatusSucceeded},
	} {
		require.NoError(t, taskRepo.Create(ctx, &model.TaskTracker{
			TaskID:      tt.id,
			ScheduleID:  schedID,
			ServiceName: "svc",
			SchemaName:  "sch",
			TableName:   "t-" + tt.id.String()[:8],
			JobName:     "j-" + tt.id.String()[:8],
			Status:      tt.status,
			MaxRetries:  0,
			CreatedAt:   time.Now(),
		}))
	}
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE schedule_id = $1", schedID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	n, err := taskRepo.BulkCancelByScheduleIDTx(ctx, tx, schedID, "user1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.EqualValues(t, 2, n) // pending + running; succeeded untouched

	for _, id := range []uuid.UUID{pendingID, runningID} {
		got, err := taskRepo.GetByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, model.TaskStatusCancelled, got.Status)
		assert.NotNil(t, got.CancelledAt)
		assert.Equal(t, "user1", *got.CancelledBy)
	}

	// succeeded task must be untouched
	got, err := taskRepo.GetByID(ctx, succeededID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusSucceeded, got.Status)
}

func TestTaskTrackerRepository_CreateAndGet_RoundTripsImageTag(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	repo := postgres.NewTaskTrackerRepository(db, discardLogger())

	ctx := context.Background()
	parent := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "tt-" + uuid.New().String()[:8],
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "cron",
	}
	require.NoError(t, schedulerRepo.Create(ctx, parent))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", parent.ScheduleID)

	task := &model.TaskTracker{
		TaskID:          uuid.New(),
		ScheduleID:      parent.ScheduleID,
		CreatedAt:       time.Now(),
		ServiceName:     "service-1",
		SchemaName:      "analytics",
		TableName:       "daily_metrics",
		JobName:         "job-x",
		Status:          model.TaskStatusPending,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "v5",
		ImageTag:        "registry/img:abcdef",
	}
	require.NoError(t, repo.Create(ctx, task))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task.TaskID)

	got, err := repo.GetByID(ctx, task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "v5", got.ManifestVersion)
	assert.Equal(t, "registry/img:abcdef", got.ImageTag)
}

func TestTaskTrackerRepository_CreateAndGet_RoundTripsInheritedFromTaskID(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	repo := postgres.NewTaskTrackerRepository(db, discardLogger())

	ctx := context.Background()
	parent := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "tt-inh-" + uuid.New().String()[:8],
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "rebase",
	}
	require.NoError(t, schedulerRepo.Create(ctx, parent))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", parent.ScheduleID)

	// Inherited row: non-NULL pointer.
	rootTaskID := uuid.New()
	inherited := &model.TaskTracker{
		TaskID:              uuid.New(),
		ScheduleID:          parent.ScheduleID,
		CreatedAt:           time.Now(),
		ServiceName:         "svc",
		SchemaName:          "s",
		TableName:           "inherited_table",
		JobName:             "job-i",
		Status:              model.TaskStatusSucceeded,
		RetryCount:          0,
		MaxRetries:          0,
		ManifestVersion:     "vOLD",
		ImageTag:            "img:OLD",
		InheritedFromTaskID: &rootTaskID,
	}
	require.NoError(t, repo.Create(ctx, inherited))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", inherited.TaskID)

	gotInherited, err := repo.GetByID(ctx, inherited.TaskID)
	require.NoError(t, err)
	require.NotNil(t, gotInherited.InheritedFromTaskID, "InheritedFromTaskID round-trip lost the pointer")
	assert.Equal(t, rootTaskID, *gotInherited.InheritedFromTaskID)

	// Real-execution row: NULL pointer should round-trip as nil.
	real := &model.TaskTracker{
		TaskID:          uuid.New(),
		ScheduleID:      parent.ScheduleID,
		CreatedAt:       time.Now(),
		ServiceName:     "svc",
		SchemaName:      "s",
		TableName:       "real_table",
		JobName:         "job-r",
		Status:          model.TaskStatusPending,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "vNEW",
		ImageTag:        "img:NEW",
		// InheritedFromTaskID intentionally nil
	}
	require.NoError(t, repo.Create(ctx, real))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", real.TaskID)

	gotReal, err := repo.GetByID(ctx, real.TaskID)
	require.NoError(t, err)
	assert.Nil(t, gotReal.InheritedFromTaskID, "real-execution row should round-trip InheritedFromTaskID as nil")
}

func TestTaskRepository_UpdateStatusIfChangedTx_DoesNotReviveCancelledTask(t *testing.T) {
	db := newTestDB(t)
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	scheduler := createScheduler(t, schedulerRepo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduler.ScheduleID)

	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	task := createTask(t, taskRepo, scheduler.ScheduleID)
	defer db.ExecContext(context.Background(), "DELETE FROM task_tracker WHERE task_id = $1", task.TaskID)

	// Mark the task cancelled (simulating BulkCancelByScheduleIDTx).
	_, err := db.ExecContext(context.Background(),
		`UPDATE task_tracker SET status = 'cancelled', cancelled_at = NOW(), cancelled_by = 'test' WHERE task_id = $1`,
		task.TaskID,
	)
	require.NoError(t, err)

	// A late task.status.updated:v1 event tries to flip it to 'succeeded'.
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	affected, err := taskRepo.UpdateStatusIfChangedTx(context.Background(), tx, task.TaskID, "succeeded", 0)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assert.Equal(t, int32(0), affected, "cancelled task must not be revived")

	got, err := taskRepo.GetByID(context.Background(), task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusCancelled, got.Status, "status must remain cancelled")
}
