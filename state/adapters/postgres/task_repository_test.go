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
