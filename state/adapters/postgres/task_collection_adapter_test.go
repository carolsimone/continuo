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

// GetByID reads one task's identity and image, and reports a missing task as
// run.ErrTaskNotFound.
func TestTaskCollectionAdapter_GetByID(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	sched := createScheduler(t, schedRepo, "get-by-id-"+uuid.NewString()[:8])
	taskID := uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM task_tracker WHERE task_id = $1`, taskID)
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, sched.ScheduleID)
	})
	require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
		TaskID: taskID, ScheduleID: sched.ScheduleID, CreatedAt: time.Now(), ServiceName: "service-1",
		SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		ImageTag: "service-1:v3",
	}))
	adapter := postgres.NewTaskCollectionAdapter(taskRepo, nil)

	got, err := adapter.GetByID(ctx, taskID)
	require.NoError(t, err)
	assert.Equal(t, taskID, got.TaskID)
	assert.Equal(t, sched.ScheduleID, got.ScheduleID)
	assert.Equal(t, run.NodeID{ServiceName: "service-1", SchemaName: "raw", TableName: "orders"}, got.Node())
	assert.Equal(t, "service-1:v3", got.ImageTag)
	assert.Equal(t, run.TaskStatusSucceeded, got.Status)

	_, err = adapter.GetByID(ctx, uuid.New())
	assert.ErrorIs(t, err, run.ErrTaskNotFound)
}
