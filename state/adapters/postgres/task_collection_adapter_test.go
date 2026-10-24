package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, logger)
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
	adapter := postgres.NewTaskCollectionAdapter(taskRepo, nil, "default")

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

// RunReleasesOf reads, through the unit of work's transaction, the release
// each task's run is stamped with. A task whose run names no release, and an
// id no task has, are absent. With a pool of one connection, held by the
// transaction, a read on another connection would wait until the deadline.
func TestTaskCollectionAdapter_RunReleasesOf(t *testing.T) {
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, logger)
	suffix := uuid.NewString()[:8]
	release := "rel-" + suffix
	stamped := createScheduler(t, schedRepo, "run-releases-stamped-"+suffix)
	legacy := createScheduler(t, schedRepo, "run-releases-legacy-"+suffix)
	stampedTask, legacyTask := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM task_tracker WHERE task_id IN ($1, $2)`, stampedTask, legacyTask)
		_, _ = db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id IN ($1, $2)`, stamped.ScheduleID, legacy.ScheduleID)
	})
	_, err := db.Exec(`UPDATE scheduler_tracker SET release_id = $2, promotion_seq = 1 WHERE schedule_id = $1`, stamped.ScheduleID, release)
	require.NoError(t, err)
	for id, sched := range map[uuid.UUID]uuid.UUID{stampedTask: stamped.ScheduleID, legacyTask: legacy.ScheduleID} {
		require.NoError(t, taskRepo.Create(context.Background(), &postgres.TaskTracker{
			TaskID: id, ScheduleID: sched, CreatedAt: time.Now(), ServiceName: "service-1",
			SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		}))
	}

	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	adapter := postgres.NewTaskCollectionAdapter(taskRepo, tx, "default")

	got, err := adapter.RunReleasesOf(ctx, []uuid.UUID{stampedTask, legacyTask, uuid.New()})

	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]string{stampedTask: release}, got)
}
