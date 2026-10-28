package postgres_test

import (
	"context"
	"sort"
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
func TestTaskCollection_GetByID(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	sched := createScheduler(t, schedRepo, "get-by-id-"+uuid.NewString()[:8])
	taskID := uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE task_id = $1`, taskID)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, sched.ScheduleID)
	})
	require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
		TenantID: pkgevents.DefaultTenantID,
		TaskID:   taskID, ScheduleID: sched.ScheduleID, CreatedAt: time.Now(), ServiceName: "service-1",
		SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		ImageTag: "service-1:v3",
	}))
	collection := taskRepo.ForTenant("default")

	got, err := collection.GetByID(ctx, taskID)
	require.NoError(t, err)
	assert.Equal(t, taskID, got.TaskID)
	assert.Equal(t, sched.ScheduleID, got.ScheduleID)
	assert.Equal(t, run.NodeID{ServiceName: "service-1", SchemaName: "raw", TableName: "orders"}, got.Node())
	assert.Equal(t, "service-1:v3", got.ImageTag)
	assert.Equal(t, run.TaskStatusSucceeded, got.Status)

	_, err = collection.GetByID(ctx, uuid.New())
	assert.ErrorIs(t, err, run.ErrTaskNotFound)
}

// RunReleasesOf reads, through the unit of work's transaction, the release
// each task's run is stamped with. A task whose run names no release, and an
// id no task has, are absent. With a pool of one connection, held by the
// transaction, a read on another connection would wait until the deadline.
func TestTaskCollection_RunReleasesOf(t *testing.T) {
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	suffix := uuid.NewString()[:8]
	release := "rel-" + suffix
	stamped := createScheduler(t, schedRepo, "run-releases-stamped-"+suffix)
	legacy := createScheduler(t, schedRepo, "run-releases-legacy-"+suffix)
	stampedTask, legacyTask := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE task_id IN ($1, $2)`, stampedTask, legacyTask)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id IN ($1, $2)`, stamped.ScheduleID, legacy.ScheduleID)
	})
	_, err := db.Exec(`UPDATE runs SET release_id = $2, promotion_seq = 1 WHERE run_id = $1`, stamped.ScheduleID, release)
	require.NoError(t, err)
	for id, sched := range map[uuid.UUID]uuid.UUID{stampedTask: stamped.ScheduleID, legacyTask: legacy.ScheduleID} {
		require.NoError(t, taskRepo.Create(context.Background(), &postgres.TaskRow{
			TenantID: pkgevents.DefaultTenantID,
			TaskID:   id, ScheduleID: sched, CreatedAt: time.Now(), ServiceName: "service-1",
			SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		}))
	}

	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	collection := taskRepo.ForTenant("default").WithTx(tx)

	got, err := collection.RunReleasesOf(ctx, []uuid.UUID{stampedTask, legacyTask, uuid.New()})

	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]string{stampedTask: release}, got)
}

// ListByRunID pages a run's tasks in a stable order. Every task of a run is
// created with the same created_at, so the order among them comes from the
// task id: reading the run page by page lists each task exactly once, in
// ascending task id.
func TestTaskCollection_ListByRunIDPagesTasksOfOneCreatedAtInTaskIDOrder(t *testing.T) {
	const (
		taskCount = 450
		pageSize  = 200
	)
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	sched := createScheduler(t, schedRepo, "list-by-run-paging-"+uuid.NewString()[:8])
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE run_id = $1`, sched.ScheduleID)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, sched.ScheduleID)
	})
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	want := make([]string, 0, taskCount)
	for range taskCount {
		id := uuid.New()
		want = append(want, id.String())
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
			TenantID: pkgevents.DefaultTenantID,
			TaskID:   id, ScheduleID: sched.ScheduleID, CreatedAt: createdAt, ServiceName: "service-1",
			SchemaName: "raw", TableName: "orders", JobName: "job-" + id.String(), Status: run.TaskStatusPending,
		}))
	}
	sort.Strings(want)
	collection := taskRepo.ForTenant(pkgevents.DefaultTenantID)

	got := make([]string, 0, taskCount)
	for offset := 0; ; offset += pageSize {
		page, total, err := collection.ListByRunID(ctx, sched.ScheduleID, nil, pageSize, offset)
		require.NoError(t, err)
		require.Equal(t, taskCount, total)
		for _, task := range page {
			got = append(got, task.TaskID.String())
		}
		if len(page) < pageSize {
			break
		}
	}

	assert.Equal(t, want, got)
}
