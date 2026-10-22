package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inTaskTx runs fn with a TaskCollection bound to a transaction it commits.
func inTaskTx(t *testing.T, db *sqlx.DB, fn func(tasks run.TaskCollection)) {
	t.Helper()
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	fn(postgres.NewTaskCollectionAdapter(postgres.NewTaskTrackerRepository(db, discardLogger()), tx))
	require.NoError(t, tx.Commit())
}

// taskState reads a task's status and whether it was dispatched.
func taskState(t *testing.T, db *sqlx.DB, id uuid.UUID) (run.TaskStatus, *time.Time) {
	t.Helper()
	var row struct {
		Status       run.TaskStatus `db:"status"`
		DispatchedAt *time.Time     `db:"dispatched_at"`
	}
	require.NoError(t, db.Get(&row, `SELECT status, dispatched_at FROM task_tracker WHERE task_id = $1`, id))
	return row.Status, row.DispatchedAt
}

// A status update reads the task's attempt, retry budget and node; a task
// that is not stored reads as absent.
func TestTaskCollection_LoadAttemptStateReadsTheTask(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	id := run.TaskIDFor(tenant, r.ScheduleID(), "e2e_schema.ftable_e")
	_, err := db.Exec(`UPDATE task_tracker SET status = 'failed', attempt = 1 WHERE task_id = $1`, id)
	require.NoError(t, err)

	inTaskTx(t, db, func(tasks run.TaskCollection) {
		got, found, err := tasks.LoadAttemptState(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, run.TaskStatusFailed, got.Status)
		assert.Equal(t, int32(1), got.RetryCount)
		assert.Equal(t, int32(2), got.MaxRetries)
		assert.Equal(t, topology.NodeKey("e2e_schema.ftable_e"), got.NodeKey)
		assert.NotEmpty(t, got.JobName)
		assert.NotEmpty(t, got.ServiceName)
		assert.Equal(t, "e2e_schema", got.SchemaName)
		assert.Equal(t, "ftable_e", got.TableName)

		_, found, err = tasks.LoadAttemptState(ctx, uuid.New())
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// MarkDispatched makes only the tasks still pending and undispatched
// dispatched, stamps them and names them; dispatching them again changes
// nothing.
func TestTaskCollection_MarkDispatchedDispatchesOnlyPendingUndispatchedTasks(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	id := func(table string) uuid.UUID {
		return run.TaskIDFor(tenant, r.ScheduleID(), topology.NodeKey("e2e_schema."+table))
	}
	setTaskStatus(t, db, r, "e2e_schema.ftable_h", run.TaskStatusSkipped)
	at := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

	inTaskTx(t, db, func(tasks run.TaskCollection) {
		stamped, err := tasks.MarkDispatched(ctx, []uuid.UUID{id("ftable_a"), id("ftable_c"), id("ftable_g"), id("ftable_h")}, at)
		require.NoError(t, err)
		assert.ElementsMatch(t, []uuid.UUID{id("ftable_c"), id("ftable_g")}, stamped)

		again, err := tasks.MarkDispatched(ctx, []uuid.UUID{id("ftable_c"), id("ftable_g")}, at.Add(time.Minute))
		require.NoError(t, err)
		assert.Empty(t, again)
	})

	status, dispatched := taskState(t, db, id("ftable_c"))
	assert.Equal(t, run.TaskStatusDispatched, status)
	require.NotNil(t, dispatched)
	assert.True(t, at.Equal(*dispatched))
	_, dispatched = taskState(t, db, id("ftable_h"))
	assert.Nil(t, dispatched, "a skipped task is never dispatched")
}

// MarkSkipped skips only the tasks still pending and undispatched and counts
// them: a dispatched task may be running, and a finished task keeps its
// status.
func TestTaskCollection_MarkSkippedSkipsOnlyPendingUndispatchedTasks(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	id := func(table string) uuid.UUID {
		return run.TaskIDFor(tenant, r.ScheduleID(), topology.NodeKey("e2e_schema."+table))
	}
	setTaskStatus(t, db, r, "e2e_schema.ftable_h", run.TaskStatusSucceeded)

	inTaskTx(t, db, func(tasks run.TaskCollection) {
		n, err := tasks.MarkSkipped(ctx, []uuid.UUID{id("ftable_a"), id("ftable_d"), id("ftable_f"), id("ftable_h")})
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	for table, want := range map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusPending, "ftable_d": run.TaskStatusSkipped,
		"ftable_f": run.TaskStatusSkipped, "ftable_h": run.TaskStatusSucceeded,
	} {
		status, _ := taskState(t, db, id(table))
		assert.Equal(t, want, status, table)
	}
}

// A derived run reads every task of its source, in node-key order.
func TestTaskCollection_ListRunTasksReadsEveryTaskInKeyOrder(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	r := startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)
	setTaskStatus(t, db, r, "e2e_schema.ftable_c", run.TaskStatusFailed)

	inTaskTx(t, db, func(tasks run.TaskCollection) {
		got, err := tasks.ListRunTasks(ctx, r.ScheduleID())
		require.NoError(t, err)
		require.Len(t, got, 8)
		keys := make([]topology.NodeKey, 0, len(got))
		for _, task := range got {
			keys = append(keys, task.NodeKey)
		}
		assert.Equal(t, []topology.NodeKey{
			"e2e_schema.ftable_a", "e2e_schema.ftable_b", "e2e_schema.ftable_c", "e2e_schema.ftable_d",
			"e2e_schema.ftable_e", "e2e_schema.ftable_f", "e2e_schema.ftable_g", "e2e_schema.ftable_h",
		}, keys)
		assert.Equal(t, run.TaskStatusFailed, got[2].Status)
		assert.Equal(t, run.TaskIDFor(tenant, r.ScheduleID(), "e2e_schema.ftable_c"), got[2].TaskID)
	})
}
