package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAcceptDispatch_StampsTaskOperation dispatches a single-node test run and
// asserts the created task_tracker row carries operation='test'.
func TestAcceptDispatch_StampsTaskOperation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())

	r, _, err := run.NewSingleNodeRun("op-task", run.NodeID{ServiceName: "svc", SchemaName: "analytics", TableName: "tbl_op"},
		run.MetadataSourceLatest, model.OperationTest, nil, "user-1", time.Now())
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM task_tracker WHERE schedule_id = $1`, r.ScheduleID())
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, r.ScheduleID())
	})

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := postgres.NewRunRepository(tx, schedRepo)
	tasks := postgres.NewTaskCollectionAdapter(taskRepo, tx)

	require.NoError(t, runRepo.SaveRun(ctx, r))

	taskID := uuid.New()
	_, err = r.AcceptDispatch(ctx, tasks, []run.DispatchedTask{{
		TaskID: taskID, ServiceName: "svc", SchemaName: "analytics", TableName: "tbl_op",
		Status: run.TaskStatusPending, MaxRetries: 3,
	}}, time.Now())
	require.NoError(t, err)

	require.NoError(t, runRepo.SaveRun(ctx, r))
	require.NoError(t, tx.Commit())

	var op string
	require.NoError(t, db.Get(&op, `SELECT operation FROM task_tracker WHERE task_id = $1`, taskID))
	assert.Equal(t, "test", op)
}
