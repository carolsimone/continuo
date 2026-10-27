package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStart_StampsTaskOperation starts a single-node test run and asserts the
// created task row carries operation='test'.
func TestStart_StampsTaskOperation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, pkgevents.DefaultTenantID, discardLogger())

	r, err := run.NewSingleNodeRun("op-task", model.OperationTest, nil, "user-1", run.ReleaseStamp{}, time.Now())
	require.NoError(t, err)
	// One statement deletes the run and, through the tasks table's ON DELETE
	// CASCADE, its tasks, so no concurrent reader sees this single-node run
	// without its tasks.
	t.Cleanup(func() {
		db.Exec(`DELETE FROM runs WHERE run_id = $1`, r.ScheduleID())
	})

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := postgres.NewRunRepository(tx, schedRepo)
	tasks := postgres.NewTaskCollectionAdapter(taskRepo, tx, "default")

	require.NoError(t, runRepo.SaveRun(ctx, r))

	one := 1
	g := topology.NewGraph([]topology.Node{{
		Key: "analytics.tbl_op", ServiceName: "svc", SchemaName: "analytics", TableName: "tbl_op", NodeType: "dbt-model", TestCount: &one,
	}}, nil)
	plan, err := runplan.SingleNode{Key: "analytics.tbl_op", ServiceName: "svc", Operation: model.OperationTest}.Select(g)
	require.NoError(t, err)
	_, err = r.Start(ctx, tasks, "default", plan, g, testTaskDefaults, time.Now())
	require.NoError(t, err)
	taskID := run.TaskIDFor("default", r.ScheduleID(), "analytics.tbl_op")

	require.NoError(t, runRepo.SaveRun(ctx, r))
	require.NoError(t, tx.Commit())

	var op string
	require.NoError(t, db.Get(&op, `SELECT operation FROM tasks WHERE task_id = $1`, taskID))
	assert.Equal(t, "test", op)
}
