package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusFlow delivers task.status.updated:v1 for one started run through
// TaskStatusUpdatedHandler and a real unit of work, one transaction each.
type statusFlow struct {
	db     *sqlx.DB
	tenant string
	r      *run.Run
}

func (f statusFlow) deliver(t *testing.T, table string, status run.TaskStatus, attempt int32) {
	t.Helper()
	ctx := context.Background()
	u := newPlanningUoW(f.db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	require.NoError(t, handlers.NewTaskStatusUpdatedHandler(f.tenant, discardLogger()).Handle(ctx, u, events.TaskStatusUpdated{
		TaskID:     run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(table)),
		ScheduleID: f.r.ScheduleID(),
		Status:     status,
		RetryCount: attempt,
	}, uuid.Nil))
	require.NoError(t, u.Commit())
}

// dispatched lists the node keys of the run's query.model:v1 rows, in
// node-key order.
func (f statusFlow) dispatched(t *testing.T) []string {
	t.Helper()
	var keys []string
	require.NoError(t, f.db.Select(&keys, `
		SELECT t.node_key FROM state_outbox o
		JOIN task_tracker t ON t.task_id = (o.payload->>'task_id')::uuid
		WHERE o.aggregate_id = $1 AND o.stream_name = $2
		ORDER BY t.node_key COLLATE "C"`, f.r.ScheduleID(), streams.QueryModelV1))
	return keys
}

func (f statusFlow) statuses(t *testing.T) map[string]run.TaskStatus {
	t.Helper()
	var rows []struct {
		Key    string         `db:"node_key"`
		Status run.TaskStatus `db:"status"`
	}
	require.NoError(t, f.db.Select(&rows, `SELECT node_key, status FROM task_tracker WHERE schedule_id = $1`, f.r.ScheduleID()))
	out := make(map[string]run.TaskStatus, len(rows))
	for _, row := range rows {
		out[row.Key] = row.Status
	}
	return out
}

func (f statusFlow) run(t *testing.T) (run.SchedulerStatus, int32) {
	t.Helper()
	var row struct {
		Status   run.SchedulerStatus `db:"status"`
		Terminal int32               `db:"terminal_task_count"`
	}
	require.NoError(t, f.db.Get(&row, `SELECT status, terminal_task_count FROM scheduler_tracker WHERE schedule_id = $1`, f.r.ScheduleID()))
	return row.Status, row.Terminal
}

// The e2e-schedule-failure run, one status update at a time: each success
// writes the dispatches it makes ready in its own transaction, a redelivered
// success writes none, the two last failures skip ftable_f and ftable_h, and
// the last terminal status finalizes the run failed.
func TestTaskStatusUpdated_TheExampleRunMovesInItsStatusTransactions(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	assert.Equal(t, []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b"}, f.dispatched(t))

	f.deliver(t, "e2e_schema.ftable_a", run.TaskStatusSucceeded, 0)
	f.deliver(t, "e2e_schema.ftable_a", run.TaskStatusSucceeded, 0)
	assert.Equal(t, []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b", "e2e_schema.ftable_g"}, f.dispatched(t))

	f.deliver(t, "e2e_schema.ftable_b", run.TaskStatusSucceeded, 0)
	f.deliver(t, "e2e_schema.ftable_c", run.TaskStatusSucceeded, 0)
	f.deliver(t, "e2e_schema.ftable_d", run.TaskStatusSucceeded, 0)
	f.deliver(t, "e2e_schema.ftable_e", run.TaskStatusFailed, 0)
	f.deliver(t, "e2e_schema.ftable_e", run.TaskStatusFailed, 2)
	f.deliver(t, "e2e_schema.ftable_g", run.TaskStatusFailed, 2)

	assert.Equal(t, []string{
		"e2e_schema.ftable_a", "e2e_schema.ftable_b", "e2e_schema.ftable_c", "e2e_schema.ftable_d",
		"e2e_schema.ftable_e", "e2e_schema.ftable_g",
	}, f.dispatched(t))
	assert.Equal(t, map[string]run.TaskStatus{
		"e2e_schema.ftable_a": run.TaskStatusSucceeded, "e2e_schema.ftable_b": run.TaskStatusSucceeded,
		"e2e_schema.ftable_c": run.TaskStatusSucceeded, "e2e_schema.ftable_d": run.TaskStatusSucceeded,
		"e2e_schema.ftable_e": run.TaskStatusFailed, "e2e_schema.ftable_f": run.TaskStatusSkipped,
		"e2e_schema.ftable_g": run.TaskStatusFailed, "e2e_schema.ftable_h": run.TaskStatusSkipped,
	}, f.statuses(t))
	status, terminal := f.run(t)
	assert.Equal(t, run.SchedulerStatusFailed, status)
	assert.Equal(t, int32(8), terminal)
}

// cascade-2000: the root's last failure skips its 1,999 descendants and
// finalizes the run in one status transaction.
func TestTaskStatusUpdated_TheCascadeRootsFailureFinishesTheRun(t *testing.T) {
	db := newTestDB(t)
	tenant := storeShape(t, db, cascade2000)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, cascade2000.schedule(), model.OperationRun)}

	f.deliver(t, "bench.fail_root", run.TaskStatusFailed, 2)

	var skipped int
	require.NoError(t, db.Get(&skipped, `SELECT count(*) FROM task_tracker WHERE schedule_id = $1 AND status = 'skipped'`, f.r.ScheduleID()))
	assert.Equal(t, 1999, skipped)
	status, terminal := f.run(t)
	assert.Equal(t, run.SchedulerStatusFailed, status)
	assert.Equal(t, int32(2000), terminal)
}
