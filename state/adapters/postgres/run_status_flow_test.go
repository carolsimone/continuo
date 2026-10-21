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

// attemptRow is one task_attempts row as these tests read it.
type attemptRow struct {
	Attempt int32  `db:"attempt"`
	JobName string `db:"job_name"`
	Status  string `db:"status"`
}

// attempts returns the attempt rows of the run's task at key, in attempt
// order.
func (f statusFlow) attempts(t *testing.T, key string) []attemptRow {
	t.Helper()
	var rows []attemptRow
	require.NoError(t, f.db.Select(&rows, `
		SELECT attempt, job_name, status FROM task_attempts
		WHERE task_id = $1 ORDER BY attempt`, run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key))))
	return rows
}

// task returns the job name, status and attempt task_tracker holds for the
// run's task at key.
func (f statusFlow) task(t *testing.T, key string) (string, run.TaskStatus, int32) {
	t.Helper()
	var row struct {
		JobName string         `db:"job_name"`
		Status  run.TaskStatus `db:"status"`
		Attempt int32          `db:"attempt"`
	}
	require.NoError(t, f.db.Get(&row, `SELECT job_name, status, attempt FROM task_tracker WHERE task_id = $1`,
		run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key))))
	return row.JobName, row.Status, row.Attempt
}

// A task that runs and fails three times has one row per attempt, each named
// with its attempt number, and the task ends failed at its last attempt.
func TestTaskStatusUpdated_EveryAttemptOfAThreeAttemptTaskHasItsRow(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	const key = "e2e_schema.ftable_a"

	for attempt := int32(0); attempt <= 2; attempt++ {
		f.deliver(t, key, run.TaskStatusRunning, attempt)
		f.deliver(t, key, run.TaskStatusFailed, attempt)
	}

	job, status, attempt := f.task(t, key)
	assert.Equal(t, []attemptRow{
		{Attempt: 0, JobName: job, Status: "failed"},
		{Attempt: 1, JobName: job + "-r1", Status: "failed"},
		{Attempt: 2, JobName: job + "-r2", Status: "failed"},
	}, f.attempts(t, key))
	assert.Equal(t, run.TaskStatusFailed, status)
	assert.Equal(t, int32(2), attempt)
}

// Cancelling a run cancels its running attempt in the cancel transaction; a
// failure reported afterwards finds the run ended and leaves the attempt
// cancelled.
func TestCancel_CancelsTheOpenAttemptInTheCancelTransaction(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	const key = "e2e_schema.ftable_a"
	f.deliver(t, key, run.TaskStatusRunning, 0)
	job, _, _ := f.task(t, key)
	require.Equal(t, []attemptRow{{Attempt: 0, JobName: job, Status: "running"}}, f.attempts(t, key))

	ctx := context.Background()
	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	r, err := u.Run().LoadRunForUpdate(ctx, f.r.ScheduleID())
	require.NoError(t, err)
	_, err = r.Cancel(ctx, u.TaskCollection(), "tester", "stop", u.Clock().Now())
	require.NoError(t, err)
	require.NoError(t, u.Run().SaveRun(ctx, r))
	require.NoError(t, u.Commit())

	rows := f.attempts(t, key)
	require.Len(t, rows, 1)
	assert.Equal(t, "cancelled", rows[0].Status)

	f.deliver(t, key, run.TaskStatusFailed, 0)

	rows = f.attempts(t, key)
	require.Len(t, rows, 1)
	assert.Equal(t, "cancelled", rows[0].Status)
	status, _ := f.run(t)
	assert.Equal(t, run.SchedulerStatusCancelled, status)
}

// A failure that reports the exhausted budget for a retry Job Kubernetes
// refused is recorded at the refused retry, and the attempt before it keeps
// its row.
func TestTaskStatusUpdated_ARefusedRetryJobIsRecordedAtTheRetry(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	const key = "e2e_schema.ftable_a"

	f.deliver(t, key, run.TaskStatusRunning, 0)
	f.deliver(t, key, run.TaskStatusFailed, 0)
	f.deliver(t, key, run.TaskStatusFailed, 2)

	job, status, attempt := f.task(t, key)
	assert.Equal(t, []attemptRow{
		{Attempt: 0, JobName: job, Status: "failed"},
		{Attempt: 1, JobName: job + "-r1", Status: "failed"},
	}, f.attempts(t, key))
	assert.Equal(t, run.TaskStatusFailed, status)
	assert.Equal(t, int32(2), attempt)
}
