package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusFlow drives one started run through its reports, each in a
// transaction of its own, and reads back what they wrote.
type statusFlow struct {
	db     *sqlx.DB
	tenant string
	r      *run.Run
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

// cascade-2000: the root's failure, of a class that is not retried, skips its
// 1,999 descendants and finalizes the run in one report transaction, its
// retries unspent.
func TestAttemptReport_TheCascadeRootsFailureFinishesTheRun(t *testing.T) {
	db := newTestDB(t)
	tenant := storeShape(t, db, cascade2000)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, cascade2000.schedule(), model.OperationRun)}

	f.report(t, "bench.fail_root", 0, run.AttemptFailed, model.FailureReasonDeadlineExceeded)

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
func TestAttemptReport_EveryAttemptOfAThreeAttemptTaskHasItsRow(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	const key = "e2e_schema.ftable_a"

	for attempt := int32(0); attempt <= 2; attempt++ {
		f.report(t, key, attempt, run.AttemptRunning, "")
		f.report(t, key, attempt, run.AttemptFailed, model.FailureReasonExitCode)
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
// failure reported afterwards finds the run ended and changes nothing.
func TestCancel_CancelsTheOpenAttemptInTheCancelTransaction(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	const key = "e2e_schema.ftable_a"
	f.report(t, key, 0, run.AttemptRunning, "")
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

	f.report(t, key, 0, run.AttemptFailed, model.FailureReasonExitCode)

	rows = f.attempts(t, key)
	require.Len(t, rows, 1)
	assert.Equal(t, "cancelled", rows[0].Status)
	status, _ := f.run(t)
	assert.Equal(t, run.SchedulerStatusCancelled, status)
}
