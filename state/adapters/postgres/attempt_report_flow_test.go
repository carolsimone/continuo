package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// report delivers one task.status.updated:v2 report of attempt n of the run's
// task at key through AttemptReportHandler and a real unit of work, in one
// transaction.
func (f statusFlow) report(t *testing.T, key string, n int32, status run.AttemptStatus, reason model.FailureReason) {
	t.Helper()
	f.reportWith(t, run.AttemptReport{
		TaskID: run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key)), Attempt: n, Status: status, FailureReason: reason,
	})
}

func (f statusFlow) reportWith(t *testing.T, rep run.AttemptReport) {
	t.Helper()
	ctx := context.Background()
	u := newPlanningUoW(f.db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	require.NoError(t, handlers.NewAttemptReportHandler(f.tenant, testTaskDefaults, noRunMetrics{}, discardLogger()).Handle(ctx, u,
		events.AttemptReported{RunID: f.r.ScheduleID(), Report: rep}, uuid.Nil))
	require.NoError(t, u.Commit())
}

// dueIn reads how long after the database clock the attempt n of the run's
// task at key is next asked about; nil when it has no next check.
func (f statusFlow) dueIn(t *testing.T, key string, n int32) *time.Duration {
	t.Helper()
	var secs *float64
	require.NoError(t, f.db.Get(&secs, `
		SELECT EXTRACT(EPOCH FROM next_check_at - now())::float8 FROM task_attempts WHERE task_id = $1 AND attempt = $2`,
		run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key)), n))
	if secs == nil {
		return nil
	}
	d := time.Duration(*secs * float64(time.Second))
	return &d
}

// outboxRows counts the run's outbox rows: one per dispatch it wrote.
func (f statusFlow) outboxRows(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.Get(&n, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, f.r.ScheduleID()))
	return n
}

// Creating a run stores attempt 0 of each ready task dispatched, due for its
// first check five minutes after the database clock, and the task dispatched.
func TestStart_StoresAttemptZeroOfEveryReadyTaskDueInFiveMinutes(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}

	for _, key := range []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b"} {
		jobName, status, attempt := f.task(t, key)
		assert.Equal(t, run.TaskStatusDispatched, status, key)
		assert.Equal(t, []attemptRow{{Attempt: 0, JobName: jobName, Status: "dispatched"}}, f.attempts(t, key), key)
		assert.Zero(t, attempt)
		due := f.dueIn(t, key, 0)
		require.NotNil(t, due, key)
		assert.InDelta(t, (5 * time.Minute).Seconds(), due.Seconds(), 5, key)
	}
	assert.Empty(t, f.attempts(t, "e2e_schema.ftable_c"), "a task waiting for its upstreams has no attempt")

	ctx := context.Background()
	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	inFlight, err := u.TaskCollection(f.tenant).HasInFlight(ctx, f.r.ScheduleID())
	require.NoError(t, err)
	assert.True(t, inFlight, "the tasks the run dispatched when it was created are in flight")
}

// A retryable failure stores the failed attempt with its class, reason and
// facts, and the next attempt dispatched and due in five minutes, with its
// dispatch, in the transaction that records the failure.
func TestAttemptReport_ARetryIsDispatchedInTheTransactionThatRecordsTheFailure(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	before := f.outboxRows(t)
	code, logKey := int32(137), "logs/a-0.log"

	f.reportWith(t, run.AttemptReport{
		TaskID: run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a"), Attempt: 0, Status: run.AttemptFailed,
		FailureReason: model.FailureReasonOOMKilled, ExitCode: &code, LogS3Key: &logKey,
	})

	jobName, status, attempt := f.task(t, "e2e_schema.ftable_a")
	assert.Equal(t, run.TaskStatusDispatched, status)
	assert.Equal(t, int32(1), attempt)
	assert.Equal(t, []attemptRow{
		{Attempt: 0, JobName: jobName, Status: "failed"},
		{Attempt: 1, JobName: jobName + "-r1", Status: "dispatched"},
	}, f.attempts(t, "e2e_schema.ftable_a"))
	var facts struct {
		Class  string `db:"failure_class"`
		Reason string `db:"failure_reason"`
		Exit   int32  `db:"exit_code"`
		Log    string `db:"log_s3_key"`
	}
	require.NoError(t, db.Get(&facts, `SELECT failure_class, failure_reason, exit_code, log_s3_key FROM task_attempts
		WHERE task_id = $1 AND attempt = 0`, run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a")))
	assert.Equal(t, "resource", facts.Class)
	assert.Equal(t, "oom_killed", facts.Reason)
	assert.Equal(t, int32(137), facts.Exit)
	assert.Equal(t, logKey, facts.Log)
	assert.Nil(t, f.dueIn(t, "e2e_schema.ftable_a", 0), "a final attempt has no next check")
	require.NotNil(t, f.dueIn(t, "e2e_schema.ftable_a", 1))
	assert.InDelta(t, (5 * time.Minute).Seconds(), f.dueIn(t, "e2e_schema.ftable_a", 1).Seconds(), 5)
	assert.Equal(t, before+1, f.outboxRows(t), "the retry's dispatch is written with it")
}

// The answer to a question about a long attempt moves its next check to 30
// minutes after the database clock and nothing else.
func TestAttemptReport_AnEqualRankReportMovesOnlyTheNextCheck(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")
	_, err := db.Exec(`UPDATE task_attempts SET next_check_at = now() + interval '1 minute' WHERE task_id = $1`,
		run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a"))
	require.NoError(t, err)

	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")

	_, status, _ := f.task(t, "e2e_schema.ftable_a")
	assert.Equal(t, run.TaskStatusRunning, status)
	require.NotNil(t, f.dueIn(t, "e2e_schema.ftable_a", 0))
	assert.InDelta(t, (30 * time.Minute).Seconds(), f.dueIn(t, "e2e_schema.ftable_a", 0).Seconds(), 5)
}

// flowSnapshot is what a run's reports write: its status and terminal count,
// each task's status and attempt rows by node key, and its outbox rows.
type flowSnapshot struct {
	status   run.SchedulerStatus
	terminal int32
	statuses map[string]run.TaskStatus
	attempts map[string][]attemptRow
	outbox   int
}

func (f statusFlow) snapshot(t *testing.T) flowSnapshot {
	t.Helper()
	s := flowSnapshot{statuses: f.statuses(t), attempts: map[string][]attemptRow{}, outbox: f.outboxRows(t)}
	s.status, s.terminal = f.run(t)
	for key := range s.statuses {
		s.attempts[key] = f.attempts(t, key)
	}
	return s
}

// A report naming one run for a task another run holds is refused, and the
// transaction it ran in holds no write: committed anyway, neither run, none
// of their tasks or attempts and none of their outbox rows has changed.
func TestAttemptReport_AReportNamingAnotherRunIsRefused(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenantA := storeFailureSchedule(t, db)
	a := statusFlow{db: db, tenant: tenantA, r: startScheduleRun(t, db, tenantA, "e2e-schedule-failure", model.OperationRun)}
	tenantB := storeEdges(t, db, "e2e-schedule-other-run", [][2]string{{"ftable_a", "ftable_g"}}, "ftable_a", "ftable_g")
	b := statusFlow{db: db, tenant: tenantB, r: startScheduleRun(t, db, tenantB, "e2e-schedule-other-run", model.OperationRun)}
	beforeA, beforeB := a.snapshot(t), b.snapshot(t)

	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	err := handlers.NewAttemptReportHandler(tenantA, testTaskDefaults, noRunMetrics{}, discardLogger()).Handle(ctx, u,
		events.AttemptReported{RunID: a.r.ScheduleID(), Report: run.AttemptReport{
			TaskID:  run.TaskIDFor(tenantB, b.r.ScheduleID(), "e2e_schema.ftable_a"),
			Attempt: 0, Status: run.AttemptSucceeded,
		}}, uuid.Nil)
	require.ErrorIs(t, err, run.ErrTaskNotHeld)
	require.NoError(t, u.Commit())

	assert.Equal(t, beforeA, a.snapshot(t), "the run the report names")
	assert.Equal(t, beforeB, b.snapshot(t), "the run that holds the task")
}

// The example run, report by report: ftable_e and ftable_g fail three times
// each, every failure but the last dispatching the next attempt, and the
// run finalizes failed once every task has ended.
func TestAttemptReport_TheExampleRunMovesInItsReportTransactions(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}

	for _, key := range []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b", "e2e_schema.ftable_c", "e2e_schema.ftable_d"} {
		f.report(t, key, 0, run.AttemptQueued, "")
		f.report(t, key, 0, run.AttemptRunning, "")
		f.report(t, key, 0, run.AttemptSucceeded, "")
	}
	for n := int32(0); n <= 2; n++ {
		f.report(t, "e2e_schema.ftable_e", n, run.AttemptFailed, model.FailureReasonExitCode)
		f.report(t, "e2e_schema.ftable_g", n, run.AttemptFailed, model.FailureReasonExitCode)
	}

	assert.Equal(t, map[string]run.TaskStatus{
		"e2e_schema.ftable_a": run.TaskStatusSucceeded, "e2e_schema.ftable_b": run.TaskStatusSucceeded,
		"e2e_schema.ftable_c": run.TaskStatusSucceeded, "e2e_schema.ftable_d": run.TaskStatusSucceeded,
		"e2e_schema.ftable_e": run.TaskStatusFailed, "e2e_schema.ftable_f": run.TaskStatusSkipped,
		"e2e_schema.ftable_g": run.TaskStatusFailed, "e2e_schema.ftable_h": run.TaskStatusSkipped,
	}, f.statuses(t))
	assert.Len(t, f.attempts(t, "e2e_schema.ftable_e"), 3)
	status, terminal := f.run(t)
	assert.Equal(t, run.SchedulerStatusFailed, status)
	assert.Equal(t, int32(8), terminal)
	assert.Equal(t, 2+2+2+4, f.outboxRows(t), "two roots, then ftable_g, ftable_c, ftable_d and ftable_e, then two retries each of ftable_e and ftable_g")
}

// A stalled run is saved failed with failure_reason stalled, its pending
// tasks skipped.
func TestFailStalled_SavesTheRunFailedAsStalled(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptSucceeded, "")
	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptSucceeded, "")
	// A defect leaves the tasks the two successes made ready undispatched.
	_, err := db.Exec(`UPDATE task_tracker SET status = 'pending', dispatched_at = NULL WHERE schedule_id = $1 AND status = 'dispatched'`,
		f.r.ScheduleID())
	require.NoError(t, err)

	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	r, err := u.Run().LoadRunForUpdate(ctx, f.r.ScheduleID())
	require.NoError(t, err)
	check, err := r.FailStalled(ctx, u.TaskCollection(f.tenant), u.TaskGraph(f.tenant), testTaskDefaults,
		time.Now().Add(run.StallAfter+time.Minute))
	require.NoError(t, err)
	require.True(t, check.Stalled)
	require.NoError(t, u.Run().SaveRun(ctx, r))
	require.NoError(t, u.Commit())

	var row struct {
		Status   string  `db:"status"`
		Reason   *string `db:"failure_reason"`
		Terminal int32   `db:"terminal_task_count"`
	}
	require.NoError(t, db.Get(&row, `SELECT status, failure_reason, terminal_task_count FROM scheduler_tracker WHERE schedule_id = $1`, f.r.ScheduleID()))
	assert.Equal(t, "failed", row.Status)
	require.NotNil(t, row.Reason)
	assert.Equal(t, "stalled", *row.Reason)
	assert.Equal(t, int32(8), row.Terminal)
	reloaded, err := newPlanningUoW(db).Run().GetRun(ctx, f.r.ScheduleID())
	require.NoError(t, err)
	require.NotNil(t, reloaded.FailureReason())
	assert.Equal(t, run.RunFailureStalled, *reloaded.FailureReason())
}
