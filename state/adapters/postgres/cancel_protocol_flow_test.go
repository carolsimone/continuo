package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cancelByID cancels run id by by for reason the way the cancel RPCs do: it
// locks the run, cancels it, saves it and writes its events to the outbox, in
// one transaction it commits.
func cancelByID(t *testing.T, db *sqlx.DB, id uuid.UUID, by, reason string) {
	t.Helper()
	ctx := context.Background()
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		r, err := u.Run().LoadRunForUpdate(ctx, id)
		require.NoError(t, err)
		events, err := r.Cancel(ctx, u.TaskCollection(pkgevents.DefaultTenantID), by, reason, u.Clock().Now())
		require.NoError(t, err)
		require.NoError(t, u.Run().SaveRun(ctx, r))
		require.NoError(t, u.Outbox().Append(ctx, events, uuid.Nil))
	})
}

// cancelRow is the cancel columns of one scheduler_tracker row.
type cancelRow struct {
	Status      run.SchedulerStatus `db:"status"`
	CancelledAt *time.Time          `db:"cancelled_at"`
	CompletedAt *time.Time          `db:"completed_at"`
	CancelledBy *string             `db:"cancelled_by"`
	Reason      *string             `db:"cancellation_reason"`
	Terminal    int32               `db:"terminal_task_count"`
}

func readCancelRow(t *testing.T, db *sqlx.DB, id uuid.UUID) cancelRow {
	t.Helper()
	var row cancelRow
	require.NoError(t, db.Get(&row, `
		SELECT status, cancelled_at, completed_at, cancelled_by, cancellation_reason, terminal_task_count
		FROM scheduler_tracker WHERE schedule_id = $1`, id))
	return row
}

// stopRequested reports whether attempt n of the run's task at key was asked
// to stop.
func (f statusFlow) stopRequested(t *testing.T, key string, n int32) bool {
	t.Helper()
	var requested bool
	require.NoError(t, f.db.Get(&requested, `
		SELECT stop_requested_at IS NOT NULL FROM task_attempts WHERE task_id = $1 AND attempt = $2`,
		run.TaskIDFor(f.tenant, f.r.ScheduleID(), topology.NodeKey(key)), n))
	return requested
}

// A cancel of a run with an attempt in flight saves the run cancelling, with
// no completed_at, its task in flight stopping, its pending tasks cancelled
// and counted terminal, and the stop of the running attempt requested, due
// five minutes after the database clock; it writes one outbox row.
func TestCancel_SavesTheRunCancellingWithItsStoppingTasksAndRequestedStops(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")
	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptSucceeded, "")
	before := f.outboxRows(t)

	cancelByID(t, db, f.r.ScheduleID(), "alice", "bad data")

	row := readCancelRow(t, db, f.r.ScheduleID())
	assert.Equal(t, run.SchedulerStatusCancelling, row.Status)
	assert.NotNil(t, row.CancelledAt)
	assert.Nil(t, row.CompletedAt, "a cancelling run has not ended")
	require.NotNil(t, row.CancelledBy)
	assert.Equal(t, "alice", *row.CancelledBy)
	require.NotNil(t, row.Reason)
	assert.Equal(t, "bad data", *row.Reason)
	assert.Equal(t, int32(7), row.Terminal, "ftable_b and the six cancelled tasks")
	assert.Equal(t, map[string]run.TaskStatus{
		"e2e_schema.ftable_a": run.TaskStatusStopping, "e2e_schema.ftable_b": run.TaskStatusSucceeded,
		"e2e_schema.ftable_c": run.TaskStatusCancelled, "e2e_schema.ftable_d": run.TaskStatusCancelled,
		"e2e_schema.ftable_e": run.TaskStatusCancelled, "e2e_schema.ftable_f": run.TaskStatusCancelled,
		"e2e_schema.ftable_g": run.TaskStatusCancelled, "e2e_schema.ftable_h": run.TaskStatusCancelled,
	}, f.statuses(t))
	job, _, _ := f.task(t, "e2e_schema.ftable_a")
	assert.Equal(t, []attemptRow{{Attempt: 0, JobName: job, Status: "running"}}, f.attempts(t, "e2e_schema.ftable_a"),
		"the attempt keeps the status it last reported")
	assert.True(t, f.stopRequested(t, "e2e_schema.ftable_a", 0))
	due := f.dueIn(t, "e2e_schema.ftable_a", 0)
	require.NotNil(t, due)
	assert.InDelta(t, (5 * time.Minute).Seconds(), due.Seconds(), 5)
	assert.False(t, f.stopRequested(t, "e2e_schema.ftable_b", 0), "an ended attempt is not asked to stop")
	assert.Equal(t, before+1, f.outboxRows(t), "one cancel is written")
}

// A cancel of a run with nothing in flight saves it cancelled at once, with
// completed_at at the cancel, and its pending tasks cancelled and counted
// terminal; no stop is requested.
func TestCancel_SavesARunWithNothingInFlightCancelled(t *testing.T) {
	db := newTestDB(t)
	storeFailureSchedule(t, db)
	runID, ids := insertTasks(t, db, map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusSucceeded, "ftable_c": run.TaskStatusPending, "ftable_d": run.TaskStatusPending,
	})
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
	})

	cancelByID(t, db, runID, "alice", "bad data")

	row := readCancelRow(t, db, runID)
	assert.Equal(t, run.SchedulerStatusCancelled, row.Status)
	require.NotNil(t, row.CancelledAt)
	require.NotNil(t, row.CompletedAt)
	assert.True(t, row.CompletedAt.Equal(*row.CancelledAt), "the run ends at its cancel")
	assert.Equal(t, int32(2), row.Terminal)
	for table, want := range map[string]run.TaskStatus{
		"ftable_a": run.TaskStatusSucceeded, "ftable_c": run.TaskStatusCancelled, "ftable_d": run.TaskStatusCancelled,
	} {
		var got run.TaskStatus
		require.NoError(t, db.Get(&got, `SELECT status FROM task_tracker WHERE task_id = $1`, ids[table]))
		assert.Equal(t, want, got, table)
	}
	var outbox int
	require.NoError(t, db.Get(&outbox, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, runID))
	assert.Equal(t, 1, outbox)
}

// Each stopped report cancels its stopping task in its own transaction; a
// running report before it leaves the task stopping and the stop's next check
// where the cancel set it; the last stopped report ends the run cancelled.
// No report writes an outbox row.
func TestAttemptReport_AStoppedReportCancelsTheTaskAndTheLastEndsTheRun(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	cancelByID(t, db, f.r.ScheduleID(), "alice", "bad data")
	outbox := f.outboxRows(t)

	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptStopped, "")

	statuses := f.statuses(t)
	assert.Equal(t, run.TaskStatusCancelled, statuses["e2e_schema.ftable_a"])
	assert.Equal(t, run.TaskStatusStopping, statuses["e2e_schema.ftable_b"])
	job, _, _ := f.task(t, "e2e_schema.ftable_a")
	assert.Equal(t, []attemptRow{{Attempt: 0, JobName: job, Status: "stopped"}}, f.attempts(t, "e2e_schema.ftable_a"))
	assert.Nil(t, f.dueIn(t, "e2e_schema.ftable_a", 0), "a stopped attempt has no next check")
	status, terminal := f.run(t)
	assert.Equal(t, run.SchedulerStatusCancelling, status)
	assert.Equal(t, int32(7), terminal)

	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptRunning, "")
	assert.Equal(t, run.TaskStatusStopping, f.statuses(t)["e2e_schema.ftable_b"])
	due := f.dueIn(t, "e2e_schema.ftable_b", 0)
	require.NotNil(t, due)
	assert.InDelta(t, (5 * time.Minute).Seconds(), due.Seconds(), 5, "the stop's next check stands")

	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptStopped, "")

	row := readCancelRow(t, db, f.r.ScheduleID())
	assert.Equal(t, run.SchedulerStatusCancelled, row.Status)
	assert.NotNil(t, row.CompletedAt)
	assert.Equal(t, int32(8), row.Terminal)
	assert.Equal(t, run.TaskStatusCancelled, f.statuses(t)["e2e_schema.ftable_b"])
	assert.Equal(t, outbox, f.outboxRows(t), "a cancelling run dispatches nothing")
}

// A stopping task whose container exited 0 before the stop landed wrote its
// table: through AttemptReportHandler the task ends succeeded and
// node_materializations records the version it loaded, naming the run; the
// run still ends cancelled.
func TestAttemptReport_AStoppingSuccessRecordsTheLoadedVersion(t *testing.T) {
	db := newTestDB(t)
	tenant := storeFailureSchedule(t, db)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM node_materializations WHERE tenant_id = $1`, tenant)
	})
	f := statusFlow{db: db, tenant: tenant, r: startScheduleRun(t, db, tenant, "e2e-schedule-failure", model.OperationRun)}
	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptRunning, "")
	cancelByID(t, db, f.r.ScheduleID(), "alice", "bad data")

	f.report(t, "e2e_schema.ftable_a", 0, run.AttemptSucceeded, "")

	assert.Equal(t, run.TaskStatusSucceeded, f.statuses(t)["e2e_schema.ftable_a"])
	var built struct {
		ContentHash string    `db:"content_hash"`
		RunID       uuid.UUID `db:"run_id"`
		TaskID      uuid.UUID `db:"task_id"`
		ReleaseID   string    `db:"release_id"`
	}
	require.NoError(t, db.Get(&built, `
		SELECT content_hash, run_id, task_id, release_id FROM node_materializations
		WHERE tenant_id = $1 AND node_key = 'e2e_schema.ftable_a'`, tenant))
	assert.Equal(t, "h-ftable_a", built.ContentHash)
	assert.Equal(t, f.r.ScheduleID(), built.RunID)
	assert.Equal(t, run.TaskIDFor(tenant, f.r.ScheduleID(), "e2e_schema.ftable_a"), built.TaskID)
	assert.Equal(t, "rel-1", built.ReleaseID)

	f.report(t, "e2e_schema.ftable_b", 0, run.AttemptStopped, "")
	status, _ := f.run(t)
	assert.Equal(t, run.SchedulerStatusCancelled, status, "the run ends cancelled though a task succeeded")
}

// A cancelling run still holds its schedule: it blocks a new run of the
// schedule, is the schedule's active run, shows as running in the schedule
// list, is an active run of the live release's drift report and, with tasks
// state did not plan, is listed among the unplanned active runs.
func TestActiveRunQueriesCoverCancelling(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	id := insertSeedRun(t, db, run.SchedulerStatusCancelling)
	var schedule string
	require.NoError(t, db.Get(&schedule, `SELECT schedule_name FROM scheduler_tracker WHERE schedule_id = $1`, id))
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	blocked, err := repo.HasActiveSchedule(ctx, schedule)
	require.NoError(t, err)
	assert.True(t, blocked, "a cancelling run blocks its schedule")

	active, err := repo.GetActiveScheduler(ctx, schedule)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, id, active.ScheduleID)
	assert.Equal(t, run.SchedulerStatusCancelling, active.Status)

	last, err := repo.GetLastRunPerSchedule(ctx)
	require.NoError(t, err)
	require.Contains(t, last, schedule)
	assert.True(t, last[schedule].IsRunning, "a cancelling run is still running")

	_, drifts, err := postgres.NewRunReadRepository(db).ListActiveRunDrifts(ctx, tenant)
	require.NoError(t, err)
	var driftRuns []uuid.UUID
	for _, d := range drifts {
		driftRuns = append(driftRuns, d.RunID)
	}
	assert.Contains(t, driftRuns, id)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		unplanned, err := u.Run().ListUnplannedActive(ctx)
		require.NoError(t, err)
		assert.Contains(t, unplanned, id)
	})
}
