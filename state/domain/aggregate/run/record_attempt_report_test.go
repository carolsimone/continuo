package run_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reportAt is when the reports of these tests are applied.
var reportAt = startNow.Add(time.Minute)

// report applies one report of attempt n of the task of r at table.
func report(t *testing.T, r *run.Run, tc *fakeTaskCollection, g run.TaskGraph, table string, n int32,
	status run.AttemptStatus, reason model.FailureReason) []run.DomainEvent {
	t.Helper()
	events, _, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, table), Attempt: n, Status: status, FailureReason: reason}, taskDefaults, reportAt)
	require.NoError(t, err)
	return events
}

// queued and running move the task and the run's progress clock.
func TestRecordAttemptReport_QueuedAndRunningMoveTheTask(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptQueued, ""))
	assert.Equal(t, run.TaskStatusQueued, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, reportAt, *r.LastHeartbeatAt())

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, ""))
	assert.Equal(t, run.TaskStatusRunning, tc.statuses[taskOf(r, "ftable_a")])
	a, _ := attemptOf(t, tc, r, "ftable_a", 0)
	assert.Equal(t, run.AttemptRunning, a.Status)
	assert.Equal(t, 30*time.Minute, a.NextCheckIn, "an acknowledged attempt is next asked about after 30 minutes")
}

// The answer to a question about a long attempt moves its facts and its next
// check, not its status, the task or the run's progress clock.
func TestRecordAttemptReport_AnEqualRankReportMovesNoStatus(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	saves := len(tc.saves)
	heartbeat := *r.LastHeartbeatAt()

	_, _, err := r.RecordAttemptReport(context.Background(), tc, g, run.AttemptReport{
		TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptRunning, LogS3Key: ptrTo("logs/a")}, taskDefaults, reportAt.Add(time.Hour))
	require.NoError(t, err)

	require.Len(t, tc.saves, saves+1, "the attempt's facts are saved")
	assert.Equal(t, "logs/a", *tc.saves[saves].LogS3Key)
	assert.Equal(t, heartbeat, *r.LastHeartbeatAt())
	assert.Equal(t, run.TaskStatusRunning, tc.statuses[taskOf(r, "ftable_a")])
}

// A success dispatches attempt 0 of each task it makes ready, with its node's
// timeout, and the attempt row is stored dispatched in the same transaction.
func TestRecordAttemptReport_ASuccessDispatchesAttemptZeroOfTheTasksItMakesReady(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	events, succeeded, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptSucceeded}, taskDefaults, reportAt)
	require.NoError(t, err)

	assert.True(t, succeeded)
	require.Len(t, events, 1)
	created := createdByKey(tc)[fkey("ftable_g")]
	node, _ := failureSchedule().Node(fkey("ftable_g"))
	assert.Equal(t, run.TaskDispatched{
		ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: created.TaskID, Attempt: 0,
		KnownStatus: run.AttemptDispatched, Node: node, JobName: created.JobName, Operation: model.OperationRun,
		MaxRetries: 2, Timeout: 24 * time.Hour,
	}, events[0])
	a, ok := attemptOf(t, tc, r, "ftable_g", 0)
	require.True(t, ok)
	assert.Equal(t, run.AttemptDispatched, a.Status)
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[created.TaskID])
	assert.Equal(t, run.TaskStatusSucceeded, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, int32(1), r.TerminalTaskCount())
}

// A retryable failure with budget left dispatches the next attempt in the
// transaction that records it: the task never shows failed, nothing is
// skipped and nothing counts terminal.
func TestRecordAttemptReport_ARetryableFailureDispatchesTheNextAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")

	events := report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonOOMKilled)

	id := taskOf(r, "ftable_a")
	jobName, err := domain.ComputeJobName("service-1", "e2e_schema", "ftable_a", r.ScheduleID().String(), 1)
	require.NoError(t, err)
	node, _ := failureSchedule().Node(fkey("ftable_a"))
	assert.Equal(t, []run.DomainEvent{run.TaskDispatched{
		ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: id, Attempt: 1,
		KnownStatus: run.AttemptDispatched, Node: node, JobName: jobName, Operation: model.OperationRun,
		MaxRetries: 2, Timeout: 24 * time.Hour,
	}}, events)
	first, _ := attemptOf(t, tc, r, "ftable_a", 0)
	assert.Equal(t, run.AttemptFailed, first.Status)
	assert.Equal(t, "resource", *first.FailureClass)
	assert.Equal(t, "oom_killed", *first.FailureReason)
	next, ok := attemptOf(t, tc, r, "ftable_a", 1)
	require.True(t, ok)
	assert.Equal(t, run.Attempt{TaskID: id, Number: 1, JobName: jobName, Status: run.AttemptDispatched,
		NextCheckIn: 5 * time.Minute}, next, "the retry is stored dispatched, its dispatch time on the database clock")
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[id])
	assert.Equal(t, int32(1), tc.attempts[id])
	assert.Empty(t, tc.skipped)
	assert.Zero(t, r.TerminalTaskCount())
}

// The design's trace: attempts 0 and 1 fail and are retried, attempt 2 fails
// on the task's last allowed attempt, so the task fails and every pending task
// below it is skipped.
func TestRecordAttemptReport_ThreeAttemptsThenTheTaskFails(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	id := taskOf(r, "ftable_a")

	assert.Len(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonOOMKilled), 1)
	assert.Len(t, report(t, r, tc, g, "ftable_a", 1, run.AttemptFailed, model.FailureReasonExitCode), 1)
	assert.Empty(t, report(t, r, tc, g, "ftable_a", 2, run.AttemptFailed, model.FailureReasonExitCode))

	assert.Equal(t, run.TaskStatusFailed, tc.statuses[id])
	assert.Equal(t, int32(2), tc.attempts[id])
	for n := int32(0); n <= 2; n++ {
		a, ok := attemptOf(t, tc, r, "ftable_a", n)
		require.True(t, ok, "attempt %d", n)
		assert.Equal(t, run.AttemptFailed, a.Status, "attempt %d", n)
	}
	assert.Len(t, tc.skipped, 6, "ftable_c to ftable_h all read ftable_a")
}

// A failure state would never retry fails the task at once, budget or not: a
// timeout would take as long again, and an invalid dispatch can never run.
func TestRecordAttemptReport_ANonRetryableFailureFailsTheTaskWithBudgetLeft(t *testing.T) {
	for _, reason := range []model.FailureReason{model.FailureReasonStartTimeout, model.FailureReasonDeadlineExceeded,
		model.FailureReasonDbtNothingToDo, model.FailureReasonJobNameCollision} {
		t.Run(string(reason), func(t *testing.T) {
			r, tc, g := startedFailureRun(t)
			report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
			report(t, r, tc, g, "ftable_g", 0, run.AttemptRunning, "")

			assert.Empty(t, report(t, r, tc, g, "ftable_g", 0, run.AttemptFailed, reason))

			assert.Equal(t, run.TaskStatusFailed, tc.statuses[taskOf(r, "ftable_g")])
			_, retried := attemptOf(t, tc, r, "ftable_g", 1)
			assert.False(t, retried)
			assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_h")}, tc.skipped)
		})
	}
}

// With a budget of 0 a task fails on its first failure.
func TestRecordAttemptReport_ABudgetOfZeroFailsOnTheFirstFailure(t *testing.T) {
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()
	tc.derived = true
	_, err = r.Start(context.Background(), tc, "default", plan, g, run.TaskDefaults{MaxRetries: 0, Timeout: time.Hour}, startNow)
	require.NoError(t, err)
	mg := &memTaskGraph{g: g, tc: tc}

	events, _, err := r.RecordAttemptReport(context.Background(), tc, mg,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptFailed, FailureReason: model.FailureReasonExitCode},
		run.TaskDefaults{MaxRetries: 0, Timeout: time.Hour}, reportAt)
	require.NoError(t, err)

	assert.Empty(t, events)
	assert.Equal(t, run.TaskStatusFailed, tc.statuses[taskOf(r, "ftable_a")])
	_, retried := attemptOf(t, tc, r, "ftable_a", 1)
	assert.False(t, retried)
	assert.Len(t, tc.skipped, 6, "ftable_c to ftable_h all read ftable_a")
}

// A report names its attempt, so a delayed report of an attempt the task has
// moved past updates only that attempt's row: the newer attempt stays as its
// own reports left it.
func TestRecordAttemptReport_ALateReportOfAnOlderAttemptLeavesTheNewerOneAlone(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonExitCode)
	report(t, r, tc, g, "ftable_a", 1, run.AttemptRunning, "")
	saves := len(tc.saves)

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonExitCode))
	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, ""))

	assert.Len(t, tc.saves, saves, "an ended attempt takes no report")
	assert.Equal(t, run.TaskStatusRunning, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, int32(1), tc.attempts[taskOf(r, "ftable_a")])
	current, _ := attemptOf(t, tc, r, "ftable_a", 1)
	assert.Equal(t, run.AttemptRunning, current.Status)
}

// A report for an attempt state never dispatched is refused: the attempt
// after the current one, and attempt 0 of a task that waits for its upstreams.
func TestRecordAttemptReport_AReportForAnAttemptStateNeverIssuedIsRefused(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	for _, c := range []struct {
		table string
		n     int32
	}{{"ftable_a", 1}, {"ftable_c", 0}} {
		_, _, err := r.RecordAttemptReport(context.Background(), tc, g,
			run.AttemptReport{TaskID: taskOf(r, c.table), Attempt: c.n, Status: run.AttemptRunning}, taskDefaults, reportAt)
		assert.ErrorIs(t, err, run.ErrAttemptNotIssued, "%s attempt %d", c.table, c.n)
	}
	_, _, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: uuid.New(), Attempt: 0, Status: run.AttemptRunning}, taskDefaults, reportAt)
	assert.ErrorIs(t, err, run.ErrTaskNotHeld)
}

// A report for a run that has ended writes nothing: every attempt of it has
// ended.
func TestRecordAttemptReport_ATerminalRunWritesNothing(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	_, err := r.Cancel(context.Background(), tc, "user", "", reportAt)
	require.NoError(t, err)
	tc.lockOrder = nil

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, ""))
	assert.Empty(t, tc.lockOrder, "nothing is even locked")
}

// The task row is locked before the attempt row, as every writer of both does.
func TestRecordAttemptReport_LocksTheTaskBeforeItsAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	tc.lockOrder = nil
	id := taskOf(r, "ftable_a")

	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")

	assert.Equal(t, []string{"task " + id.String(), "attempt " + id.String() + "/0"}, tc.lockOrder)
}

// The example run: ftable_e and ftable_g fail for good after their budgets,
// their descendants are skipped, and the last final task finalizes the run
// failed with every task counted.
func TestRecordAttemptReport_TheExampleRunFinalizesFailed(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")
	report(t, r, tc, g, "ftable_c", 0, run.AttemptSucceeded, "")
	report(t, r, tc, g, "ftable_d", 0, run.AttemptSucceeded, "")
	for n := int32(0); n <= 2; n++ {
		report(t, r, tc, g, "ftable_e", n, run.AttemptFailed, model.FailureReasonExitCode)
	}
	report(t, r, tc, g, "ftable_g", 0, run.AttemptFailed, model.FailureReasonExitCode)
	report(t, r, tc, g, "ftable_g", 1, run.AttemptFailed, model.FailureReasonExitCode)
	events := report(t, r, tc, g, "ftable_g", 2, run.AttemptFailed, model.FailureReasonExitCode)

	require.Len(t, events, 1)
	fin, ok := events[0].(run.RunFinalized)
	require.True(t, ok, "%T", events[0])
	assert.Equal(t, run.SchedulerStatusFailed, fin.Outcome)
	assert.Equal(t, int32(8), r.TerminalTaskCount())
	assert.Nil(t, r.FailureReason(), "a run whose task failed records no run failure reason")
	assert.ElementsMatch(t, []uuid.UUID{taskOf(r, "ftable_f"), taskOf(r, "ftable_h")}, tc.skipped)
}

// A task that fails for good with budget left does not hold the run open.
func TestRecordAttemptReport_ANonRetryableFailureWithBudgetLeftLetsTheRunFinalize(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")
	events := report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonStartTimeout)

	require.Len(t, events, 1)
	assert.Equal(t, run.SchedulerStatusFailed, events[0].(run.RunFinalized).Outcome)
}

// A run with no attempt in flight and no progress for five minutes fails as
// stalled: its pending tasks are skipped and counted.
func TestFailStalled_FailsARunWithNothingInFlight(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")
	// A defect leaves ftable_c and ftable_g pending without dispatching them.
	for _, table := range []string{"ftable_c", "ftable_g"} {
		tc.statuses[taskOf(r, table)] = run.TaskStatusPending
		tc.dispatched = removeID(tc.dispatched, taskOf(r, table))
	}

	events, stalled, err := r.FailStalled(context.Background(), tc, reportAt.Add(run.StallAfter))

	require.NoError(t, err)
	assert.True(t, stalled)
	require.Len(t, events, 1)
	assert.Equal(t, run.SchedulerStatusFailed, events[0].(run.RunFinalized).Outcome)
	require.NotNil(t, r.FailureReason())
	assert.Equal(t, run.RunFailureStalled, *r.FailureReason())
	assert.Len(t, tc.skipped, 6, "every task left pending is skipped")
	assert.Equal(t, int32(8), r.TerminalTaskCount())
}

// A run with an attempt in flight, or that moved within five minutes, is
// never stalled: every legitimate wait has its own clock.
func TestFailStalled_LeavesARunWithWorkInFlightOrRecentProgress(t *testing.T) {
	r, tc, _ := startedFailureRun(t)
	_, stalled, err := r.FailStalled(context.Background(), tc, startNow.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, stalled, "ftable_a and ftable_b are dispatched")

	for id := range tc.statuses {
		tc.statuses[id] = run.TaskStatusPending
	}
	_, stalled, err = r.FailStalled(context.Background(), tc, startNow.Add(run.StallAfter-time.Second))
	require.NoError(t, err)
	assert.False(t, stalled, "the run moved under five minutes ago")
	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
}

// A cron fire whose selection cannot run fails with the reason that says why.
func TestMarkDispatchTerminal_RecordsTheRunFailureReason(t *testing.T) {
	r := newCronRun(t, model.OperationRun)
	_, err := r.MarkDispatchTerminal(false, run.RunFailureInvalidNodeType, "bad node type", startNow)
	require.NoError(t, err)
	require.NotNil(t, r.FailureReason())
	assert.Equal(t, run.RunFailureInvalidNodeType, *r.FailureReason())

	skipped := newCronRun(t, model.OperationTest)
	_, err = skipped.MarkDispatchTerminal(true, "", "no tests", startNow)
	require.NoError(t, err)
	assert.Nil(t, skipped.FailureReason(), "a skipped run records no failure reason")
}

func ptrTo[T any](v T) *T { return &v }

func removeID(ids []uuid.UUID, id uuid.UUID) []uuid.UUID {
	out := ids[:0]
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}
