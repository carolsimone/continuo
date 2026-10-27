package run_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
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
	assert.Equal(t, reportAt, *r.LastProgressAt())

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
	heartbeat := *r.LastProgressAt()

	_, _, err := r.RecordAttemptReport(context.Background(), tc, g, run.AttemptReport{
		TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptRunning, LogS3Key: ptrTo("logs/a")}, taskDefaults, reportAt.Add(time.Hour))
	require.NoError(t, err)

	require.Len(t, tc.saves, saves+1, "the attempt's facts are saved")
	assert.Equal(t, "logs/a", *tc.saves[saves].LogS3Key)
	assert.Equal(t, heartbeat, *r.LastProgressAt())
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
		ID: r.RunID(), TenantID: r.TenantID(), ScheduleName: "e2e-schedule-failure", TaskID: created.TaskID, Attempt: 0,
		KnownStatus: run.AttemptDispatched, Node: node, JobName: created.JobName, Operation: model.OperationRun,
		Timeout: 24 * time.Hour,
	}, events[0])
	a, ok := attemptOf(t, tc, r, "ftable_g", 0)
	require.True(t, ok)
	assert.Equal(t, run.AttemptDispatched, a.Status)
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[created.TaskID])
	assert.Equal(t, run.TaskStatusSucceeded, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, int32(1), r.TerminalTaskCount())
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_a")}, tc.releases, "the task that ended releases its lease")
	assert.Equal(t, created.TaskID, tc.leases[fkey("ftable_g")], "the task dispatched holds its node's lease")
}

// A success claims the leases of the tasks it makes ready, in the transaction
// that records it: a task whose node is free is dispatched holding the node's
// lease; one whose node another run's task holds waits for it, undispatched.
func TestDispatchReady_TakesTheLeasesOfTheTasksItMakesReady(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	tc.leases[fkey("ftable_g")] = otherRunsTask

	afterA := report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
	afterB := report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")

	gTask, cTask := taskOf(r, "ftable_g"), taskOf(r, "ftable_c")
	assert.Empty(t, afterA, "ftable_g waits for its node")
	assert.Equal(t, run.TaskStatusPending, tc.statuses[gTask])
	assert.Equal(t, map[uuid.UUID]run.WaitReason{gTask: run.WaitReasonNodeLease}, tc.waits)
	_, issued := attemptOf(t, tc, r, "ftable_g", 0)
	assert.False(t, issued, "a task waiting for its node has no attempt")

	require.Len(t, afterB, 1)
	assert.Equal(t, cTask, afterB[0].(run.TaskDispatched).TaskID)
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[cTask])
	assert.Equal(t, [][]run.LeaseClaim{
		{claimOf(r, "ftable_a"), claimOf(r, "ftable_b")},
		{claimOf(r, "ftable_g")},
		{claimOf(r, "ftable_c")},
	}, tc.leaseTakes)
	assert.Equal(t, map[topology.NodeKey]uuid.UUID{fkey("ftable_g"): otherRunsTask, fkey("ftable_c"): cTask}, tc.leases,
		"ftable_a and ftable_b released theirs as they ended")
}

// A retryable failure with budget left dispatches the next attempt in the
// transaction that records it: the task never shows failed, nothing is
// skipped and nothing counts terminal.
func TestRecordAttemptReport_ARetryableFailureDispatchesTheNextAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")

	events := report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonOOMKilled)

	id := taskOf(r, "ftable_a")
	jobName, err := domain.ComputeJobName("service-1", "e2e_schema", "ftable_a", r.RunID().String(), 1)
	require.NoError(t, err)
	node, _ := failureSchedule().Node(fkey("ftable_a"))
	assert.Equal(t, []run.DomainEvent{run.TaskDispatched{
		ID: r.RunID(), TenantID: r.TenantID(), ScheduleName: "e2e-schedule-failure", TaskID: id, Attempt: 1,
		KnownStatus: run.AttemptDispatched, Node: node, JobName: jobName, Operation: model.OperationRun,
		Timeout: 24 * time.Hour,
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

// A report naming this run for a task another run holds is refused before
// the attempt is read: the task, its attempts and the run stay as they are.
func TestRecordAttemptReport_ATaskOfAnotherRunIsNotHeld(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	id := taskOf(r, "ftable_a")
	other := tc.created[id]
	other.ScheduleID = uuid.New()
	tc.created[id] = other
	tc.lockOrder, tc.saves, tc.added = nil, nil, nil
	heartbeat := *r.LastProgressAt()

	events, succeeded, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: id, Attempt: 0, Status: run.AttemptSucceeded}, taskDefaults, reportAt)

	require.ErrorIs(t, err, run.ErrTaskNotHeld)
	assert.Empty(t, events)
	assert.False(t, succeeded)
	assert.Equal(t, []string{"task " + id.String()}, tc.lockOrder, "the attempt is not read")
	assert.Empty(t, tc.saves, "no attempt is saved")
	assert.Empty(t, tc.added, "no attempt is added")
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[id])
	a, _ := attemptOf(t, tc, r, "ftable_a", 0)
	assert.Equal(t, run.AttemptDispatched, a.Status)
	assert.Zero(t, r.TerminalTaskCount())
	assert.Equal(t, heartbeat, *r.LastProgressAt())
}

// A report for a run that has ended writes nothing: every attempt of it has
// ended.
func TestRecordAttemptReport_ATerminalRunWritesNothing(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	_, err := r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "ended", reportAt)
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
	assert.Equal(t, run.RunStatusFailed, fin.Outcome)
	assert.Equal(t, int32(8), r.TerminalTaskCount())
	assert.Nil(t, r.FailureReason(), "a run whose task failed records no run failure reason")
	assert.ElementsMatch(t, []uuid.UUID{taskOf(r, "ftable_f"), taskOf(r, "ftable_h")}, tc.skipped)
}

// Every task succeeding, in level order, dispatches each level as its
// upstreams succeed, and the last success finalizes the run succeeded.
func TestRecordAttemptReport_TheLastSuccessFinalizesTheRunSucceeded(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	tables := []string{"ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h"}
	for i, table := range tables {
		events, succeeded, err := r.RecordAttemptReport(context.Background(), tc, g,
			run.AttemptReport{TaskID: taskOf(r, table), Attempt: 0, Status: run.AttemptSucceeded}, taskDefaults, reportAt)
		require.NoError(t, err, table)
		assert.True(t, succeeded, "%s: the report made the task succeeded", table)
		if i < len(tables)-1 {
			assert.False(t, r.IsTerminal(), "%s: tasks are left", table)
			continue
		}
		require.NotEmpty(t, events)
		fin, ok := events[len(events)-1].(run.RunFinalized)
		require.True(t, ok, "%T", events[len(events)-1])
		assert.Equal(t, run.RunStatusSucceeded, fin.Outcome)
	}
	assert.Equal(t, int32(8), r.TerminalTaskCount())
}

// A failed read of the task fails the report and changes nothing, so the
// consumer redelivers it.
func TestRecordAttemptReport_PropagatesALoadError(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	tc.loadErr = errors.New("read replica unavailable")

	events, _, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptSucceeded}, taskDefaults, reportAt)

	require.ErrorIs(t, err, tc.loadErr)
	assert.Empty(t, events)
	assert.Equal(t, int32(0), r.TerminalTaskCount())
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[taskOf(r, "ftable_a")])
}

// A task that fails for good with budget left does not hold the run open.
func TestRecordAttemptReport_ANonRetryableFailureWithBudgetLeftLetsTheRunFinalize(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")
	events := report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonStartTimeout)

	require.Len(t, events, 1)
	assert.Equal(t, run.RunStatusFailed, events[0].(run.RunFinalized).Outcome)
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

	check, err := r.FailStalled(context.Background(), tc, g, taskDefaults, reportAt.Add(run.StallAfter))

	require.NoError(t, err)
	assert.True(t, check.Stalled)
	assert.Empty(t, check.Granted)
	require.Len(t, check.Events, 1)
	assert.Equal(t, run.RunStatusFailed, check.Events[0].(run.RunFinalized).Outcome)
	require.NotNil(t, r.FailureReason())
	assert.Equal(t, run.RunFailureStalled, *r.FailureReason())
	assert.Len(t, tc.skipped, 6, "every task left pending is skipped")
	assert.Equal(t, int32(8), r.TerminalTaskCount())
}

// A run with an attempt in flight, or that moved within five minutes, is
// never stalled: every legitimate wait has its own clock.
func TestFailStalled_LeavesARunWithWorkInFlightOrRecentProgress(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	check, err := r.FailStalled(context.Background(), tc, g, taskDefaults, startNow.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, check.Stalled, "ftable_a and ftable_b are dispatched")
	assert.Empty(t, check.Events)

	for id := range tc.statuses {
		tc.statuses[id] = run.TaskStatusPending
	}
	tc.grantableReads = nil
	check, err = r.FailStalled(context.Background(), tc, g, taskDefaults, startNow.Add(run.StallAfter-time.Second))
	require.NoError(t, err)
	assert.False(t, check.Stalled, "the run moved under five minutes ago")
	assert.Empty(t, tc.grantableReads, "a run that moved within five minutes is not read")
	assert.Equal(t, run.RunStatusRunning, r.Status())
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

// cancellingFailureRun starts the e2e-schedule-failure run and cancels it at
// reportAt: ftable_a and ftable_b, dispatched with the run, are stopping with
// a stop requested of attempt 0; every other task is cancelled.
func cancellingFailureRun(t *testing.T) (*run.Run, *fakeTaskCollection, *memTaskGraph) {
	t.Helper()
	r, tc, g := startedFailureRun(t)
	_, err := r.Cancel(context.Background(), tc, "alice", "bad data", reportAt)
	require.NoError(t, err)
	require.Equal(t, run.RunStatusCancelling, r.Status())
	return r, tc, g
}

// A stopped report of a stopping task's attempt cancels the task and counts
// it terminal; the run waits for its other stopping task.
func TestRecordAttemptReport_AStoppedReportCancelsTheStoppingTask(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptStopped, ""))

	assert.Equal(t, run.TaskStatusCancelled, tc.statuses[taskOf(r, "ftable_a")])
	a, _ := attemptOf(t, tc, r, "ftable_a", 0)
	assert.Equal(t, run.AttemptStopped, a.Status)
	assert.Equal(t, int32(7), r.TerminalTaskCount())
	assert.Equal(t, run.RunStatusCancelling, r.Status(), "ftable_b is still stopping")
}

// A stopping task whose container exited 0 before it was stopped wrote its
// table: the task ends succeeded and the report says so, so the handler
// records the version it loaded; nothing it would make ready is dispatched.
func TestRecordAttemptReport_AStoppingTaskThatSucceedsEndsSucceeded(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	added, reads := len(tc.added), g.reads

	events, succeeded, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptSucceeded}, taskDefaults, reportAt)

	require.NoError(t, err)
	assert.True(t, succeeded, "the report made the task succeeded")
	assert.Empty(t, events)
	assert.Equal(t, run.TaskStatusSucceeded, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, run.TaskStatusCancelled, tc.statuses[taskOf(r, "ftable_g")], "ftable_g stays cancelled")
	assert.Len(t, tc.added, added, "no attempt is dispatched")
	assert.Equal(t, reads, g.reads, "the run's graph is not read")
	assert.Equal(t, int32(7), r.TerminalTaskCount())
}

// A stopping task whose attempt failed fails, whatever the failure's class
// and the task's budget: a cancelling run starts no attempt.
func TestRecordAttemptReport_AStoppingTaskThatFailsIsNotRetried(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonOOMKilled))

	assert.Equal(t, run.TaskStatusFailed, tc.statuses[taskOf(r, "ftable_a")])
	_, retried := attemptOf(t, tc, r, "ftable_a", 1)
	assert.False(t, retried)
	assert.Empty(t, tc.skipped, "the tasks below it are already cancelled")
	assert.Equal(t, int32(7), r.TerminalTaskCount())
}

// A stopping task's attempt that reports running moves only the attempt: the
// task stays stopping, the run's progress clock stays where the cancel left
// it, and the attempt's next check stays the one the stop set.
func TestRecordAttemptReport_ARunningReportLeavesTheTaskStopping(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	heartbeat := *r.LastProgressAt()
	saves := len(tc.saves)

	_, _, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptRunning}, taskDefaults, reportAt.Add(time.Minute))
	require.NoError(t, err)

	assert.Equal(t, run.TaskStatusStopping, tc.statuses[taskOf(r, "ftable_a")])
	require.Len(t, tc.saves, saves+1)
	assert.Equal(t, run.AttemptRunning, tc.saves[saves].Status)
	assert.Zero(t, tc.saves[saves].NextCheckIn, "the attempt's next check stays the one the stop set")
	assert.Equal(t, heartbeat, *r.LastProgressAt())
}

// A cancelling run dispatches nothing: no task a success makes ready, no
// retry of a retryable failure; the last report ends the run.
func TestRecordAttemptReport_ACancellingRunDispatchesNothing(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	added, reads := len(tc.added), g.reads

	first := report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
	last := report(t, r, tc, g, "ftable_b", 0, run.AttemptFailed, model.FailureReasonOOMKilled)

	assert.Empty(t, first)
	assert.Equal(t, []run.DomainEvent{run.RunFinalized{ID: r.RunID(), Name: "e2e-schedule-failure",
		Outcome: run.RunStatusCancelled}}, last)
	assert.Len(t, tc.added, added, "no attempt is dispatched")
	assert.Equal(t, reads, g.reads, "the run's graph is not read")
}

// The run ends cancelled once its last stopping task ends, even when another
// stopping task failed.
func TestRecordAttemptReport_TheLastStoppedTaskEndsTheRunCancelled(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonExitCode))

	events := report(t, r, tc, g, "ftable_b", 0, run.AttemptStopped, "")

	assert.Equal(t, []run.DomainEvent{run.RunFinalized{ID: r.RunID(), Name: "e2e-schedule-failure",
		Outcome: run.RunStatusCancelled}}, events)
	assert.Equal(t, run.RunStatusCancelled, r.Status())
	require.NotNil(t, r.CompletedAt())
	assert.Equal(t, reportAt, *r.CompletedAt())
	assert.Nil(t, r.FailureReason())
	assert.Equal(t, int32(8), r.TerminalTaskCount())
	assert.Equal(t, run.TaskStatusFailed, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, run.TaskStatusCancelled, tc.statuses[taskOf(r, "ftable_b")])
}

// A stopped report of an attempt state never asked to stop is refused before
// anything is written.
func TestRecordAttemptReport_AStoppedReportForATaskNotStoppingIsRefused(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	saves := len(tc.saves)

	events, succeeded, err := r.RecordAttemptReport(context.Background(), tc, g,
		run.AttemptReport{TaskID: taskOf(r, "ftable_a"), Attempt: 0, Status: run.AttemptStopped}, taskDefaults, reportAt)

	require.ErrorIs(t, err, run.ErrStopNotRequested)
	assert.Empty(t, events)
	assert.False(t, succeeded)
	assert.Len(t, tc.saves, saves, "nothing is saved")
	a, _ := attemptOf(t, tc, r, "ftable_a", 0)
	assert.Equal(t, run.AttemptDispatched, a.Status)
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[taskOf(r, "ftable_a")])
}

// A stopped report delivered again after it cancelled its task changes
// nothing and is not refused: the attempt has ended.
func TestRecordAttemptReport_ARepeatedStoppedReportChangesNothing(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	report(t, r, tc, g, "ftable_a", 0, run.AttemptStopped, "")
	saves, terminal := len(tc.saves), r.TerminalTaskCount()

	assert.Empty(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptStopped, ""))

	assert.Len(t, tc.saves, saves)
	assert.Equal(t, terminal, r.TerminalTaskCount())
}

// A cancelling run left with no task stopping and no progress for five
// minutes ends cancelled, with no failure reason; one with a task stopping
// waits for it.
func TestFailStalled_EndsACancellingRunWithNothingInFlightCancelled(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	check, err := r.FailStalled(context.Background(), tc, g, taskDefaults, reportAt.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, check.Stalled, "ftable_a and ftable_b are stopping")

	// A defect ends both stopping tasks without moving the run.
	tc.statuses[taskOf(r, "ftable_a")] = run.TaskStatusCancelled
	tc.statuses[taskOf(r, "ftable_b")] = run.TaskStatusCancelled
	check, err = r.FailStalled(context.Background(), tc, g, taskDefaults, reportAt.Add(run.StallAfter-time.Second))
	require.NoError(t, err)
	assert.False(t, check.Stalled, "the run moved under five minutes ago")

	check, err = r.FailStalled(context.Background(), tc, g, taskDefaults, reportAt.Add(run.StallAfter))

	require.NoError(t, err)
	assert.True(t, check.Stalled)
	assert.Equal(t, []run.DomainEvent{run.RunFinalized{ID: r.RunID(), Name: "e2e-schedule-failure",
		Outcome: run.RunStatusCancelled}}, check.Events)
	assert.Empty(t, tc.grantableReads, "a cancelling run is granted nothing")
	assert.Equal(t, run.RunStatusCancelled, r.Status())
	assert.Nil(t, r.FailureReason())
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
