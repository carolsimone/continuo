package run_test

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// otherRunsTask is a task of another run that holds a node's write lease.
var otherRunsTask = uuid.MustParse("9b07c1d2-5e3f-4a6b-8c7d-0e1f2a3b4c5d")

// grantAt is when a freed node is granted to the task waiting for it.
var grantAt = reportAt.Add(time.Minute)

// claimOf is the lease claim of the task of r at table.
func claimOf(r *run.Run, table string) run.LeaseClaim {
	return run.LeaseClaim{TaskID: taskOf(r, table), RunID: r.ScheduleID(), NodeKey: fkey(table)}
}

// waitingFailureRun starts the e2e-schedule-failure run while another run's
// task holds ftable_a: ftable_a waits for its node's lease and ftable_b is
// dispatched holding its own.
func waitingFailureRun(t *testing.T) (*run.Run, *fakeTaskCollection, *memTaskGraph) {
	t.Helper()
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()
	tc.derived = true
	tc.leases[fkey("ftable_a")] = otherRunsTask
	_, err = r.Start(context.Background(), tc, "default", plan, g, taskDefaults, startNow)
	require.NoError(t, err)
	require.Equal(t, run.WaitReasonNodeLease, tc.waits[taskOf(r, "ftable_a")])
	return r, tc, &memTaskGraph{g: g, tc: tc}
}

// A task holds its node's lease across every retry, and the lease is released
// once, by the report that ends the task, whether it ends succeeded or failed
// for good.
func TestLease_HeldAcrossARetryAndReleasedOnlyWhenTheTaskEnds(t *testing.T) {
	for name, last := range map[string]struct {
		status run.AttemptStatus
		reason model.FailureReason
		ends   run.TaskStatus
	}{
		"succeeded on its last attempt": {run.AttemptSucceeded, "", run.TaskStatusSucceeded},
		"failed with its budget spent":  {run.AttemptFailed, model.FailureReasonExitCode, run.TaskStatusFailed},
	} {
		t.Run(name, func(t *testing.T) {
			r, tc, g := startedFailureRun(t)
			a := taskOf(r, "ftable_a")
			require.Equal(t, a, tc.leases[fkey("ftable_a")], "the dispatched task holds its node")

			report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
			assert.Len(t, report(t, r, tc, g, "ftable_a", 0, run.AttemptFailed, model.FailureReasonOOMKilled), 1,
				"attempt 1 is dispatched")
			assert.Empty(t, tc.releases, "a retry keeps the lease")
			assert.Len(t, report(t, r, tc, g, "ftable_a", 1, run.AttemptFailed, model.FailureReasonExitCode), 1,
				"attempt 2 is dispatched")
			assert.Empty(t, tc.releases, "a retry keeps the lease")
			assert.Equal(t, a, tc.leases[fkey("ftable_a")])
			assert.Equal(t, run.TaskStatusDispatched, tc.statuses[a])

			report(t, r, tc, g, "ftable_a", 2, last.status, last.reason)

			assert.Equal(t, last.ends, tc.statuses[a])
			assert.Equal(t, []uuid.UUID{a}, tc.releases, "released once, by the report that ends the task")
			assert.NotContains(t, tc.leases, fkey("ftable_a"))
		})
	}
}

// A report that ends a task takes the leases of the tasks it makes ready
// before it releases the ended task's own lease, so its transaction inserts
// every lease it takes before it deletes one, as a run's start does; two
// transactions that take overlapping nodes then never wait on each other in a
// cycle.
func TestLease_AReportTakesTheLeasesOfWhatItMakesReadyBeforeItReleasesItsOwn(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	tc.leaseWrites = nil

	report(t, r, tc, g, "ftable_a", 0, run.AttemptSucceeded, "")
	report(t, r, tc, g, "ftable_b", 0, run.AttemptSucceeded, "")

	assert.Equal(t, []string{
		"take " + string(fkey("ftable_g")), "release " + string(fkey("ftable_a")),
		"take " + string(fkey("ftable_c")), "release " + string(fkey("ftable_b")),
	}, tc.leaseWrites)
}

// A stopping task keeps its node's lease while its attempt runs on, and
// releases it with the report that ends it: stopped, or the failure its
// container reached first.
func TestLease_AStoppingTaskReleasesOnItsStoppedReport(t *testing.T) {
	r, tc, g := cancellingFailureRun(t)
	a, b := taskOf(r, "ftable_a"), taskOf(r, "ftable_b")
	assert.Empty(t, tc.releases, "a stopping task keeps its lease")

	report(t, r, tc, g, "ftable_a", 0, run.AttemptRunning, "")
	assert.Empty(t, tc.releases, "a stopping task keeps its lease")

	report(t, r, tc, g, "ftable_a", 0, run.AttemptStopped, "")

	assert.Equal(t, run.TaskStatusCancelled, tc.statuses[a])
	assert.Equal(t, []uuid.UUID{a}, tc.releases)
	assert.Equal(t, map[topology.NodeKey]uuid.UUID{fkey("ftable_b"): b}, tc.leases, "ftable_b is still stopping")

	report(t, r, tc, g, "ftable_b", 0, run.AttemptFailed, model.FailureReasonOOMKilled)

	assert.Equal(t, run.TaskStatusFailed, tc.statuses[b])
	assert.Equal(t, []uuid.UUID{a, b}, tc.releases)
	assert.Empty(t, tc.leases)
	assert.Equal(t, run.SchedulerStatusCancelled, r.Status())
}

// Once the other run's task releases ftable_a, the grant takes its lease for
// the waiting task and dispatches it as a ready task is dispatched: the task
// is dispatched and waits no more, its attempt 0 is stored dispatched and due
// for its first check, a TaskDispatched records it with its node's timeout,
// and the run's progress clock moves.
func TestGrantLease_DispatchesTheWaitingTask(t *testing.T) {
	r, tc, g := waitingFailureRun(t)
	delete(tc.leases, fkey("ftable_a"))
	r.ResetChanges()
	a := taskOf(r, "ftable_a")

	events, dispatched, err := r.GrantLease(context.Background(), tc, g, a, taskDefaults, grantAt)

	require.NoError(t, err)
	assert.True(t, dispatched)
	task := createdByKey(tc)[fkey("ftable_a")]
	node, _ := failureSchedule().Node(fkey("ftable_a"))
	assert.Equal(t, []run.DomainEvent{run.TaskDispatched{
		ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: a, Attempt: 0,
		KnownStatus: run.AttemptDispatched, Node: node, JobName: task.JobName, Operation: model.OperationRun,
		Timeout: 24 * time.Hour,
	}}, events)
	assert.Equal(t, []run.LeaseClaim{claimOf(r, "ftable_a")}, tc.waiterTakes)
	assert.Equal(t, a, tc.leases[fkey("ftable_a")])
	assert.Equal(t, run.TaskStatusDispatched, tc.statuses[a])
	assert.Empty(t, tc.waits, "the task waits no more")
	require.NotNil(t, task.DispatchedAt)
	assert.Equal(t, grantAt, *task.DispatchedAt)
	attempt, ok := attemptOf(t, tc, r, "ftable_a", 0)
	require.True(t, ok)
	at := grantAt
	assert.Equal(t, run.Attempt{TaskID: a, Number: 0, JobName: task.JobName, Status: run.AttemptDispatched,
		DispatchedAt: &at, NextCheckIn: 5 * time.Minute}, attempt)
	assert.Equal(t, grantAt, *r.LastHeartbeatAt())
	assert.True(t, r.Changes().IsHeartbeatDirty())
}

// The grant dispatches nothing unless the run is running and the task still
// waits for its node's lease: a cancelling or ended run is left before the
// task is locked, and a task already dispatched or waiting only for its
// upstreams is left as it is.
func TestGrantLease_DoesNothingOnACancellingRunOrATaskNoLongerWaiting(t *testing.T) {
	for name, c := range map[string]struct {
		table   string
		prepare func(t *testing.T, r *run.Run, tc *fakeTaskCollection)
		locks   bool
	}{
		"a cancelling run": {table: "ftable_a", prepare: func(t *testing.T, r *run.Run, tc *fakeTaskCollection) {
			_, err := r.Cancel(context.Background(), tc, "alice", "bad data", reportAt)
			require.NoError(t, err)
			require.Equal(t, run.SchedulerStatusCancelling, r.Status())
		}},
		"an ended run": {table: "ftable_a", prepare: func(t *testing.T, r *run.Run, _ *fakeTaskCollection) {
			_, err := r.MarkDispatchTerminal(false, run.RunFailureEmptySelection, "ended", reportAt)
			require.NoError(t, err)
		}},
		"a dispatched task":                {table: "ftable_b", locks: true},
		"a task waiting for its upstreams": {table: "ftable_c", locks: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, tc, g := waitingFailureRun(t)
			delete(tc.leases, fkey("ftable_a"))
			if c.prepare != nil {
				c.prepare(t, r, tc)
			}
			r.ResetChanges()
			tc.lockOrder = nil
			statuses, leases, waits := maps.Clone(tc.statuses), maps.Clone(tc.leases), maps.Clone(tc.waits)
			added, heartbeat := len(tc.added), *r.LastHeartbeatAt()
			id := taskOf(r, c.table)

			events, dispatched, err := r.GrantLease(context.Background(), tc, g, id, taskDefaults, grantAt)

			require.NoError(t, err)
			assert.False(t, dispatched)
			assert.Empty(t, events)
			assert.Equal(t, c.locks, slices.Contains(tc.lockOrder, "task "+id.String()), "the task is locked")
			assert.Empty(t, tc.waiterTakes, "no lease is taken")
			assert.Equal(t, statuses, tc.statuses)
			assert.Equal(t, leases, tc.leases)
			assert.Equal(t, waits, tc.waits)
			assert.Len(t, tc.added, added, "no attempt is dispatched")
			assert.Equal(t, heartbeat, *r.LastHeartbeatAt())
			assert.False(t, r.Changes().IsHeartbeatDirty())
		})
	}
}

// A waiting task whose node is still held by another run's task stays
// waiting: the grant takes nothing and dispatches nothing.
func TestGrantLease_LeavesATaskWhoseNodeIsStillHeld(t *testing.T) {
	r, tc, g := waitingFailureRun(t)
	a := taskOf(r, "ftable_a")
	added := len(tc.added)

	events, dispatched, err := r.GrantLease(context.Background(), tc, g, a, taskDefaults, grantAt)

	require.NoError(t, err)
	assert.False(t, dispatched)
	assert.Empty(t, events)
	assert.Equal(t, []run.LeaseClaim{claimOf(r, "ftable_a")}, tc.waiterTakes)
	assert.Equal(t, otherRunsTask, tc.leases[fkey("ftable_a")])
	assert.Equal(t, run.TaskStatusPending, tc.statuses[a])
	assert.Equal(t, run.WaitReasonNodeLease, tc.waits[a])
	assert.Len(t, tc.added, added)
}

// A grant for a task the run does not hold is refused before a lease is
// taken: no task has the id, or another run holds it.
func TestGrantLease_RefusesATaskTheRunDoesNotHold(t *testing.T) {
	r, tc, g := waitingFailureRun(t)
	delete(tc.leases, fkey("ftable_a"))
	a := taskOf(r, "ftable_a")
	moved := tc.created[a]
	moved.ScheduleID = uuid.New()
	tc.created[a] = moved

	for _, id := range []uuid.UUID{uuid.New(), a} {
		events, dispatched, err := r.GrantLease(context.Background(), tc, g, id, taskDefaults, grantAt)

		require.ErrorIs(t, err, run.ErrTaskNotHeld)
		assert.False(t, dispatched)
		assert.Empty(t, events)
	}
	assert.Empty(t, tc.waiterTakes)
	assert.NotContains(t, tc.leases, fkey("ftable_a"))
}
