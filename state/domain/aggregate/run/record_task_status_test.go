package run_test

import (
	"context"
	"slices"
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

// memTaskGraph answers run.TaskGraph from a run's graph and the task
// statuses a fakeTaskCollection holds, the way the Postgres queries do.
type memTaskGraph struct {
	g     topology.Graph
	tc    *fakeTaskCollection
	reads int
}

func (m *memTaskGraph) task(k topology.NodeKey) (run.Task, bool) {
	for _, t := range m.tc.bulkCreated {
		if t.NodeKey == k {
			return t, true
		}
	}
	return run.Task{}, false
}

func (m *memTaskGraph) waiting(k topology.NodeKey) (run.Task, bool) {
	t, ok := m.task(k)
	if !ok || m.tc.statuses[t.TaskID] != run.TaskStatusPending || slices.Contains(m.tc.dispatched, t.TaskID) {
		return run.Task{}, false
	}
	return t, true
}

func (m *memTaskGraph) ReadyCandidates(_ context.Context, _ uuid.UUID, _ string, key topology.NodeKey) ([]run.ReadyCandidate, error) {
	m.reads++
	var out []run.ReadyCandidate
	for _, d := range m.g.Downstreams(key) {
		t, ok := m.waiting(d)
		if !ok {
			continue
		}
		var upstream []run.TaskStatus
		for _, u := range m.g.Upstreams(d) {
			if ut, ok := m.task(u); ok {
				upstream = append(upstream, m.tc.statuses[ut.TaskID])
			}
		}
		n, _ := m.g.Node(d)
		out = append(out, run.ReadyCandidate{
			TaskID: t.TaskID, Node: n, JobName: t.JobName, MaxRetries: int32(t.MaxRetries), //nolint:gosec // G115: test fixture; MaxRetries is a small literal.
			UpstreamStatuses: upstream,
		})
	}
	return out, nil
}

func (m *memTaskGraph) PendingDescendants(_ context.Context, _ uuid.UUID, _ string, key topology.NodeKey) ([]uuid.UUID, error) {
	m.reads++
	seen := map[topology.NodeKey]bool{}
	stack := []topology.NodeKey{key}
	for len(stack) > 0 {
		k := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, d := range m.g.Downstreams(k) {
			if _, ok := m.waiting(d); ok && !seen[d] {
				seen[d] = true
				stack = append(stack, d)
			}
		}
	}
	keys := make([]topology.NodeKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]uuid.UUID, 0, len(keys))
	for _, k := range keys {
		t, _ := m.task(k)
		out = append(out, t.TaskID)
	}
	return out, nil
}

// startedFailureRun starts the e2e-schedule-failure run: ftable_a and
// ftable_b are dispatched, every other task waits.
func startedFailureRun(t *testing.T) (*run.Run, *fakeTaskCollection, *memTaskGraph) {
	t.Helper()
	g := failureSchedule()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newCronRun(t, model.OperationRun)
	tc := newFakeTaskCollection()
	tc.derived = true
	_, err = r.Start(context.Background(), tc, "default", plan, g, 2, startNow)
	require.NoError(t, err)
	return r, tc, &memTaskGraph{g: g, tc: tc}
}

func taskOf(r *run.Run, table string) uuid.UUID {
	return run.TaskIDFor("default", r.ScheduleID(), fkey(table))
}

func dispatchedTasks(events []run.DomainEvent) []uuid.UUID {
	var out []uuid.UUID
	for _, e := range events {
		if d, ok := e.(run.TaskDispatched); ok {
			out = append(out, d.TaskID)
		}
	}
	return out
}

// record applies one status for the task of r at table.
func record(t *testing.T, r *run.Run, tc *fakeTaskCollection, g run.TaskGraph, table string, status run.TaskStatus, attempt int32) []run.DomainEvent {
	t.Helper()
	events, _, err := r.RecordTaskStatus(context.Background(), tc, g, taskOf(r, table), status, attempt, startNow.Add(time.Minute))
	require.NoError(t, err)
	return events
}

// ftable_a's success makes ftable_g ready but not ftable_c, which still
// waits for ftable_b; ftable_b's success then makes ftable_c ready. Each
// dispatch carries the task's node, Job name, the run's operation and the
// task's retry budget.
func TestRecordTaskStatus_ASuccessDispatchesTheTasksItMakesReady(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	events := record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0)
	require.Len(t, events, 1)
	d, ok := events[0].(run.TaskDispatched)
	require.True(t, ok, "%T", events[0])
	created := createdByKey(tc)[fkey("ftable_g")]
	node, _ := failureSchedule().Node(fkey("ftable_g"))
	assert.Equal(t, run.TaskDispatched{
		ID: r.ScheduleID(), ScheduleName: "e2e-schedule-failure", TaskID: created.TaskID, Node: node,
		JobName: created.JobName, Operation: model.OperationRun, MaxRetries: 2,
	}, d)

	events = record(t, r, tc, g, "ftable_b", run.TaskStatusSucceeded, 0)
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_c")}, dispatchedTasks(events))
	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
}

// A success delivered again is a replay: nothing is dispatched twice.
func TestRecordTaskStatus_ARedeliveredSuccessDispatchesNothing(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	require.Len(t, record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0), 1)

	assert.Empty(t, record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0))
	assert.Equal(t, 1, g.reads, "the replay reads no graph")
	stamped := 0
	for _, id := range tc.dispatched {
		if id == taskOf(r, "ftable_g") {
			stamped++
		}
	}
	assert.Equal(t, 1, stamped)
}

// A failure with retries left skips nothing; the failure on the task's last
// allowed attempt skips every waiting task below it and counts them terminal.
func TestRecordTaskStatus_AFinalFailureSkipsTheWaitingTasksBelowIt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0)

	assert.Empty(t, record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 0))
	assert.Empty(t, tc.skipped, "a retryable failure skips nothing")
	assert.Equal(t, int32(2), r.TerminalTaskCount(), "ftable_a and ftable_g")

	assert.Empty(t, record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 2))
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_h")}, tc.skipped)
	assert.Equal(t, run.TaskStatusSkipped, tc.statuses[taskOf(r, "ftable_h")])
	assert.Equal(t, int32(3), r.TerminalTaskCount(), "ftable_a, ftable_g and ftable_h")
}

// The example run end to end: ftable_e's last failure skips ftable_f,
// ftable_g's skips ftable_h, and the last terminal status finalizes the run
// failed with every task counted.
func TestRecordTaskStatus_TheExampleRunFinalizesFailed(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_g")}, dispatchedTasks(record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0)))
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_c")}, dispatchedTasks(record(t, r, tc, g, "ftable_b", run.TaskStatusSucceeded, 0)))
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_d"), taskOf(r, "ftable_e")},
		dispatchedTasks(record(t, r, tc, g, "ftable_c", run.TaskStatusSucceeded, 0)))
	assert.Empty(t, record(t, r, tc, g, "ftable_d", run.TaskStatusSucceeded, 0), "ftable_f still waits for ftable_e")
	assert.Empty(t, record(t, r, tc, g, "ftable_e", run.TaskStatusFailed, 2))
	events := record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 2)

	require.Len(t, events, 1)
	fin, ok := events[0].(run.RunFinalized)
	require.True(t, ok, "%T", events[0])
	assert.Equal(t, run.SchedulerStatusFailed, fin.Outcome)
	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
	assert.Equal(t, int32(8), r.TerminalTaskCount())
	assert.ElementsMatch(t, []uuid.UUID{taskOf(r, "ftable_f"), taskOf(r, "ftable_h")}, tc.skipped)
}

// A newer attempt's status after an older attempt's terminal also moves the
// run: a retry that succeeds dispatches what it makes ready, and a retry that
// fails for good skips what it blocks.
func TestRecordTaskStatus_ANewerAttemptsTerminalMovesTheRun(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	record(t, r, tc, g, "ftable_a", run.TaskStatusFailed, 0)
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_g")},
		dispatchedTasks(record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 1)))

	record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 1)
	record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 2)
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_h")}, tc.skipped)
}

// panicTaskGraph fails a test that reads a run's graph.
type panicTaskGraph struct{}

func (panicTaskGraph) ReadyCandidates(context.Context, uuid.UUID, string, topology.NodeKey) ([]run.ReadyCandidate, error) {
	panic("ReadyCandidates read for a run state did not plan")
}

func (panicTaskGraph) PendingDescendants(context.Context, uuid.UUID, string, topology.NodeKey) ([]uuid.UUID, error) {
	panic("PendingDescendants read for a run state did not plan")
}

// A run whose tasks state did not plan records its statuses and finalizes,
// but reads nothing through the graph: its tasks do not record when they were
// dispatched.
func TestRecordTaskStatus_ARunStateDidNotPlanReadsNoGraph(t *testing.T) {
	ctx := context.Background()
	tc := newFakeTaskCollection()
	id1, id2 := uuid.New(), uuid.New()
	r := runningRunWithProjection(t, tc, id1, id2)
	require.Nil(t, r.PlannedAt())

	_, applied, err := r.RecordTaskStatus(ctx, tc, panicTaskGraph{}, id1, run.TaskStatusSucceeded, 0, time.Now())
	require.NoError(t, err)
	assert.True(t, applied)
	tc.maxRetries[id2] = 2
	tc.hasFailed = true
	events, _, err := r.RecordTaskStatus(ctx, tc, panicTaskGraph{}, id2, run.TaskStatusFailed, 2, time.Now())
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
}

// attemptOf returns the stored attempt n of the task of r at table.
func attemptOf(t *testing.T, tc *fakeTaskCollection, r *run.Run, table string, n int32) (run.Attempt, bool) {
	t.Helper()
	a, ok := tc.attemptRows[attemptKey{taskOf(r, table), n}]
	return a, ok
}

// A RUNNING report creates the attempt's row running, named with the task's
// Job name and dispatched when the task was.
func TestRecordTaskStatus_RunningCreatesTheAttemptRow(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	task := createdByKey(tc)[fkey("ftable_a")]
	require.NotNil(t, task.DispatchedAt)

	record(t, r, tc, g, "ftable_a", run.TaskStatusRunning, 0)

	a, ok := attemptOf(t, tc, r, "ftable_a", 0)
	require.True(t, ok)
	assert.Equal(t, run.Attempt{
		TaskID: task.TaskID, Number: 0, JobName: task.JobName, Status: run.AttemptRunning, DispatchedAt: task.DispatchedAt,
	}, a)
}

// A retry's attempt is named with its number and carries no dispatch time,
// which the database clock stamps when the row is first saved.
func TestRecordTaskStatus_ARetryAttemptIsNamedWithItsNumber(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	task := createdByKey(tc)[fkey("ftable_a")]

	record(t, r, tc, g, "ftable_a", run.TaskStatusRunning, 1)

	a, ok := attemptOf(t, tc, r, "ftable_a", 1)
	require.True(t, ok)
	want, err := domain.ComputeJobName(task.ServiceName, task.SchemaName, task.TableName, r.ScheduleID().String(), 1)
	require.NoError(t, err)
	assert.Equal(t, want, a.JobName)
	assert.Nil(t, a.DispatchedAt)
	assert.Equal(t, run.AttemptRunning, a.Status)
}

// A RUNNING delivered after its attempt's FAILED does not lower the attempt,
// and nothing is saved for it.
func TestRecordTaskStatus_AFinalStatusIsNeverLowered(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	record(t, r, tc, g, "ftable_a", run.TaskStatusFailed, 0)
	record(t, r, tc, g, "ftable_a", run.TaskStatusRunning, 0)

	a, ok := attemptOf(t, tc, r, "ftable_a", 0)
	require.True(t, ok)
	assert.Equal(t, run.AttemptFailed, a.Status)
	assert.Len(t, tc.saves, 1)
}

// A report for an attempt older than the task's current one updates that
// attempt's row; the task stays where its newer attempt put it.
func TestRecordTaskStatus_AnOlderAttemptsReportUpdatesOnlyItsRow(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	record(t, r, tc, g, "ftable_a", run.TaskStatusRunning, 1)

	record(t, r, tc, g, "ftable_a", run.TaskStatusFailed, 0)

	a, ok := attemptOf(t, tc, r, "ftable_a", 0)
	require.True(t, ok)
	assert.Equal(t, run.AttemptFailed, a.Status)
	latest, ok := attemptOf(t, tc, r, "ftable_a", 1)
	require.True(t, ok)
	assert.Equal(t, run.AttemptRunning, latest.Status)
	assert.Equal(t, run.TaskStatusRunning, tc.statuses[taskOf(r, "ftable_a")])
	assert.Equal(t, int32(1), tc.attempts[taskOf(r, "ftable_a")])
}

// A FAILED that reports the exhausted budget for a retry Job Kubernetes
// refused (no row at the reported count) is recorded at the refused retry;
// the attempt that failed before it is unchanged, and the task takes the
// reported count, so its last failure skips what it blocks.
func TestRecordTaskStatus_ARefusedRetryJobIsRecordedAtTheRetry(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	record(t, r, tc, g, "ftable_a", run.TaskStatusSucceeded, 0)
	record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 0)
	first, ok := attemptOf(t, tc, r, "ftable_g", 0)
	require.True(t, ok)
	task := createdByKey(tc)[fkey("ftable_g")]
	require.Equal(t, 2, task.MaxRetries)

	record(t, r, tc, g, "ftable_g", run.TaskStatusFailed, 2)

	retry, ok := attemptOf(t, tc, r, "ftable_g", 1)
	require.True(t, ok)
	assert.Equal(t, run.AttemptFailed, retry.Status)
	want, err := domain.ComputeJobName(task.ServiceName, task.SchemaName, task.TableName, r.ScheduleID().String(), 1)
	require.NoError(t, err)
	assert.Equal(t, want, retry.JobName)
	unchanged, _ := attemptOf(t, tc, r, "ftable_g", 0)
	assert.Equal(t, first, unchanged)
	_, ok = attemptOf(t, tc, r, "ftable_g", 2)
	assert.False(t, ok, "no attempt 2 ran")
	assert.Equal(t, int32(2), tc.attempts[task.TaskID])
	assert.Equal(t, []uuid.UUID{taskOf(r, "ftable_h")}, tc.skipped)
}

// A FAILED that reports the exhausted budget for a task whose first Job was
// refused is recorded at attempt 0.
func TestRecordTaskStatus_ARefusedFirstJobIsRecordedAtAttemptZero(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	task := createdByKey(tc)[fkey("ftable_a")]

	record(t, r, tc, g, "ftable_a", run.TaskStatusFailed, 2)

	a, ok := attemptOf(t, tc, r, "ftable_a", 0)
	require.True(t, ok)
	assert.Equal(t, run.AttemptFailed, a.Status)
	assert.Equal(t, task.JobName, a.JobName)
	assert.Len(t, tc.attemptRows, 1)
}

// A report for a run that has ended reads and writes no attempt.
func TestRecordTaskStatus_ATerminalRunWritesNoAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	_, err := r.Cancel(context.Background(), tc, "tester", "stop", startNow)
	require.NoError(t, err)
	tc.lockOrder = nil

	record(t, r, tc, g, "ftable_a", run.TaskStatusFailed, 0)

	assert.Empty(t, tc.attemptRows)
	assert.Empty(t, tc.saves)
	assert.Empty(t, tc.lockOrder)
}

// The task row is locked before any of its attempt rows.
func TestRecordTaskStatus_LocksTheTaskBeforeItsAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)
	id := taskOf(r, "ftable_a")

	record(t, r, tc, g, "ftable_a", run.TaskStatusRunning, 0)

	require.NotEmpty(t, tc.lockOrder)
	assert.Equal(t, "task "+id.String(), tc.lockOrder[0])
	assert.Contains(t, tc.lockOrder, "attempt "+id.String()+"/0")
}

// A PENDING report describes no attempt and writes none.
func TestRecordTaskStatus_APendingReportWritesNoAttempt(t *testing.T) {
	r, tc, g := startedFailureRun(t)

	record(t, r, tc, g, "ftable_a", run.TaskStatusPending, 0)

	assert.Empty(t, tc.attemptRows)
}
