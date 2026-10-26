package attemptreconcile_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	now      = time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	defaults = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}
)

// fakeAttempts serves the due attempts and stalled runs of a pass and the
// questions it locks; it records each question's next window.
type fakeAttempts struct {
	due       []repository.AttemptKey
	stalled   []uuid.UUID
	questions map[repository.AttemptKey]run.Question
	marked    map[repository.AttemptKey]time.Duration

	dueStops      []uuid.UUID
	stops         map[uuid.UUID]run.StopQuestion
	cancelling    [2]int
	overdueAfter  time.Duration
	lockQuestions int

	// calls, when set, records each listing of the stalled runs.
	calls *[]string
}

func (f *fakeAttempts) DueAttempts(context.Context, int) ([]repository.AttemptKey, error) {
	return f.due, nil
}
func (f *fakeAttempts) StalledRuns(_ context.Context, idle time.Duration, _ int) ([]uuid.UUID, error) {
	if idle != run.StallAfter {
		panic("a run is stalled after run.StallAfter")
	}
	if f.calls != nil {
		*f.calls = append(*f.calls, "stalled runs")
	}
	return f.stalled, nil
}
func (f *fakeAttempts) LockQuestion(_ context.Context, key repository.AttemptKey) (run.Question, bool, error) {
	f.lockQuestions++
	q, ok := f.questions[key]
	return q, ok, nil
}
func (f *fakeAttempts) MarkQuestioned(_ context.Context, key repository.AttemptKey, next time.Duration) error {
	f.marked[key] = next
	return nil
}

func (f *fakeAttempts) DueStops(context.Context, int) ([]uuid.UUID, error) { return f.dueStops, nil }
func (f *fakeAttempts) LockStops(_ context.Context, id uuid.UUID) (run.StopQuestion, bool, error) {
	q, ok := f.stops[id]
	return q, ok, nil
}
func (f *fakeAttempts) CountCancelling(_ context.Context, overdueAfter time.Duration) (int, int, error) {
	f.overdueAfter = overdueAfter
	return f.cancelling[0], f.cancelling[1], nil
}

type recordingOutbox struct{ events []run.DomainEvent }

func (o *recordingOutbox) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.events = append(o.events, events...)
	return nil
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

type countingMetrics struct {
	stalled                       int
	cancelling, overdue, observed int
	cancelDurations               []time.Duration
}

func (m *countingMetrics) RunStalled() { m.stalled++ }
func (m *countingMetrics) RunsCancelling(total, overdue int) {
	m.cancelling, m.overdue, m.observed = total, overdue, m.observed+1
}
func (m *countingMetrics) RunCancelled(after time.Duration) {
	m.cancelDurations = append(m.cancelDurations, after)
}
func (*countingMetrics) LeaseWaiters(int)           {}
func (*countingMetrics) LeaseGranted(time.Duration) {}
func (*countingMetrics) LeaseRepaired()             {}

// runs serves one run for update and records saves.
type runs struct {
	repository.RunRepository
	r     *run.Run
	saved int
}

func (s *runs) LoadRunForUpdate(context.Context, uuid.UUID) (*run.Run, error) { return s.r, nil }
func (s *runs) SaveRun(context.Context, *run.Run) error                       { s.saved++; return nil }

// idleTasks is a run with no task in flight and one pending task. The first
// grantFailures reads of the waits next in line fail; grantReads counts the
// reads.
type idleTasks struct {
	run.TaskCollection
	inFlight      bool
	grantFailures int
	grantReads    int
}

func (t *idleTasks) HasInFlight(context.Context, uuid.UUID) (bool, error) { return t.inFlight, nil }
func (t *idleTasks) SkipPending(context.Context, uuid.UUID) (int, error)  { return 1, nil }

// GrantableWaits fails while grantFailures allows, then finds no task of the
// run next in line for a free node.
func (t *idleTasks) GrantableWaits(context.Context, uuid.UUID) ([]run.LeaseWait, error) {
	t.grantReads++
	if t.grantReads <= t.grantFailures {
		return nil, errors.New("grant failed: duplicate attempt")
	}
	return nil, nil
}

func newLoop(f *uow.FakeUnitOfWork, metrics *countingMetrics) *attemptreconcile.Loop {
	return attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newFake(attempts *fakeAttempts, outbox *recordingOutbox) *uow.FakeUnitOfWork {
	f := &uow.FakeUnitOfWork{AttemptRepo: attempts}
	f.SetOutboxPublisher(outbox)
	f.SetClock(fixedClock{})
	return f
}

// Each due attempt is dispatched again with the status state knows it in, in
// its own transaction, and its next check moves to its next window.
func TestPass_AsksAboutEachDueAttemptWithItsKnownStatus(t *testing.T) {
	quiet := repository.AttemptKey{TaskID: uuid.New(), Attempt: 0}
	silent := repository.AttemptKey{TaskID: uuid.New(), Attempt: 1}
	node := topology.Node{Key: "e2e_schema.ftable_a", ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_a", NodeType: "dbt-model"}
	attempts := &fakeAttempts{due: []repository.AttemptKey{quiet, silent}, marked: map[repository.AttemptKey]time.Duration{},
		questions: map[repository.AttemptKey]run.Question{
			quiet:  {RunID: uuid.New(), TaskID: quiet.TaskID, Attempt: 0, Status: run.AttemptDispatched, ChecksSent: 0, Node: node, Operation: model.OperationRun},
			silent: {RunID: uuid.New(), TaskID: silent.TaskID, Attempt: 1, Status: run.AttemptRunning, ChecksSent: 2, Node: node, Operation: model.OperationRun},
		}}
	outbox := &recordingOutbox{}
	f := newFake(attempts, outbox)

	require.NoError(t, newLoop(f, &countingMetrics{}).Pass(context.Background()))

	require.Len(t, outbox.events, 2)
	first := outbox.events[0].(run.TaskDispatched)
	assert.Equal(t, run.AttemptDispatched, first.KnownStatus)
	assert.Equal(t, 24*time.Hour, first.Timeout)
	assert.Equal(t, run.AttemptRunning, outbox.events[1].(run.TaskDispatched).KnownStatus)
	assert.Equal(t, map[repository.AttemptKey]time.Duration{quiet: 10 * time.Minute, silent: 30 * time.Minute}, attempts.marked)
	assert.Equal(t, 2, f.CommitCalled, "one transaction per question")
}

// An attempt another pass holds, or that ended since it was listed, is left
// alone.
func TestPass_LeavesAnAttemptItCannotLock(t *testing.T) {
	attempts := &fakeAttempts{due: []repository.AttemptKey{{TaskID: uuid.New()}}, marked: map[repository.AttemptKey]time.Duration{},
		questions: map[repository.AttemptKey]run.Question{}}
	outbox := &recordingOutbox{}

	require.NoError(t, newLoop(newFake(attempts, outbox), &countingMetrics{}).Pass(context.Background()))

	assert.Empty(t, outbox.events)
	assert.Empty(t, attempts.marked)
}

// A stalled run is failed, saved and counted, and its finalization written.
func TestPass_FailsAStalledRunAndCountsIt(t *testing.T) {
	stale := now.Add(-run.StallAfter - time.Minute)
	total := int32(2)
	r := run.HydrateRun(uuid.New(), "daily", run.SchedulerStatusRunning, run.InitStatusCompleted, run.KindCron, nil, "system",
		model.OperationRun, stale, &stale, nil, &stale, nil, nil, nil, &total, 1, nil)
	runRepo := &runs{r: r}
	outbox := &recordingOutbox{}
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, outbox)
	f.SetRunRepo(runRepo)
	f.SetTaskCollection(&idleTasks{})
	metrics := &countingMetrics{}

	require.NoError(t, newLoop(f, metrics).Pass(context.Background()))

	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
	require.NotNil(t, r.FailureReason())
	assert.Equal(t, run.RunFailureStalled, *r.FailureReason())
	assert.Equal(t, 1, runRepo.saved)
	assert.Equal(t, 1, metrics.stalled)
	require.Len(t, outbox.events, 1)
	assert.IsType(t, run.RunFinalized{}, outbox.events[0])
}

// The cancel of a cancelling run is sent again naming the attempts that have
// not answered, in a transaction of its own, and each attempt's next check
// moves to the stop's next window.
func TestPass_ResendsTheCancelNamingTheUnansweredAttempts(t *testing.T) {
	runID := uuid.New()
	a := repository.AttemptKey{TaskID: uuid.New(), Attempt: 0}
	b := repository.AttemptKey{TaskID: uuid.New(), Attempt: 2}
	targets := []run.StopTarget{
		{TaskID: a.TaskID, Attempt: a.Attempt, JobName: "job-a", ChecksSent: 0},
		{TaskID: b.TaskID, Attempt: b.Attempt, JobName: "job-b", ChecksSent: 2},
	}
	attempts := &fakeAttempts{marked: map[repository.AttemptKey]time.Duration{}, dueStops: []uuid.UUID{runID},
		stops: map[uuid.UUID]run.StopQuestion{runID: {RunID: runID, ScheduleName: "daily", CancelledBy: "alice", Reason: "bad data",
			CancelledAt: now.Add(-6 * time.Minute), Attempts: targets}}}
	outbox := &recordingOutbox{}
	f := newFake(attempts, outbox)

	require.NoError(t, newLoop(f, &countingMetrics{}).Pass(context.Background()))

	require.Len(t, outbox.events, 1)
	assert.Equal(t, run.RunCancelled{ID: runID, Name: "daily", By: "alice", CancellationReason: "bad data", Attempts: targets}, outbox.events[0])
	assert.Equal(t, map[repository.AttemptKey]time.Duration{a: run.NextStopCheckAfter(1), b: run.NextStopCheckAfter(3)}, attempts.marked)
	assert.Equal(t, 10*time.Minute, attempts.marked[a])
	assert.Equal(t, 1, f.CommitCalled, "one transaction per cancelling run")
}

// A cancelling run another pass holds, or whose stops answered since it was
// listed, is left alone.
func TestPass_LeavesACancelItCannotLock(t *testing.T) {
	attempts := &fakeAttempts{marked: map[repository.AttemptKey]time.Duration{}, dueStops: []uuid.UUID{uuid.New()},
		stops: map[uuid.UUID]run.StopQuestion{}}
	outbox := &recordingOutbox{}

	require.NoError(t, newLoop(newFake(attempts, outbox), &countingMetrics{}).Pass(context.Background()))

	assert.Empty(t, outbox.events)
	assert.Empty(t, attempts.marked)
}

// A stop-requested attempt is only ever asked about by the cancel: the pass
// locks it through LockStops, never LockQuestion, and a dispatch question is
// not appended for it.
func TestPass_AsksNoDispatchQuestionAboutAStopRequestedAttempt(t *testing.T) {
	runID := uuid.New()
	stopped := repository.AttemptKey{TaskID: uuid.New(), Attempt: 0}
	attempts := &fakeAttempts{marked: map[repository.AttemptKey]time.Duration{}, dueStops: []uuid.UUID{runID},
		questions: map[repository.AttemptKey]run.Question{},
		stops: map[uuid.UUID]run.StopQuestion{runID: {RunID: runID, ScheduleName: "daily",
			Attempts: []run.StopTarget{{TaskID: stopped.TaskID, Attempt: 0, JobName: "job"}}}}}
	outbox := &recordingOutbox{}

	require.NoError(t, newLoop(newFake(attempts, outbox), &countingMetrics{}).Pass(context.Background()))

	require.Len(t, outbox.events, 1)
	assert.IsType(t, run.RunCancelled{}, outbox.events[0], "the attempt is asked about with the cancel, not a dispatch")
	assert.Zero(t, attempts.lockQuestions)
}

// Every pass sets the cancelling gauges from the database's counts, with the
// overdue limit the loop owns.
func TestPass_SetsTheCancellingGauges(t *testing.T) {
	attempts := &fakeAttempts{cancelling: [2]int{3, 1}}
	metrics := &countingMetrics{}

	require.NoError(t, newLoop(newFake(attempts, &recordingOutbox{}), metrics).Pass(context.Background()))

	assert.Equal(t, 15*time.Minute, attemptreconcile.CancelOverdueAfter)
	assert.Equal(t, attemptreconcile.CancelOverdueAfter, attempts.overdueAfter)
	assert.Equal(t, 3, metrics.cancelling)
	assert.Equal(t, 1, metrics.overdue)
	assert.Equal(t, 1, metrics.observed)
}

// A cancelling run past the overdue limit is logged at error level, naming
// the attempts it waits for.
func TestPass_LogsAnOverdueCancelNamingItsAttempts(t *testing.T) {
	runID, taskID := uuid.New(), uuid.New()
	overdue := run.StopQuestion{RunID: runID, ScheduleName: "daily", CancelledAt: now.Add(-attemptreconcile.CancelOverdueAfter - time.Minute),
		Attempts: []run.StopTarget{{TaskID: taskID, Attempt: 1, JobName: "job-x"}}}
	young := run.StopQuestion{RunID: uuid.New(), ScheduleName: "weekly", CancelledAt: now.Add(-time.Minute),
		Attempts: []run.StopTarget{{TaskID: uuid.New(), Attempt: 0, JobName: "job-y"}}}
	attempts := &fakeAttempts{marked: map[repository.AttemptKey]time.Duration{}, dueStops: []uuid.UUID{runID, young.RunID},
		stops: map[uuid.UUID]run.StopQuestion{runID: overdue, young.RunID: young}}
	var logs bytes.Buffer
	loop := attemptreconcile.New(func() uow.UnitOfWork { return newFake(attempts, &recordingOutbox{}) }, "default", defaults, &countingMetrics{}, nil,
		slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	var errorLines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") {
			errorLines = append(errorLines, line)
		}
	}
	require.Len(t, errorLines, 1, "only the run past the limit is logged as an error")
	assert.Contains(t, errorLines[0], runID.String())
	assert.Contains(t, errorLines[0], taskID.String())
	assert.Contains(t, errorLines[0], "job-x")
}

// failingCommit is a unit of work whose commit fails.
type failingCommit struct{ *uow.FakeUnitOfWork }

func (failingCommit) Commit() error { return errors.New("commit failed") }

// A stalled run whose transaction fails to commit is not counted: it is
// still active, and the next pass fails and counts it.
func TestPass_DoesNotCountAStalledRunWhoseCommitFails(t *testing.T) {
	stale := now.Add(-run.StallAfter - time.Minute)
	total := int32(2)
	r := run.HydrateRun(uuid.New(), "daily", run.SchedulerStatusRunning, run.InitStatusCompleted, run.KindCron, nil, "system",
		model.OperationRun, stale, &stale, nil, &stale, nil, nil, nil, &total, 1, nil)
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, &recordingOutbox{})
	f.SetRunRepo(&runs{r: r})
	f.SetTaskCollection(&idleTasks{})
	metrics := &countingMetrics{}
	loop := attemptreconcile.New(func() uow.UnitOfWork { return failingCommit{f} }, "default", defaults, metrics, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.Error(t, loop.Pass(context.Background()))

	assert.Zero(t, metrics.stalled)
}

// A run listed as stalled that has an attempt in flight by the time its lock
// is taken is left running.
func TestPass_LeavesARunThatIsNoLongerStalled(t *testing.T) {
	stale := now.Add(-run.StallAfter - time.Minute)
	total := int32(2)
	r := run.HydrateRun(uuid.New(), "daily", run.SchedulerStatusRunning, run.InitStatusCompleted, run.KindCron, nil, "system",
		model.OperationRun, stale, &stale, nil, &stale, nil, nil, nil, &total, 1, nil)
	runRepo := &runs{r: r}
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, &recordingOutbox{})
	f.SetRunRepo(runRepo)
	f.SetTaskCollection(&idleTasks{inFlight: true})
	metrics := &countingMetrics{}

	require.NoError(t, newLoop(f, metrics).Pass(context.Background()))

	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
	assert.Zero(t, runRepo.saved)
	assert.Zero(t, metrics.stalled)
}

// staleRun is a running run whose progress clock is older than run.StallAfter.
func staleRun() *run.Run { return idleRun(run.StallAfter + time.Minute) }

// idleRun is a running run that has made no progress for idle.
func idleRun(idle time.Duration) *run.Run {
	stale := now.Add(-idle)
	total := int32(2)
	return run.HydrateRun(uuid.New(), "daily", run.SchedulerStatusRunning, run.InitStatusCompleted, run.KindCron, nil, "system",
		model.OperationRun, stale, &stale, nil, &stale, nil, nil, nil, &total, 1, nil)
}

// errorLines returns the lines of logs logged at error level.
func errorLines(logs *bytes.Buffer) []string {
	var out []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") {
			out = append(out, line)
		}
	}
	return out
}

// A stall check whose grant fails is rolled back and tried once more at once:
// a grant that fails only once is not taken for a defect, and the run, with
// nothing to grant and nothing in flight, fails as stalled on the retry.
func TestPass_RetriesAFailedGrantOnceAtOnce(t *testing.T) {
	r := staleRun()
	tasks := &idleTasks{grantFailures: 1}
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, &recordingOutbox{})
	f.SetRunRepo(&runs{r: r})
	f.SetTaskCollection(tasks)
	metrics := &countingMetrics{}
	var logs bytes.Buffer
	loop := attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics, nil,
		slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	assert.Equal(t, 2, tasks.grantReads, "the grant is tried twice")
	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
	assert.Equal(t, 1, metrics.stalled)
	lines := errorLines(&logs)
	require.Len(t, lines, 1)
	assert.NotContains(t, lines[0], "grant_error", "the retry's check failed the run with nothing to grant")
}

// A grant that fails on every try ends its run once the run has made no
// progress for run.StallAfterFailedGrant: after the grant fails twice the run
// is checked without it and failed as stalled, counted, and logged at error
// level with the grant's error. The pass itself succeeds.
func TestPass_FailsARunWhoseGrantKeepsFailingOnceItIsIdleForTheBound(t *testing.T) {
	r := idleRun(run.StallAfterFailedGrant + time.Minute)
	runRepo := &runs{r: r}
	tasks := &idleTasks{grantFailures: 100}
	outbox := &recordingOutbox{}
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, outbox)
	f.SetRunRepo(runRepo)
	f.SetTaskCollection(tasks)
	metrics := &countingMetrics{}
	var logs bytes.Buffer
	loop := attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics, nil,
		slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	assert.Equal(t, 2, tasks.grantReads, "the check without the grant reads no waits")
	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
	require.NotNil(t, r.FailureReason())
	assert.Equal(t, run.RunFailureStalled, *r.FailureReason())
	assert.Equal(t, 1, runRepo.saved)
	assert.Equal(t, 1, metrics.stalled)
	require.Len(t, outbox.events, 1)
	assert.IsType(t, run.RunFinalized{}, outbox.events[0])
	lines := errorLines(&logs)
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], r.ScheduleID().String())
	assert.Contains(t, lines[0], "grant_error=")
	assert.Contains(t, lines[0], "duplicate attempt")
}

// A run idle for less than run.StallAfterFailedGrant whose grant fails is left
// running for the next pass, which tries the grant again; the pass returns the
// grant's error.
func TestPass_LeavesARunWhoseGrantFailsWhileItIsIdleForLessThanTheBound(t *testing.T) {
	r := idleRun(run.StallAfterFailedGrant - time.Minute)
	runRepo := &runs{r: r}
	tasks := &idleTasks{grantFailures: 100}
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, &recordingOutbox{})
	f.SetRunRepo(runRepo)
	f.SetTaskCollection(tasks)
	metrics := &countingMetrics{}

	err := newLoop(f, metrics).Pass(context.Background())

	require.ErrorIs(t, err, run.ErrGrantFailed)
	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
	assert.Zero(t, runRepo.saved)
	assert.Zero(t, metrics.stalled)
	assert.Equal(t, 2, tasks.grantReads)
}

// The pass runs its beforeStall hook once, before it lists the stalled runs.
func TestPass_RunsItsHookBeforeItListsTheStalledRuns(t *testing.T) {
	var calls []string
	attempts := &fakeAttempts{calls: &calls}
	hook := func(context.Context) error {
		calls = append(calls, "hook")
		return nil
	}
	loop := attemptreconcile.New(func() uow.UnitOfWork { return newFake(attempts, &recordingOutbox{}) }, "default", defaults,
		&countingMetrics{}, hook, slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	assert.Equal(t, []string{"hook", "stalled runs"}, calls)
}

// A run whose waiting task the hook dispatches (the lease waker granting a node
// freed since its last pass) is no longer stalled when the pass lists the
// stalled runs, so it is left running.
func TestPass_ARunTheHookDispatchesIsNotFailed(t *testing.T) {
	r := staleRun()
	runRepo := &runs{r: r}
	attempts := &fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}
	f := newFake(attempts, &recordingOutbox{})
	f.SetRunRepo(runRepo)
	f.SetTaskCollection(&idleTasks{})
	hook := func(context.Context) error {
		attempts.stalled = nil
		return nil
	}
	metrics := &countingMetrics{}
	loop := attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics, hook,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	assert.Equal(t, run.SchedulerStatusRunning, r.Status())
	assert.Zero(t, runRepo.saved)
	assert.Zero(t, metrics.stalled)
}

// A hook that fails is logged as an error and does not stop the pass: the
// stalled runs are still listed and failed.
func TestPass_AFailedHookIsLoggedAndTheStallScanStillRuns(t *testing.T) {
	r := staleRun()
	f := newFake(&fakeAttempts{stalled: []uuid.UUID{r.ScheduleID()}}, &recordingOutbox{})
	f.SetRunRepo(&runs{r: r})
	f.SetTaskCollection(&idleTasks{})
	metrics := &countingMetrics{}
	var logs bytes.Buffer
	loop := attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics,
		func(context.Context) error { return errors.New("grant failed") }, slog.New(slog.NewTextHandler(&logs, nil)))

	require.NoError(t, loop.Pass(context.Background()))

	assert.Equal(t, run.SchedulerStatusFailed, r.Status())
	assert.Equal(t, 1, metrics.stalled)
	var hookErrors []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "grant failed") {
			hookErrors = append(hookErrors, line)
		}
	}
	assert.Len(t, hookErrors, 1, "the hook's failure is logged once")
}
