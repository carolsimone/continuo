package attemptreconcile_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
}

func (f *fakeAttempts) DueAttempts(context.Context, int) ([]repository.AttemptKey, error) {
	return f.due, nil
}
func (f *fakeAttempts) StalledRuns(_ context.Context, idle time.Duration, _ int) ([]uuid.UUID, error) {
	if idle != run.StallAfter {
		panic("a run is stalled after run.StallAfter")
	}
	return f.stalled, nil
}
func (f *fakeAttempts) LockQuestion(_ context.Context, key repository.AttemptKey) (run.Question, bool, error) {
	q, ok := f.questions[key]
	return q, ok, nil
}
func (f *fakeAttempts) MarkQuestioned(_ context.Context, key repository.AttemptKey, next time.Duration) error {
	f.marked[key] = next
	return nil
}

type recordingOutbox struct{ events []run.DomainEvent }

func (o *recordingOutbox) Append(_ context.Context, events []run.DomainEvent, _ uuid.UUID) error {
	o.events = append(o.events, events...)
	return nil
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

type countingMetrics struct{ stalled int }

func (m *countingMetrics) RunStalled() { m.stalled++ }

// runs serves one run for update and records saves.
type runs struct {
	repository.RunRepository
	r     *run.Run
	saved int
}

func (s *runs) LoadRunForUpdate(context.Context, uuid.UUID) (*run.Run, error) { return s.r, nil }
func (s *runs) SaveRun(context.Context, *run.Run) error                       { s.saved++; return nil }

// idleTasks is a run with no task in flight and one pending task.
type idleTasks struct {
	run.TaskCollection
	inFlight bool
}

func (t *idleTasks) HasInFlight(context.Context, uuid.UUID) (bool, error) { return t.inFlight, nil }
func (t *idleTasks) SkipPending(context.Context, uuid.UUID) (int, error)  { return 1, nil }

func newLoop(f *uow.FakeUnitOfWork, metrics *countingMetrics) *attemptreconcile.Loop {
	return attemptreconcile.New(func() uow.UnitOfWork { return f }, "default", defaults, metrics,
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
	loop := attemptreconcile.New(func() uow.UnitOfWork { return failingCommit{f} }, "default", defaults, metrics,
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
