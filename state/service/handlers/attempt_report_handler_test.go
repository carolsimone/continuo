package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeTaskStatusRunRepo is an in-memory RunRepository for
// AttemptReportHandler tests. LoadRunForUpdate returns the stored run;
// SaveRun records the call and stores the updated aggregate. Other methods
// panic on accidental use.
type fakeTaskStatusRunRepo struct {
	stored     *run.Run
	loadErr    error
	saveErr    error
	saveCalled bool
}

func (f *fakeTaskStatusRunRepo) LoadRunForUpdate(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}

func (f *fakeTaskStatusRunRepo) SaveRun(_ context.Context, r *run.Run) error {
	f.saveCalled = true
	if f.saveErr != nil {
		return f.saveErr
	}
	f.stored = r
	return nil
}

func (f *fakeTaskStatusRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not implemented in fake")
}
func (f *fakeTaskStatusRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeTaskStatusRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeTaskStatusRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}
func (f *fakeTaskStatusRunRepo) RunExists(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("RunExists not implemented in fake")
}
func (f *fakeTaskStatusRunRepo) ListUnplannedActive(context.Context) ([]uuid.UUID, error) {
	panic("ListUnplannedActive is not used here")
}

func (f *fakeTaskStatusRunRepo) CountReleaseRuns(_ context.Context, _ string, _ run.Kind) (int, error) {
	panic("CountReleaseRuns not implemented in fake")
}

// fakeTaskStatusOutboxPub is an in-memory OutboxPublisher that captures
// appended domain events for assertion.
type fakeTaskStatusOutboxPub struct {
	appended  []run.DomainEvent
	msgProcID uuid.UUID
	err       error
}

func (f *fakeTaskStatusOutboxPub) Append(_ context.Context, evts []run.DomainEvent, msgProcID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.appended = append(f.appended, evts...)
	f.msgProcID = msgProcID
	return nil
}

// fakeTaskStatusTaskCollection is an in-memory TaskCollection for handler
// unit tests. It controls the responses of every method called by
// Run.RecordAttemptReport; unused methods panic on accidental use.
type fakeTaskStatusTaskCollection struct {
	// GetStatus / LoadAttemptState response; runID is the run that holds the
	// task, the run the unit of work loads unless a test sets another.
	runID        uuid.UUID
	prevStatus   run.TaskStatus
	prevAttempt  int32
	maxRetries   int32
	prevExists   bool
	getStatusErr error

	// SetStatusAndAttempt response (rowsAffected)
	updateAffected int
	updateErr      error

	// HasFailed response
	hasFailed    bool
	hasFailedErr error

	// GetByID response
	task   run.Task
	getErr error

	// attemptRows holds the stored attempts; lockOrder the LoadAttemptState
	// ("task <id>") and LoadAttempt ("attempt <id>/<n>") calls in call order.
	attemptRows map[statusAttemptKey]run.Attempt
	lockOrder   []string
}

// statusAttemptKey identifies one stored attempt.
type statusAttemptKey struct {
	task    uuid.UUID
	attempt int32
}

func (f *fakeTaskStatusTaskCollection) LoadAttempt(_ context.Context, id uuid.UUID, attempt int32) (run.Attempt, bool, error) {
	f.lockOrder = append(f.lockOrder, fmt.Sprintf("attempt %s/%d", id, attempt))
	a, ok := f.attemptRows[statusAttemptKey{id, attempt}]
	return a, ok, nil
}

func (f *fakeTaskStatusTaskCollection) SaveAttempt(_ context.Context, a run.Attempt) error {
	if f.attemptRows == nil {
		f.attemptRows = map[statusAttemptKey]run.Attempt{}
	}
	f.attemptRows[statusAttemptKey{a.TaskID, a.Number}] = a
	return nil
}
func (f *fakeTaskStatusTaskCollection) AddAttempts(ctx context.Context, attempts []run.Attempt) error {
	for _, a := range attempts {
		if err := f.SaveAttempt(ctx, a); err != nil {
			return err
		}
	}
	return nil
}
func (f *fakeTaskStatusTaskCollection) HasInFlight(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeTaskStatusTaskCollection) SkipPending(context.Context, uuid.UUID) (int, error) {
	return 0, nil
}

func (f *fakeTaskStatusTaskCollection) GetByID(_ context.Context, _ uuid.UUID) (run.Task, error) {
	if f.getErr != nil {
		return run.Task{}, f.getErr
	}
	return f.task, nil
}

func (f *fakeTaskStatusTaskCollection) GetStatus(_ context.Context, _ uuid.UUID) (run.TaskStatus, bool, error) {
	return f.prevStatus, f.prevExists, f.getStatusErr
}

func (f *fakeTaskStatusTaskCollection) LoadAttemptState(_ context.Context, id uuid.UUID) (run.TaskAttemptState, bool, error) {
	f.lockOrder = append(f.lockOrder, "task "+id.String())
	return run.TaskAttemptState{RunID: f.runID, Status: f.prevStatus, Attempt: f.prevAttempt, MaxRetries: f.maxRetries,
		NodeKey: "raw.orders", ServiceName: "svc", SchemaName: "raw", TableName: "orders"}, f.prevExists, f.getStatusErr
}

func (f *fakeTaskStatusTaskCollection) MarkDispatched(_ context.Context, _ []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	return nil, nil
}

func (f *fakeTaskStatusTaskCollection) ListRunTasks(_ context.Context, _ uuid.UUID) ([]run.Task, error) {
	panic("ListRunTasks not implemented in fake")
}

func (f *fakeTaskStatusTaskCollection) RunReleasesOf(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]string, error) {
	panic("RunReleasesOf not implemented in fake")
}

func (f *fakeTaskStatusTaskCollection) MarkSkipped(_ context.Context, _ []uuid.UUID) (int, error) {
	return 0, nil
}

func (f *fakeTaskStatusTaskCollection) SetStatusAndAttempt(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	return f.updateAffected, f.updateErr
}

func (f *fakeTaskStatusTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasFailed, f.hasFailedErr
}

func (f *fakeTaskStatusTaskCollection) BulkCreate(_ context.Context, _ []run.Task) error {
	panic("BulkCreate not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) GetByNode(_ context.Context, _ uuid.UUID, _ run.NodeID) (run.Task, error) {
	panic("GetByNode not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) BeginStop(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (run.StopCounts, error) {
	panic("BeginStop not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) RequestStops(_ context.Context, _ uuid.UUID) ([]run.StopTarget, error) {
	panic("RequestStops not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) Update(_ context.Context, _ run.Task) error {
	panic("Update not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasNonSucceeded not implemented in fake")
}

// ReleaseLease accepts the release a final report makes; no task in these
// tests holds a lease.
func (f *fakeTaskStatusTaskCollection) ReleaseLease(context.Context, uuid.UUID) error {
	return nil
}
func (f *fakeTaskStatusTaskCollection) TakeLeases(context.Context, []run.LeaseClaim) ([]uuid.UUID, error) {
	panic("TakeLeases not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) WaitForLeases(context.Context, []uuid.UUID) error {
	panic("WaitForLeases not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) TakeLeaseForWaiter(context.Context, run.LeaseClaim) (bool, error) {
	panic("TakeLeaseForWaiter not implemented in fake")
}

func taskStatusTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTaskStatusHandlerUoW wires a FakeUnitOfWork with the three fakes needed
// by AttemptReportHandler. A task collection that names no run holds its task
// in the run runRepo stores.
func newTaskStatusHandlerUoW(
	runRepo *fakeTaskStatusRunRepo,
	outbox *fakeTaskStatusOutboxPub,
	taskColl *fakeTaskStatusTaskCollection,
) *uow.FakeUnitOfWork {
	if taskColl.runID == uuid.Nil && runRepo.stored != nil {
		taskColl.runID = runRepo.stored.ScheduleID()
	}
	u := &uow.FakeUnitOfWork{Graph: unconnectedTasks{}}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outbox)
	u.SetTaskCollection(taskColl)
	return u
}

// unconnectedTasks is a run.TaskGraph in which no task reads another.
type unconnectedTasks struct{}

func (unconnectedTasks) ReadyCandidates(context.Context, uuid.UUID, string, topology.NodeKey) ([]run.ReadyCandidate, error) {
	return nil, nil
}

func (unconnectedTasks) PendingDescendants(context.Context, uuid.UUID, string, topology.NodeKey) ([]uuid.UUID, error) {
	return nil, nil
}

func (unconnectedTasks) Node(context.Context, string, topology.NodeKey) (topology.Node, bool, error) {
	return topology.Node{}, false, nil
}

// newRunningTaskStatusRun builds a RUNNING Run with non-zero counters for
// handler tests. Bypasses constructors via HydrateRun so the event buffer is
// clean.
func newRunningTaskStatusRun(id uuid.UUID, name string, total, terminal int32) *run.Run {
	return newRunningTaskStatusRunOfKind(run.KindCron, id, name, total, terminal)
}

// newRunningTaskStatusRunOfKind is newRunningTaskStatusRun for a run of kind.
func newRunningTaskStatusRunOfKind(kind run.Kind, id uuid.UUID, name string, total, terminal int32) *run.Run {
	return run.HydrateRun(
		id, name,
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		kind,
		nil,
		identity.SystemUserID,
		model.OperationRun,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		&total,
		terminal,
		map[string]run.ServiceMetadata{},
	)
}

// newTerminalTaskStatusRun builds a terminal Run for idempotency tests.
func newTerminalTaskStatusRun(id uuid.UUID, name string, status run.SchedulerStatus) *run.Run {
	return newTerminalTaskStatusRunOfKind(run.KindCron, id, name, status)
}

// newTerminalTaskStatusRunOfKind is newTerminalTaskStatusRun for a run of kind.
func newTerminalTaskStatusRunOfKind(kind run.Kind, id uuid.UUID, name string, status run.SchedulerStatus) *run.Run {
	now := time.Now()
	return run.HydrateRun(
		id, name,
		status,
		run.InitStatusCompleted,
		kind,
		nil,
		identity.SystemUserID,
		model.OperationRun,
		now,
		nil, &now, nil, nil,
		nil, nil,
		nil,
		0,
		map[string]run.ServiceMetadata{},
	)
}

// held stores attempt n of taskID in status, as the transaction that
// dispatched it wrote it.
func (f *fakeTaskStatusTaskCollection) held(taskID uuid.UUID, n int32, status run.AttemptStatus) *fakeTaskStatusTaskCollection {
	if f.attemptRows == nil {
		f.attemptRows = map[statusAttemptKey]run.Attempt{}
	}
	f.attemptRows[statusAttemptKey{taskID, n}] = run.Attempt{TaskID: taskID, Number: n, Status: status}
	return f
}

// report is a report of attempt n of taskID in runID.
func report(runID, taskID uuid.UUID, n int32, status run.AttemptStatus) events.AttemptReported {
	r := events.AttemptReported{RunID: runID, Report: run.AttemptReport{TaskID: taskID, Attempt: n, Status: status}}
	if status == run.AttemptFailed {
		r.Report.FailureReason = model.FailureReasonExitCode
	}
	return r
}

func newAttemptReportHandler() *handlers.AttemptReportHandler {
	return newAttemptReportHandlerWith(&fakeRunMetrics{})
}

func newAttemptReportHandlerWith(metrics *fakeRunMetrics) *handlers.AttemptReportHandler {
	return handlers.NewAttemptReportHandler(pkgevents.DefaultTenantID, run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}, metrics, taskStatusTestLogger())
}

// fakeRunMetrics records the cancel durations the handler observes.
type fakeRunMetrics struct{ cancelled []time.Duration }

func (*fakeRunMetrics) RunStalled()                    {}
func (*fakeRunMetrics) RunsCancelling(_, _ int)        {}
func (m *fakeRunMetrics) RunCancelled(d time.Duration) { m.cancelled = append(m.cancelled, d) }

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// cancellingRunReport is a cancelling run of one task, cancelled at
// cancelledAt, whose running attempt reports stopped, in a unit of work whose
// clock reads 90 s after the cancel.
func cancellingRunReport(cancelledAt time.Time) (*fakeTaskStatusRunRepo, *uow.FakeUnitOfWork, events.AttemptReported) {
	scheduleID, taskID := uuid.New(), uuid.New()
	total, by := int32(1), "alice"
	cancelling := run.HydrateRun(scheduleID, "daily", run.SchedulerStatusCancelling, run.InitStatusCompleted, run.KindCron, nil,
		identity.SystemUserID, model.OperationRun, cancelledAt.Add(-time.Hour), nil, nil, &cancelledAt, &cancelledAt, &by, nil,
		&total, 0, map[string]run.ServiceMetadata{})
	runRepo := &fakeTaskStatusRunRepo{stored: cancelling}
	taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusStopping, prevExists: true, updateAffected: 1}).
		held(taskID, 0, run.AttemptRunning)
	u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
	u.SetClock(fixedClock{cancelledAt.Add(90 * time.Second)})
	return runRepo, u, report(scheduleID, taskID, 0, run.AttemptStopped)
}

// The stopped report that ends a cancelling run's last task finalizes the run
// cancelled and observes the time from its cancel to now once its transaction
// commits; a run that ends any other way observes nothing.
func TestAttemptReportHandler_ObservesTheCancelDurationWhenARunEndsCancelled(t *testing.T) {
	cancelledAt := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	runRepo, u, rep := cancellingRunReport(cancelledAt)
	metrics := &fakeRunMetrics{}
	require.NoError(t, u.Begin(context.Background()))

	require.NoError(t, newAttemptReportHandlerWith(metrics).Handle(context.Background(), u, rep, uuid.New()))

	assert.Equal(t, run.SchedulerStatusCancelled, runRepo.stored.Status())
	assert.Empty(t, metrics.cancelled, "nothing is observed before the commit")
	require.NoError(t, u.Commit())
	assert.Equal(t, []time.Duration{90 * time.Second}, metrics.cancelled)

	t.Run("a report whose transaction rolls back observes nothing", func(t *testing.T) {
		_, u, rep := cancellingRunReport(cancelledAt)
		m := &fakeRunMetrics{}
		require.NoError(t, u.Begin(context.Background()))

		require.NoError(t, newAttemptReportHandlerWith(m).Handle(context.Background(), u, rep, uuid.New()))
		require.NoError(t, u.Rollback())

		assert.Empty(t, m.cancelled)
		require.NoError(t, u.Begin(context.Background()))
		require.NoError(t, u.Commit())
		assert.Empty(t, m.cancelled, "the next transaction commits nothing of the rolled-back one")
	})

	t.Run("a run that succeeds observes nothing", func(t *testing.T) {
		id, task := uuid.New(), uuid.New()
		repo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(id, "daily", 1, 0)}
		coll := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1}).held(task, 0, run.AttemptRunning)
		m := &fakeRunMetrics{}

		u := newTaskStatusHandlerUoW(repo, &fakeTaskStatusOutboxPub{}, coll)
		require.NoError(t, u.Begin(context.Background()))

		require.NoError(t, newAttemptReportHandlerWith(m).Handle(context.Background(), u, report(id, task, 0, run.AttemptSucceeded), uuid.New()))
		require.NoError(t, u.Commit())

		assert.Equal(t, run.SchedulerStatusSucceeded, repo.stored.Status())
		assert.Empty(t, m.cancelled)
	})
}

// A queued or running report moves its attempt to that status, saves the run
// and writes nothing to the outbox.
func TestAttemptReport_AReportThatStartsTheAttemptSendsNothing(t *testing.T) {
	for _, status := range []run.AttemptStatus{run.AttemptQueued, run.AttemptRunning} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID, taskID := uuid.New(), uuid.New()
			runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "daily", 2, 0)}
			outbox := &fakeTaskStatusOutboxPub{}
			taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusDispatched, prevExists: true, updateAffected: 1}).
				held(taskID, 0, run.AttemptDispatched)
			u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

			require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, status), uuid.New()))

			assert.True(t, runRepo.saveCalled, "SaveRun must be called")
			assert.Empty(t, outbox.appended)
			assert.Equal(t, status, taskColl.attemptRows[statusAttemptKey{taskID, 0}].Status)
		})
	}
}

// The success of the last open task finalizes the run: its RunFinalized event
// is appended to the outbox with the report's dedup row as its provenance.
func TestAttemptReport_TheLastSuccessFinalizesTheRun(t *testing.T) {
	scheduleID, taskID, msgProcID := uuid.New(), uuid.New(), uuid.New()
	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "daily", 2, 1)}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1}).
		held(taskID, 0, run.AttemptRunning)
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), msgProcID))

	require.Len(t, outbox.appended, 1, "finalization must emit exactly one RunFinalized event")
	assert.Equal(t, msgProcID, outbox.msgProcID, "msgProcID must be forwarded to Append")
	finalized, ok := outbox.appended[0].(run.RunFinalized)
	require.True(t, ok, "event must be RunFinalized")
	assert.Equal(t, scheduleID, finalized.ID)
	assert.Equal(t, run.SchedulerStatusSucceeded, finalized.Outcome)
}

// nodeGraph is a run.TaskGraph holding one node and no edges.
type nodeGraph struct {
	unconnectedTasks
	node topology.Node
}

func (g nodeGraph) Node(context.Context, string, topology.NodeKey) (topology.Node, bool, error) {
	return g.node, true, nil
}

// A retryable failure below the task's budget writes the dispatch of the next
// attempt, with the install's timeout for a node that declares none.
func TestAttemptReport_ARetryableFailureDispatchesTheNextAttempt(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	runRepo := &fakeTaskStatusRunRepo{stored: newReleaseTaskStatusRun(scheduleID, "rel-1")}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, maxRetries: 2, prevExists: true, updateAffected: 1}).
		held(taskID, 0, run.AttemptRunning)
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)
	u.Graph = nodeGraph{node: topology.Node{Key: "raw.orders", ServiceName: "svc", SchemaName: "raw", TableName: "orders"}}

	require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptFailed), uuid.New()))

	require.Len(t, outbox.appended, 1)
	d, ok := outbox.appended[0].(run.TaskDispatched)
	require.True(t, ok, "the next attempt is dispatched")
	assert.Equal(t, int32(1), d.Attempt)
	assert.Equal(t, run.AttemptDispatched, d.KnownStatus)
	assert.Equal(t, 24*time.Hour, d.Timeout)
	assert.Equal(t, run.AttemptDispatched, taskColl.attemptRows[statusAttemptKey{taskID, 1}].Status)
}

// A report for a run that has ended changes nothing: every attempt of such a
// run has ended.
func TestAttemptReport_ARunThatEndedChangesNothing(t *testing.T) {
	for _, status := range []run.SchedulerStatus{run.SchedulerStatusSucceeded, run.SchedulerStatusFailed, run.SchedulerStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID, taskID := uuid.New(), uuid.New()
			runRepo := &fakeTaskStatusRunRepo{stored: newTerminalTaskStatusRun(scheduleID, "terminal-guard", status)}
			outbox := &fakeTaskStatusOutboxPub{}
			taskColl := &fakeTaskStatusTaskCollection{}
			u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

			require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptFailed), uuid.New()))

			assert.Empty(t, outbox.appended)
			assert.Empty(t, taskColl.lockOrder, "neither the task nor an attempt is read")
		})
	}
}

// A report for a task the run does not hold, or for an attempt state never
// dispatched, surfaces its error, which the binding marks permanent; the run
// is not saved.
func TestAttemptReport_AReportThatCanNeverApplyFailsTheMessage(t *testing.T) {
	cases := map[string]struct {
		taskColl *fakeTaskStatusTaskCollection
		want     error
	}{
		"a task the run does not hold":  {&fakeTaskStatusTaskCollection{prevExists: false}, run.ErrTaskNotHeld},
		"an attempt state never issued": {&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true}, run.ErrAttemptNotIssued},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheduleID := uuid.New()
			runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "no-row", 2, 0)}
			outbox := &fakeTaskStatusOutboxPub{}
			u := newTaskStatusHandlerUoW(runRepo, outbox, tc.taskColl)

			err := newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, uuid.New(), 0, run.AttemptSucceeded), uuid.New())

			require.ErrorIs(t, err, tc.want)
			assert.False(t, runRepo.saveCalled, "SaveRun must not be called when RecordAttemptReport errors")
			assert.Empty(t, outbox.appended)
		})
	}
}

// A LoadRunForUpdate failure short-circuits the handler before any mutation.
func TestAttemptReport_LoadRunErrorPropagates(t *testing.T) {
	runRepo := &fakeTaskStatusRunRepo{loadErr: errors.New("row lock failed")}
	outbox := &fakeTaskStatusOutboxPub{}
	u := newTaskStatusHandlerUoW(runRepo, outbox, &fakeTaskStatusTaskCollection{})

	err := newAttemptReportHandler().Handle(context.Background(), u, report(uuid.New(), uuid.New(), 0, run.AttemptSucceeded), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "load run")
	assert.False(t, runRepo.saveCalled)
	assert.Empty(t, outbox.appended)
}

// A SaveRun failure surfaces an error and prevents the outbox append.
func TestAttemptReport_SaveRunErrorPropagates(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "save-error", 1, 0), saveErr: errors.New("db down")}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1}).
		held(taskID, 0, run.AttemptRunning)
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	err := newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "save run")
	assert.Empty(t, outbox.appended, "save failure must short-circuit the outbox append")
}

// An outbox Append failure surfaces after SaveRun, so the binding rolls the
// whole transaction back.
func TestAttemptReport_AppendErrorPropagates(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "outbox-error", 1, 0)}
	outbox := &fakeTaskStatusOutboxPub{err: errors.New("outbox down")}
	taskColl := (&fakeTaskStatusTaskCollection{prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1}).
		held(taskID, 0, run.AttemptRunning)
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	err := newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append outbox")
	assert.True(t, runRepo.saveCalled, "SaveRun is called before the outbox append")
}

// hashTopology answers NodeContentHash from hashes, keyed
// "<release>/<node key>", and records each lookup as
// "<tenant>/<release>/<node key>".
type hashTopology struct {
	fakeTopologyRepo
	hashes map[string]string
	asked  []string
}

var _ repository.TopologyRepository = (*hashTopology)(nil)

func (f *hashTopology) NodeContentHash(_ context.Context, tenantID, releaseID string, key topology.NodeKey) (string, bool, error) {
	f.asked = append(f.asked, tenantID+"/"+releaseID+"/"+string(key))
	h, ok := f.hashes[releaseID+"/"+string(key)]
	return h, ok, nil
}

// builtRecorder captures the versions recorded as loaded and their tenants.
type builtRecorder struct {
	fakeMaterializationRepo
	tenants []string
	built   []seeds.Built
}

var _ repository.MaterializationRepository = (*builtRecorder)(nil)

func (f *builtRecorder) RecordBuilt(_ context.Context, tenantID string, b seeds.Built) error {
	f.tenants = append(f.tenants, tenantID)
	f.built = append(f.built, b)
	return nil
}

type taskStatusClock struct{ at time.Time }

func (c taskStatusClock) Now() time.Time { return c.at }

// plannedRun builds a RUNNING run of total tasks, none terminal, that state
// planned on releaseID with operation op.
func plannedRun(id uuid.UUID, releaseID string, op model.Operation, total int32) *run.Run {
	r := run.HydrateRun(
		id, "daily",
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		identity.SystemUserID,
		op,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		&total,
		0,
		map[string]run.ServiceMetadata{},
	)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: releaseID, PromotionSeq: 1})
	planned := time.Now()
	r.HydratePlannedAt(&planned)
	return r
}

// newReleaseTaskStatusRun builds a RUNNING one-task run state planned on
// releaseID.
func newReleaseTaskStatusRun(id uuid.UUID, releaseID string) *run.Run {
	return plannedRun(id, releaseID, model.OperationRun, 1)
}

// A succeeded task of a run state planned records the version it loaded: the
// node's content hash in the run's release and the task's image.
func TestAttemptReport_RecordsTheVersionAPlannedRunLoaded(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	runRepo := &fakeTaskStatusRunRepo{stored: newReleaseTaskStatusRun(scheduleID, "rel-1")}
	taskColl := (&fakeTaskStatusTaskCollection{
		prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1,
		task: run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "RAW", TableName: "Orders", ImageTag: "svc:7"},
	}).held(taskID, 0, run.AttemptRunning)
	topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
	mats := &builtRecorder{}
	u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
	u.TopologyRepo, u.MaterializationRepo = topo, mats
	u.SetClock(taskStatusClock{at: at})

	require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), uuid.New()))

	assert.Equal(t, []string{"default/rel-1/raw.orders"}, topo.asked)
	assert.Equal(t, []string{"default"}, mats.tenants)
	assert.Equal(t, []seeds.Built{{
		Key: "raw.orders", ContentHash: "h-orders", ImageTag: "svc:7", ReleaseID: "rel-1",
		RunID: scheduleID, TaskID: taskID, At: at, Source: seeds.SourceBuild,
	}}, mats.built)
}

// A success records a loaded version for a run state planned whose operation
// writes the table: run, build and full refresh. A test only reads the table,
// and a run whose tasks state did not plan may have built another release's
// version, so neither records.
func TestAttemptReport_RecordsBuiltVersionsForPlannedWritingRuns(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}
	unplanned := newRunningTaskStatusRun(scheduleID, "daily", 1, 0)
	unplanned.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 3})

	cases := []struct {
		name    string
		run     *run.Run
		records bool
	}{
		{"a planned run", plannedRun(scheduleID, "rel-1", model.OperationRun, 1), true},
		{"a planned build", plannedRun(scheduleID, "rel-1", model.OperationBuild, 1), true},
		{"a planned full refresh", plannedRun(scheduleID, "rel-1", model.OperationFullRefresh, 1), true},
		{"a planned test", plannedRun(scheduleID, "rel-1", model.OperationTest, 1), false},
		{"a run state did not plan", unplanned, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskColl := (&fakeTaskStatusTaskCollection{
				prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, task: orders,
			}).held(taskID, 0, run.AttemptRunning)
			topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(&fakeTaskStatusRunRepo{stored: tc.run}, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), uuid.New()))

			if tc.records {
				assert.Len(t, mats.built, 1)
			} else {
				assert.Empty(t, topo.asked)
				assert.Empty(t, mats.built)
			}
		})
	}
}

// Nothing is recorded for a run that names no release, for a task that did
// not succeed, for a task whose row is gone, or for a node the run's release
// lacks or holds without a content hash.
func TestAttemptReport_RecordsNothingWithoutAVersion(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}
	cases := []struct {
		name    string
		run     *run.Run
		status  run.AttemptStatus
		getErr  error
		hashes  map[string]string
		lookups int
	}{
		{"a run without a release", newRunningTaskStatusRun(scheduleID, "daily", 1, 0), run.AttemptSucceeded, nil,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a failed task", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.AttemptFailed, nil,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a task row that is gone", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.AttemptSucceeded, run.ErrTaskNotFound,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a node the release lacks", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.AttemptSucceeded, nil,
			map[string]string{}, 1},
		{"a node without a content hash", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.AttemptSucceeded, nil,
			map[string]string{"rel-1/raw.orders": ""}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runRepo := &fakeTaskStatusRunRepo{stored: tc.run}
			taskColl := (&fakeTaskStatusTaskCollection{
				prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, hasFailed: tc.status == run.AttemptFailed,
				task: orders, getErr: tc.getErr,
			}).held(taskID, 0, run.AttemptRunning)
			topo := &hashTopology{hashes: tc.hashes}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, tc.status), uuid.New()))

			assert.Len(t, topo.asked, tc.lookups)
			assert.Empty(t, mats.built)
		})
	}
}

// A version is recorded only when this report made the task succeeded. A
// success the run does not apply — the run is already over, the report is
// for an older attempt, or the attempt already succeeded — loads nothing and
// records nothing, while an applied success records even when other tasks keep
// the run open.
func TestAttemptReport_RecordsAVersionOnlyForAnAppliedSuccess(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}
	releaseRun := func(total, terminal int32) func() *run.Run {
		return func() *run.Run {
			r := newRunningTaskStatusRunOfKind(run.KindPromoteSeed, scheduleID, "promote-seed-abc12345", total, terminal)
			r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1"})
			planned := time.Now()
			r.HydratePlannedAt(&planned)
			return r
		}
	}
	cancelledRun := func() *run.Run {
		r := newTerminalTaskStatusRunOfKind(run.KindPromoteSeed, scheduleID, "promote-seed-abc12345", run.SchedulerStatusCancelled)
		r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1"})
		return r
	}
	cases := []struct {
		name        string
		run         func() *run.Run
		prevAttempt int32
		attempt     int32
		held        run.AttemptStatus
		recorded    bool
	}{
		{"a run that is already over", cancelledRun, 0, 0, run.AttemptRunning, false},
		{"an older attempt", releaseRun(2, 0), 2, 1, run.AttemptFailed, false},
		{"a replay of a finished attempt", releaseRun(2, 1), 0, 0, run.AttemptSucceeded, false},
		{"a success that leaves the run open", releaseRun(2, 0), 0, 0, run.AttemptRunning, true},
		{"a success that finishes the run", releaseRun(1, 0), 0, 0, run.AttemptRunning, true},
		{"a retry that succeeds after a failed attempt", releaseRun(2, 0), 1, 1, run.AttemptRunning, true},
		{"a success with no running report before it", releaseRun(2, 0), 0, 0, run.AttemptDispatched, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskColl := (&fakeTaskStatusTaskCollection{
				prevStatus: run.TaskStatusRunning, prevAttempt: tc.prevAttempt, prevExists: true,
				updateAffected: 1, task: orders,
			}).held(taskID, tc.attempt, tc.held)
			topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(&fakeTaskStatusRunRepo{stored: tc.run()}, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, tc.attempt, run.AttemptSucceeded), uuid.New()))

			if tc.recorded {
				require.Len(t, mats.built, 1)
				assert.Equal(t, topology.NodeKey("raw.orders"), mats.built[0].Key)
				return
			}
			assert.Empty(t, mats.built, "a success that was not applied records no loaded version")
			assert.Empty(t, topo.asked, "and does not even look the node up")
		})
	}
}

// A failure to read the task fails the message, so it is redelivered.
func TestAttemptReport_TaskReadFailureFailsTheMessage(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	boom := errors.New("connection reset by peer")
	u := newTaskStatusHandlerUoW(&fakeTaskStatusRunRepo{stored: newReleaseTaskStatusRun(scheduleID, "rel-1")},
		&fakeTaskStatusOutboxPub{}, (&fakeTaskStatusTaskCollection{
			prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, getErr: boom,
		}).held(taskID, 0, run.AttemptRunning))
	u.TopologyRepo, u.MaterializationRepo = &hashTopology{}, &builtRecorder{}

	err := newAttemptReportHandler().Handle(context.Background(), u, report(scheduleID, taskID, 0, run.AttemptSucceeded), uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}
