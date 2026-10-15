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
// TaskStatusUpdated handler tests. LoadRunForUpdate returns the stored run;
// SaveRun records the call and stores the updated aggregate. Other methods
// panic on accidental use.
type fakeTaskStatusRunRepo struct {
	stored     *run.Run
	loadErr    error
	saveErr    error
	saveCalled bool
	// byID is what GetRun returns, for tests that walk a rerun's source runs;
	// getCalls counts those reads.
	byID     map[uuid.UUID]*run.Run
	getCalls int
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

func (f *fakeTaskStatusRunRepo) GetRun(_ context.Context, id uuid.UUID) (*run.Run, error) {
	if f.byID == nil {
		panic("GetRun not implemented in fake")
	}
	f.getCalls++
	r, ok := f.byID[id]
	if !ok {
		return nil, fmt.Errorf("run %s not in fake", id)
	}
	return r, nil
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
// Run.RecordTaskStatus; unused methods panic on accidental use.
type fakeTaskStatusTaskCollection struct {
	// GetStatus / LoadAttemptState response
	prevStatus   run.TaskStatus
	prevAttempt  int32
	prevExists   bool
	getStatusErr error

	// Exists response
	existsValue bool
	existsErr   error

	// SetStatusAndAttempt response (rowsAffected)
	updateAffected int
	updateErr      error

	// HasRetryableFailed response
	hasRetryable    bool
	hasRetryableErr error

	// HasFailed response
	hasFailed    bool
	hasFailedErr error

	// GetByID response
	task   run.Task
	getErr error
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

func (f *fakeTaskStatusTaskCollection) LoadAttemptState(_ context.Context, _ uuid.UUID) (run.TaskAttemptState, bool, error) {
	return run.TaskAttemptState{Status: f.prevStatus, RetryCount: f.prevAttempt}, f.prevExists, f.getStatusErr
}

func (f *fakeTaskStatusTaskCollection) MarkDispatched(_ context.Context, _ []uuid.UUID, _ time.Time) ([]uuid.UUID, error) {
	panic("MarkDispatched not implemented in fake")
}

func (f *fakeTaskStatusTaskCollection) MarkSkipped(_ context.Context, _ []uuid.UUID) (int, error) {
	panic("MarkSkipped not implemented in fake")
}

func (f *fakeTaskStatusTaskCollection) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.existsValue, f.existsErr
}

func (f *fakeTaskStatusTaskCollection) SetStatusAndAttempt(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	return f.updateAffected, f.updateErr
}

func (f *fakeTaskStatusTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return f.hasRetryable, f.hasRetryableErr
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
func (f *fakeTaskStatusTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (int, error) {
	panic("BulkCancel not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) Update(_ context.Context, _ run.Task) error {
	panic("Update not implemented in fake")
}
func (f *fakeTaskStatusTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasNonSucceeded not implemented in fake")
}

func taskStatusTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTaskStatusHandlerUoW wires a FakeUnitOfWork with the three fakes needed
// by the TaskStatusUpdated handler.
func newTaskStatusHandlerUoW(
	runRepo *fakeTaskStatusRunRepo,
	outbox *fakeTaskStatusOutboxPub,
	taskColl *fakeTaskStatusTaskCollection,
) *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outbox)
	u.SetTaskCollection(taskColl)
	return u
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

// TestTaskStatusUpdated_NonTerminalStatusNoEvents verifies the happy path
// where a RUNNING status update produces no domain events: RecordTaskStatus
// returns nil events, SaveRun is called, Outbox.Append receives nil/empty.
func TestTaskStatusUpdated_NonTerminalStatusNoEvents(t *testing.T) {
	scheduleID := uuid.New()
	taskID := uuid.New()

	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "daily", 2, 0)}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus:     run.TaskStatusPending,
		prevExists:     true,
		updateAffected: 1,
	}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     taskID,
		ScheduleID: scheduleID,
		Status:     run.TaskStatusRunning,
		RetryCount: 1,
	}, uuid.New()))

	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	assert.Empty(t, outbox.appended, "non-terminal update must not emit events")
}

// TestTaskStatusUpdated_TerminalStatusFinalizesRun verifies that when
// RecordTaskStatus reaches the finalization condition it emits a RunFinalized
// event, SaveRun is called, and Outbox.Append receives the event.
func TestTaskStatusUpdated_TerminalStatusFinalizesRun(t *testing.T) {
	scheduleID := uuid.New()
	taskID := uuid.New()
	msgProcID := uuid.New()

	// Two tasks total, one already terminal: this event makes terminal==total.
	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "daily", 2, 1)}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus:     run.TaskStatusRunning,
		prevExists:     true,
		updateAffected: 1,
		hasRetryable:   false,
		hasFailed:      false,
	}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     taskID,
		ScheduleID: scheduleID,
		Status:     run.TaskStatusSucceeded,
		RetryCount: 0,
	}, msgProcID))

	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	require.Len(t, outbox.appended, 1, "finalization must emit exactly one RunFinalized event")
	assert.Equal(t, msgProcID, outbox.msgProcID, "msgProcID must be forwarded to Append")

	finalized, ok := outbox.appended[0].(run.RunFinalized)
	require.True(t, ok, "event must be RunFinalized")
	assert.Equal(t, scheduleID, finalized.ID)
	assert.Equal(t, "daily", finalized.Name)
	assert.Equal(t, run.SchedulerStatusSucceeded, finalized.Outcome)
}

// TestTaskStatusUpdated_AlreadyTerminalRunIsNoop verifies that when the Run
// is already in a terminal status, RecordTaskStatus returns nil events and
// SaveRun is still called (aggregate handles the guard internally).
func TestTaskStatusUpdated_AlreadyTerminalRunIsNoop(t *testing.T) {
	for _, status := range []run.SchedulerStatus{
		run.SchedulerStatusSucceeded,
		run.SchedulerStatusFailed,
		run.SchedulerStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID := uuid.New()

			runRepo := &fakeTaskStatusRunRepo{stored: newTerminalTaskStatusRun(scheduleID, "terminal-guard", status)}
			outbox := &fakeTaskStatusOutboxPub{}
			taskColl := &fakeTaskStatusTaskCollection{}
			u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

			h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
			require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
				TaskID:     uuid.New(),
				ScheduleID: scheduleID,
				Status:     run.TaskStatusFailed,
				RetryCount: 0,
			}, uuid.New()))

			assert.True(t, runRepo.saveCalled, "SaveRun is called even for already-terminal runs")
			assert.Empty(t, outbox.appended, "terminal run must not emit domain events")
		})
	}
}

// TestTaskStatusUpdated_ErrTaskRowNotProjectedPropagates verifies that when
// the task row does not yet exist, RecordTaskStatus surfaces
// run.ErrTaskRowNotProjected as a transient error. The handler must propagate
// it (so the binding rolls back and the consumer redelivers), and SaveRun
// must not be called.
func TestTaskStatusUpdated_ErrTaskRowNotProjectedPropagates(t *testing.T) {
	scheduleID := uuid.New()

	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "no-row", 2, 0)}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := &fakeTaskStatusTaskCollection{
		// GetStatus reports row not present.
		prevExists: false,
		// Exists confirms the row is not yet projected.
		existsValue: false,
	}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     run.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, run.ErrTaskRowNotProjected,
		"transient missing-row error must propagate so the consumer redelivers")
	assert.False(t, runRepo.saveCalled, "SaveRun must not be called when RecordTaskStatus errors")
	assert.Empty(t, outbox.appended)
}

// TestTaskStatusUpdated_LoadRunErrorPropagates verifies that a
// LoadRunForUpdate failure short-circuits the handler before any mutation.
func TestTaskStatusUpdated_LoadRunErrorPropagates(t *testing.T) {
	runRepo := &fakeTaskStatusRunRepo{loadErr: errors.New("row lock failed")}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := &fakeTaskStatusTaskCollection{}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		Status:     run.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "load run")
	assert.False(t, runRepo.saveCalled, "SaveRun must not be called after load failure")
	assert.Empty(t, outbox.appended)
}

// TestTaskStatusUpdated_SaveRunErrorPropagates verifies that a SaveRun
// failure surfaces an error and prevents the outbox append.
func TestTaskStatusUpdated_SaveRunErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeTaskStatusRunRepo{
		stored:  newRunningTaskStatusRun(scheduleID, "save-error", 2, 0),
		saveErr: errors.New("db down"),
	}
	outbox := &fakeTaskStatusOutboxPub{}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus:     run.TaskStatusPending,
		prevExists:     true,
		updateAffected: 1,
	}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     run.TaskStatusRunning,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "save run")
	assert.Empty(t, outbox.appended, "save failure must short-circuit the outbox append")
}

// TestTaskStatusUpdated_AppendErrorPropagates verifies that an outbox Append
// failure on the finalize branch surfaces an error after SaveRun, so the
// binding rolls back the entire transaction.
func TestTaskStatusUpdated_AppendErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	// terminal==total: triggers finalization so Append receives actual events.
	runRepo := &fakeTaskStatusRunRepo{stored: newRunningTaskStatusRun(scheduleID, "outbox-error", 1, 0)}
	outbox := &fakeTaskStatusOutboxPub{err: errors.New("outbox down")}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus:     run.TaskStatusRunning,
		prevExists:     true,
		updateAffected: 1,
		hasRetryable:   false,
		hasFailed:      false,
	}
	u := newTaskStatusHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     run.TaskStatusSucceeded,
	}, uuid.New())

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

// newReleaseTaskStatusRun builds a RUNNING one-task promote-seed run that names
// releaseID.
func newReleaseTaskStatusRun(id uuid.UUID, releaseID string) *run.Run {
	r := newRunningTaskStatusRunOfKind(run.KindPromoteSeed, id, "promote-seed-abc12345", 1, 0)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: releaseID})
	return r
}

// A succeeded task of a run that names a release records the version it
// loaded: the node's content hash in that release and the task's image.
func TestTaskStatusUpdated_RecordsTheVersionAReleaseRunLoaded(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	runRepo := &fakeTaskStatusRunRepo{stored: newReleaseTaskStatusRun(scheduleID, "rel-1")}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1,
		task: run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "RAW", TableName: "Orders", ImageTag: "svc:7"},
	}
	topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
	mats := &builtRecorder{}
	u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
	u.TopologyRepo, u.MaterializationRepo = topo, mats
	u.SetClock(taskStatusClock{at: at})

	require.NoError(t, handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
		events.TaskStatusUpdated{TaskID: taskID, ScheduleID: scheduleID, Status: run.TaskStatusSucceeded}, uuid.New()))

	assert.Equal(t, []string{"default/rel-1/raw.orders"}, topo.asked)
	assert.Equal(t, []string{"default"}, mats.tenants)
	assert.Equal(t, []seeds.Built{{
		Key: "raw.orders", ContentHash: "h-orders", ImageTag: "svc:7", ReleaseID: "rel-1",
		RunID: scheduleID, TaskID: taskID, At: at, Source: seeds.SourceBuild,
	}}, mats.built)
}

// A cron run names a release but its tasks were chosen elsewhere: its
// succeeded task records nothing.
func TestTaskStatusUpdated_CronRunRecordsNoBuiltVersion(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	r := newRunningTaskStatusRun(scheduleID, "daily", 1, 0)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 3})
	runRepo := &fakeTaskStatusRunRepo{stored: r}
	taskColl := &fakeTaskStatusTaskCollection{
		prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1,
		task: run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders"},
	}
	topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
	mats := &builtRecorder{}
	u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
	u.TopologyRepo, u.MaterializationRepo = topo, mats

	require.NoError(t, handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
		events.TaskStatusUpdated{TaskID: taskID, ScheduleID: scheduleID, Status: run.TaskStatusSucceeded}, uuid.New()))

	assert.Empty(t, topo.asked)
	assert.Empty(t, mats.built)
}

// hydratedRunOfKind builds a RUNNING one-task run of kind that names rel-1 and
// descends from source (nil for none), for the source-chain cases.
func hydratedRunOfKind(kind run.Kind, id uuid.UUID, source *uuid.UUID) *run.Run {
	total := int32(1)
	r := run.HydrateRun(
		id, "chain",
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		kind,
		source,
		identity.SystemUserID,
		model.OperationRun,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		&total,
		0,
		map[string]run.ServiceMetadata{},
	)
	r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1})
	return r
}

// A rerun records the version its task loaded only when its chain of source
// runs ends at a promote-seed run: any other root, a missing source, or a chain
// that never ends leaves the tasks to orchestrator's choice and records nothing.
// A source chain that loops is cut off at a bounded depth rather than walked
// forever.
func TestTaskStatusUpdated_RerunRecordsBuiltVersionOnlyWhenItsChainStartsAtAPromoteSeed(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	idA, idB := uuid.New(), uuid.New()
	ptr := func(id uuid.UUID) *uuid.UUID { return &id }
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}

	cases := []struct {
		name    string
		current *run.Run
		sources []*run.Run
		records bool
	}{
		{"a rerun of a cron run",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(idA)),
			[]*run.Run{hydratedRunOfKind(run.KindCron, idA, nil)}, false},
		{"a rerun of a trigger run",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(idA)),
			[]*run.Run{hydratedRunOfKind(run.KindTrigger, idA, nil)}, false},
		{"a rerun without a source run",
			hydratedRunOfKind(run.KindRerun, scheduleID, nil),
			nil, false},
		{"a rerun of a promote-seed run",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(idA)),
			[]*run.Run{hydratedRunOfKind(run.KindPromoteSeed, idA, nil)}, true},
		{"a rerun of a rerun of a promote-seed run",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(idA)),
			[]*run.Run{
				hydratedRunOfKind(run.KindRerun, idA, ptr(idB)),
				hydratedRunOfKind(run.KindPromoteSeed, idB, nil),
			}, true},
		{"a rerun whose source is itself",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(scheduleID)),
			[]*run.Run{hydratedRunOfKind(run.KindRerun, scheduleID, ptr(scheduleID))}, false},
		{"a rerun in a two-run source cycle",
			hydratedRunOfKind(run.KindRerun, scheduleID, ptr(idA)),
			[]*run.Run{
				hydratedRunOfKind(run.KindRerun, idA, ptr(idB)),
				hydratedRunOfKind(run.KindRerun, idB, ptr(idA)),
			}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runRepo := &fakeTaskStatusRunRepo{stored: tc.current, byID: map[uuid.UUID]*run.Run{}}
			for _, src := range tc.sources {
				runRepo.byID[src.ScheduleID()] = src
			}
			taskColl := &fakeTaskStatusTaskCollection{
				prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, task: orders,
			}
			topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
				events.TaskStatusUpdated{TaskID: taskID, ScheduleID: scheduleID, Status: run.TaskStatusSucceeded}, uuid.New()))

			if tc.records {
				assert.Len(t, mats.built, 1)
			} else {
				assert.Empty(t, topo.asked)
				assert.Empty(t, mats.built)
			}
			// A looping chain ends at the depth bound, not after unbounded reads.
			assert.Less(t, runRepo.getCalls, 100)
		})
	}
}

// Nothing is recorded for a run that names no release, for a task that did
// not succeed, for a task whose row is gone, or for a node the run's release
// lacks or holds without a content hash.
func TestTaskStatusUpdated_RecordsNothingWithoutAVersion(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}
	cases := []struct {
		name    string
		run     *run.Run
		status  run.TaskStatus
		getErr  error
		hashes  map[string]string
		lookups int
	}{
		{"a run without a release", newRunningTaskStatusRun(scheduleID, "daily", 1, 0), run.TaskStatusSucceeded, nil,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a failed task", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.TaskStatusFailed, nil,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a task row that is gone", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.TaskStatusSucceeded, run.ErrTaskNotFound,
			map[string]string{"rel-1/raw.orders": "h-orders"}, 0},
		{"a node the release lacks", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.TaskStatusSucceeded, nil,
			map[string]string{}, 1},
		{"a node without a content hash", newReleaseTaskStatusRun(scheduleID, "rel-1"), run.TaskStatusSucceeded, nil,
			map[string]string{"rel-1/raw.orders": ""}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runRepo := &fakeTaskStatusRunRepo{stored: tc.run}
			taskColl := &fakeTaskStatusTaskCollection{
				prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, hasFailed: tc.status == run.TaskStatusFailed,
				task: orders, getErr: tc.getErr,
			}
			topo := &hashTopology{hashes: tc.hashes}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(runRepo, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
				events.TaskStatusUpdated{TaskID: taskID, ScheduleID: scheduleID, Status: tc.status}, uuid.New()))

			assert.Len(t, topo.asked, tc.lookups)
			assert.Empty(t, mats.built)
		})
	}
}

// A version is recorded only when this delivery applied the task's success. A
// success the run ignores — the run is already over, the attempt was
// superseded, the task already finished that attempt, or the task row refused
// the write — loads nothing and records nothing, while a success the run
// accepts records even when other tasks keep the run open.
func TestTaskStatusUpdated_RecordsAVersionOnlyForAnAppliedSuccess(t *testing.T) {
	scheduleID, taskID := uuid.New(), uuid.New()
	orders := run.Task{TaskID: taskID, ScheduleID: scheduleID, ServiceName: "svc", SchemaName: "raw", TableName: "orders", ImageTag: "svc:7"}
	releaseRun := func(total, terminal int32) func() *run.Run {
		return func() *run.Run {
			r := newRunningTaskStatusRunOfKind(run.KindPromoteSeed, scheduleID, "promote-seed-abc12345", total, terminal)
			r.HydrateRelease(run.ReleaseStamp{ReleaseID: "rel-1"})
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
		prev        run.TaskStatus
		prevAttempt int32
		attempt     int32
		affected    int
		recorded    bool
	}{
		{"a run that is already over", cancelledRun, run.TaskStatusRunning, 0, 0, 1, false},
		{"a superseded attempt", releaseRun(2, 0), run.TaskStatusRunning, 2, 1, 1, false},
		{"a replay of a finished attempt", releaseRun(2, 1), run.TaskStatusSucceeded, 0, 0, 1, false},
		{"a task row that refuses the write", releaseRun(2, 0), run.TaskStatusRunning, 0, 0, 0, false},
		{"a success that leaves the run open", releaseRun(2, 0), run.TaskStatusRunning, 0, 0, 1, true},
		{"a success that finishes the run", releaseRun(1, 0), run.TaskStatusRunning, 0, 0, 1, true},
		{"a retry that succeeds after a failed attempt", releaseRun(2, 1), run.TaskStatusFailed, 0, 1, 1, true},
		{"a first success the task row had no status for", releaseRun(2, 0), "", 0, 0, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskColl := &fakeTaskStatusTaskCollection{
				prevStatus: tc.prev, prevAttempt: tc.prevAttempt, prevExists: tc.prev != "", existsValue: true,
				updateAffected: tc.affected, task: orders,
			}
			topo := &hashTopology{hashes: map[string]string{"rel-1/raw.orders": "h-orders"}}
			mats := &builtRecorder{}
			u := newTaskStatusHandlerUoW(&fakeTaskStatusRunRepo{stored: tc.run()}, &fakeTaskStatusOutboxPub{}, taskColl)
			u.TopologyRepo, u.MaterializationRepo = topo, mats

			require.NoError(t, handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
				events.TaskStatusUpdated{TaskID: taskID, ScheduleID: scheduleID, Status: run.TaskStatusSucceeded, RetryCount: tc.attempt}, uuid.New()))

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
func TestTaskStatusUpdated_TaskReadFailureFailsTheMessage(t *testing.T) {
	scheduleID := uuid.New()
	boom := errors.New("connection reset by peer")
	u := newTaskStatusHandlerUoW(&fakeTaskStatusRunRepo{stored: newReleaseTaskStatusRun(scheduleID, "rel-1")},
		&fakeTaskStatusOutboxPub{}, &fakeTaskStatusTaskCollection{
			prevStatus: run.TaskStatusRunning, prevExists: true, updateAffected: 1, getErr: boom,
		})
	u.TopologyRepo, u.MaterializationRepo = &hashTopology{}, &builtRecorder{}

	err := handlers.NewTaskStatusUpdatedHandler(pkgevents.DefaultTenantID, taskStatusTestLogger()).Handle(context.Background(), u,
		events.TaskStatusUpdated{TaskID: uuid.New(), ScheduleID: scheduleID, Status: run.TaskStatusSucceeded}, uuid.New())
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}
