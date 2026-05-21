package handlers_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/ports"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDispatchedRunRepo is an in-memory RunRepository for
// RunEntriesDispatched handler tests. LoadRunForUpdate returns the stored
// run (nil tx is ignored — handler unit tests never hold a real transaction).
// SaveRun records the call and stores the updated aggregate. Other methods
// panic on accidental use.
type fakeDispatchedRunRepo struct {
	stored     *run.Run
	loadErr    error
	saveErr    error
	saveCalled bool
}

func (f *fakeDispatchedRunRepo) LoadRunForUpdate(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}

func (f *fakeDispatchedRunRepo) SaveRun(_ context.Context, _ *sqlx.Tx, r *run.Run) error {
	f.saveCalled = true
	if f.saveErr != nil {
		return f.saveErr
	}
	f.stored = r
	return nil
}

func (f *fakeDispatchedRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not implemented in fake")
}
func (f *fakeDispatchedRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeDispatchedRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeDispatchedRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]ports.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}

// fakeDispatchedOutboxPub is an in-memory OutboxPublisher that captures
// the appended domain events for assertion.
type fakeDispatchedOutboxPub struct {
	appended  []run.DomainEvent
	msgProcID uuid.UUID
	err       error
}

func (f *fakeDispatchedOutboxPub) Append(_ context.Context, _ *sqlx.Tx, evts []run.DomainEvent, msgProcID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.appended = append(f.appended, evts...)
	f.msgProcID = msgProcID
	return nil
}

// fakeDispatchedTaskCollection is an in-memory TaskCollection for handler
// unit tests. BulkCreate records the tasks; other methods either return safe
// no-op values or panic on accidental use.
type fakeDispatchedTaskCollection struct {
	bulkCreated []run.Task
	bulkErr     error
}

func (f *fakeDispatchedTaskCollection) BulkCreate(_ context.Context, tasks []run.Task) error {
	if f.bulkErr != nil {
		return f.bulkErr
	}
	f.bulkCreated = append(f.bulkCreated, tasks...)
	return nil
}
func (f *fakeDispatchedTaskCollection) GetStatus(_ context.Context, _ uuid.UUID) (run.TaskStatus, bool, error) {
	return "", false, nil
}
func (f *fakeDispatchedTaskCollection) LoadStatusAndAttempt(_ context.Context, _ uuid.UUID) (run.TaskStatus, int32, bool, error) {
	return "", 0, false, nil
}
func (f *fakeDispatchedTaskCollection) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeDispatchedTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeDispatchedTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeDispatchedTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeDispatchedTaskCollection) GetByNode(_ context.Context, _ uuid.UUID, _ run.NodeID) (run.Task, error) {
	panic("GetByNode not implemented in fake")
}
func (f *fakeDispatchedTaskCollection) SetStatusAndAttempt(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	panic("SetStatusAndAttempt not implemented in fake")
}
func (f *fakeDispatchedTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string) (int, error) {
	panic("BulkCancel not implemented in fake")
}
func (f *fakeDispatchedTaskCollection) Update(_ context.Context, _ run.Task) error {
	panic("Update not implemented in fake")
}

// newPendingDispatchedRun builds a PENDING Run for handler tests via HydrateRun,
// bypassing the New* constructors so the aggregate's event buffer is empty.
func newPendingDispatchedRun(id uuid.UUID, name string) *run.Run {
	return run.HydrateRun(
		id, name,
		run.SchedulerStatusPending,
		run.InitStatusInProgress,
		run.KindCron,
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{},
		0,
		map[string]run.ServiceMetadata{},
	)
}

// newTerminalDispatchedRun builds a Run in a terminal status for idempotency
// tests.
func newTerminalDispatchedRun(id uuid.UUID, name string, status run.SchedulerStatus) *run.Run {
	now := time.Now()
	return run.HydrateRun(
		id, name,
		status,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		now,
		nil, &now, nil, nil,
		nil, nil,
		sql.NullInt32{},
		0,
		map[string]run.ServiceMetadata{},
	)
}

// newDispatchedHandlerUoW wires a FakeUnitOfWork with the three fakes needed
// by the RunEntriesDispatched handler.
func newDispatchedHandlerUoW(
	runRepo *fakeDispatchedRunRepo,
	outbox *fakeDispatchedOutboxPub,
	taskColl *fakeDispatchedTaskCollection,
) *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outbox)
	u.SetTaskCollection(taskColl)
	return u
}

func dispatchedTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestRunEntriesDispatched_PendingTasksTransitionToRunning is the canonical
// happy path: a PENDING run receives an all-pending projection, tasks are
// bulk-created, and the run transitions to RUNNING. No domain events are
// emitted; the outbox append receives an empty slice.
func TestRunEntriesDispatched_PendingTasksTransitionToRunning(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{stored: newPendingDispatchedRun(scheduleID, "daily")}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	t1, t2 := uuid.New(), uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: t1, ServiceName: "svc-a", SchemaName: "public", TableName: "orders", Status: run.TaskStatusPending, MaxRetries: 3},
			{TaskID: t2, ServiceName: "svc-b", SchemaName: "raw", TableName: "events", Status: run.TaskStatusPending, MaxRetries: 2},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	require.Len(t, taskColl.bulkCreated, 2, "BulkCreate must receive both tasks")
	for _, task := range taskColl.bulkCreated {
		assert.Equal(t, scheduleID, task.ScheduleID)
		assert.NotEmpty(t, task.JobName, "job_name must be computed")
	}
	// No events on RUNNING transition — outbox receives nil/empty slice.
	assert.Empty(t, outbox.appended, "no domain events on RUNNING transition")
}

// TestRunEntriesDispatched_AllSucceededAutoRollup verifies that when every
// projected task arrives in terminal-succeeded state the run auto-rolls up
// to SUCCEEDED and emits a RunFinalized domain event via Outbox.Append.
func TestRunEntriesDispatched_AllSucceededAutoRollup(t *testing.T) {
	scheduleID := uuid.New()
	msgProcID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{stored: newPendingDispatchedRun(scheduleID, "rebase-rollup")}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	root1, root2 := uuid.New(), uuid.New()
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a",
				Status: run.TaskStatusSucceeded, InheritedFromTaskID: &root1},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "b",
				Status: run.TaskStatusSucceeded, InheritedFromTaskID: &root2},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, msgProcID))

	assert.True(t, runRepo.saveCalled)
	require.Len(t, taskColl.bulkCreated, 2)

	require.Len(t, outbox.appended, 1, "auto-rollup must emit exactly one RunFinalized event")
	assert.Equal(t, msgProcID, outbox.msgProcID, "msgProcID must be forwarded to Append")

	finalized, ok := outbox.appended[0].(run.RunFinalized)
	require.True(t, ok, "event must be RunFinalized")
	assert.Equal(t, scheduleID, finalized.ID)
	assert.Equal(t, "rebase-rollup", finalized.Name)
	assert.Equal(t, run.SchedulerStatusSucceeded, finalized.Outcome)
}

// TestRunEntriesDispatched_MixedTerminalAutoRollupAsFailed verifies that a
// projection with all-terminal tasks but at least one non-succeeded results
// in a FAILED auto-rollup.
func TestRunEntriesDispatched_MixedTerminalAutoRollupAsFailed(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{stored: newPendingDispatchedRun(scheduleID, "mixed-terminal")}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 2,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a", Status: run.TaskStatusSucceeded},
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "b", Status: run.TaskStatusFailed},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, outbox.appended, 1, "mixed-terminal must emit RunFinalized")
	finalized, ok := outbox.appended[0].(run.RunFinalized)
	require.True(t, ok)
	assert.Equal(t, run.SchedulerStatusFailed, finalized.Outcome,
		"mixed terminal (one failed) must roll up to FAILED")
}

// TestRunEntriesDispatched_NoopWhenSchedulerTerminal verifies that dispatch
// against a run already in any terminal status is a complete no-op: no
// BulkCreate, no domain events, SaveRun still called.
func TestRunEntriesDispatched_NoopWhenSchedulerTerminal(t *testing.T) {
	for _, status := range []run.SchedulerStatus{
		run.SchedulerStatusSucceeded,
		run.SchedulerStatusFailed,
		run.SchedulerStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID := uuid.New()
			runRepo := &fakeDispatchedRunRepo{stored: newTerminalDispatchedRun(scheduleID, "terminal-guard", status)}
			outbox := &fakeDispatchedOutboxPub{}
			taskColl := &fakeDispatchedTaskCollection{}
			u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

			evt := events.RunEntriesDispatched{
				ScheduleID:     scheduleID,
				TotalTaskCount: 1,
				AllTasks: []events.RunEntriesDispatchedTask{
					{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "t",
						Status: run.TaskStatusSucceeded},
				},
			}

			h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
			require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

			assert.True(t, runRepo.saveCalled, "SaveRun is called even for already-terminal runs")
			assert.Empty(t, taskColl.bulkCreated, "terminal run → no tasks bulk-created")
			assert.Empty(t, outbox.appended, "terminal run → no domain events")
		})
	}
}

// TestRunEntriesDispatched_LoadErrorPropagates verifies that a
// LoadRunForUpdate failure short-circuits the handler before any mutation.
func TestRunEntriesDispatched_LoadErrorPropagates(t *testing.T) {
	runRepo := &fakeDispatchedRunRepo{loadErr: errors.New("row lock failed")}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatched{
		ScheduleID:     uuid.New(),
		TotalTaskCount: 0,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "load run")
	assert.False(t, runRepo.saveCalled, "SaveRun must not be called after load failure")
	assert.Empty(t, taskColl.bulkCreated)
	assert.Empty(t, outbox.appended)
}

// TestRunEntriesDispatched_SaveErrorPropagates verifies that a SaveRun
// failure surfaces an error and prevents the outbox append.
func TestRunEntriesDispatched_SaveErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{
		stored:  newPendingDispatchedRun(scheduleID, "save-error"),
		saveErr: errors.New("db down"),
	}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 1,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "t",
				Status: run.TaskStatusPending},
		},
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "save run")
	assert.Empty(t, outbox.appended, "save failure must short-circuit the outbox append")
}

// TestRunEntriesDispatched_AppendErrorPropagates verifies that an outbox
// append failure on the auto-rollup branch surfaces an error after SaveRun
// so the binding rolls back the entire transaction.
func TestRunEntriesDispatched_AppendErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{stored: newPendingDispatchedRun(scheduleID, "outbox-error")}
	outbox := &fakeDispatchedOutboxPub{err: errors.New("outbox down")}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	// Trigger auto-rollup so Append receives actual events (not an empty slice
	// that some implementations might skip).
	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 1,
		AllTasks: []events.RunEntriesDispatchedTask{
			{TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a",
				Status: run.TaskStatusSucceeded},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, evt, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append outbox")
	assert.True(t, runRepo.saveCalled, "SaveRun is called before the outbox append")
}

// TestRunEntriesDispatched_InvalidTaskNameWrapsAsPermanent asserts that when
// the dispatched payload carries an invalid task identity that
// pkg/domain.ComputeJobName refuses, the handler returns an error matching
// both run.ErrInvalidDispatchedTask (the domain sentinel surfaced by the
// aggregate) and pkg/events.ErrPermanent (the infra contract that tells the
// Redis binding to ACK-and-drop). Without the ErrPermanent wrap, the binding
// would NACK the poison payload and keep retrying it forever.
func TestRunEntriesDispatched_InvalidTaskNameWrapsAsPermanent(t *testing.T) {
	scheduleID := uuid.New()
	runRepo := &fakeDispatchedRunRepo{stored: newPendingDispatchedRun(scheduleID, "daily")}
	outbox := &fakeDispatchedOutboxPub{}
	taskColl := &fakeDispatchedTaskCollection{}
	u := newDispatchedHandlerUoW(runRepo, outbox, taskColl)

	// Override the job-name hook to simulate a poison payload whose task
	// identity fields cannot be turned into a valid k8s job-name.
	run.SetComputeJobNameFn(func(_, _, _, _ string) (string, error) {
		return "", errors.New("computed job_name is empty after sanitization")
	})
	t.Cleanup(run.ResetComputeJobNameFn)

	evt := events.RunEntriesDispatched{
		ScheduleID:     scheduleID,
		TotalTaskCount: 1,
		AllTasks: []events.RunEntriesDispatchedTask{
			{
				TaskID:      uuid.New(),
				ServiceName: " ", SchemaName: " ", TableName: " ",
				Status:     run.TaskStatusPending,
				MaxRetries: 3,
			},
		},
	}

	h := handlers.NewRunEntriesDispatchedHandler(dispatchedTestLogger())
	err := h.Handle(context.Background(), u, evt, uuid.New())

	if err == nil {
		t.Fatal("expected error for invalid task name")
	}
	if !errors.Is(err, run.ErrInvalidDispatchedTask) {
		t.Errorf("error must wrap run.ErrInvalidDispatchedTask, got: %v", err)
	}
	if !errors.Is(err, pkgevents.ErrPermanent) {
		t.Errorf("error must wrap pkgevents.ErrPermanent so the binding ACKs-and-drops, got: %v", err)
	}
	if outbox.appended != nil {
		t.Errorf("OutboxPublisher.Append must not be called when AcceptDispatch fails, got: %+v", outbox.appended)
	}
}
