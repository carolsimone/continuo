package handlers_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	repository "github.com/carolsimone/continuo/state/domain/repository"
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
	// GetStatus / LoadStatusAndAttempt response
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
}

func (f *fakeTaskStatusTaskCollection) GetStatus(_ context.Context, _ uuid.UUID) (run.TaskStatus, bool, error) {
	return f.prevStatus, f.prevExists, f.getStatusErr
}

func (f *fakeTaskStatusTaskCollection) LoadStatusAndAttempt(_ context.Context, _ uuid.UUID) (run.TaskStatus, int32, bool, error) {
	return f.prevStatus, f.prevAttempt, f.prevExists, f.getStatusErr
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
func (f *fakeTaskStatusTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string) (int, error) {
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
	return run.HydrateRun(
		id, name,
		run.SchedulerStatusRunning,
		run.InitStatusCompleted,
		run.KindCron,
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: true, Int32: total},
		terminal,
		map[string]run.ServiceMetadata{},
	)
}

// newTerminalTaskStatusRun builds a terminal Run for idempotency tests.
func newTerminalTaskStatusRun(id uuid.UUID, name string, status run.SchedulerStatus) *run.Run {
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

			h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
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

	h := handlers.NewTaskStatusUpdatedHandler(taskStatusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     run.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append outbox")
	assert.True(t, runRepo.saveCalled, "SaveRun is called before the outbox append")
}
