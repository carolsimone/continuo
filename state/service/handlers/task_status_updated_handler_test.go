package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStatusSchedulerRepo records every *Tx method the
// TaskStatusUpdatedHandler calls and stubs the rest with a panic so
// accidental use surfaces immediately.
type fakeStatusSchedulerRepo struct {
	scheduler *model.SchedulerTracker
	getErr    error

	incrementResultTerminal int32
	incrementResultTotal    int32
	incrementErr            error
	incrementCalls          int

	decrementCalls []int32
	decrementErr   error

	finalizedTo string
	finalizeErr error
}

func (f *fakeStatusSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.scheduler, f.getErr
}
func (f *fakeStatusSchedulerRepo) IncrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (int32, int32, error) {
	f.incrementCalls++
	if f.incrementErr != nil {
		return 0, 0, f.incrementErr
	}
	return f.incrementResultTerminal, f.incrementResultTotal, nil
}
func (f *fakeStatusSchedulerRepo) DecrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, n int32) error {
	if f.decrementErr != nil {
		return f.decrementErr
	}
	f.decrementCalls = append(f.decrementCalls, n)
	return nil
}
func (f *fakeStatusSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status string) error {
	if f.finalizeErr != nil {
		return f.finalizeErr
	}
	f.finalizedTo = status
	return nil
}

// Unused methods — panic on accidental use.
func (f *fakeStatusSchedulerRepo) Create(context.Context, *model.SchedulerTracker) error {
	panic("Create not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) GetByID(context.Context, uuid.UUID) (*model.SchedulerTracker, error) {
	panic("GetByID not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) Update(context.Context, *model.SchedulerTracker) error {
	panic("Update not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) Cancel(context.Context, uuid.UUID, string, string) error {
	panic("Cancel not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) CancelTx(context.Context, *sqlx.Tx, uuid.UUID, string, string) error {
	panic("CancelTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) List(context.Context, postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	panic("List not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) HasActiveSchedule(context.Context, string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) UpdateInitializationStatus(context.Context, uuid.UUID, string) error {
	panic("UpdateInitializationStatus not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) ResetInProgressInitializations(context.Context) (int, error) {
	panic("ResetInProgressInitializations not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) GetLastRunPerSchedule(context.Context) (map[string]postgres.LastRunData, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) GetActiveScheduler(context.Context, string) (*model.SchedulerTracker, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) UpdateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("UpdateTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) CreateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("CreateTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) SetTotalTaskCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("SetTotalTaskCountTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) SetTerminalTaskCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("SetTerminalTaskCountTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) UpdateInitializationStatusTx(context.Context, *sqlx.Tx, uuid.UUID, string) error {
	panic("UpdateInitializationStatusTx not implemented in fake")
}
func (f *fakeStatusSchedulerRepo) UpdateStatusTx(context.Context, *sqlx.Tx, uuid.UUID, string) error {
	panic("UpdateStatusTx not implemented in fake")
}

// fakeStatusTaskRepo records calls and stubs the rest with a panic.
type fakeStatusTaskRepo struct {
	prevStatus     string
	getStatusErr   error
	getStatusCalls int

	updateAffected   int32
	updateErr        error
	updateCalls      []updateStatusCall
	existsValue      bool
	existsErr        error
	existsCalls      int
	hasRetryable     bool
	hasRetryableErr  error
	hasRetryableCalls int
	hasFailed        bool
	hasFailedErr     error
	hasFailedCalls   int
}

type updateStatusCall struct {
	taskID     uuid.UUID
	status     string
	retryCount int32
}

func (f *fakeStatusTaskRepo) GetStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (string, error) {
	f.getStatusCalls++
	return f.prevStatus, f.getStatusErr
}
func (f *fakeStatusTaskRepo) UpdateStatusIfChangedTx(_ context.Context, _ *sqlx.Tx, taskID uuid.UUID, status string, retryCount int32) (int32, error) {
	if f.updateErr != nil {
		return 0, f.updateErr
	}
	f.updateCalls = append(f.updateCalls, updateStatusCall{taskID, status, retryCount})
	return f.updateAffected, nil
}
func (f *fakeStatusTaskRepo) ExistsTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	f.existsCalls++
	return f.existsValue, f.existsErr
}
func (f *fakeStatusTaskRepo) HasRetryableFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	f.hasRetryableCalls++
	return f.hasRetryable, f.hasRetryableErr
}
func (f *fakeStatusTaskRepo) HasFailedTaskTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (bool, error) {
	f.hasFailedCalls++
	return f.hasFailed, f.hasFailedErr
}

// Unused methods — panic on accidental use.
func (f *fakeStatusTaskRepo) Create(context.Context, *model.TaskTracker) error {
	panic("Create not implemented in fake")
}
func (f *fakeStatusTaskRepo) GetByID(context.Context, uuid.UUID) (*model.TaskTracker, error) {
	panic("GetByID not implemented in fake")
}
func (f *fakeStatusTaskRepo) GetByScheduleAndNode(context.Context, uuid.UUID, string, string, string) (*model.TaskTracker, error) {
	panic("GetByScheduleAndNode not implemented in fake")
}
func (f *fakeStatusTaskRepo) Update(context.Context, *model.TaskTracker) error {
	panic("Update not implemented in fake")
}
func (f *fakeStatusTaskRepo) Delete(context.Context, uuid.UUID) error {
	panic("Delete not implemented in fake")
}
func (f *fakeStatusTaskRepo) ListByScheduleID(context.Context, uuid.UUID, *model.TaskStatus, int, int) ([]*model.TaskTracker, int, error) {
	panic("ListByScheduleID not implemented in fake")
}
func (f *fakeStatusTaskRepo) List(context.Context, postgres.TaskFilters) ([]*model.TaskTracker, int, error) {
	panic("List not implemented in fake")
}
func (f *fakeStatusTaskRepo) UpdateTx(context.Context, *sqlx.Tx, *model.TaskTracker) error {
	panic("UpdateTx not implemented in fake")
}
func (f *fakeStatusTaskRepo) BulkCreateTx(context.Context, *sqlx.Tx, []*model.TaskTracker) error {
	panic("BulkCreateTx not implemented in fake")
}
func (f *fakeStatusTaskRepo) ListAllByScheduleID(context.Context, uuid.UUID) ([]*model.TaskTracker, error) {
	panic("ListAllByScheduleID not implemented in fake")
}
func (f *fakeStatusTaskRepo) ResetTasksTx(context.Context, *sqlx.Tx, []uuid.UUID) (int32, error) {
	panic("ResetTasksTx not implemented in fake")
}
func (f *fakeStatusTaskRepo) HasNonSucceededTask(context.Context, uuid.UUID) (bool, error) {
	panic("HasNonSucceededTask not implemented in fake")
}
func (f *fakeStatusTaskRepo) BulkCancelByScheduleIDTx(context.Context, *sqlx.Tx, uuid.UUID, string) (int64, error) {
	panic("BulkCancelByScheduleIDTx not implemented in fake")
}

// fakeStatusOutboxRepo captures outbox writes.
type fakeStatusOutboxRepo struct {
	created []*postgres.OutboxEntry
	err     error
}

func (f *fakeStatusOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, e)
	return nil
}
func (f *fakeStatusOutboxRepo) ListPending(context.Context, int) ([]*postgres.OutboxEntry, error) {
	panic("ListPending not implemented in fake")
}
func (f *fakeStatusOutboxRepo) MarkPublished(context.Context, uuid.UUID) error {
	panic("MarkPublished not implemented in fake")
}
func (f *fakeStatusOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error {
	panic("IncrementRetry not implemented in fake")
}

func statusTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newStatusUoW assembles a FakeUnitOfWork already in a transaction
// (Begin called) wired to fresh fakes.
func newStatusUoW(
	sched *fakeStatusSchedulerRepo,
	task *fakeStatusTaskRepo,
	outbox *fakeStatusOutboxRepo,
) *uow.FakeUnitOfWork {
	u := &uow.FakeUnitOfWork{Scheduler: sched, Task: task, Outbox: outbox}
	_ = u.Begin(context.Background())
	return u
}

func newRunningScheduler(scheduleID uuid.UUID, name string) *model.SchedulerTracker {
	return &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         name,
		Status:               model.SchedulerStatusRunning,
		InitializationStatus: "completed",
	}
}

// TestTaskStatusUpdatedHandler_TerminalCountAndFinalization verifies that
// processing two SUCCEEDED events increments terminal_task_count and (when
// terminal==total with no failed tasks) finalizes the run as succeeded with
// a run.finalized:v1 outbox entry.
func TestTaskStatusUpdatedHandler_TerminalCountAndFinalization(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	// First call: terminal=1, total=2 — no finalize yet.
	sched := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "happy-path"),
		incrementResultTerminal: 1,
		incrementResultTotal:    2,
	}
	task := &fakeStatusTaskRepo{prevStatus: string(model.TaskStatusRunning), updateAffected: 1}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
		RetryCount: 0,
	}, uuid.New()))

	require.Len(t, task.updateCalls, 1)
	assert.Equal(t, string(model.TaskStatusSucceeded), task.updateCalls[0].status)
	assert.Equal(t, 1, sched.incrementCalls)
	assert.Empty(t, sched.finalizedTo, "terminal<total → no finalize")
	assert.Empty(t, outbox.created)
	assert.Empty(t, sched.decrementCalls)

	// Second call: terminal=2, total=2 — must finalize succeeded.
	tB := uuid.New()
	msgProcID := uuid.New()
	sched2 := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "happy-path"),
		incrementResultTerminal: 2,
		incrementResultTotal:    2,
	}
	task2 := &fakeStatusTaskRepo{prevStatus: string(model.TaskStatusRunning), updateAffected: 1}
	outbox2 := &fakeStatusOutboxRepo{}
	u2 := newStatusUoW(sched2, task2, outbox2)

	require.NoError(t, h.Handle(context.Background(), u2, events.TaskStatusUpdated{
		TaskID:     tB,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, msgProcID))

	assert.Equal(t, string(model.SchedulerStatusSucceeded), sched2.finalizedTo)
	require.Len(t, outbox2.created, 1)
	entry := outbox2.created[0]
	assert.Equal(t, "run.finalized:v1", entry.EventType)
	assert.Equal(t, "run.finalized:v1", entry.StreamName)
	assert.Equal(t, "scheduler_tracker", entry.AggregateType)
	assert.Equal(t, scheduleID, entry.AggregateID)
	assert.Equal(t, "pending", entry.Status)
	require.NotNil(t, entry.MessageProcessingID)
	assert.Equal(t, msgProcID, *entry.MessageProcessingID)

	var fin pkgevents.RunFinalized
	require.NoError(t, json.Unmarshal(entry.Payload, &fin))
	assert.Equal(t, scheduleID.String(), fin.ScheduleID)
	assert.Equal(t, "happy-path", fin.ScheduleName)
	assert.Equal(t, "succeeded", fin.Status)
}

// TestTaskStatusUpdatedHandler_AnyFailedMarksRunFailed verifies that a
// FAILED task at terminal==total (with no retryable failures) finalizes
// the run as failed.
func TestTaskStatusUpdatedHandler_AnyFailedMarksRunFailed(t *testing.T) {
	scheduleID := uuid.New()
	tB := uuid.New()

	sched := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "fail-run"),
		incrementResultTerminal: 2,
		incrementResultTotal:    2,
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
		hasRetryable:   false,
		hasFailed:      true, // any failed → finalize as failed
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tB,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusFailed,
		RetryCount: 3,
	}, uuid.New()))

	assert.Equal(t, string(model.SchedulerStatusFailed), sched.finalizedTo)
	require.Len(t, outbox.created, 1)
	var fin pkgevents.RunFinalized
	require.NoError(t, json.Unmarshal(outbox.created[0].Payload, &fin))
	assert.Equal(t, "failed", fin.Status)
	assert.Equal(t, 1, task.hasRetryableCalls, "must consult HasRetryableFailedTaskTx when terminal==total")
	assert.Equal(t, 1, task.hasFailedCalls, "non-retryable → must consult HasFailedTaskTx")
}

// TestTaskStatusUpdatedHandler_RetriesWhenTaskRowMissing verifies that
// handling an event for a non-existent task_id returns a transient error
// (the binding rolls back and the consumer reclaim loop retries). The
// error must NOT be wrapped with ErrPermanent.
func TestTaskStatusUpdatedHandler_RetriesWhenTaskRowMissing(t *testing.T) {
	scheduleID := uuid.New()
	unknownTaskID := uuid.New()

	sched := &fakeStatusSchedulerRepo{scheduler: newRunningScheduler(scheduleID, "no-row")}
	task := &fakeStatusTaskRepo{
		prevStatus:     "",
		updateAffected: 0, // 0 rows → either replay or missing
		existsValue:    false,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     unknownTaskID,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "task_tracker row not found")
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"transient retry path must NOT be wrapped with ErrPermanent")

	assert.Equal(t, 1, task.existsCalls)
	assert.Equal(t, 0, sched.incrementCalls)
	assert.Empty(t, sched.finalizedTo)
	assert.Empty(t, outbox.created)
}

// TestTaskStatusUpdatedHandler_ReplayUnchangedStatusIsNoop verifies that
// when UpdateStatusIfChangedTx returns 0 affected but the row exists
// (replay/idempotent retry), the handler does NOT increment terminal_count
// and does NOT finalize.
func TestTaskStatusUpdatedHandler_ReplayUnchangedStatusIsNoop(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{scheduler: newRunningScheduler(scheduleID, "replay")}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusSucceeded),
		updateAffected: 0, // no-op update
		existsValue:    true,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New()))

	assert.Equal(t, 1, task.existsCalls)
	assert.Equal(t, 0, sched.incrementCalls, "replay must not increment terminal_count")
	assert.Empty(t, sched.decrementCalls)
	assert.Empty(t, sched.finalizedTo)
	assert.Empty(t, outbox.created)
}

// TestTaskStatusUpdatedHandler_RetryableFailureDoesNotFinalize verifies
// that when terminal==total but the schedule still has a retryable failed
// task, the run is NOT finalised (the upcoming RUNNING retry event will
// decrement the counter and re-open the run).
func TestTaskStatusUpdatedHandler_RetryableFailureDoesNotFinalize(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "retryable"),
		incrementResultTerminal: 1,
		incrementResultTotal:    1,
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
		hasRetryable:   true, // retry still pending → skip finalize
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusFailed,
		RetryCount: 0,
	}, uuid.New()))

	assert.Equal(t, 1, sched.incrementCalls, "terminal task must still increment terminal_count")
	assert.Equal(t, 1, task.hasRetryableCalls)
	assert.Equal(t, 0, task.hasFailedCalls, "retryable short-circuit must skip HasFailedTaskTx")
	assert.Empty(t, sched.finalizedTo, "retryable failure must not finalize")
	assert.Empty(t, outbox.created, "no finalize → no outbox")
}

// TestTaskStatusUpdatedHandler_FailedToRunningDecrementsTerminalCount
// verifies the FAILED→RUNNING k8s retry path: a non-terminal event
// arriving after a previously-terminal status must decrement
// terminal_task_count to keep it accurate.
func TestTaskStatusUpdatedHandler_FailedToRunningDecrementsTerminalCount(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{scheduler: newRunningScheduler(scheduleID, "retry-revives")}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusFailed),
		updateAffected: 1,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusRunning,
		RetryCount: 1,
	}, uuid.New()))

	require.Equal(t, []int32{1}, sched.decrementCalls,
		"FAILED→RUNNING must decrement terminal_count by 1")
	assert.Equal(t, 0, sched.incrementCalls, "non-terminal new status must not increment")
	assert.Empty(t, sched.finalizedTo)
	assert.Empty(t, outbox.created)
}

// TestTaskStatusUpdatedHandler_RunningToRunningNoDecrement verifies that
// when neither the previous nor the new status is terminal, neither
// counter is touched.
func TestTaskStatusUpdatedHandler_RunningToRunningNoDecrement(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{scheduler: newRunningScheduler(scheduleID, "running")}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusPending),
		updateAffected: 1,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusRunning,
		RetryCount: 0,
	}, uuid.New()))

	assert.Empty(t, sched.decrementCalls)
	assert.Equal(t, 0, sched.incrementCalls)
	assert.Empty(t, sched.finalizedTo)
}

// TestTaskStatusUpdatedHandler_NoopWhenSchedulerTerminal verifies that a
// stale task.status.updated event arriving after the run has already
// finalised is a complete no-op: no task update, no terminal-count
// mutation, no finalize, no outbox. Mirror of the dispatched handler's
// terminal guard.
func TestTaskStatusUpdatedHandler_NoopWhenSchedulerTerminal(t *testing.T) {
	for _, terminalStatus := range []model.SchedulerStatus{
		model.SchedulerStatusFailed,
		model.SchedulerStatusSucceeded,
		model.SchedulerStatusCancelled,
	} {
		terminalStatus := terminalStatus
		t.Run(string(terminalStatus), func(t *testing.T) {
			scheduleID := uuid.New()
			tA := uuid.New()

			sched := &fakeStatusSchedulerRepo{
				scheduler: &model.SchedulerTracker{
					ScheduleID:           scheduleID,
					ScheduleName:         "terminal-guard",
					Status:               terminalStatus,
					InitializationStatus: "completed",
				},
			}
			task := &fakeStatusTaskRepo{prevStatus: string(model.TaskStatusFailed)}
			outbox := &fakeStatusOutboxRepo{}
			u := newStatusUoW(sched, task, outbox)

			h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
			require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
				TaskID:     tA,
				ScheduleID: scheduleID,
				Status:     model.TaskStatusRunning,
				RetryCount: 1,
			}, uuid.New()))

			assert.Empty(t, task.updateCalls, "terminal scheduler → no task update")
			assert.Equal(t, 0, task.getStatusCalls, "terminal scheduler → no prev-status read")
			assert.Equal(t, 0, sched.incrementCalls)
			assert.Empty(t, sched.decrementCalls,
				"terminal scheduler → terminal_count must not regress")
			assert.Empty(t, sched.finalizedTo)
			assert.Empty(t, outbox.created)
		})
	}
}

// TestTaskStatusUpdatedHandler_InitNotCompletedSkipsFinalize verifies that
// the finalize gate respects initialization_status != "completed": even
// when terminal==total and no failures are retryable, finalization.Decide
// returns "" (run not finalised) when initialization is still in progress.
func TestTaskStatusUpdatedHandler_InitNotCompletedSkipsFinalize(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{
		scheduler: &model.SchedulerTracker{
			ScheduleID:           scheduleID,
			ScheduleName:         "init-pending",
			Status:               model.SchedulerStatusRunning,
			InitializationStatus: "in_progress",
		},
		incrementResultTerminal: 1,
		incrementResultTotal:    1,
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New()))

	assert.Equal(t, 1, sched.incrementCalls)
	assert.Empty(t, sched.finalizedTo,
		"initialization_status != completed → finalization.Decide returns empty outcome")
	assert.Empty(t, outbox.created)
}

// TestTaskStatusUpdatedHandler_GetSchedulerErrorPropagates verifies that a
// scheduler-lookup failure short-circuits the handler before any mutation.
func TestTaskStatusUpdatedHandler_GetSchedulerErrorPropagates(t *testing.T) {
	sched := &fakeStatusSchedulerRepo{getErr: errors.New("row lock failed")}
	task := &fakeStatusTaskRepo{}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: uuid.New(),
		Status:     model.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "get scheduler for update")
	assert.Empty(t, task.updateCalls)
}

// TestTaskStatusUpdatedHandler_UpdateStatusErrorPropagates verifies that
// a task-update failure bubbles up so the binding rolls back the tx.
func TestTaskStatusUpdatedHandler_UpdateStatusErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeStatusSchedulerRepo{scheduler: newRunningScheduler(scheduleID, "update-err")}
	task := &fakeStatusTaskRepo{updateErr: errors.New("db down")}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update task status")
	assert.Equal(t, 0, sched.incrementCalls)
}

// TestTaskStatusUpdatedHandler_IncrementErrorPropagates verifies that an
// IncrementTerminalCountTx failure surfaces an error (rolling back the
// tx — no partial finalize).
func TestTaskStatusUpdatedHandler_IncrementErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeStatusSchedulerRepo{
		scheduler:    newRunningScheduler(scheduleID, "incr-err"),
		incrementErr: errors.New("counter wedged"),
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "increment terminal count")
	assert.Empty(t, sched.finalizedTo)
	assert.Empty(t, outbox.created)
}

// TestTaskStatusUpdatedHandler_OutboxErrorPropagates verifies that an
// outbox-write failure on the finalize branch bubbles up so the binding
// rolls back the entire tx (no half-committed finalize).
func TestTaskStatusUpdatedHandler_OutboxErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "outbox-err"),
		incrementResultTerminal: 1,
		incrementResultTotal:    1,
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
		hasRetryable:   false,
		hasFailed:      false,
	}
	outbox := &fakeStatusOutboxRepo{err: errors.New("outbox down")}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	err := h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     uuid.New(),
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSucceeded,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create outbox entry for run.finalized")
	// FinalizeRunTx ran before the outbox write — that's fine since the
	// binding rolls back the whole tx on error.
	assert.Equal(t, string(model.SchedulerStatusSucceeded), sched.finalizedTo)
}

// TestTaskStatusUpdatedHandler_SkippedIsTerminal verifies that the
// SKIPPED status counts as terminal for finalization purposes.
func TestTaskStatusUpdatedHandler_SkippedIsTerminal(t *testing.T) {
	scheduleID := uuid.New()
	tA := uuid.New()

	sched := &fakeStatusSchedulerRepo{
		scheduler:               newRunningScheduler(scheduleID, "skipped"),
		incrementResultTerminal: 1,
		incrementResultTotal:    2,
	}
	task := &fakeStatusTaskRepo{
		prevStatus:     string(model.TaskStatusRunning),
		updateAffected: 1,
	}
	outbox := &fakeStatusOutboxRepo{}
	u := newStatusUoW(sched, task, outbox)

	h := handlers.NewTaskStatusUpdatedHandler(statusTestLogger())
	require.NoError(t, h.Handle(context.Background(), u, events.TaskStatusUpdated{
		TaskID:     tA,
		ScheduleID: scheduleID,
		Status:     model.TaskStatusSkipped,
	}, uuid.New()))

	assert.Equal(t, 1, sched.incrementCalls, "SKIPPED must count as terminal")
	assert.Empty(t, sched.finalizedTo, "terminal<total → still no finalize")
}
