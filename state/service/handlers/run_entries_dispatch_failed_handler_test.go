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

// fakeRunFailedSchedulerRepo is a minimal stub of
// postgres.SchedulerTrackerRepository for RunEntriesDispatchFailed handler
// tests. Only the *Tx methods the handler calls record arguments; the rest
// panic so accidental use surfaces immediately.
type fakeRunFailedSchedulerRepo struct {
	scheduler   *model.SchedulerTracker
	getErr      error
	finalizedTo string
	finalizeErr error
}

func (f *fakeRunFailedSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.scheduler, f.getErr
}

func (f *fakeRunFailedSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, status string) error {
	f.finalizedTo = status
	return f.finalizeErr
}

// Unused methods — panic on accidental use.
func (f *fakeRunFailedSchedulerRepo) Create(context.Context, *model.SchedulerTracker) error {
	panic("Create not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) GetByID(context.Context, uuid.UUID) (*model.SchedulerTracker, error) {
	panic("GetByID not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) Update(context.Context, *model.SchedulerTracker) error {
	panic("Update not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) Cancel(context.Context, uuid.UUID, string, string) error {
	panic("Cancel not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) CancelTx(context.Context, *sqlx.Tx, uuid.UUID, string, string) error {
	panic("CancelTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) List(context.Context, postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	panic("List not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) HasActiveSchedule(context.Context, string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) UpdateInitializationStatus(context.Context, uuid.UUID, string) error {
	panic("UpdateInitializationStatus not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) ResetInProgressInitializations(context.Context) (int, error) {
	panic("ResetInProgressInitializations not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) GetLastRunPerSchedule(context.Context) (map[string]postgres.LastRunData, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) GetActiveScheduler(context.Context, string) (*model.SchedulerTracker, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) UpdateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("UpdateTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) UpdateInitializationStatusTx(context.Context, *sqlx.Tx, uuid.UUID, string) error {
	panic("UpdateInitializationStatusTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) CreateTx(context.Context, *sqlx.Tx, *model.SchedulerTracker) error {
	panic("CreateTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) IncrementTerminalCountTx(context.Context, *sqlx.Tx, uuid.UUID) (int32, int32, error) {
	panic("IncrementTerminalCountTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) DecrementTerminalCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("DecrementTerminalCountTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) SetTotalTaskCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("SetTotalTaskCountTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) SetTerminalTaskCountTx(context.Context, *sqlx.Tx, uuid.UUID, int32) error {
	panic("SetTerminalTaskCountTx not implemented in fake")
}
func (f *fakeRunFailedSchedulerRepo) UpdateStatusTx(context.Context, *sqlx.Tx, uuid.UUID, string) error {
	panic("UpdateStatusTx not implemented in fake")
}

// fakeRunFailedOutboxRepo captures outbox writes for the dispatch_failed
// handler tests. Only Create is exercised; other methods panic.
type fakeRunFailedOutboxRepo struct {
	created []*postgres.OutboxEntry
	err     error
}

func (f *fakeRunFailedOutboxRepo) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	if f.err != nil {
		return f.err
	}
	f.created = append(f.created, e)
	return nil
}
func (f *fakeRunFailedOutboxRepo) ListPending(context.Context, int) ([]*postgres.OutboxEntry, error) {
	panic("ListPending not implemented in fake")
}
func (f *fakeRunFailedOutboxRepo) MarkPublished(context.Context, uuid.UUID) error {
	panic("MarkPublished not implemented in fake")
}
func (f *fakeRunFailedOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error {
	panic("IncrementRetry not implemented in fake")
}

func runFailedTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed
// verifies the happy path: a non-terminal scheduler is locked, finalized as
// failed, and a run.finalized:v1 outbox entry is written with
// MessageProcessingID set to the dedup ID returned by Dedup().
func TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed(t *testing.T) {
	scheduleID := uuid.New()
	msgProcID := uuid.New()
	sched := &model.SchedulerTracker{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Status:       model.SchedulerStatusRunning,
	}
	schedRepo := &fakeRunFailedSchedulerRepo{scheduler: sched}
	outboxRepo := &fakeRunFailedOutboxRepo{}
	u := &uow.FakeUnitOfWork{Scheduler: schedRepo, OutboxStore: outboxRepo}

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
	}, msgProcID)
	require.NoError(t, err)
	assert.Equal(t, string(model.SchedulerStatusFailed), schedRepo.finalizedTo)
	require.Len(t, outboxRepo.created, 1)
	entry := outboxRepo.created[0]
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
	assert.Equal(t, "test-schedule", fin.ScheduleName)
	assert.Equal(t, "failed", fin.Status)
}

// TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal verifies
// that re-delivery against a scheduler already in any terminal state is a
// no-op: no finalize, no outbox write. Replaces the DB-integration
// "Succeeded" variant; covers all three terminal statuses for completeness.
func TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal(t *testing.T) {
	for _, status := range []model.SchedulerStatus{
		model.SchedulerStatusSucceeded,
		model.SchedulerStatusFailed,
		model.SchedulerStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID := uuid.New()
			sched := &model.SchedulerTracker{
				ScheduleID:   scheduleID,
				ScheduleName: "test-schedule",
				Status:       status,
			}
			schedRepo := &fakeRunFailedSchedulerRepo{scheduler: sched}
			outboxRepo := &fakeRunFailedOutboxRepo{}
			u := &uow.FakeUnitOfWork{Scheduler: schedRepo, OutboxStore: outboxRepo}

			h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
			err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
				ScheduleID:   scheduleID,
				ScheduleName: "test-schedule",
				Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
			}, uuid.New())
			require.NoError(t, err)
			assert.Empty(t, schedRepo.finalizedTo,
				"already-terminal scheduler must not be re-finalized")
			assert.Empty(t, outboxRepo.created,
				"no run.finalized:v1 must be emitted for already-terminal scheduler")
		})
	}
}

// TestRunEntriesDispatchFailedHandler_FinalizeErrorPropagates verifies that
// a FinalizeRunTx failure bubbles up and prevents the outbox write — the
// binding will then rollback the transaction.
func TestRunEntriesDispatchFailedHandler_FinalizeErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()
	sched := &model.SchedulerTracker{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Status:       model.SchedulerStatusRunning,
	}
	schedRepo := &fakeRunFailedSchedulerRepo{
		scheduler:   sched,
		finalizeErr: errors.New("db down"),
	}
	outboxRepo := &fakeRunFailedOutboxRepo{}
	u := &uow.FakeUnitOfWork{Scheduler: schedRepo, OutboxStore: outboxRepo}

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
	}, uuid.New())
	require.Error(t, err)
	assert.Empty(t, outboxRepo.created,
		"finalize failure must short-circuit the outbox write")
}

// TestRunEntriesDispatchFailedHandler_GetByIDErrorPropagates verifies that a
// scheduler lookup failure surfaces an error before any state mutation.
func TestRunEntriesDispatchFailedHandler_GetByIDErrorPropagates(t *testing.T) {
	schedRepo := &fakeRunFailedSchedulerRepo{getErr: errors.New("row lock failed")}
	outboxRepo := &fakeRunFailedOutboxRepo{}
	u := &uow.FakeUnitOfWork{Scheduler: schedRepo, OutboxStore: outboxRepo}

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   uuid.New(),
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonEmptyProjection,
	}, uuid.New())
	require.Error(t, err)
	assert.Empty(t, schedRepo.finalizedTo)
	assert.Empty(t, outboxRepo.created)
}
