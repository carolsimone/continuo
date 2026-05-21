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
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDispatchFailedRunRepo is an in-memory RunRepository for
// RunEntriesDispatchFailed handler tests. LoadRunForUpdate returns the stored
// run (nil tx is ignored — handler unit tests never hold a real transaction).
// SaveRun records the call and stores the updated aggregate. Other methods
// panic on accidental use.
type fakeDispatchFailedRunRepo struct {
	stored     *run.Run
	loadErr    error
	saveErr    error
	saveCalled bool
}

func (f *fakeDispatchFailedRunRepo) LoadRunForUpdate(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}

func (f *fakeDispatchFailedRunRepo) SaveRun(_ context.Context, _ *sqlx.Tx, r *run.Run) error {
	f.saveCalled = true
	if f.saveErr != nil {
		return f.saveErr
	}
	f.stored = r
	return nil
}

func (f *fakeDispatchFailedRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not implemented in fake")
}
func (f *fakeDispatchFailedRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not implemented in fake")
}
func (f *fakeDispatchFailedRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not implemented in fake")
}
func (f *fakeDispatchFailedRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not implemented in fake")
}

// fakeDispatchFailedOutboxPub is an in-memory OutboxPublisher that captures
// the appended domain events for assertion.
type fakeDispatchFailedOutboxPub struct {
	appended  []run.DomainEvent
	msgProcID uuid.UUID
	err       error
}

func (f *fakeDispatchFailedOutboxPub) Append(_ context.Context, _ *sqlx.Tx, evts []run.DomainEvent, msgProcID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.appended = append(f.appended, evts...)
	f.msgProcID = msgProcID
	return nil
}

// newPendingTestRun builds a PENDING Run with a deterministic ID and name
// for handler tests, using HydrateRun to bypass the New* constructors.
func newPendingTestRun(id uuid.UUID, name string) *run.Run {
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

// newTerminalTestRun builds a Run in the given terminal status for idempotency
// tests.
func newTerminalTestRun(id uuid.UUID, name string, status run.SchedulerStatus) *run.Run {
	now := time.Now()
	return run.HydrateRun(
		id, name,
		status,
		run.InitStatusInProgress,
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

func runFailedTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed
// verifies the happy path: a PENDING run is loaded, MarkDispatchFailed
// transitions it to FAILED, SaveRun persists the change, and Outbox.Append
// receives RunFinalized + RunDispatchFailed domain events with the correct
// message processing ID.
func TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed(t *testing.T) {
	scheduleID := uuid.New()
	msgProcID := uuid.New()

	runRepo := &fakeDispatchFailedRunRepo{stored: newPendingTestRun(scheduleID, "test-schedule")}
	outboxPub := &fakeDispatchFailedOutboxPub{}

	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outboxPub)

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
	}, msgProcID)
	require.NoError(t, err)

	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	assert.Equal(t, msgProcID, outboxPub.msgProcID, "msgProcID must be forwarded to Append")

	// MarkDispatchFailed emits RunFinalized (status=FAILED) + RunDispatchFailed.
	require.Len(t, outboxPub.appended, 2)

	var finalized run.RunFinalized
	var dispatchFailed run.RunDispatchFailed
	for _, e := range outboxPub.appended {
		switch ev := e.(type) {
		case run.RunFinalized:
			finalized = ev
		case run.RunDispatchFailed:
			dispatchFailed = ev
		}
	}
	assert.Equal(t, scheduleID, finalized.ID)
	assert.Equal(t, "test-schedule", finalized.Name)
	assert.Equal(t, run.SchedulerStatusFailed, finalized.Outcome)
	assert.Equal(t, scheduleID, dispatchFailed.ID)
	assert.Equal(t, string(pkgevents.DispatchFailedReasonTargetNotFound), dispatchFailed.Reason)
}

// TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal verifies
// that re-delivery against a run already in any terminal state is a no-op:
// MarkDispatchFailed returns nil events, so Append receives an empty slice
// and the run is saved without changes.
func TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal(t *testing.T) {
	for _, status := range []run.SchedulerStatus{
		run.SchedulerStatusSucceeded,
		run.SchedulerStatusFailed,
		run.SchedulerStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			scheduleID := uuid.New()

			runRepo := &fakeDispatchFailedRunRepo{stored: newTerminalTestRun(scheduleID, "test-schedule", status)}
			outboxPub := &fakeDispatchFailedOutboxPub{}

			u := &uow.FakeUnitOfWork{}
			u.SetRunRepo(runRepo)
			u.SetOutboxPublisher(outboxPub)

			h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
			err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
				ScheduleID:   scheduleID,
				ScheduleName: "test-schedule",
				Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
			}, uuid.New())
			require.NoError(t, err)

			assert.True(t, runRepo.saveCalled, "SaveRun is called even for already-terminal runs")
			assert.Empty(t, outboxPub.appended,
				"already-terminal run must not emit any domain events")
		})
	}
}

// TestRunEntriesDispatchFailedHandler_LoadErrorPropagates verifies that a
// LoadRunForUpdate failure short-circuits the handler before any mutation.
func TestRunEntriesDispatchFailedHandler_LoadErrorPropagates(t *testing.T) {
	runRepo := &fakeDispatchFailedRunRepo{loadErr: errors.New("row lock failed")}
	outboxPub := &fakeDispatchFailedOutboxPub{}

	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outboxPub)

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   uuid.New(),
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonEmptyProjection,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "load run")
	assert.False(t, runRepo.saveCalled, "SaveRun must not be called after load failure")
	assert.Empty(t, outboxPub.appended)
}

// TestRunEntriesDispatchFailedHandler_SaveErrorPropagates verifies that a
// SaveRun failure surfaces an error and prevents the outbox append.
func TestRunEntriesDispatchFailedHandler_SaveErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()

	runRepo := &fakeDispatchFailedRunRepo{
		stored:  newPendingTestRun(scheduleID, "test-schedule"),
		saveErr: errors.New("db down"),
	}
	outboxPub := &fakeDispatchFailedOutboxPub{}

	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outboxPub)

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "save run")
	assert.Empty(t, outboxPub.appended, "save failure must short-circuit the outbox append")
}

// TestRunEntriesDispatchFailedHandler_AppendErrorPropagates verifies that an
// outbox append failure surfaces an error after the run has been saved,
// signalling the binding to roll back the entire transaction.
func TestRunEntriesDispatchFailedHandler_AppendErrorPropagates(t *testing.T) {
	scheduleID := uuid.New()

	runRepo := &fakeDispatchFailedRunRepo{stored: newPendingTestRun(scheduleID, "test-schedule")}
	outboxPub := &fakeDispatchFailedOutboxPub{err: errors.New("outbox down")}

	u := &uow.FakeUnitOfWork{}
	u.SetRunRepo(runRepo)
	u.SetOutboxPublisher(outboxPub)

	h := handlers.NewRunEntriesDispatchFailedHandler(runFailedTestLogger())
	err := h.Handle(context.Background(), u, events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		Reason:       pkgevents.DispatchFailedReasonTargetNotFound,
	}, uuid.New())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append outbox")
	assert.True(t, runRepo.saveCalled, "SaveRun is called before the outbox append")
}
