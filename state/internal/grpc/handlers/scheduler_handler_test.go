package handlers

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	"github.com/carolsimone/continuo/state/ports"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubActivator is a minimal fake activator for unit tests.
type stubActivator struct {
	id         uuid.UUID
	err        error
	calledWith string
}

func (s *stubActivator) ActivateSchedule(_ context.Context, name string, _ string, _ *uuid.UUID) (uuid.UUID, error) {
	s.calledWith = name
	return s.id, s.err
}

func (s *stubActivator) PrepareActivation(_ context.Context, _ string, _ string, _ *uuid.UUID) (*model.SchedulerTracker, error) {
	return nil, nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// noopUoWFactory returns a UoW factory whose UoW always panics on use. Used for
// handler tests that do not exercise cancel methods.
func noopUoWFactory() func() uow.UnitOfWork {
	return func() uow.UnitOfWork {
		panic("uow not expected to be called in this test")
	}
}

func TestSchedulerHandler_ActivateSchedule_Success(t *testing.T) {
	id := uuid.New()
	stub := &stubActivator{id: id}
	catalog := &stubCatalogRepo{existsActive: map[string]bool{"e2e-schedule": true}}
	h := NewSchedulerHandler(nil, stub, catalog, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "e2e-schedule",
	})

	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.ScheduleId)
	assert.Equal(t, "e2e-schedule", stub.calledWith)
}

func TestSchedulerHandler_ActivateSchedule_EmptyName(t *testing.T) {
	h := NewSchedulerHandler(nil, &stubActivator{}, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "schedule_name is required")
}

func TestActivateSchedule_NotInCatalog_ReturnsNotFound(t *testing.T) {
	catalog := &stubCatalogRepo{existsActive: map[string]bool{}}
	stub := &stubActivator{id: uuid.New()}
	h := NewSchedulerHandler(nil, stub, catalog, nil, noopUoWFactory(), newTestLogger())

	_, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "unknown-schedule",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Empty(t, stub.calledWith, "activator must not be called when schedule is not in catalog")
}

func TestActivateSchedule_CatalogHasSchedule_Activates(t *testing.T) {
	id := uuid.New()
	catalog := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	stub := &stubActivator{id: id}
	h := NewSchedulerHandler(nil, stub, catalog, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "daily",
	})

	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.ScheduleId)
}

// ---- stubs for new dependencies ----

type stubCatalogRepo struct {
	existsActive map[string]bool
}

func (s *stubCatalogRepo) UpsertAll(_ context.Context, _ []string, _ map[string]model.ServiceMetadata) error {
	return nil
}
func (s *stubCatalogRepo) SoftDeleteAbsent(_ context.Context, _ []string) error { return nil }
func (s *stubCatalogRepo) ListActive(_ context.Context) ([]string, error) {
	names := make([]string, 0)
	for k, v := range s.existsActive {
		if v {
			names = append(names, k)
		}
	}
	return names, nil
}
func (s *stubCatalogRepo) ExistsActive(_ context.Context, name string) (bool, error) {
	return s.existsActive[name], nil
}
func (s *stubCatalogRepo) GetServiceMetadata(_ context.Context, _ string) (map[string]model.ServiceMetadata, error) {
	return map[string]model.ServiceMetadata{}, nil
}
func (s *stubCatalogRepo) UpsertAllTx(_ context.Context, _ *sqlx.Tx, _ []string, _ map[string]map[string]model.ServiceMetadata) error {
	return nil
}
func (s *stubCatalogRepo) SoftDeleteAbsentTx(_ context.Context, _ *sqlx.Tx, _ []string) error {
	return nil
}
func (s *stubCatalogRepo) ListAll(_ context.Context) ([]postgres.ScheduleCatalogRow, error) {
	return nil, nil
}

type stubSchedulerRepo struct {
	hasActive       bool
	lastRunData     map[string]postgres.LastRunData
	getActiveResult *model.SchedulerTracker
	getActiveErr    error
	cancelErr       error
	getByIDResult   *model.SchedulerTracker
	getByIDErr      error
}

func (s *stubSchedulerRepo) Create(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *stubSchedulerRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return s.getByIDResult, s.getByIDErr
}
func (s *stubSchedulerRepo) Update(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (s *stubSchedulerRepo) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error {
	return s.cancelErr
}

func (s *stubSchedulerRepo) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return s.getActiveResult, s.getActiveErr
}
func (s *stubSchedulerRepo) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (s *stubSchedulerRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return s.hasActive, nil
}
func (s *stubSchedulerRepo) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) ResetInProgressInitializations(_ context.Context) (int, error) {
	return 0, nil
}
func (s *stubSchedulerRepo) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	if s.lastRunData != nil {
		return s.lastRunData, nil
	}
	return map[string]postgres.LastRunData{}, nil
}
func (s *stubSchedulerRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (s *stubSchedulerRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (s *stubSchedulerRepo) IncrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (int32, int32, error) {
	return 0, 0, nil
}
func (s *stubSchedulerRepo) DecrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (s *stubSchedulerRepo) SetTotalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (s *stubSchedulerRepo) SetTerminalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (s *stubSchedulerRepo) UpdateStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return s.getByIDResult, s.getByIDErr
}
func (s *stubSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string) error {
	return s.cancelErr
}

// ---- TriggerSchedule tests ----

func TestTriggerSchedule_Success(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{hasActive: false}
	activator := &stubActivator{id: uuid.New()}
	h := NewSchedulerHandler(repo, activator, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ScheduleId)
}

func TestTriggerSchedule_AlreadyRunning(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{hasActive: true}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	_, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestTriggerSchedule_NotFound(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{}}
	repo := &stubSchedulerRepo{hasActive: false}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	_, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "nonexistent",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestTriggerSchedule_ActivatorReturnsNilUUID(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{hasActive: false}
	// activator returns uuid.Nil (race: schedule became active between our check and activation)
	activator := &stubActivator{id: uuid.Nil}
	h := NewSchedulerHandler(repo, activator, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	_, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestListAllSchedules_Empty(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{}}
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ListAllSchedules(context.Background(), &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Schedules)
}

func TestListAllSchedules_MergesData(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{
		lastRunData: map[string]postgres.LastRunData{
			"daily": {ScheduleName: "daily", ScheduleID: uuid.New(), Status: model.SchedulerStatusSucceeded, IsRunning: false},
		},
	}
	schedulesConfig := &scheduler.SchedulesConfig{
		Timezone: "Europe/Paris",
		Schedules: []scheduler.ScheduleEntry{
			{Name: "daily", Cron: "0 1 * * *", Description: "Runs daily"},
		},
	}
	h := NewSchedulerHandler(repo, nil, catalogRepo, schedulesConfig, noopUoWFactory(), newTestLogger())

	resp, err := h.ListAllSchedules(context.Background(), &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Schedules, 1)
	assert.Equal(t, "daily", resp.Schedules[0].ScheduleName)
	assert.Equal(t, "0 1 * * *", resp.Schedules[0].CronExpression)
	assert.Equal(t, "succeeded", resp.Schedules[0].LastRunStatus)
	assert.False(t, resp.Schedules[0].IsRunning)
	assert.Equal(t, "Europe/Paris", resp.Schedules[0].Timezone)
}

func TestListAllSchedules_PopulatesLastRunId(t *testing.T) {
	id := uuid.New()
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{
		lastRunData: map[string]postgres.LastRunData{
			"daily": {
				ScheduleName: "daily",
				ScheduleID:   id,
				Status:       model.SchedulerStatusSucceeded,
				CreatedAt:    time.Now(),
				IsRunning:    false,
			},
		},
	}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ListAllSchedules(context.Background(), &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Schedules, 1)
	assert.Equal(t, id.String(), resp.Schedules[0].LastRunId)
}

// ---- fake Run repository for cancel tests ----

// cancelFakeRunRepo is an in-memory RunRepository for CancelScheduler /
// CancelSchedule handler unit tests.
type cancelFakeRunRepo struct {
	// stored is the run returned by LoadRunForUpdate and GetActiveScheduler.
	stored *run.Run
	// loadErr is returned by LoadRunForUpdate when non-nil.
	loadErr error
	// getActiveErr is returned by GetActiveScheduler when non-nil.
	getActiveErr error
	// saveErr is returned by SaveRun when non-nil.
	saveErr error
	// saveCalled tracks whether SaveRun was invoked.
	saveCalled bool
}

func (f *cancelFakeRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not used in cancel tests")
}

func (f *cancelFakeRunRepo) LoadRunForUpdate(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}

func (f *cancelFakeRunRepo) SaveRun(_ context.Context, _ *sqlx.Tx, r *run.Run) error {
	f.saveCalled = true
	if f.saveErr != nil {
		return f.saveErr
	}
	f.stored = r
	return nil
}

func (f *cancelFakeRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	panic("HasActiveSchedule not used in cancel tests")
}

func (f *cancelFakeRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	if f.getActiveErr != nil {
		return nil, f.getActiveErr
	}
	return f.stored, nil
}

func (f *cancelFakeRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]ports.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not used in cancel tests")
}

// cancelFakeOutbox is an in-memory OutboxPublisher for cancel handler tests.
type cancelFakeOutbox struct {
	appended  []run.DomainEvent
	appendErr error
}

func (f *cancelFakeOutbox) Append(_ context.Context, _ *sqlx.Tx, evts []run.DomainEvent, _ uuid.UUID) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appended = append(f.appended, evts...)
	return nil
}

// cancelFakeTaskCollection is an in-memory TaskCollection for cancel handler tests.
// BulkCancel is the only method called during Run.Cancel.
type cancelFakeTaskCollection struct {
	bulkCancelCalled bool
	bulkCancelN      int
	bulkCancelErr    error
}

func (f *cancelFakeTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string) (int, error) {
	f.bulkCancelCalled = true
	return f.bulkCancelN, f.bulkCancelErr
}
func (f *cancelFakeTaskCollection) BulkCreate(_ context.Context, _ []run.Task) error {
	panic("BulkCreate not used in cancel tests")
}
func (f *cancelFakeTaskCollection) GetByNode(_ context.Context, _ uuid.UUID, _ run.NodeID) (run.Task, error) {
	panic("GetByNode not used in cancel tests")
}
func (f *cancelFakeTaskCollection) GetStatus(_ context.Context, _ uuid.UUID) (run.TaskStatus, bool, error) {
	panic("GetStatus not used in cancel tests")
}
func (f *cancelFakeTaskCollection) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("Exists not used in cancel tests")
}
func (f *cancelFakeTaskCollection) UpdateStatusIfChanged(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	panic("UpdateStatusIfChanged not used in cancel tests")
}
func (f *cancelFakeTaskCollection) HasRetryableFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasRetryableFailed not used in cancel tests")
}
func (f *cancelFakeTaskCollection) HasFailed(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasFailed not used in cancel tests")
}
func (f *cancelFakeTaskCollection) Update(_ context.Context, _ run.Task) error {
	panic("Update not used in cancel tests")
}
func (f *cancelFakeTaskCollection) HasNonSucceeded(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("HasNonSucceeded not used in cancel tests")
}

// newCancelUoWFactory wires a FakeUnitOfWork with the three fakes needed by the
// cancel handler methods and returns a factory that always returns the same
// instance so tests can inspect its state after the call.
func newCancelUoWFactory(runRepo *cancelFakeRunRepo, outbox *cancelFakeOutbox, tasks *cancelFakeTaskCollection) (*uow.FakeUnitOfWork, func() uow.UnitOfWork) {
	fu := &uow.FakeUnitOfWork{}
	fu.SetRunRepo(runRepo)
	fu.SetOutboxPublisher(outbox)
	fu.SetTaskCollection(tasks)
	return fu, func() uow.UnitOfWork { return fu }
}

// newPendingRun builds a minimal PENDING Run aggregate for cancel handler tests.
func newPendingRun(scheduleID uuid.UUID, name string) *run.Run {
	return run.HydrateRun(
		scheduleID, name,
		run.SchedulerStatusPending,
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: false},
		0,
		nil,
	)
}

// ---- CancelScheduler tests ----

func TestCancelScheduler_EmptyScheduleID(t *testing.T) {
	_, factory := newCancelUoWFactory(&cancelFakeRunRepo{}, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestCancelScheduler_InvalidScheduleIDFormat(t *testing.T) {
	_, factory := newCancelUoWFactory(&cancelFakeRunRepo{}, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId: "not-a-uuid",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestCancelScheduler_NotFound(t *testing.T) {
	runRepo := &cancelFakeRunRepo{loadErr: postgres.ErrNotFound}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId: uuid.NewString(),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestCancelScheduler_AlreadyTerminal(t *testing.T) {
	id := uuid.New()
	// Build a CANCELLED (terminal) Run.
	cancelledRun := run.HydrateRun(
		id, "daily",
		run.SchedulerStatusCancelled,
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: false},
		0,
		nil,
	)
	runRepo := &cancelFakeRunRepo{stored: cancelledRun}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId: id.String(),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestCancelScheduler_Success(t *testing.T) {
	id := uuid.New()
	pendingRun := newPendingRun(id, "daily")
	runRepo := &cancelFakeRunRepo{stored: pendingRun}
	outbox := &cancelFakeOutbox{}
	tasks := &cancelFakeTaskCollection{}
	_, factory := newCancelUoWFactory(runRepo, outbox, tasks)
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	resp, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId:  id.String(),
		CancelledBy: "ops-user",
	})
	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.Scheduler.ScheduleId)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED, resp.Scheduler.Status)
	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	assert.True(t, tasks.bulkCancelCalled, "BulkCancel must be called")
	require.Len(t, outbox.appended, 1, "one domain event must be appended")
}

func TestCancelScheduler_SaveError(t *testing.T) {
	id := uuid.New()
	pendingRun := newPendingRun(id, "daily")
	runRepo := &cancelFakeRunRepo{stored: pendingRun, saveErr: errors.New("disk full")}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId: id.String(),
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
}

// ---- CancelSchedule tests ----

func TestCancelSchedule_EmptyScheduleName(t *testing.T) {
	_, factory := newCancelUoWFactory(&cancelFakeRunRepo{}, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestCancelSchedule_NoActiveRun(t *testing.T) {
	// GetActiveScheduler returns nil — no active run exists.
	runRepo := &cancelFakeRunRepo{stored: nil}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestCancelSchedule_GetActiveSchedulerError(t *testing.T) {
	runRepo := &cancelFakeRunRepo{getActiveErr: errors.New("db error")}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
}

func TestCancelSchedule_Success(t *testing.T) {
	id := uuid.New()
	pendingRun := newPendingRun(id, "daily")
	runRepo := &cancelFakeRunRepo{stored: pendingRun}
	outbox := &cancelFakeOutbox{}
	tasks := &cancelFakeTaskCollection{}
	_, factory := newCancelUoWFactory(runRepo, outbox, tasks)
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	resp, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
		CancelledBy:  "ops-user",
	})
	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.ScheduleId)
	assert.True(t, runRepo.saveCalled, "SaveRun must be called")
	assert.True(t, tasks.bulkCancelCalled, "BulkCancel must be called")
	require.Len(t, outbox.appended, 1, "one domain event must be appended")
}

func TestCancelSchedule_AlreadyTerminal(t *testing.T) {
	id := uuid.New()
	cancelledRun := run.HydrateRun(
		id, "daily",
		run.SchedulerStatusCancelled,
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		sql.NullInt32{Valid: false},
		0,
		nil,
	)
	runRepo := &cancelFakeRunRepo{stored: cancelledRun}
	_, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestCancelSchedule_OutboxError(t *testing.T) {
	id := uuid.New()
	pendingRun := newPendingRun(id, "daily")
	runRepo := &cancelFakeRunRepo{stored: pendingRun}
	outbox := &cancelFakeOutbox{appendErr: errors.New("outbox unavailable")}
	tasks := &cancelFakeTaskCollection{}
	_, factory := newCancelUoWFactory(runRepo, outbox, tasks)
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
}

// ---- fakeSchedulerRepo for non-cancel tests ----

type fakeSchedulerRepo struct {
	tracker *model.SchedulerTracker
}

func (f *fakeSchedulerRepo) Create(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (f *fakeSchedulerRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.tracker, nil
}
func (f *fakeSchedulerRepo) Update(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (f *fakeSchedulerRepo) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error  { return nil }
func (f *fakeSchedulerRepo) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (f *fakeSchedulerRepo) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (f *fakeSchedulerRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (f *fakeSchedulerRepo) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeSchedulerRepo) ResetInProgressInitializations(_ context.Context) (int, error) {
	return 0, nil
}
func (f *fakeSchedulerRepo) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	return map[string]postgres.LastRunData{}, nil
}
func (f *fakeSchedulerRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (f *fakeSchedulerRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeSchedulerRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (f *fakeSchedulerRepo) IncrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (int32, int32, error) {
	return 0, 0, nil
}
func (f *fakeSchedulerRepo) DecrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (f *fakeSchedulerRepo) SetTotalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (f *fakeSchedulerRepo) SetTerminalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (f *fakeSchedulerRepo) UpdateStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return f.tracker, nil
}
func (f *fakeSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (f *fakeSchedulerRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string) error {
	return nil
}

// ---- GetSchedulerInitStatus tests ----

func TestGetSchedulerInitStatus_ReturnsStatus(t *testing.T) {
	tracker := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		InitializationStatus: "completed",
	}
	repo := &stubSchedulerRepo{getByIDResult: tracker}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: tracker.ScheduleID.String(),
	})

	require.NoError(t, err)
	assert.Equal(t, "completed", resp.InitializationStatus)
}

func TestGetSchedulerInitStatus_NotFound(t *testing.T) {
	repo := &stubSchedulerRepo{getByIDErr: postgres.ErrNotFound}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: uuid.NewString(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestGetSchedulerInitStatus_EmptyScheduleID(t *testing.T) {
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestGetSchedulerInitStatus_InvalidScheduleIDFormat(t *testing.T) {
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: "not-a-uuid",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}
