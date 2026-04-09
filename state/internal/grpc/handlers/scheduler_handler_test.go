package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/scheduler"
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

func (s *stubActivator) ActivateSchedule(_ context.Context, name string) (uuid.UUID, error) {
	s.calledWith = name
	return s.id, s.err
}

func (s *stubActivator) PrepareActivation(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSchedulerHandler_ActivateSchedule_Success(t *testing.T) {
	id := uuid.New()
	stub := &stubActivator{id: id}
	catalog := &stubCatalogRepo{existsActive: map[string]bool{"e2e-schedule": true}}
	h := NewSchedulerHandler(nil, stub, catalog, nil, newTestLogger())

	resp, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "e2e-schedule",
	})

	require.NoError(t, err)
	assert.Equal(t, id.String(), resp.ScheduleId)
	assert.Equal(t, "e2e-schedule", stub.calledWith)
}

func TestSchedulerHandler_ActivateSchedule_EmptyName(t *testing.T) {
	h := NewSchedulerHandler(nil, &stubActivator{}, nil, nil, newTestLogger())

	_, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "schedule_name is required")
}

func TestActivateSchedule_NotInCatalog_ReturnsNotFound(t *testing.T) {
	catalog := &stubCatalogRepo{existsActive: map[string]bool{}}
	stub := &stubActivator{id: uuid.New()}
	h := NewSchedulerHandler(nil, stub, catalog, nil, newTestLogger())

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
	h := NewSchedulerHandler(nil, stub, catalog, nil, newTestLogger())

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

func (s *stubCatalogRepo) UpsertAll(_ context.Context, _ []string, _ map[string]string) error { return nil }
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
func (s *stubCatalogRepo) GetManifestVersions(_ context.Context, _ string) (map[string]string, error) {
	return map[string]string{}, nil
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

// ---- TriggerSchedule tests ----

func TestTriggerSchedule_Success(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{hasActive: false}
	activator := &stubActivator{id: uuid.New()}
	h := NewSchedulerHandler(repo, activator, catalogRepo, nil, newTestLogger())

	resp, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ScheduleId)
}

func TestTriggerSchedule_AlreadyRunning(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{hasActive: true}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, newTestLogger())

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
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, newTestLogger())

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
	h := NewSchedulerHandler(repo, activator, catalogRepo, nil, newTestLogger())

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
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, newTestLogger())

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
	h := NewSchedulerHandler(repo, nil, catalogRepo, schedulesConfig, newTestLogger())

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
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, newTestLogger())

	resp, err := h.ListAllSchedules(context.Background(), &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Schedules, 1)
	assert.Equal(t, id.String(), resp.Schedules[0].LastRunId)
}

// ---- CancelSchedule tests ----

func TestCancelSchedule_Success(t *testing.T) {
	tracker := &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "daily"}
	repo := &stubSchedulerRepo{getActiveResult: tracker}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	resp, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
		CancelledBy:  "user",
	})
	require.NoError(t, err)
	assert.Equal(t, tracker.ScheduleID.String(), resp.ScheduleId)
}

func TestCancelSchedule_EmptyScheduleName(t *testing.T) {
	h := NewSchedulerHandler(&stubSchedulerRepo{}, nil, nil, nil, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestCancelSchedule_NoActiveRun(t *testing.T) {
	// getActiveResult is nil, getActiveErr is nil
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestCancelSchedule_GetActiveSchedulerError(t *testing.T) {
	repo := &stubSchedulerRepo{getActiveErr: errors.New("db error")}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
}

func TestCancelSchedule_CancelNotCancellable(t *testing.T) {
	tracker := &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "daily"}
	repo := &stubSchedulerRepo{
		getActiveResult: tracker,
		cancelErr:       postgres.ErrNotCancellable,
	}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestCancelSchedule_CancelInternalError(t *testing.T) {
	tracker := &model.SchedulerTracker{ScheduleID: uuid.New(), ScheduleName: "daily"}
	repo := &stubSchedulerRepo{
		getActiveResult: tracker,
		cancelErr:       errors.New("db connection lost"),
	}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.CancelSchedule(context.Background(), &statev1.CancelScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
}

// ---- CancelScheduler fix test ----

func TestCancelScheduler_AlreadyTerminal(t *testing.T) {
	repo := &stubSchedulerRepo{cancelErr: postgres.ErrNotCancellable}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	schedID := uuid.NewString()
	_, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId: schedID,
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// ---- fakeSchedulerRepo for transition tests ----

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

// ---- UpdateScheduler transition tests ----

func TestUpdateScheduler_RejectsInvalidTransition(t *testing.T) {
	id := uuid.New()
	repo := &fakeSchedulerRepo{
		tracker: &model.SchedulerTracker{
			ScheduleID: id,
			Status:     model.SchedulerStatusPending,
		},
	}
	h := NewSchedulerHandler(repo, &stubActivator{}, nil, nil, newTestLogger())

	_, err := h.UpdateScheduler(context.Background(), &statev1.UpdateSchedulerRequest{
		ScheduleId: id.String(),
		Status:     statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED, // pending → succeeded is not allowed
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, model.SchedulerStatusPending, repo.tracker.Status, "status must not change on invalid transition")
}

func TestUpdateScheduler_AllowsValidTransition(t *testing.T) {
	id := uuid.New()
	repo := &fakeSchedulerRepo{
		tracker: &model.SchedulerTracker{
			ScheduleID: id,
			Status:     model.SchedulerStatusRunning,
		},
	}
	h := NewSchedulerHandler(repo, &stubActivator{}, nil, nil, newTestLogger())

	resp, err := h.UpdateScheduler(context.Background(), &statev1.UpdateSchedulerRequest{
		ScheduleId: id.String(),
		Status:     statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED,
	})

	require.NoError(t, err)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED, resp.Scheduler.Status)
}

// ---- GetSchedulerInitStatus tests ----

func TestGetSchedulerInitStatus_ReturnsStatus(t *testing.T) {
	tracker := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		InitializationStatus: "completed",
	}
	repo := &stubSchedulerRepo{getByIDResult: tracker}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	resp, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: tracker.ScheduleID.String(),
	})

	require.NoError(t, err)
	assert.Equal(t, "completed", resp.InitializationStatus)
}

func TestGetSchedulerInitStatus_NotFound(t *testing.T) {
	repo := &stubSchedulerRepo{getByIDErr: postgres.ErrNotFound}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: uuid.NewString(),
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestGetSchedulerInitStatus_EmptyScheduleID(t *testing.T) {
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestGetSchedulerInitStatus_InvalidScheduleIDFormat(t *testing.T) {
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, newTestLogger())

	_, err := h.GetSchedulerInitStatus(context.Background(), &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: "not-a-uuid",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestUpdateSchedulerInitStatus_AutoTransitionsToRunningOnCompleted(t *testing.T) {
	id := uuid.New()
	repo := &fakeSchedulerRepo{
		tracker: &model.SchedulerTracker{
			ScheduleID:           id,
			Status:               model.SchedulerStatusPending,
			InitializationStatus: "in_progress",
		},
	}
	h := NewSchedulerHandler(repo, &stubActivator{}, nil, nil, newTestLogger())

	resp, err := h.UpdateSchedulerInitStatus(context.Background(), &statev1.UpdateSchedulerInitStatusRequest{
		ScheduleId:           id.String(),
		InitializationStatus: "completed",
	})

	require.NoError(t, err)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING, resp.Scheduler.Status)
	assert.Equal(t, model.SchedulerStatusRunning, repo.tracker.Status)
}

func TestUpdateSchedulerInitStatus_NoAutoTransitionWhenAlreadyRunning(t *testing.T) {
	// Idempotency: if scheduler is already running, calling completed again must not error.
	id := uuid.New()
	repo := &fakeSchedulerRepo{
		tracker: &model.SchedulerTracker{
			ScheduleID:           id,
			Status:               model.SchedulerStatusRunning,
			InitializationStatus: "completed",
		},
	}
	h := NewSchedulerHandler(repo, &stubActivator{}, nil, nil, newTestLogger())

	resp, err := h.UpdateSchedulerInitStatus(context.Background(), &statev1.UpdateSchedulerInitStatusRequest{
		ScheduleId:           id.String(),
		InitializationStatus: "completed",
	})

	require.NoError(t, err)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING, resp.Scheduler.Status)
}
