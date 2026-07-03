package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	schedulerpkg "github.com/carolsimone/continuo/state/internal/scheduler"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestEffectiveCancelledBy pins how the cancelling actor is resolved: the
// authenticated metadata user wins; a system caller keeps a request-supplied
// label (e.g. the watchdog); and a system caller with no label falls back to
// the system sentinel so the persisted row never goes blank.
func TestEffectiveCancelledBy(t *testing.T) {
	// ctxWithUser threads userID through the real identity interceptor so
	// FromContext returns a non-system Identity, mirroring an authenticated RPC.
	ctxWithUser := func(userID string) context.Context {
		in := metadata.NewIncomingContext(context.Background(),
			metadata.New(map[string]string{identity.MetadataKey: userID}))
		var out context.Context
		_, _ = identity.UnaryServerInterceptor()(in, nil, &grpc.UnaryServerInfo{},
			func(c context.Context, _ any) (any, error) { out = c; return nil, nil })
		return out
	}

	cases := []struct {
		name      string
		ctx       context.Context
		requestBy string
		want      string
	}{
		{"authenticated user is authoritative", ctxWithUser("okta|alice"), "ignored-body", "okta|alice"},
		{"system caller keeps request label", context.Background(), "watchdog", "watchdog"},
		{"system caller with empty request defaults to system", context.Background(), "", identity.SystemUserID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, effectiveCancelledBy(tc.ctx, tc.requestBy))
		})
	}
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// noopUoWFactory returns a UoW factory whose UoW always panics on use. Used for
// handler tests that do not exercise the activation or cancel paths.
func noopUoWFactory() func() uow.UnitOfWork {
	return func() uow.UnitOfWork {
		panic("uow not expected to be called in this test")
	}
}

// ---- stubs for schedule catalog ----

// stubCatalogRepo satisfies both postgres.ScheduleCatalogRepository and
// repository.ScheduleCatalogRepository for handler unit tests.
type stubCatalogRepo struct {
	existsActive map[string]bool
}

func (s *stubCatalogRepo) UpsertAll(_ context.Context, _ []string, _ map[string]run.ServiceMetadata) error {
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
func (s *stubCatalogRepo) GetServiceMetadata(_ context.Context, _ string) (map[string]run.ServiceMetadata, error) {
	return map[string]run.ServiceMetadata{}, nil
}
func (s *stubCatalogRepo) UpsertAllTx(_ context.Context, _ *sqlx.Tx, _ []string, _ map[string]map[string]run.ServiceMetadata) error {
	return nil
}
func (s *stubCatalogRepo) SoftDeleteAbsentTx(_ context.Context, _ *sqlx.Tx, _ []string) error {
	return nil
}
func (s *stubCatalogRepo) ListAll(_ context.Context) ([]postgres.ScheduleCatalogRow, error) {
	return nil, nil
}
func (s *stubCatalogRepo) ListAllForUpdateTx(_ context.Context, _ *sqlx.Tx) ([]postgres.ScheduleCatalogRow, error) {
	return nil, nil
}
func (s *stubCatalogRepo) GetCatalog(_ context.Context) (*catalog.ScheduleCatalog, error) {
	return nil, nil
}
func (s *stubCatalogRepo) LoadCatalogForUpdate(_ context.Context) (*catalog.ScheduleCatalog, error) {
	return nil, nil
}
func (s *stubCatalogRepo) SaveCatalog(_ context.Context, _ *catalog.ScheduleCatalog) error {
	return nil
}

type stubSchedulerRepo struct {
	hasActive       bool
	lastRunData     map[string]postgres.LastRunData
	getActiveResult *postgres.SchedulerTracker
	getActiveErr    error
	cancelErr       error
	getByIDResult   *postgres.SchedulerTracker
	getByIDErr      error
	stuckCandidates []postgres.StuckCandidate
	stuckErr        error
	lastStuckCutoff time.Time
}

func (s *stubSchedulerRepo) ListStuckCandidates(_ context.Context, cutoff time.Time) ([]postgres.StuckCandidate, error) {
	s.lastStuckCutoff = cutoff
	return s.stuckCandidates, s.stuckErr
}

func (s *stubSchedulerRepo) Create(_ context.Context, _ *postgres.SchedulerTracker) error { return nil }
func (s *stubSchedulerRepo) GetByID(_ context.Context, _ uuid.UUID) (*postgres.SchedulerTracker, error) {
	return s.getByIDResult, s.getByIDErr
}
func (s *stubSchedulerRepo) GetActiveScheduler(_ context.Context, _ string) (*postgres.SchedulerTracker, error) {
	return s.getActiveResult, s.getActiveErr
}
func (s *stubSchedulerRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return s.hasActive, nil
}
func (s *stubSchedulerRepo) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	if s.lastRunData != nil {
		return s.lastRunData, nil
	}
	return map[string]postgres.LastRunData{}, nil
}
func (s *stubSchedulerRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *postgres.SchedulerTracker) error {
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
func (s *stubSchedulerRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*postgres.SchedulerTracker, error) {
	return s.getByIDResult, s.getByIDErr
}
func (s *stubSchedulerRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (s *stubSchedulerRepo) UpdateRunRowTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ postgres.RunRowUpdate) error {
	return nil
}
func (s *stubSchedulerRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string, _ time.Time) error {
	return s.cancelErr
}

// ---- fake run repo for activation tests ----

// activateFakeRunRepo satisfies repository.RunRepository for ActivateSchedule /
// TriggerSchedule handler tests. HasActiveSchedule drives the policy check;
// SaveRun records the run so tests can inspect it.
type activateFakeRunRepo struct {
	hasActive bool
	savedRun  *run.Run
	saveErr   error
}

func (f *activateFakeRunRepo) GetRun(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("GetRun not used in activate tests")
}
func (f *activateFakeRunRepo) LoadRunForUpdate(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	panic("LoadRunForUpdate not used in activate tests")
}
func (f *activateFakeRunRepo) SaveRun(_ context.Context, r *run.Run) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.savedRun = r
	return nil
}
func (f *activateFakeRunRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return f.hasActive, nil
}
func (f *activateFakeRunRepo) GetActiveScheduler(_ context.Context, _ string) (*run.Run, error) {
	panic("GetActiveScheduler not used in activate tests")
}
func (f *activateFakeRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not used in activate tests")
}

// ---- fake outbox for activation tests ----

// activateFakeOutbox satisfies the OutboxPublisher port for activation tests.
type activateFakeOutbox struct {
	appended  []run.DomainEvent
	appendErr error
}

func (f *activateFakeOutbox) Append(_ context.Context, evts []run.DomainEvent, _ uuid.UUID) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appended = append(f.appended, evts...)
	return nil
}

// newActivateUoWFactory wires a FakeUnitOfWork for activation handler tests
// and returns both the fake (for inspection) and a factory.
func newActivateUoWFactory(catalogRepo *stubCatalogRepo, runRepo *activateFakeRunRepo, outbox *activateFakeOutbox) (*uow.FakeUnitOfWork, func() uow.UnitOfWork) {
	fu := &uow.FakeUnitOfWork{}
	fu.SetCatalogRepo(catalogRepo)
	fu.SetRunRepo(runRepo)
	fu.SetOutboxPublisher(outbox)
	return fu, func() uow.UnitOfWork { return fu }
}

// ---- ActivateSchedule tests ----

func TestSchedulerHandler_ActivateSchedule_EmptyName(t *testing.T) {
	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "schedule_name is required")
}

func TestSchedulerHandler_ActivateSchedule_Success(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"e2e-schedule": true}}
	runRepo := &activateFakeRunRepo{hasActive: false}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	resp, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "e2e-schedule",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.ScheduleId)
	assert.NotNil(t, runRepo.savedRun, "a new Run must be persisted")
	require.Len(t, outbox.appended, 1, "RunStarted event must be emitted")
}

func TestActivateSchedule_NotInCatalog_ReturnsNotFound(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{}}
	runRepo := &activateFakeRunRepo{}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	_, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "unknown-schedule",
	})

	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Nil(t, runRepo.savedRun, "no run must be created when schedule is not in catalog")
}

func TestActivateSchedule_AlreadyActive_ReturnsEmptyID(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	runRepo := &activateFakeRunRepo{hasActive: true}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	resp, err := h.ActivateSchedule(context.Background(), &statev1.ActivateScheduleRequest{
		ScheduleName: "daily",
	})

	require.NoError(t, err)
	assert.Empty(t, resp.ScheduleId, "cron activation silently skips when a run is already active")
}

// ---- TriggerSchedule tests ----

func TestTriggerSchedule_Success(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	runRepo := &activateFakeRunRepo{hasActive: false}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	resp, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ScheduleId)
}

func TestTriggerSchedule_AlreadyRunning(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	runRepo := &activateFakeRunRepo{hasActive: true}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	_, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "daily",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestTriggerSchedule_NotFound(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{}}
	runRepo := &activateFakeRunRepo{}
	outbox := &activateFakeOutbox{}
	_, factory := newActivateUoWFactory(catalogRepo, runRepo, outbox)

	activate := svchandlers.NewActivateScheduleHandler(newTestLogger())
	h := NewSchedulerHandler(nil, activate, nil, nil, factory, newTestLogger())

	_, err := h.TriggerSchedule(context.Background(), &statev1.TriggerScheduleRequest{
		ScheduleName: "nonexistent",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestListAllSchedules_Empty(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{}}
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, catalogRepo, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ListAllSchedules(context.Background(), &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Schedules)
}

func TestListStuckCandidates_RequiresCutoff(t *testing.T) {
	repo := &stubSchedulerRepo{}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	_, err := h.ListStuckCandidates(context.Background(), &statev1.ListStuckCandidatesRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestListStuckCandidates_ForwardsCutoffAndMapsCandidates(t *testing.T) {
	id := uuid.New()
	cutoff := time.Now().Add(-30 * time.Minute)
	repo := &stubSchedulerRepo{
		stuckCandidates: []postgres.StuckCandidate{{ScheduleName: "stuck", ScheduleID: id}},
	}
	h := NewSchedulerHandler(repo, nil, nil, nil, noopUoWFactory(), newTestLogger())

	resp, err := h.ListStuckCandidates(context.Background(), &statev1.ListStuckCandidatesRequest{
		Cutoff: timestamppb.New(cutoff),
	})
	require.NoError(t, err)
	assert.WithinDuration(t, cutoff, repo.lastStuckCutoff, time.Millisecond)
	require.Len(t, resp.Candidates, 1)
	assert.Equal(t, "stuck", resp.Candidates[0].ScheduleName)
	assert.Equal(t, id.String(), resp.Candidates[0].ScheduleId)
}

func TestListAllSchedules_MergesData(t *testing.T) {
	catalogRepo := &stubCatalogRepo{existsActive: map[string]bool{"daily": true}}
	repo := &stubSchedulerRepo{
		lastRunData: map[string]postgres.LastRunData{
			"daily": {ScheduleName: "daily", ScheduleID: uuid.New(), Status: run.SchedulerStatusSucceeded, IsRunning: false},
		},
	}
	schedulesConfig := &schedulerpkg.SchedulesConfig{
		Timezone: "Europe/Paris",
		Schedules: []schedulerpkg.ScheduleEntry{
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
				Status:       run.SchedulerStatusSucceeded,
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

func (f *cancelFakeRunRepo) LoadRunForUpdate(_ context.Context, _ uuid.UUID) (*run.Run, error) {
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.stored, nil
}

func (f *cancelFakeRunRepo) SaveRun(_ context.Context, r *run.Run) error {
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

func (f *cancelFakeRunRepo) GetLastRunPerSchedule(_ context.Context) (map[string]repository.LastRunSummary, error) {
	panic("GetLastRunPerSchedule not used in cancel tests")
}

// cancelFakeOutbox is an in-memory OutboxPublisher for cancel handler tests.
type cancelFakeOutbox struct {
	appended  []run.DomainEvent
	appendErr error
}

func (f *cancelFakeOutbox) Append(_ context.Context, evts []run.DomainEvent, _ uuid.UUID) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appended = append(f.appended, evts...)
	return nil
}

// assertCancelEvents verifies a cancel emits exactly two domain events:
// RunCancelled (the work-suppression guard, → schedule.cancelled:v1) and
// RunFinalized{cancelled} (the terminal projection, → run.finalized:v1).
func assertCancelEvents(t *testing.T, evts []run.DomainEvent, by string) {
	t.Helper()
	require.Len(t, evts, 2, "cancel appends RunCancelled + RunFinalized")
	var sawCancelled, sawFinalized bool
	for _, e := range evts {
		switch ev := e.(type) {
		case run.RunCancelled:
			sawCancelled = true
			assert.Equal(t, by, ev.By)
		case run.RunFinalized:
			sawFinalized = true
			assert.Equal(t, run.SchedulerStatusCancelled, ev.Outcome)
		}
	}
	assert.True(t, sawCancelled, "RunCancelled must be appended")
	assert.True(t, sawFinalized, "RunFinalized must be appended")
}

// cancelFakeTaskCollection is an in-memory TaskCollection for cancel handler tests.
// BulkCancel is the only method called during Run.Cancel.
type cancelFakeTaskCollection struct {
	bulkCancelCalled bool
	bulkCancelN      int
	bulkCancelErr    error
}

func (f *cancelFakeTaskCollection) BulkCancel(_ context.Context, _ uuid.UUID, _ string, _ time.Time) (int, error) {
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
func (f *cancelFakeTaskCollection) LoadStatusAndAttempt(_ context.Context, _ uuid.UUID) (run.TaskStatus, int32, bool, error) {
	panic("LoadStatusAndAttempt not used in cancel tests")
}
func (f *cancelFakeTaskCollection) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	panic("Exists not used in cancel tests")
}
func (f *cancelFakeTaskCollection) SetStatusAndAttempt(_ context.Context, _ uuid.UUID, _ run.TaskStatus, _ int32) (int, error) {
	panic("SetStatusAndAttempt not used in cancel tests")
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
		identity.SystemUserID,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		nil,
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
		identity.SystemUserID,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		nil,
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
	assertCancelEvents(t, outbox.appended, "ops-user")
}

// fixedClock is a ports.Clock that always returns a pinned instant, letting a
// test assert exact timestamp equality across the response and the persisted
// aggregate.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

// TestCancelScheduler_CompletedAtMatchesPersistedRow is the acceptance test for
// timestamp consistency on cancel: the completed_at reported in the gRPC
// response must equal the value persisted for the row. The handler stamps the
// aggregate from ports.Clock at one instant; the response is built from that
// aggregate (CompletedAt()) and the postgres adapter persists the aggregate's
// CancelledAt()/CompletedAt() verbatim via CancelTx. With a pinned clock this
// asserts response.CompletedAt == aggregate.CompletedAt == aggregate.CancelledAt
// == the clock instant — i.e. one timestamp per logical event, response equals
// persisted value.
func TestCancelScheduler_CompletedAtMatchesPersistedRow(t *testing.T) {
	id := uuid.New()
	pinned := time.Date(2026, 6, 11, 9, 30, 0, 0, time.UTC)
	pendingRun := newPendingRun(id, "daily")
	runRepo := &cancelFakeRunRepo{stored: pendingRun}
	fu, factory := newCancelUoWFactory(runRepo, &cancelFakeOutbox{}, &cancelFakeTaskCollection{})
	fu.SetClock(fixedClock{now: pinned})
	h := NewSchedulerHandler(nil, nil, nil, nil, factory, newTestLogger())

	resp, err := h.CancelScheduler(context.Background(), &statev1.CancelSchedulerRequest{
		ScheduleId:  id.String(),
		CancelledBy: "ops-user",
	})
	require.NoError(t, err)

	// The response's completed_at is the pinned clock instant.
	require.NotNil(t, resp.Scheduler.CompletedAt)
	assert.True(t, resp.Scheduler.CompletedAt.AsTime().Equal(pinned),
		"response completed_at must equal the clock instant")

	// The persisted aggregate (what the postgres adapter feeds CancelTx as
	// cancelled_at == completed_at) carries the identical instant, so the stored
	// row equals the response.
	require.NotNil(t, runRepo.stored.CompletedAt())
	require.NotNil(t, runRepo.stored.CancelledAt())
	assert.True(t, runRepo.stored.CompletedAt().Equal(pinned),
		"persisted completed_at must equal the clock instant")
	assert.True(t, runRepo.stored.CancelledAt().Equal(pinned),
		"persisted cancelled_at must equal the clock instant")
	assert.True(t, resp.Scheduler.CompletedAt.AsTime().Equal(*runRepo.stored.CompletedAt()),
		"response completed_at must equal the persisted row value")
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
	assertCancelEvents(t, outbox.appended, "ops-user")
}

func TestCancelSchedule_AlreadyTerminal(t *testing.T) {
	id := uuid.New()
	cancelledRun := run.HydrateRun(
		id, "daily",
		run.SchedulerStatusCancelled,
		run.InitStatusCompleted,
		run.Kind("cron"),
		nil,
		identity.SystemUserID,
		time.Now(),
		nil, nil, nil, nil,
		nil, nil,
		nil,
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

// ---- GetSchedulerInitStatus tests ----

func TestGetSchedulerInitStatus_ReturnsStatus(t *testing.T) {
	tracker := &postgres.SchedulerTracker{
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
