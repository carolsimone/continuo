package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testdeps"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

type rerunFixture struct {
	Handler       *RerunHandler
	SchedulerRepo postgres.SchedulerTrackerRepository
	TaskRepo      postgres.TaskTrackerRepository
	DB            *sqlx.DB
	Tenant        string
	Cleanup       func()
}

func setupRerunFixture(t *testing.T) *rerunFixture {
	t.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		testdeps.Unavailable(t, "no test DB available: %v", err)
	}
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	clk := ports.SystemClock{}
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, catalogRepo, clk, logger)
	}
	tenant := fixtureTenant(t, db)
	useCase := svchandlers.NewTriggerRerunHandler(runstart.Planner{TenantID: tenant, Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}, logger)
	handler := NewRerunHandler(useCase, factory, logger)
	return &rerunFixture{Handler: handler, SchedulerRepo: schedulerRepo, TaskRepo: taskRepo, DB: db, Tenant: tenant, Cleanup: func() { db.Close() }}
}

func TestRerunHandler_HappyPath_FailedSource(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()

	// Reuse rebase fixture's seeders — they're not rebase-specific.
	fx := &rebaseFixture{
		Handler: nil, SchedulerRepo: rfx.SchedulerRepo, TaskRepo: rfx.TaskRepo,
		DB: rfx.DB, Tenant: rfx.Tenant, Cleanup: func() {},
	}

	scheduleName := "rerun-happy-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, run.SchedulerStatusFailed)

	resp, err := rfx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{
		SourceRunId: srcID.String(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.RunId)
	require.Equal(t, scheduleName, resp.ScheduleName)

	newID := uuid.MustParse(resp.RunId)
	t.Cleanup(func() {
		rfx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		rfx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := rfx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	require.Equal(t, "rerun", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	require.Equal(t, srcID, *tracker.SourceRunID)
	require.Equal(t, run.SchedulerStatusRunning, tracker.Status)

	// The failed task is dispatched again on the source's release.
	requireDispatched(t, rfx.DB, newID, run.TaskIDFor(rfx.Tenant, newID, "s.t"), scheduleName)
}

// A source run that names no release cannot be rerun.
func TestRerunHandler_RejectsASourceWithoutARelease(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()
	fx := &rebaseFixture{SchedulerRepo: rfx.SchedulerRepo, TaskRepo: rfx.TaskRepo, DB: rfx.DB, Tenant: rfx.Tenant, Cleanup: func() {}}

	srcID := seedRunWithTaskStatus(t, fx, "rerun-rej-norelease-"+uuid.New().String()[:8], run.SchedulerStatusFailed, run.TaskStatusFailed)
	_, err := rfx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{SourceRunId: srcID.String()})
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestRerunHandler_RejectsEmptySourceRunID(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()
	_, err := rfx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{SourceRunId: ""})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRerunHandler_RejectsMalformedSourceRunID(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()
	_, err := rfx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{SourceRunId: "not-a-uuid"})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestRerunHandler_RejectsSourceNotFound(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()
	_, err := rfx.Handler.TriggerRerun(context.Background(), &statev1.TriggerRerunRequest{SourceRunId: uuid.New().String()})
	requireGRPCCode(t, err, codes.NotFound)
}
