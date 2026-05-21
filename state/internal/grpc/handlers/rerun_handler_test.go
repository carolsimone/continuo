package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	ports "github.com/carolsimone/continuo/state/service/ports"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
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
	Cleanup       func()
}

func setupRerunFixture(t *testing.T) *rerunFixture {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	logger := newTestLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	taskExecutionRepo := postgres.NewTaskExecutionRepository(db, logger)
	clk := ports.SystemClock{}
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, taskExecutionRepo, catalogRepo, clk, logger)
	}
	useCase := svchandlers.NewTriggerRerunHandler(logger)
	handler := NewRerunHandler(useCase, factory, logger)
	return &rerunFixture{Handler: handler, SchedulerRepo: schedulerRepo, TaskRepo: taskRepo, DB: db, Cleanup: func() { db.Close() }}
}

func TestRerunHandler_HappyPath_FailedSource(t *testing.T) {
	rfx := setupRerunFixture(t)
	defer rfx.Cleanup()

	// Reuse rebase fixture's seeders — they're not rebase-specific.
	fx := &rebaseFixture{
		Handler: nil, SchedulerRepo: rfx.SchedulerRepo, TaskRepo: rfx.TaskRepo,
		DB: rfx.DB, Cleanup: func() {},
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
	require.Equal(t, run.SchedulerStatusPending, tracker.Status)

	var payloadRaw []byte
	require.NoError(t, rfx.DB.GetContext(context.Background(), &payloadRaw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = 'trigger.rerun:v1' LIMIT 1`,
		newID,
	))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(payloadRaw, &payload))
	require.Equal(t, newID.String(), payload["schedule_id"])
	require.Equal(t, scheduleName, payload["schedule_name"])
	require.Equal(t, "rerun", payload["kind"])
	require.Equal(t, srcID.String(), payload["source_run_id"])
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
