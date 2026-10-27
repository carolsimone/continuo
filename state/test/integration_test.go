package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/adapters/schedulesconfig"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	"github.com/carolsimone/continuo/state/internal/grpc/runservice"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	stateClient statev1.StateServiceClient
	runClient   runv1.RunServiceClient
	stateConn   *grpc.ClientConn
	stateServer *grpcserver.Server
	stateDB     *sqlx.DB
)

// TestMain sets up a real PostgreSQL testcontainer and a live gRPC state server,
// shared across all tests in the package.
func TestMain(m *testing.M) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// ---- Start PostgreSQL ----
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		logger.Error("Failed to start PostgreSQL container", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			logger.Error("Failed to terminate PostgreSQL container", "error", err)
		}
	}()

	host, err := pgContainer.Host(ctx)
	if err != nil {
		logger.Error("Failed to get PostgreSQL host", "error", err)
		os.Exit(1)
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		logger.Error("Failed to get PostgreSQL port", "error", err)
		os.Exit(1)
	}

	connStr := fmt.Sprintf("host=%s port=%s user=testuser password=testpass dbname=testdb sslmode=disable", host, port.Port())

	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// ---- Run migrations from db/migration/state/V*.sql ----
	// Loaded from disk rather than hardcoded so the test schema stays in
	// lock-step with production. See state/test/migrations.go.
	if err := ApplyMigrations(db.DB); err != nil {
		logger.Error("Failed to apply migrations", "error", err)
		os.Exit(1)
	}
	stateDB = db

	// ---- Build repositories ----
	schedulerRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	attemptRepo := postgres.NewTaskAttemptReadRepository(db)

	// ---- Build handlers ----
	clk := ports.SystemClock{}
	integrationUoWFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, clk, logger)
	}
	activateHandler := svchandlers.NewStartRunHandler(defaultPlanner, logger)
	schedulerHandler := handlers.NewSchedulerHandler(schedulerRepo, activateHandler, nil, nil, integrationUoWFactory, logger)
	taskHandler := handlers.NewTaskHandler(func(tenantID string) handlers.TaskReads { return taskRepo.ForTenant(tenantID) }, logger)
	attemptHandler := handlers.NewTaskAttemptHandler(attemptRepo, logger)
	rerunUC := svchandlers.NewTriggerRerunHandler(defaultPlanner, logger)
	rerunHandler := handlers.NewRerunHandler(rerunUC, integrationUoWFactory, logger)
	singleNodeRunUC := svchandlers.NewTriggerSingleNodeRunHandler(defaultPlanner, logger)
	singleNodeRunHandler := handlers.NewSingleNodeRunHandler(singleNodeRunUC, integrationUoWFactory, logger)
	rebaseUC := svchandlers.NewTriggerRebaseHandler(defaultPlanner, logger)
	rebaseHandler := handlers.NewRebaseHandler(rebaseUC, integrationUoWFactory, logger)
	runReadRepo := postgres.NewRunReadRepository(db)
	previewRebaseHandler := handlers.NewPreviewRebaseHandler(runreads.NewRebasePreviewer(defaultPlanner, integrationUoWFactory, runReadRepo), logger)
	nodeRunsOf := func(tenantID string) postgres.NodeRunRepository {
		return postgres.NewNodeRunRepository(db, tenantID, logger)
	}
	nodeRunHandler := handlers.NewNodeRunHandler(nodeRunsOf, 24*time.Hour, logger)
	runReadHandler := handlers.NewRunReadHandler(runreads.New(runReadRepo), logger)
	runService := runservice.NewServer(runservice.Reads{
		Runs:      runreads.NewRunDetails(schedulerRepo, postgres.NewTaskReadRepository(taskRepo)),
		Attempts:  runreads.NewAttempts(attemptRepo),
		Nodes:     runreads.NewNodes(postgres.NewNodeReadRepository(nodeRunsOf), 24*time.Hour),
		Schedules: runreads.NewSchedules(postgres.NewCatalogRepository(db, logger), schedulerRepo, schedulesconfig.New(nil)),
		Graphs:    runreads.New(runReadRepo),
	}, logger)

	// ---- Create gRPC server on a random port ----
	stateServer, err = grpcserver.NewServer(0, schedulerHandler, taskHandler, attemptHandler, rerunHandler, singleNodeRunHandler, rebaseHandler, previewRebaseHandler, nodeRunHandler, runReadHandler, runService, logger, false)
	if err != nil {
		logger.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := stateServer.Start(); err != nil {
			logger.Error("gRPC server stopped", "error", err)
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// ---- Create gRPC client ----
	stateConn, err = grpc.NewClient(stateServer.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("Failed to create gRPC client", "error", err)
		os.Exit(1)
	}
	stateClient = statev1.NewStateServiceClient(stateConn)
	runClient = runv1.NewRunServiceClient(stateConn)

	// ---- Run tests ----
	code := m.Run()

	stateConn.Close()
	stateServer.Shutdown(ctx)
	os.Exit(code)
}

// ============================================================================
// Integration tests
// ============================================================================

func TestStateService(t *testing.T) {
	t.Run("CancelSchedule/success", func(t *testing.T) {
		ctx := context.Background()

		// Activate a run through the aggregate path: seed the catalog, then
		// manually trigger the schedule. TriggerSchedule mints a PENDING Run via
		// the Run aggregate (kind='trigger'), which CancelSchedule then cancels.
		seedActiveSchedule(t, "daily-cancel-success")
		promoteSchedules(t, stateDB, "daily-cancel-success")
		trigResp, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
			ScheduleName: "daily-cancel-success",
		})
		require.NoError(t, err)
		schedID := trigResp.ScheduleId

		// A manual trigger must persist kind='trigger', not 'cron'.
		assert.Equal(t, "trigger", schedulerKind(t, schedID))

		// Cancel it by name
		resp, err := stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName:       "daily-cancel-success",
			CancelledBy:        "user",
			CancellationReason: "manual stop",
		})
		require.NoError(t, err)
		assert.Equal(t, schedID, resp.ScheduleId)

		// The trigger dispatched the schedule's task, so the run is cancelling
		// until that attempt reports stopped.
		var stored string
		require.NoError(t, stateDB.GetContext(ctx, &stored,
			`SELECT status FROM runs WHERE run_id = $1`, schedID))
		assert.Equal(t, "cancelling", stored)

		// Verify via GetScheduler
		getResp, err := stateClient.GetScheduler(ctx, &statev1.GetSchedulerRequest{
			ScheduleId: resp.ScheduleId,
		})
		require.NoError(t, err)
		assert.Equal(t, "user", getResp.Scheduler.CancelledBy)
		assert.Equal(t, "manual stop", getResp.Scheduler.CancellationReason)
		assert.NotNil(t, getResp.Scheduler.CancelledAt)
	})

	t.Run("CancelSchedule/a_second_cancel_returns_the_cancelling_run", func(t *testing.T) {
		ctx := context.Background()

		seedActiveSchedule(t, "daily-cancel-twice")
		promoteSchedules(t, stateDB, "daily-cancel-twice")
		trigResp, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: "daily-cancel-twice"})
		require.NoError(t, err)
		_, err = stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName: "daily-cancel-twice", CancelledBy: "alice", CancellationReason: "first",
		})
		require.NoError(t, err)
		type cancelRow struct {
			Status      string     `db:"status"`
			CancelledAt *time.Time `db:"cancelled_at"`
			CancelledBy *string    `db:"cancelled_by"`
			Reason      *string    `db:"cancellation_reason"`
		}
		var before cancelRow
		require.NoError(t, stateDB.GetContext(ctx, &before,
			`SELECT status, cancelled_at, cancelled_by, cancellation_reason FROM runs WHERE run_id = $1`, trigResp.ScheduleId))
		require.Equal(t, "cancelling", before.Status)
		var outboxBefore int
		require.NoError(t, stateDB.GetContext(ctx, &outboxBefore,
			`SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, trigResp.ScheduleId))

		second, err := stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName: "daily-cancel-twice", CancelledBy: "bob", CancellationReason: "second",
		})
		require.NoError(t, err)
		assert.Equal(t, trigResp.ScheduleId, second.ScheduleId)
		byID, err := stateClient.CancelScheduler(ctx, &statev1.CancelSchedulerRequest{ScheduleId: trigResp.ScheduleId, CancelledBy: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "alice", byID.Scheduler.CancelledBy, "the answer is the run as the first cancel left it")

		var after cancelRow
		require.NoError(t, stateDB.GetContext(ctx, &after,
			`SELECT status, cancelled_at, cancelled_by, cancellation_reason FROM runs WHERE run_id = $1`, trigResp.ScheduleId))
		assert.Equal(t, before, after, "a second cancel changes nothing")
		var outboxAfter int
		require.NoError(t, stateDB.GetContext(ctx, &outboxAfter,
			`SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, trigResp.ScheduleId))
		assert.Equal(t, outboxBefore, outboxAfter, "a second cancel writes no outbox row")
	})

	t.Run("CancelSchedule/no_active_run", func(t *testing.T) {
		ctx := context.Background()

		_, err := stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName: "never-ran-" + uuid.NewString(),
		})
		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})

	t.Run("TriggerSchedule/persists_kind_trigger", func(t *testing.T) {
		ctx := context.Background()

		seedActiveSchedule(t, "daily-trigger-kind")
		promoteSchedules(t, stateDB, "daily-trigger-kind")
		resp, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
			ScheduleName: "daily-trigger-kind",
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.ScheduleId)

		// A manual trigger is a distinct run kind from a cron activation.
		assert.Equal(t, "trigger", schedulerKind(t, resp.ScheduleId))
	})
}

// seedActiveSchedule inserts an active schedule_catalog row so that
// ActivateSchedule / TriggerSchedule pass their catalog-existence pre-condition.
func seedActiveSchedule(t *testing.T, scheduleName string) {
	t.Helper()
	_, err := stateDB.ExecContext(context.Background(), `
		INSERT INTO schedule_catalog (tenant_id, schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
		VALUES ('default', $1, now(), now(), NULL, '{}'::jsonb)
		ON CONFLICT (tenant_id, schedule_name) DO UPDATE SET removed_at = NULL, last_seen_at = now()
	`, scheduleName)
	require.NoError(t, err)
}

// schedulerKind reads the persisted kind discriminator for a run directly from
// runs, bypassing the proto surface (which does not expose kind).
func schedulerKind(t *testing.T, scheduleID string) string {
	t.Helper()
	var kind string
	err := stateDB.GetContext(context.Background(), &kind,
		`SELECT kind FROM runs WHERE run_id = $1`, scheduleID)
	require.NoError(t, err)
	return kind
}
