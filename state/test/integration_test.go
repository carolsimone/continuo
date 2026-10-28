package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/runservice"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	ports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runreads"
	"github.com/carolsimone/continuo/state/service/schedules"
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
	runClient runv1.RunServiceClient
	// runCommands serves the RunService commands of the server under test;
	// a test calls it directly to act for a tenant the gRPC edge does not
	// resolve.
	runCommands *svchandlers.RunCommands
	// scheduleService serves the schedule calls of the server under test; a
	// test calls it directly to act for a tenant the gRPC edge does not
	// resolve.
	scheduleService *schedules.Service
	stateConn       *grpc.ClientConn
	stateServer     *grpcserver.Server
	stateDB         *sqlx.DB
	// stateDSN is the connection string of stateDB's database, for a test that
	// listens on one of its channels.
	stateDSN string
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
	stateDSN = connStr

	// ---- Build repositories ----
	schedulerRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	attemptRepo := postgres.NewTaskAttemptReadRepository(db)

	// ---- Build the RunService ----
	clk := ports.SystemClock{}
	integrationUoWFactory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, clk, logger)
	}
	runReadRepo := postgres.NewRunReadRepository(db)
	nodeRunsOf := func(tenantID string) postgres.NodeRunRepository {
		return postgres.NewNodeRunRepository(db, tenantID, logger)
	}
	runCommands = svchandlers.NewRunCommands(integrationUoWFactory, noRunMetrics{}, defaultPlanner, logger)
	cronParser, err := cron.NewParser()
	if err != nil {
		logger.Error("Failed to build the cron parser", "error", err)
		os.Exit(1)
	}
	scheduleService = schedules.New(
		postgres.NewScheduleRepository(db, nil, cronParser, logger),
		postgres.NewLiveScheduleModelRepository(db),
		schedulerRepo, cronParser, clk,
	)
	runService := runservice.NewServer(runservice.Reads{
		Runs:     runreads.NewRunDetails(schedulerRepo, postgres.NewTaskReadRepository(taskRepo)),
		Attempts: runreads.NewAttempts(attemptRepo),
		Nodes:    runreads.NewNodes(postgres.NewNodeReadRepository(nodeRunsOf), 24*time.Hour),
		Graphs:   runreads.New(runReadRepo),
		Rebases:  runreads.NewRebasePreviewer(defaultPlanner, integrationUoWFactory, runReadRepo),
	}, runCommands, scheduleService, logger)

	// ---- Create gRPC server on a random port ----
	stateServer, err = grpcserver.NewServer(0, runService, logger, false)
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

func TestRunService_StartAndCancelByName(t *testing.T) {
	t.Run("CancelActiveRun/success", func(t *testing.T) {
		ctx := context.Background()

		// Promote a release carrying the schedule, then start it: StartRun
		// creates a run of kind trigger, which CancelActiveRun then cancels.
		promoteSchedules(t, stateDB, "daily-cancel-success")
		runID := startRun(t, "daily-cancel-success")

		// A manual start persists kind='trigger', not 'cron'.
		assert.Equal(t, "trigger", schedulerKind(t, runID))

		resp, err := runClient.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
			RequestId:          uuid.NewString(),
			ScheduleName:       "daily-cancel-success",
			CancelledBy:        "user",
			CancellationReason: "manual stop",
		})
		require.NoError(t, err)
		assert.Equal(t, runID, resp.GetRunId())

		// The start dispatched the schedule's task, so the run is cancelling
		// until that attempt reports stopped.
		var stored string
		require.NoError(t, stateDB.GetContext(ctx, &stored,
			`SELECT status FROM runs WHERE run_id = $1`, runID))
		assert.Equal(t, "cancelling", stored)

		got, err := runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: resp.GetRunId()})
		require.NoError(t, err)
		assert.Equal(t, "user", got.GetRun().GetCancelledBy())
		assert.Equal(t, "manual stop", got.GetRun().GetCancellationReason())
		assert.NotNil(t, got.GetRun().GetCancelledAt())
	})

	t.Run("CancelActiveRun/a_second_cancel_returns_the_cancelling_run", func(t *testing.T) {
		ctx := context.Background()

		promoteSchedules(t, stateDB, "daily-cancel-twice")
		runID := startRun(t, "daily-cancel-twice")
		_, err := runClient.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
			RequestId: uuid.NewString(), ScheduleName: "daily-cancel-twice", CancelledBy: "alice", CancellationReason: "first",
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
			`SELECT status, cancelled_at, cancelled_by, cancellation_reason FROM runs WHERE run_id = $1`, runID))
		require.Equal(t, "cancelling", before.Status)
		var outboxBefore int
		require.NoError(t, stateDB.GetContext(ctx, &outboxBefore,
			`SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, runID))

		second, err := runClient.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
			RequestId: uuid.NewString(), ScheduleName: "daily-cancel-twice", CancelledBy: "bob", CancellationReason: "second",
		})
		require.NoError(t, err)
		assert.Equal(t, runID, second.GetRunId())
		byID, err := runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: runID, CancelledBy: "bob"})
		require.NoError(t, err)
		assert.Equal(t, "alice", byID.GetRun().GetCancelledBy(), "the answer is the run as the first cancel left it")

		var after cancelRow
		require.NoError(t, stateDB.GetContext(ctx, &after,
			`SELECT status, cancelled_at, cancelled_by, cancellation_reason FROM runs WHERE run_id = $1`, runID))
		assert.Equal(t, before, after, "a second cancel changes nothing")
		var outboxAfter int
		require.NoError(t, stateDB.GetContext(ctx, &outboxAfter,
			`SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, runID))
		assert.Equal(t, outboxBefore, outboxAfter, "a second cancel writes no outbox row")
	})

	t.Run("CancelActiveRun/no_active_run", func(t *testing.T) {
		ctx := context.Background()

		_, err := runClient.CancelActiveRun(ctx, &runv1.CancelActiveRunRequest{
			RequestId: uuid.NewString(), ScheduleName: "never-ran-" + uuid.NewString(),
		})
		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
	})
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
