package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	grpcserver "github.com/carolsimone/continuo/state/internal/grpc"
	"github.com/carolsimone/continuo/state/internal/grpc/handlers"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
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
	stateConn   *grpc.ClientConn
	stateServer *grpcserver.Server
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

	// ---- Build repositories ----
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)

	// ---- No-op activator (ActivateSchedule not exercised in these tests) ----
	activator := &noopScheduleActivator{}

	// ---- Build handlers ----
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	schedulerHandler := handlers.NewSchedulerHandler(schedulerRepo, activator, nil, nil, logger)
	schedulerHandler.WithCancelDeps(db, taskRepo, outboxRepo)
	taskHandler := handlers.NewTaskHandler(taskRepo, logger)
	execHandler := handlers.NewTaskExecutionHandler(execRepo, logger)
	rerunHandler := handlers.NewRerunHandler(db, schedulerRepo, taskRepo, nil, logger)
	singleNodeRunHandler := handlers.NewSingleNodeRunHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	rebaseHandler := handlers.NewRebaseHandler(db, schedulerRepo, taskRepo, outboxRepo, logger)
	nodeRunRepo := postgres.NewNodeRunRepository(db, logger)
	nodeRunHandler := handlers.NewNodeRunHandler(nodeRunRepo, logger)

	// ---- Create gRPC server on a random port ----
	stateServer, err = grpcserver.NewServer(0, schedulerHandler, taskHandler, execHandler, rerunHandler, singleNodeRunHandler, rebaseHandler, nodeRunHandler, logger)
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

	// ---- Run tests ----
	code := m.Run()

	stateConn.Close()
	stateServer.Shutdown(ctx)
	os.Exit(code)
}

// noopScheduleActivator satisfies the scheduler.ScheduleActivator interface without I/O
type noopScheduleActivator struct{}

func (n *noopScheduleActivator) ActivateSchedule(_ context.Context, _ string, _ string, _ *uuid.UUID) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (n *noopScheduleActivator) PrepareActivation(_ context.Context, _ string, _ string, _ *uuid.UUID) (*model.SchedulerTracker, error) {
	return nil, nil
}

var _ scheduler.ScheduleActivator = (*noopScheduleActivator)(nil)

// ============================================================================
// Integration tests
// ============================================================================

func TestStateService(t *testing.T) {
	t.Run("CancelSchedule/success", func(t *testing.T) {
		ctx := context.Background()

		// Create a RUNNING scheduler (explicit status bypasses PENDING default)
		schedID := uuid.NewString()
		_, err := stateClient.CreateScheduler(ctx, &statev1.CreateSchedulerRequest{
			ScheduleId:   schedID,
			ScheduleName: "daily-cancel-success",
			Status:       statev1.SchedulerStatus_SCHEDULER_STATUS_RUNNING,
		})
		require.NoError(t, err)

		// Cancel it by name
		resp, err := stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
			ScheduleName:       "daily-cancel-success",
			CancelledBy:        "user",
			CancellationReason: "manual stop",
		})
		require.NoError(t, err)
		assert.Equal(t, schedID, resp.ScheduleId)

		// Verify via GetScheduler
		getResp, err := stateClient.GetScheduler(ctx, &statev1.GetSchedulerRequest{
			ScheduleId: resp.ScheduleId,
		})
		require.NoError(t, err)
		assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_CANCELLED, getResp.Scheduler.Status)
		assert.Equal(t, "user", getResp.Scheduler.CancelledBy)
		assert.Equal(t, "manual stop", getResp.Scheduler.CancellationReason)
		assert.NotNil(t, getResp.Scheduler.CancelledAt)
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
}

