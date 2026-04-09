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

	// ---- Run migrations ----
	if _, err := db.Exec(integrationSchema); err != nil {
		logger.Error("Failed to run schema migration", "error", err)
		os.Exit(1)
	}

	// ---- Build repositories ----
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)

	// ---- No-op activator (ActivateSchedule not exercised in these tests) ----
	activator := &noopScheduleActivator{}

	// ---- Build handlers ----
	schedulerHandler := handlers.NewSchedulerHandler(schedulerRepo, activator, nil, nil, logger)
	taskHandler := handlers.NewTaskHandler(taskRepo, logger)
	execHandler := handlers.NewTaskExecutionHandler(execRepo, logger)

	// ---- Create gRPC server on a random port ----
	stateServer, err = grpcserver.NewServer(0, schedulerHandler, taskHandler, execHandler, logger)
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

func (n *noopScheduleActivator) ActivateSchedule(_ context.Context, _ string) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (n *noopScheduleActivator) PrepareActivation(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}

var _ scheduler.ScheduleActivator = (*noopScheduleActivator)(nil)

// integrationSchema is the full DDL for tables exercised by integration tests.
// Matches the schema from repository_test.go.
const integrationSchema = `
CREATE TABLE IF NOT EXISTS scheduler_tracker (
	schedule_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	schedule_name       VARCHAR(50) NOT NULL,
	status              VARCHAR(20) NOT NULL CHECK (status IN (
							'pending','running','succeeded','failed','cancelled'
						)),
	created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	started_at          TIMESTAMPTZ,
	completed_at        TIMESTAMPTZ,
	last_heartbeat_at   TIMESTAMPTZ,
	cancelled_at        TIMESTAMPTZ,
	cancelled_by        VARCHAR(255),
	cancellation_reason TEXT,
	initialization_status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (initialization_status IN (
							'pending','in_progress','completed','failed'
						)),
	manifest_versions   JSONB NOT NULL DEFAULT '{}',
	CONSTRAINT valid_timestamps CHECK (
		(started_at IS NULL OR started_at >= created_at) AND
		(completed_at IS NULL OR completed_at >= started_at)
	)
);

CREATE INDEX IF NOT EXISTS idx_int_scheduler_tracker_init_status ON scheduler_tracker(schedule_id, initialization_status);

CREATE TABLE IF NOT EXISTS task_tracker (
	task_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	schedule_id         UUID NOT NULL REFERENCES scheduler_tracker(schedule_id) ON DELETE CASCADE,
	created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	service_name        VARCHAR(100) NOT NULL,
	schema_name         VARCHAR(100) NOT NULL,
	table_name          VARCHAR(100) NOT NULL,
	status              VARCHAR(20) NOT NULL CHECK (status IN (
							'pending','running','succeeded','failed','cancelled'
						)),
	retry_count         INTEGER NOT NULL DEFAULT 0,
	max_retries         INTEGER NOT NULL,
	job_name            VARCHAR(63) NOT NULL,
	cancelled_at        TIMESTAMPTZ,
	cancelled_by        VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_int_task_tracker_schedule_id ON task_tracker(schedule_id);
CREATE INDEX IF NOT EXISTS idx_int_task_tracker_status ON task_tracker(status);

CREATE TABLE IF NOT EXISTS task_execution (
	id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	task_id                UUID NOT NULL REFERENCES task_tracker(task_id) ON DELETE CASCADE,
	created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	started_at             TIMESTAMPTZ,
	completed_at           TIMESTAMPTZ,
	execution_time_seconds DECIMAL(10, 3),
	executor_id            VARCHAR(100),
	k8s_job_name           VARCHAR(253),
	error_message          TEXT,
	cancelled_at           TIMESTAMPTZ,
	cancelled_by           VARCHAR(255),
	cancellation_reason    TEXT,
	log_s3_key             VARCHAR(500),
	CONSTRAINT valid_execution_timestamps CHECK (
		(started_at IS NULL OR started_at >= created_at) AND
		(completed_at IS NULL OR completed_at >= started_at)
	)
);

CREATE INDEX IF NOT EXISTS idx_int_task_execution_task_id ON task_execution(task_id);
CREATE INDEX IF NOT EXISTS idx_int_task_execution_created_at ON task_execution(created_at DESC);
`

// ============================================================================
// Integration tests
// ============================================================================

func TestStateService(t *testing.T) {
	t.Run("ListTaskExecutions by schedule", func(t *testing.T) {
		ctx := context.Background()

		// Create a scheduler
		schedID := uuid.NewString()
		_, err := stateClient.CreateScheduler(ctx, &statev1.CreateSchedulerRequest{
			ScheduleId:   schedID,
			ScheduleName: "test-list-exec",
		})
		require.NoError(t, err)

		// Create a task
		taskID := uuid.NewString()
		_, err = stateClient.CreateTask(ctx, &statev1.CreateTaskRequest{
			TaskId:      taskID,
			ScheduleId:  schedID,
			ServiceName: "svc",
			SchemaName:  "schema",
			TableName:   "tbl",
			MaxRetries:  3,
		})
		require.NoError(t, err)

		// Create a task execution with error message
		execID := uuid.NewString()
		_, err = stateClient.CreateTaskExecution(ctx, &statev1.CreateTaskExecutionRequest{
			Id:           execID,
			TaskId:       taskID,
			ErrorMessage: "something went wrong",
		})
		require.NoError(t, err)

		// List executions for the schedule
		resp, err := stateClient.ListTaskExecutions(ctx, &statev1.ListTaskExecutionsRequest{
			ScheduleId: schedID,
			PageSize:   500,
			PageOffset: 0,
		})
		require.NoError(t, err)
		require.Len(t, resp.TaskExecutions, 1)
		assert.Equal(t, execID, resp.TaskExecutions[0].Id)
		assert.Equal(t, taskID, resp.TaskExecutions[0].TaskId)
		assert.Equal(t, "something went wrong", resp.TaskExecutions[0].ErrorMessage)
		assert.Equal(t, int32(1), resp.TotalCount)
	})

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

func TestCreateTaskExecution(t *testing.T) {
	ctx := context.Background()

	// Create a scheduler
	schedID := uuid.NewString()
	_, err := stateClient.CreateScheduler(ctx, &statev1.CreateSchedulerRequest{
		ScheduleId:   schedID,
		ScheduleName: "test-create-exec-log-s3-key",
	})
	require.NoError(t, err)

	// Create a task
	taskID := uuid.NewString()
	_, err = stateClient.CreateTask(ctx, &statev1.CreateTaskRequest{
		TaskId:      taskID,
		ScheduleId:  schedID,
		ServiceName: "svc",
		SchemaName:  "schema",
		TableName:   "table",
		MaxRetries:  3,
	})
	require.NoError(t, err)

	// Create a task execution with log_s3_key
	execID := uuid.NewString()
	_, err = stateClient.CreateTaskExecution(ctx, &statev1.CreateTaskExecutionRequest{
		Id:        execID,
		TaskId:    taskID,
		LogS3Key:  "logs/task-executions/svc/schema/table/some-uuid.log",
	})
	require.NoError(t, err)

	// List executions and assert log_s3_key round-trips
	resp, err := stateClient.ListTaskExecutions(ctx, &statev1.ListTaskExecutionsRequest{
		ScheduleId: schedID,
		PageSize:   500,
		PageOffset: 0,
	})
	require.NoError(t, err)
	require.Len(t, resp.TaskExecutions, 1)
	assert.Equal(t, execID, resp.TaskExecutions[0].Id)
	assert.Equal(t, taskID, resp.TaskExecutions[0].TaskId)
	assert.Equal(t, "logs/task-executions/svc/schema/table/some-uuid.log", resp.TaskExecutions[0].LogS3Key)
}
