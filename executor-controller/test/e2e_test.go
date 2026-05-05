package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/messagebus"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/executor-controller/test/fakes"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_DeploymentFlow tests the complete deployment flow from command to K8s job creation
func TestE2E_DeploymentFlow(t *testing.T) {
	// Setup
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Initialize dependencies
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakePublisher := fakes.NewFakeRedisProducer()

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	// Create handlers
	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	// Create message bus
	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	ctx := context.Background()

	// Test data
	taskID := uuid.New()
	scheduleID := uuid.New()
	scheduleName := "hourly"
	serviceName := "dbt"
	schema := "public"
	tableName := "users"
	jobName := "dbt-public-users"

	// Step 1: Handle DeployJob command (writes to outbox)
	cmd := command.DeployJob{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		SchemaName:   schema,
		TableName:    tableName,
		JobName:      jobName,
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err := messageBus.Handle(ctx, cmd)
	require.NoError(t, err, "Command handling should succeed")

	// Verify outbox entry was created
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "Should have one outbox entry")

	// Step 2: Process outbox (simulates background processor)
	err = outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err, "Outbox processing should succeed")

	// Verify K8s job was created
	jobs := fakeK8s.GetCreatedJobs()
	require.Len(t, jobs, 1, "Should have created one K8s job")
	jobKey := "default/" + jobName
	assert.Contains(t, jobs, jobKey, "Job should exist in K8s")

	// Verify task.status.updated:v1 RUNNING event was published
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	require.Len(t, statusMsgs, 1, "Should have one status event")
	assert.Equal(t, taskID.String(), statusMsgs[0].Values["task_id"])
	assert.Equal(t, scheduleID.String(), statusMsgs[0].Values["schedule_id"])
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])

	// Verify job-deployed event was published to Redis
	msgs := fakePublisher.GetPublishedMessagesByStream("job.deployed:v1")
	require.Len(t, msgs, 1, "Should have published one message")
	msg := msgs[0]
	assert.Equal(t, taskID.String(), msg.Values["task_id"])
	assert.Equal(t, scheduleID.String(), msg.Values["schedule_id"])
	assert.Equal(t, scheduleName, msg.Values["schedule_name"])
	assert.Equal(t, serviceName, msg.Values["service_name"])
	assert.Equal(t, schema, msg.Values["schema_name"])
	assert.Equal(t, tableName, msg.Values["table_name"])
	assert.Equal(t, jobName, msg.Values["job_name"])

	// Verify outbox entry was marked as processed
	entries, err = outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "No pending entries should remain")
}

// TestE2E_MultipleDeployments tests handling multiple deployments concurrently
func TestE2E_MultipleDeployments(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Initialize dependencies
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakePublisher := fakes.NewFakeRedisProducer()

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	ctx := context.Background()

	// Create multiple deployment commands
	numDeployments := 5
	taskIDs := make([]uuid.UUID, numDeployments)

	for i := 0; i < numDeployments; i++ {
		taskID := uuid.New()
		taskIDs[i] = taskID

		cmd := command.DeployJob{
			TaskID:       taskID,
			ScheduleID:   uuid.New(),
			ScheduleName: "hourly",
			ServiceName:  "dbt",
			SchemaName:   "public",
			TableName:    "users",
			JobName:      "dbt-public-users-" + taskID.String()[:8],
			NodeType:     pkg_model.NodeTypeDbtModel,
		}

		err := messageBus.Handle(ctx, cmd)
		require.NoError(t, err)
	}

	// Process all outbox entries
	err := outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify all jobs were created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, numDeployments, "All jobs should be created")

	// Verify all status events were published
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	assert.Len(t, statusMsgs, numDeployments, "All status events should be published")

	// Verify all job-deployed events were published
	msgs := fakePublisher.GetPublishedMessagesByStream("job.deployed:v1")
	assert.Len(t, msgs, numDeployments, "All events should be published")

	// Verify no pending entries remain
	entries, err := outboxRepo.GetPendingBatch(ctx, 100)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "No pending entries should remain")
}

// TestE2E_RetryOnFailure tests the retry mechanism when K8s deployment fails
func TestE2E_RetryOnFailure(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Initialize dependencies
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakePublisher := fakes.NewFakeRedisProducer()

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	ctx := context.Background()

	// Create deployment command
	taskID := uuid.New()
	cmd := command.DeployJob{
		TaskID:       taskID,
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err := messageBus.Handle(ctx, cmd)
	require.NoError(t, err)

	// Simulate K8s failure
	fakeK8s.SetCreateJobError(assert.AnError)

	// First processing attempt - should fail and increment retry
	err = outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err) // ProcessBatch doesn't error itself

	// Verify entry is still pending with retry incremented
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 1, entries[0].OutboxRetryCount)

	// Fix K8s and retry
	fakeK8s.SetCreateJobError(nil)

	// Second processing attempt - should succeed
	err = outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify job was created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "Job should be created on retry")

	// Verify status event was published on retry
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	assert.Len(t, statusMsgs, 1, "Status event should be published on retry")
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])

	// Verify no pending entries remain
	entries, err = outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0)
}

// TestE2E_IdempotentDeployment tests that reprocessing doesn't create duplicate K8s jobs
func TestE2E_IdempotentDeployment(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Initialize dependencies
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakePublisher := fakes.NewFakeRedisProducer()

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	ctx := context.Background()

	// Create first deployment
	cmd1 := command.DeployJob{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users", // Same job name
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err := messageBus.Handle(ctx, cmd1)
	require.NoError(t, err)

	err = outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Create second deployment with same job name
	cmd2 := command.DeployJob{
		TaskID:       uuid.New(), // Different task
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users", // Same job name
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err = messageBus.Handle(ctx, cmd2)
	require.NoError(t, err)

	err = outboxProcessor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify only one K8s job was created (idempotent)
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "Only one K8s job should exist (idempotent)")

	// Both tasks should have status events published
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	assert.Len(t, statusMsgs, 2, "Both tasks should have status events")

	// And two job-deployed events should be published
	msgs := fakePublisher.GetPublishedMessagesByStream("job.deployed:v1")
	assert.Len(t, msgs, 2, "Both events should be published")
}

// TestE2E_BackgroundProcessing simulates the background outbox processor running continuously
func TestE2E_BackgroundProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping background processing test in short mode")
	}

	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Initialize dependencies
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakePublisher := fakes.NewFakeRedisProducer()

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	deployHandler := handlers.NewDeployHandler(unitOfWork, logger)
	outboxProcessor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)
	// Set fast poll interval for test
	outboxProcessor.PollInterval = 100 * time.Millisecond

	commandHandlers := map[string]messagebus.CommandHandler{
		"command.DeployJob": func(ctx context.Context, cmd command.Command) error {
			return deployHandler.Handle(ctx, cmd.(command.DeployJob))
		},
	}

	messageBus := messagebus.NewMessageBus(
		unitOfWork,
		commandHandlers,
		map[string][]messagebus.EventHandler{},
		logger,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start background processor
	go func() {
		outboxProcessor.Run(ctx)
	}()

	// Add commands over time
	for i := 0; i < 3; i++ {
		cmd := command.DeployJob{
			TaskID:       uuid.New(),
			ScheduleID:   uuid.New(),
			ScheduleName: "hourly",
			ServiceName:  "dbt",
			SchemaName:   "public",
			TableName:    "users",
			JobName:      "dbt-public-users-" + uuid.New().String()[:8],
			NodeType:     pkg_model.NodeTypeDbtModel,
		}

		err := messageBus.Handle(ctx, cmd)
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify all jobs were created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 3, "All jobs should be processed by background worker")

	// Verify no pending entries remain
	entries, err := outboxRepo.GetPendingBatch(ctx, 100)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "Background processor should have processed all entries")
}
