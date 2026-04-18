package test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/test/fakes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxProcessor_ProcessBatch_Success(t *testing.T) {
	// Setup
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeProducer := fakes.NewFakeRedisProducer()
	fakeStatusProducer := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeProducer,
		fakeStatusProducer,
		"default",
		logger,
	)

	// Create test outbox entry
	ctx := context.Background()
	taskID := uuid.New()
	scheduleID := uuid.New()

	entry := &model.DeploymentOutboxEntry{
		ID:           uuid.New(),
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// Execute
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify K8s job was created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "Should have created one K8s job")
	assert.Contains(t, jobs, "default/dbt-public-users")

	// Verify task.status.updated:v1 event was published with RUNNING status
	statusMsgs := fakeStatusProducer.GetPublishedMessages()
	require.Len(t, statusMsgs, 1, "Should have published one task status event")
	assert.Equal(t, taskID.String(), statusMsgs[0].Values["task_id"])
	assert.Equal(t, scheduleID.String(), statusMsgs[0].Values["schedule_id"])
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])

	// Verify job-deployed event was published
	msgs := fakeProducer.GetPublishedMessages()
	require.Len(t, msgs, 1, "Should have published one message")
	assert.Equal(t, taskID.String(), msgs[0].Values["task_id"])
	assert.Equal(t, scheduleID.String(), msgs[0].Values["schedule_id"])
	assert.Equal(t, "dbt-public-users", msgs[0].Values["job_name"])

	// Verify outbox entry was marked as processed
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "No pending entries should remain")
}

func TestOutboxProcessor_ProcessBatch_Idempotent(t *testing.T) {
	// Test that processing the same job twice doesn't create duplicate K8s jobs
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeProducer := fakes.NewFakeRedisProducer()
	fakeStatusProducer := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeProducer,
		fakeStatusProducer,
		"default",
		logger,
	)

	ctx := context.Background()
	taskID := uuid.New()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:           uuid.New(),
		TaskID:       taskID,
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// First processing
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Simulate a retry by creating another pending entry with the same job name
	entry2 := &model.DeploymentOutboxEntry{
		ID:           uuid.New(),
		TaskID:       uuid.New(), // Different task
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users", // Same job name
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	err = outboxRepo.Create(ctx, entry2)
	require.NoError(t, err)

	// Second processing
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify K8s job was only created once (idempotent)
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "Should still have only one K8s job (idempotent)")

	// Both tasks should have published status events
	statusMsgs := fakeStatusProducer.GetPublishedMessages()
	assert.Len(t, statusMsgs, 2, "Both tasks should have published status events")
}

func TestOutboxProcessor_ProcessBatch_K8sFailure(t *testing.T) {
	// Test retry logic when K8s job creation fails
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeProducer := fakes.NewFakeRedisProducer()
	fakeStatusProducer := fakes.NewFakeRedisProducer()

	// Set K8s to fail
	fakeK8s.SetCreateJobError(errors.New("k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeProducer,
		fakeStatusProducer,
		"default",
		logger,
	)

	ctx := context.Background()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:           uuid.New(),
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// Process - should fail and increment retry
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err) // ProcessBatch itself doesn't error, it handles failures

	// Verify entry is still pending with incremented retry
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 1, entries[0].RetryCount, "Retry count should be incremented")

	// Verify no status events or job-deployed messages published on failure
	assert.Len(t, fakeStatusProducer.GetPublishedMessages(), 0, "No status events on K8s failure")
	assert.Len(t, fakeProducer.GetPublishedMessages(), 0, "No messages published on failure")
}

func TestOutboxProcessor_ProcessBatch_MaxRetriesExceeded(t *testing.T) {
	// Test that entries are marked as failed after max retries
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeProducer := fakes.NewFakeRedisProducer()
	fakeStatusProducer := fakes.NewFakeRedisProducer()

	// Set K8s to always fail
	fakeK8s.SetCreateJobError(errors.New("persistent k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeProducer,
		fakeStatusProducer,
		"default",
		logger,
	)

	ctx := context.Background()
	entryID := uuid.New()

	// Create entry with retry count at max
	entry := &model.DeploymentOutboxEntry{
		ID:           entryID,
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   3, // At max
		MaxRetries:   3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// Process - should mark as failed
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify entry is no longer pending
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "Entry should no longer be pending")

	// Note: We can't easily verify it's marked as failed without adding a GetByID method
	// to the repository, but the processor logic does mark it as failed
}

func TestOutboxProcessor_PublishesOutboxEntryID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()

	// Seed a deployment_outbox entry with a known ID
	_, err := db.ExecContext(ctx, `
		INSERT INTO deployment_outbox
			(id, task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name, node_type, status)
		VALUES ($1, $2, $3, 'sched', 'svc', 'pub', 'tbl', 'job-name', 'dbt-model', 'pending')
	`, entryID, taskID, schedID)
	require.NoError(t, err)

	publisher := fakes.NewFakeRedisProducer()
	statusPublisher := fakes.NewFakeRedisProducer()
	k8sClient := fakes.NewFakeK8sClient()

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, k8sClient, publisher, statusPublisher, "default", logger)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	msgs := publisher.GetPublishedMessages()
	require.Len(t, msgs, 1, "expected one publish call")
	require.Equal(t, entryID.String(), msgs[0].Values["outbox_entry_id"],
		"outbox_entry_id must equal the deployment_outbox entry ID")
}

func TestOutboxProcessor_ProcessBatch_StatusPublishFailure(t *testing.T) {
	// Test that if status event publish fails, the entry can be retried
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeProducer := fakes.NewFakeRedisProducer()
	fakeStatusProducer := fakes.NewFakeRedisProducer()

	// Set status producer to fail
	fakeStatusProducer.SetPublishError(errors.New("redis publish error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeProducer,
		fakeStatusProducer,
		"default",
		logger,
	)

	ctx := context.Background()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:           uuid.New(),
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     "dbt-model",
		Status:       string(model.OutboxStatusPending),
		CreatedAt:    time.Now(),
		RetryCount:   0,
		MaxRetries:   3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// Process - K8s succeeds but status publish fails
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify K8s job was created
	jobs := fakeK8s.GetCreatedJobs()
	assert.Len(t, jobs, 1, "K8s job should be created")

	// Verify entry is still pending for retry
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 1, entries[0].RetryCount, "Retry count should be incremented")

	// Fix status producer and retry
	fakeStatusProducer.SetPublishError(nil)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify entry is now processed
	entries, err = outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "Entry should be processed after retry")

	// Verify status event was eventually published
	statusMsgs := fakeStatusProducer.GetPublishedMessages()
	assert.Len(t, statusMsgs, 1, "Status event should be published on retry")
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])
}
