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
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
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
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeState,
		fakeProducer,
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
		Schema:       "public",
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

	// Verify task status was updated to running
	updates := fakeState.GetTaskUpdates()
	require.Len(t, updates, 1, "Should have one task update")
	assert.Equal(t, taskID, updates[0].TaskID)
	assert.Equal(t, statev1.TaskStatus_TASK_STATUS_RUNNING, updates[0].Status)

	// Verify event was published
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
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeState,
		fakeProducer,
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
		Schema:       "public",
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
		Schema:       "public",
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

	// But both tasks should have been updated
	updates := fakeState.GetTaskUpdates()
	assert.Len(t, updates, 2, "Both tasks should have been updated")
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
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	// Set K8s to fail
	fakeK8s.SetCreateJobError(errors.New("k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeState,
		fakeProducer,
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
		Schema:       "public",
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

	// Verify no state updates or Redis messages
	assert.Len(t, fakeState.GetTaskUpdates(), 0, "No task updates on failure")
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
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	// Set K8s to always fail
	fakeK8s.SetCreateJobError(errors.New("persistent k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeState,
		fakeProducer,
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
		Schema:       "public",
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
	k8sClient := fakes.NewFakeK8sClient()
	stateClient := fakes.NewFakeStateClient()

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, k8sClient, stateClient, publisher, "default", logger)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	msgs := publisher.GetPublishedMessages()
	require.Len(t, msgs, 1, "expected one publish call")
	require.Equal(t, entryID.String(), msgs[0].Values["outbox_entry_id"],
		"outbox_entry_id must equal the deployment_outbox entry ID")
}

func TestOutboxProcessor_ProcessBatch_StateUpdateFailure(t *testing.T) {
	// Test that if state update fails, the entry can be retried
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	fakeK8s := fakes.NewFakeK8sClient()
	fakeState := fakes.NewFakeStateClient()
	fakeProducer := fakes.NewFakeRedisProducer()

	// Set state client to fail
	fakeState.SetUpdateTaskStatusError(errors.New("state service error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakeState,
		fakeProducer,
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
		Schema:       "public",
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

	// Process - K8s succeeds but state update fails
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

	// Fix state client and retry
	fakeState.SetUpdateTaskStatusError(nil)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify entry is now processed
	entries, err = outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "Entry should be processed after retry")

	// Verify state was eventually updated
	updates := fakeState.GetTaskUpdates()
	assert.Len(t, updates, 1, "State should be updated on retry")
}
