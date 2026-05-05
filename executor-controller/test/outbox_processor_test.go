package test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/test/fakes"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
	fakePublisher := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	// Create test outbox entry
	ctx := context.Background()
	taskID := uuid.New()
	scheduleID := uuid.New()

	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           taskID,
		ScheduleID:       scheduleID,
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
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
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	require.Len(t, statusMsgs, 1, "Should have published one task status event")
	assert.Equal(t, taskID.String(), statusMsgs[0].Values["task_id"])
	assert.Equal(t, scheduleID.String(), statusMsgs[0].Values["schedule_id"])
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])

	// Verify job-deployed event was published
	msgs := fakePublisher.GetPublishedMessagesByStream("job.deployed:v1")
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
	fakePublisher := fakes.NewFakeRedisProducer()

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	ctx := context.Background()
	taskID := uuid.New()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           taskID,
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
	}

	err := outboxRepo.Create(ctx, entry)
	require.NoError(t, err)

	// First processing
	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Simulate a retry by creating another pending entry with the same job name
	entry2 := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           uuid.New(), // Different task
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users", // Same job name
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
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
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
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
	fakePublisher := fakes.NewFakeRedisProducer()

	// Set K8s to fail
	fakeK8s.SetCreateJobError(errors.New("k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	ctx := context.Background()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
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
	assert.Equal(t, 1, entries[0].OutboxRetryCount, "Retry count should be incremented")

	// Verify no status events or job-deployed messages published on failure
	assert.Len(t, fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1"), 0, "No status events on K8s failure")
	assert.Len(t, fakePublisher.GetPublishedMessagesByStream("job.deployed:v1"), 0, "No messages published on failure")
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
	fakePublisher := fakes.NewFakeRedisProducer()

	// Set K8s to always fail
	fakeK8s.SetCreateJobError(errors.New("persistent k8s error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	ctx := context.Background()
	entryID := uuid.New()

	// Create entry with retry count at max
	entry := &model.DeploymentOutboxEntry{
		ID:               entryID,
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 3, // At max
		OutboxMaxRetries: 3,
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

	fakePublisher := fakes.NewFakeRedisProducer()
	k8sClient := fakes.NewFakeK8sClient()

	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, k8sClient, fakePublisher, "job.deployed:v1", "task.status.updated:v1", "node.updated:v1", "default", logger)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	msgs := fakePublisher.GetPublishedMessagesByStream("job.deployed:v1")
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
	fakePublisher := fakes.NewFakeRedisProducer()

	// Set publisher to fail (affects status publish, which happens first)
	fakePublisher.SetPublishError(errors.New("redis publish error"))

	processor := handlers.NewOutboxProcessor(
		outboxRepo,
		fakeK8s,
		fakePublisher,
		"job.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)

	ctx := context.Background()

	// Create outbox entry
	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "dbt",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
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
	assert.Equal(t, 1, entries[0].OutboxRetryCount, "Retry count should be incremented")

	// Fix publisher and retry
	fakePublisher.SetPublishError(nil)

	err = processor.ProcessBatch(ctx)
	require.NoError(t, err)

	// Verify entry is now processed
	entries, err = outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 0, "Entry should be processed after retry")

	// Verify status event was eventually published
	statusMsgs := fakePublisher.GetPublishedMessagesByStream("task.status.updated:v1")
	assert.Len(t, statusMsgs, 1, "Status event should be published on retry")
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])
}

// ── Terminal-failure propagation tests ─────────────────────────────────────
// Cover MarkTaskTerminallyFailed (publishes task.status.updated:v1 FAILED +
// node.updated:v1 FAILED + outbox MarkFailed) and the two paths that invoke
// it: permanent dispatch error on attempt 1, and retry-exhaustion.

// newUnitProcessor builds an OutboxProcessor backed by the in-memory fakes
// (no Postgres container required).
func newUnitProcessor(
	outboxRepo *fakes.FakeOutboxRepo,
	k8sClient *fakes.FakeK8sClient,
	publisher *fakes.FakeRedisProducer,
) *handlers.OutboxProcessor {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return handlers.NewOutboxProcessor(
		outboxRepo,
		k8sClient,
		publisher,
		"node.deployed:v1",
		"task.status.updated:v1",
		"node.updated:v1",
		"default",
		logger,
	)
}

// minEntry returns a minimal pending DeploymentOutboxEntry with all the fields
// that MarkTaskTerminallyFailed uses.
func minEntry() *model.DeploymentOutboxEntry {
	return &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		TaskID:           uuid.New(),
		ScheduleID:       uuid.New(),
		ScheduleName:     "hourly",
		ServiceName:      "svc-1",
		SchemaName:       "public",
		TableName:        "users",
		JobName:          "dbt-public-users",
		NodeType:         "dbt-model",
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
		TaskRetryCount:   2,
	}
}

func TestOutboxProcessor_MarkTaskTerminallyFailed_PublishesBothEventsAndMarksFailed(t *testing.T) {
	entry := minEntry()
	outboxRepo := fakes.NewFakeOutboxRepo(entry)
	k8sClient := fakes.NewFakeK8sClient()
	publisher := fakes.NewFakeRedisProducer()

	p := newUnitProcessor(outboxRepo, k8sClient, publisher)
	err := p.MarkTaskTerminallyFailed(context.Background(), entry, "image_tag missing for service svc-1")
	require.NoError(t, err)

	// task.status.updated:v1 → state task tracker
	statusMsgs := publisher.GetPublishedMessagesByStream("task.status.updated:v1")
	require.Len(t, statusMsgs, 1, "expected one task.status.updated:v1 publish")
	assert.Equal(t, entry.TaskID.String(), statusMsgs[0].Values["task_id"])
	assert.Equal(t, entry.ScheduleID.String(), statusMsgs[0].Values["schedule_id"])
	assert.Equal(t, "FAILED", statusMsgs[0].Values["status"])
	assert.Equal(t, int32(entry.TaskRetryCount), statusMsgs[0].Values["retry_count"])

	// node.updated:v1 → orchestrator CheckScheduleCompletion
	nodesMsgs := publisher.GetPublishedMessagesByStream("node.updated:v1")
	require.Len(t, nodesMsgs, 1, "expected one node.updated:v1 publish")
	nodeVals := nodesMsgs[0].Values
	assert.Equal(t, entry.TaskID.String(), nodeVals["task_id"])
	assert.Equal(t, entry.ScheduleID.String(), nodeVals["schedule_id"])
	assert.Equal(t, entry.ScheduleName, nodeVals["schedule_name"])
	assert.Equal(t, entry.ServiceName, nodeVals["service_name"])
	assert.Equal(t, entry.SchemaName, nodeVals["schema_name"])
	assert.Equal(t, entry.TableName, nodeVals["table_name"])
	assert.Equal(t, "FAILED", nodeVals["status"])

	// MarkFailed called once with the right ID and reason
	require.Len(t, outboxRepo.MarkFailedCalls, 1)
	assert.Equal(t, entry.ID, outboxRepo.MarkFailedCalls[0].ID)
	assert.True(t, strings.Contains(outboxRepo.MarkFailedCalls[0].Message, "image_tag missing"),
		"reason should contain 'image_tag missing', got: %q", outboxRepo.MarkFailedCalls[0].Message)
}

func TestOutboxProcessor_MarkTaskTerminallyFailed_PublishFailureStillMarksFailed(t *testing.T) {
	entry := minEntry()
	outboxRepo := fakes.NewFakeOutboxRepo(entry)
	k8sClient := fakes.NewFakeK8sClient()
	publisher := fakes.NewFakeRedisProducer()
	publisher.SetPublishError(errors.New("redis blip"))

	p := newUnitProcessor(outboxRepo, k8sClient, publisher)
	// A publish blip must not leave the outbox stuck — MarkFailed must still be called.
	err := p.MarkTaskTerminallyFailed(context.Background(), entry, "image_tag missing for service svc-1")
	require.NoError(t, err, "helper should return nil even when publish fails")

	// MarkFailed still called despite publish errors.
	require.Len(t, outboxRepo.MarkFailedCalls, 1)
	assert.Equal(t, entry.ID, outboxRepo.MarkFailedCalls[0].ID)
}

func TestOutboxProcessor_PermanentDispatchError_TerminatesOnAttempt1(t *testing.T) {
	entry := minEntry()
	outboxRepo := fakes.NewFakeOutboxRepo(entry)
	k8sClient := fakes.NewFakeK8sClient()
	publisher := fakes.NewFakeRedisProducer()

	// Permanent error (wraps ErrPermanent)
	k8sClient.SetCreateJobError(fmt.Errorf("%w: image_tag missing for service svc-1", pkgevents.ErrPermanent))

	p := newUnitProcessor(outboxRepo, k8sClient, publisher)
	err := p.ProcessBatch(context.Background())
	require.NoError(t, err) // ProcessBatch itself never errors on per-entry failures

	// MarkFailed called exactly once.
	assert.Len(t, outboxRepo.MarkFailedCalls, 1, "MarkFailed should be called once for the permanent failure")

	// Both terminal events published.
	assert.Len(t, publisher.GetPublishedMessagesByStream("task.status.updated:v1"), 1,
		"expected one task.status.updated:v1 (FAILED)")
	assert.Len(t, publisher.GetPublishedMessagesByStream("node.updated:v1"), 1,
		"expected one node.updated:v1 (FAILED)")

	// IncrementRetry must NOT be called — no retry budget consumed.
	assert.Len(t, outboxRepo.IncrementCalls, 0, "IncrementRetry must not be called on permanent failure")
}

func TestOutboxProcessor_TransientDispatchError_RetriesUntilExhaustion_ThenTerminates(t *testing.T) {
	entry := minEntry()
	entry.OutboxRetryCount = 3 // already at max
	entry.OutboxMaxRetries = 3

	outboxRepo := fakes.NewFakeOutboxRepo(entry)
	k8sClient := fakes.NewFakeK8sClient()
	publisher := fakes.NewFakeRedisProducer()

	// Transient error (NOT ErrPermanent)
	k8sClient.SetCreateJobError(errors.New("k8s api timeout — transient"))

	p := newUnitProcessor(outboxRepo, k8sClient, publisher)
	err := p.ProcessBatch(context.Background())
	require.NoError(t, err)

	// Retry budget exhausted → terminal propagation.
	assert.Len(t, outboxRepo.MarkFailedCalls, 1, "MarkFailed should be called once after retry exhaustion")

	// Both terminal events published.
	assert.Len(t, publisher.GetPublishedMessagesByStream("task.status.updated:v1"), 1,
		"expected one task.status.updated:v1 (FAILED)")
	assert.Len(t, publisher.GetPublishedMessagesByStream("node.updated:v1"), 1,
		"expected one node.updated:v1 (FAILED)")

	// IncrementRetry must NOT be called at the cap.
	assert.Len(t, outboxRepo.IncrementCalls, 0, "IncrementRetry must not be called at retry cap")
}

func TestOutboxProcessor_HappyPath_NoTerminalPropagation(t *testing.T) {
	entry := minEntry()
	outboxRepo := fakes.NewFakeOutboxRepo(entry)
	k8sClient := fakes.NewFakeK8sClient()
	publisher := fakes.NewFakeRedisProducer()

	p := newUnitProcessor(outboxRepo, k8sClient, publisher)
	err := p.ProcessBatch(context.Background())
	require.NoError(t, err)

	// No terminal events — only RUNNING status and node.deployed.
	nodeUpdated := publisher.GetPublishedMessagesByStream("node.updated:v1")
	assert.Len(t, nodeUpdated, 0, "node.updated:v1 must NOT be published on success")

	statusMsgs := publisher.GetPublishedMessagesByStream("task.status.updated:v1")
	require.Len(t, statusMsgs, 1)
	assert.Equal(t, "RUNNING", statusMsgs[0].Values["status"])

	// MarkFailed must never be called.
	assert.Len(t, outboxRepo.MarkFailedCalls, 0, "MarkFailed must not be called on success")

	// MarkProcessed must be called.
	assert.Len(t, outboxRepo.ProcessedCalls, 1, "MarkProcessed must be called on success")
}
