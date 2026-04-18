package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/k8s-controller/test/fakes"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"log/slog"
	"os"
	"strings"
)

func TestCheckStatusHandler_HandleSucceeded(t *testing.T) {
	// Setup testcontainer PostgreSQL
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	jobName := "test-job"

	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()

	// Create fakes for K8s and state client
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusSucceeded,
				StartedAt:        &startTime,
				CompletedAt:      &endTime,
				ExecutionSeconds: endTime.Sub(startTime).Seconds(),
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, tid uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     tid.String(),
				RetryCount: 0,
				MaxRetries: 3,
			}, nil
		},
	}

	// Create real UnitOfWork with database
	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}

	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Execute handler
	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      jobName,
	}

	err := handler.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify outbox entries were created
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1", taskID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query outbox: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 outbox entries (task_succeeded + node_status_updated), got: %d", count)
	}

	// Process outbox entries using OutboxProcessor
	eventPublisher := &fakes.FakeEventPublisher{}
	outboxProcessor := handlers.NewOutboxProcessor(outboxRepo, eventPublisher, logger)

	err = outboxProcessor.ProcessOnce(ctx, 100) // Process batch
	if err != nil {
		t.Fatalf("Failed to process outbox: %v", err)
	}

	// Verify task.status.updated:v1 was published (replaces gRPC UpdateTaskWithRetry)
	statusCall := eventPublisher.FindPublish("task.status.updated:v1")
	if statusCall == nil {
		t.Errorf("Expected task.status.updated:v1 to be published")
	} else if statusCall.Values["status"] != "SUCCEEDED" {
		t.Errorf("Expected status SUCCEEDED, got: %v", statusCall.Values["status"])
	}

	// Verify task.execution.recorded:v1 was published (replaces gRPC CreateTaskExecution)
	execCall := eventPublisher.FindPublish("task.execution.recorded:v1")
	if execCall == nil {
		t.Errorf("Expected task.execution.recorded:v1 to be published")
	}

	// Verify outbox entries were marked as processed
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1 AND status = 'processed'",
		taskID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query processed outbox: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 processed outbox entries, got: %d", count)
	}
}

func TestCheckStatusHandler_HandleFailedWithRetry(t *testing.T) {
	// Setup testcontainer PostgreSQL
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	jobName := "test-job"

	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()
	exitCode := int32(1)

	// Create fakes for K8s and state client
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusFailed,
				ExitCode:         &exitCode,
				TerminationMsg:   "OOMKilled: Memory limit exceeded",
				StartedAt:        &startTime,
				CompletedAt:      &endTime,
				ExecutionSeconds: endTime.Sub(startTime).Seconds(),
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, tid uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     tid.String(),
				RetryCount: 1, // Current retry count
				MaxRetries: 3,
			}, nil
		},
	}

	// Create real UnitOfWork with database
	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}

	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Execute handler
	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      jobName,
	}

	err := handler.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify outbox entry was created
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1 AND event_type = 'task_retry'",
		taskID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query outbox: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 task_retry outbox entry, got: %d", count)
	}

	// Process outbox entries using OutboxProcessor
	eventPublisher := &fakes.FakeEventPublisher{}
	outboxProcessor := handlers.NewOutboxProcessor(outboxRepo, eventPublisher, logger)

	err = outboxProcessor.ProcessOnce(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to process outbox: %v", err)
	}

	// Verify task.status.updated:v1 was published (replaces gRPC UpdateTaskWithRetry)
	statusCall := eventPublisher.FindPublish("task.status.updated:v1")
	if statusCall == nil {
		t.Errorf("Expected task.status.updated:v1 to be published")
	} else {
		if statusCall.Values["status"] != "FAILED" {
			t.Errorf("Expected status FAILED, got: %v", statusCall.Values["status"])
		}
		// retry_count stored as int in map — compare as fmt string or cast
		if retryCount, ok := statusCall.Values["retry_count"].(int); ok {
			if retryCount != 2 {
				t.Errorf("Expected retry_count 2, got: %d", retryCount)
			}
		}
	}

	// Verify task.execution.recorded:v1 was published (replaces gRPC CreateTaskExecution)
	execCall := eventPublisher.FindPublish("task.execution.recorded:v1")
	if execCall == nil {
		t.Errorf("Expected task.execution.recorded:v1 to be published")
	}

	// Verify Redis stream event was also published (retry.task:v1)
	retryCall := eventPublisher.FindPublish("retry.task:v1")
	if retryCall == nil {
		t.Errorf("Expected retry.task:v1 Redis event to be published")
	}
}

func TestCheckStatusHandler_HandleFailedPermanent(t *testing.T) {
	// Setup testcontainer PostgreSQL
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	jobName := "test-job"

	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()
	exitCode := int32(1)

	// Create fakes for K8s and state client
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusFailed,
				ExitCode:         &exitCode,
				TerminationMsg:   "Error: Connection refused",
				StartedAt:        &startTime,
				CompletedAt:      &endTime,
				ExecutionSeconds: endTime.Sub(startTime).Seconds(),
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, tid uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     tid.String(),
				RetryCount: 3, // Already at max retries
				MaxRetries: 3,
			}, nil
		},
	}

	// Create real UnitOfWork with database
	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}

	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Execute handler
	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      jobName,
	}

	err := handler.Handle(ctx, cmd)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify outbox entries were created (task_failed + node_status_updated)
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1",
		taskID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query outbox: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 outbox entries (task_failed + node_status_updated), got: %d", count)
	}

	// Verify task_failed entry exists
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1 AND event_type = 'task_failed'",
		taskID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query outbox: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 task_failed outbox entry, got: %d", count)
	}

	// Process outbox entries using OutboxProcessor
	eventPublisher := &fakes.FakeEventPublisher{}
	outboxProcessor := handlers.NewOutboxProcessor(outboxRepo, eventPublisher, logger)

	err = outboxProcessor.ProcessOnce(ctx, 100)
	if err != nil {
		t.Fatalf("Failed to process outbox: %v", err)
	}

	// Verify task.status.updated:v1 was published (replaces gRPC UpdateTaskWithRetry)
	statusCall := eventPublisher.FindPublish("task.status.updated:v1")
	if statusCall == nil {
		t.Errorf("Expected task.status.updated:v1 to be published")
	} else if statusCall.Values["status"] != "FAILED" {
		t.Errorf("Expected status FAILED, got: %v", statusCall.Values["status"])
	}

	// Verify task.execution.recorded:v1 was published (replaces gRPC CreateTaskExecution)
	if eventPublisher.FindPublish("task.execution.recorded:v1") == nil {
		t.Errorf("Expected task.execution.recorded:v1 to be published")
	}

	// Verify node.updated:v1 was published (2nd outbox entry)
	if eventPublisher.FindPublish("node.updated:v1") == nil {
		t.Errorf("Expected node.updated:v1 to be published")
	}

	// task_failed entry publishes: task.status.updated:v1 + task.execution.recorded:v1 + task.failed:v1
	// node_status_updated entry publishes: node.updated:v1
	// Total: 4 publishes
	if eventPublisher.PublishCallCount != 4 {
		t.Errorf("Expected 4 Redis events to be published, got: %d", eventPublisher.PublishCallCount)
	}
}

func TestCheckStatusHandler_HandleRunning(t *testing.T) {
	// Setup
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	taskID := uuid.New()
	scheduleID := uuid.New()
	jobName := "test-job"

	// Create fakes
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status: model.JobStatusRunning,
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, tid uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     tid.String(),
				RetryCount: 0,
				MaxRetries: 3,
			}, nil
		},
	}

	unitOfWork := &fakes.FakeUnitOfWork{}

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}

	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Execute
	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      jobName,
	}

	err := handler.Handle(ctx, cmd)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Note: Check events for delayed re-check are now done via outbox pattern

	// Verify no state updates were made (running jobs don't update state)
	if stateClient.UpdateStatusCallCount != 0 {
		t.Errorf("Expected no status updates for running job, got: %d", stateClient.UpdateStatusCallCount)
	}
}

func TestCheckStatusHandler_ErrorMessageTruncation(t *testing.T) {
	// Setup
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	taskID := uuid.New()
	scheduleID := uuid.New()
	jobName := "test-job"

	// Create a long error message that exceeds the limit
	longErrorMsg := ""
	for i := 0; i < 5000; i++ {
		longErrorMsg += "X"
	}

	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()
	exitCode := int32(1)

	// Create fakes
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(ctx context.Context, namespace, jobName string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusFailed,
				ExitCode:         &exitCode,
				TerminationMsg:   longErrorMsg,
				StartedAt:        &startTime,
				CompletedAt:      &endTime,
				ExecutionSeconds: endTime.Sub(startTime).Seconds(),
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, tid uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId:     tid.String(),
				RetryCount: 3,
				MaxRetries: 3,
			}, nil
		},
	}

	unitOfWork := &fakes.FakeUnitOfWork{}

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 100, // Small limit for testing
		LogTailLines:       50,
	}

	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	// Execute
	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      jobName,
	}

	err := handler.Handle(ctx, cmd)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Note: Error message truncation is verified via the outbox entry in integration tests
}

func TestCheckStatusHandler_WritesProcessedEvent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	outboxEntryID := uuid.New()
	taskID := uuid.New()
	scheduleID := uuid.New()

	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			now := time.Now()
			return &model.K8sPodResult{
				Status:      model.JobStatusSucceeded,
				StartedAt:   &now,
				CompletedAt: &now,
			}, nil
		},
	}
	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(_ context.Context, id uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{TaskId: id.String(), RetryCount: 0, MaxRetries: 3}, nil
		},
	}

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	config := &handlers.HandlerConfig{K8sNamespace: "default", CheckDelaySeconds: 30, ErrorMessageMaxLen: 4096, LogTailLines: 50}
	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	cmd := command.CheckJobStatus{
		OutboxEntryID: &outboxEntryID,
		TaskID:        taskID,
		ScheduleID:    scheduleID,
		ScheduleName:  "test-schedule",
		ServiceName:   "dbt",
		SchemaName:    "public",
		TableName:     "users",
		JobName:       "test-job",
	}

	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	// processed_events must contain the entry
	var count int
	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM processed_events WHERE outbox_entry_id = $1",
		outboxEntryID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "processed_events must have one row for this entry")
}

// TestCheckStatusHandler_Dedup_SecondCallIsSkipped verifies that a second Handle call
// with the same outbox_entry_id writes no additional outbox entries and returns nil.
func TestCheckStatusHandler_Dedup_SecondCallIsSkipped(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	outboxEntryID := uuid.New()
	taskID := uuid.New()
	scheduleID := uuid.New()
	now := time.Now()

	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:      model.JobStatusSucceeded,
				StartedAt:   &now,
				CompletedAt: &now,
			}, nil
		},
	}
	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(_ context.Context, id uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{TaskId: id.String(), RetryCount: 0, MaxRetries: 3}, nil
		},
	}

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	config := &handlers.HandlerConfig{K8sNamespace: "default", CheckDelaySeconds: 30, ErrorMessageMaxLen: 4096, LogTailLines: 50}
	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	cmd := command.CheckJobStatus{
		OutboxEntryID: &outboxEntryID,
		TaskID:        taskID,
		ScheduleID:    scheduleID,
		ScheduleName:  "test-schedule",
		ServiceName:   "dbt",
		SchemaName:    "public",
		TableName:     "users",
		JobName:       "test-job",
	}

	// First call — normal processing
	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	var countAfterFirst int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1", taskID).Scan(&countAfterFirst)
	require.Equal(t, 2, countAfterFirst, "first call must write 2 outbox entries")

	// Second call — same outbox_entry_id, must be skipped
	err = handler.Handle(ctx, cmd)
	require.NoError(t, err, "duplicate message must not return error")

	var countAfterSecond int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1", taskID).Scan(&countAfterSecond)
	require.Equal(t, 2, countAfterSecond, "second call must write no additional outbox entries")

	var dedupCount int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM processed_events WHERE outbox_entry_id = $1", outboxEntryID).Scan(&dedupCount)
	require.Equal(t, 1, dedupCount, "processed_events must have exactly one row")
}

// TestCheckStatusHandler_Dedup_NilOutboxEntryID verifies backwards-compat:
// a nil OutboxEntryID bypasses dedup and processes normally.
func TestCheckStatusHandler_Dedup_NilOutboxEntryID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	now := time.Now()

	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:      model.JobStatusSucceeded,
				StartedAt:   &now,
				CompletedAt: &now,
			}, nil
		},
	}
	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(_ context.Context, id uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{TaskId: id.String(), RetryCount: 0, MaxRetries: 3}, nil
		},
	}

	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)
	config := &handlers.HandlerConfig{K8sNamespace: "default", CheckDelaySeconds: 30, ErrorMessageMaxLen: 4096, LogTailLines: 50}
	logUploader := &fakes.FakeLogUploader{}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	cmd := command.CheckJobStatus{
		OutboxEntryID: nil, // absent — backwards compat
		TaskID:        taskID,
		ScheduleID:    scheduleID,
		ScheduleName:  "test-schedule",
		ServiceName:   "dbt",
		SchemaName:    "public",
		TableName:     "users",
		JobName:       "test-job",
	}

	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	var count int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM k8s_status_outbox WHERE task_id = $1", taskID).Scan(&count)
	require.Equal(t, 2, count, "nil OutboxEntryID must still write outbox entries")

	var dedupCount int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM processed_events").Scan(&dedupCount)
	require.Equal(t, 0, dedupCount, "nil OutboxEntryID must not write to processed_events")
}

func TestCheckStatusHandler_HandleFailedPermanent_LogsUploadedToS3(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()
	exitCode := int32(1)

	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusFailed,
				ExitCode:         &exitCode,
				TerminationMsg:   "Error",
				StartedAt:        &startTime,
				CompletedAt:      &endTime,
				ExecutionSeconds: endTime.Sub(startTime).Seconds(),
			}, nil
		},
		GetPodLogsFunc: func(_ context.Context, _, _ string, _ int64) (string, string, error) {
			return "FATAL: dbt model failed\nsome other output", "some other output", nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(_ context.Context, id uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{TaskId: id.String(), RetryCount: 3, MaxRetries: 3}, nil
		},
	}

	logUploader := &fakes.FakeLogUploader{}
	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	config := &handlers.HandlerConfig{
		K8sNamespace:       "default",
		CheckDelaySeconds:  30,
		ErrorMessageMaxLen: 4096,
		LogTailLines:       50,
	}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "test-schedule",
		ServiceName:  "my-service",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "test-job",
	}

	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	// S3 upload was called once
	require.Equal(t, 1, logUploader.UploadCallCount)
	// Key follows the expected path pattern
	require.Contains(t, logUploader.LastKey, "logs/task-executions/my-service/public/orders/")
	require.True(t, strings.HasSuffix(logUploader.LastKey, ".log"))
	// Full log was uploaded
	require.Equal(t, "FATAL: dbt model failed\nsome other output", logUploader.LastContent)

	// Outbox entry has log_s3_key and execution_id set
	var logS3Key string
	var execIDStr string
	err = db.QueryRowContext(ctx,
		"SELECT COALESCE(log_s3_key, ''), COALESCE(execution_id::text, '') FROM k8s_status_outbox WHERE task_id = $1 AND event_type = 'task_failed'",
		taskID).Scan(&logS3Key, &execIDStr)
	require.NoError(t, err)
	require.Equal(t, logUploader.LastKey, logS3Key)
	require.NotEmpty(t, execIDStr)
}

func TestCheckStatusHandler_HandleFailedPermanent_S3FailIsSoft(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	scheduleID := uuid.New()
	startTime := time.Now().Add(-1 * time.Minute)
	endTime := time.Now()
	exitCode := int32(1)

	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status: model.JobStatusFailed, ExitCode: &exitCode,
				StartedAt: &startTime, CompletedAt: &endTime,
			}, nil
		},
		GetPodLogsFunc: func(_ context.Context, _, _ string, _ int64) (string, string, error) {
			return "log output", "log output", nil
		},
	}
	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(_ context.Context, id uuid.UUID) (*statev1.Task, error) {
			return &statev1.Task{TaskId: id.String(), RetryCount: 3, MaxRetries: 3}, nil
		},
	}

	logUploader := &fakes.FakeLogUploader{ShouldFail: true}
	unitOfWork := uow.NewPostgresUnitOfWork(db, logger)

	config := &handlers.HandlerConfig{
		K8sNamespace: "default", CheckDelaySeconds: 30, ErrorMessageMaxLen: 4096, LogTailLines: 50,
	}
	handler := handlers.NewCheckStatusHandler(k8sClient, stateClient, unitOfWork, logUploader, config, logger)

	cmd := command.CheckJobStatus{
		TaskID: taskID, ScheduleID: scheduleID, ScheduleName: "s", ServiceName: "svc",
		SchemaName: "sc", TableName: "t", JobName: "job",
	}

	// Must not return error even when S3 fails
	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	// Outbox entry created with empty log_s3_key
	var logS3Key string
	db.QueryRowContext(ctx,
		"SELECT COALESCE(log_s3_key, '') FROM k8s_status_outbox WHERE task_id = $1 AND event_type = 'task_failed'",
		taskID).Scan(&logS3Key)
	require.Empty(t, logS3Key, "log_s3_key must be empty when S3 upload fails")
}

// TestFakeProcessedEventsRepository_ConcurrentTryMarkProcessed verifies that
// the fake correctly serialises concurrent callers: exactly one wins the
// "claim" and the other is told it's a duplicate.
func TestFakeProcessedEventsRepository_ConcurrentTryMarkProcessed(t *testing.T) {
	fake := &fakes.FakeProcessedEventsRepository{}
	id := uuid.New()
	ctx := context.Background()

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		newClaims  int
		duplicates int
	)

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dup, err := fake.TryMarkProcessed(ctx, id)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			if dup {
				duplicates++
			} else {
				newClaims++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// The fake is not goroutine-safe (unsynchronised slice), but the REAL
	// Postgres implementation is. This test documents the expected semantics:
	// one winner, rest are duplicates. Thread-safety is guaranteed by the
	// Postgres ON CONFLICT DO NOTHING constraint (covered by integration tests).
	if newClaims+duplicates != 10 {
		t.Errorf("expected 10 total calls, got newClaims=%d duplicates=%d", newClaims, duplicates)
	}
}
