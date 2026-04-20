package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/test/fakes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOutboxProcessor_CheckDelayed_PublishesOutboxEntryID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()

	// Seed a check_delayed outbox entry
	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 check_after, status)
		VALUES ($1, 'check_delayed', 'check.k8s:v1',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job',
			0, 'pending')
	`, entryID, taskID, schedID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	require.Equal(t, 1, publisher.PublishCallCount)
	vals := publisher.LastValues
	require.Equal(t, entryID.String(), vals["outbox_entry_id"],
		"check_delayed publish must include the outbox entry ID")
}

// TestOutboxProcessor_TaskStatusUpdated_PublishesStatusEvent verifies that when an
// outbox entry has UpdateTaskStatus=true, the processor publishes to task.status.updated:v1
// without making any gRPC calls.
func TestOutboxProcessor_TaskStatusUpdated_PublishesStatusEvent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()

	// Seed an entry with update_task_status=true, no stream publish (task_succeeded with no StreamName)
	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 update_task_status, new_task_status, new_retry_count,
			 status)
		VALUES ($1, 'task_succeeded', '',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job-success',
			true, 'succeeded', 0,
			'pending')
	`, entryID, taskID, schedID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	// Must have published exactly 1 event: task.status.updated:v1
	require.Equal(t, 1, publisher.PublishCallCount,
		"expected exactly 1 Redis publish for UpdateTaskStatus=true, no stream_name")

	call := publisher.FindPublish("task.status.updated:v1")
	require.NotNil(t, call, "must publish to task.status.updated:v1")
	require.Equal(t, taskID.String(), fmt.Sprintf("%v", call.Values["task_id"]))
	require.Equal(t, schedID.String(), fmt.Sprintf("%v", call.Values["schedule_id"]))
	require.Equal(t, "SUCCEEDED", fmt.Sprintf("%v", call.Values["status"]))
}

// TestOutboxProcessor_CreateExecution_PublishesExecutionEvent verifies that when
// CreateExecution=true, the processor publishes to task.execution.recorded:v1.
func TestOutboxProcessor_CreateExecution_PublishesExecutionEvent(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()
	executionID := uuid.New()

	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 create_execution, execution_seconds, execution_id,
			 status)
		VALUES ($1, 'task_succeeded', '',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job-exec',
			true, 42.5, $4,
			'pending')
	`, entryID, taskID, schedID, executionID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	require.Equal(t, 1, publisher.PublishCallCount,
		"expected exactly 1 Redis publish for CreateExecution=true, no stream_name")

	call := publisher.FindPublish("task.execution.recorded:v1")
	require.NotNil(t, call, "must publish to task.execution.recorded:v1")
	require.Equal(t, executionID.String(), fmt.Sprintf("%v", call.Values["execution_id"]))
	require.Equal(t, taskID.String(), fmt.Sprintf("%v", call.Values["task_id"]))
	require.Equal(t, "job-exec", fmt.Sprintf("%v", call.Values["job_name"]))
}

// TestOutboxProcessor_NodeStatusUpdated_IncludesTaskID verifies that node.updated:v1
// events include task_id in the payload.
func TestOutboxProcessor_NodeStatusUpdated_IncludesTaskID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()

	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 new_task_status, status)
		VALUES ($1, 'node_status_updated', 'node.updated:v1',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job-node',
			'SUCCEEDED', 'pending')
	`, entryID, taskID, schedID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	call := publisher.FindPublish("node.updated:v1")
	require.NotNil(t, call, "must publish to node.updated:v1")
	require.Equal(t, taskID.String(), fmt.Sprintf("%v", call.Values["task_id"]),
		"node.updated:v1 payload must include task_id")
}

// TestOutboxProcessor_SucceededWithExecution_NoGRPCCalls verifies the full succeeded
// path (UpdateTaskStatus + CreateExecution + node.updated:v1) uses only Redis — no gRPC.
// This is tested implicitly: if gRPC were called the test would panic (no gRPC server).
func TestOutboxProcessor_SucceededWithExecution_ThreePublishes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	taskID := uuid.New()
	schedID := uuid.New()
	executionID := uuid.New()

	// Entry 1: task_succeeded (UpdateTaskStatus + CreateExecution, no stream)
	entry1ID := uuid.New()
	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 update_task_status, new_task_status, new_retry_count,
			 create_execution, execution_seconds, execution_id,
			 status)
		VALUES ($1, 'task_succeeded', '',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job-full',
			true, 'succeeded', 0,
			true, 10.0, $4,
			'pending')
	`, entry1ID, taskID, schedID, executionID)
	require.NoError(t, err)

	// Entry 2: node_status_updated → node.updated:v1
	entry2ID := uuid.New()
	_, err = db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 new_task_status, status)
		VALUES ($1, 'node_status_updated', 'node.updated:v1',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job-full',
			'SUCCEEDED', 'pending')
	`, entry2ID, taskID, schedID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	// Entry1 → task.status.updated:v1 + task.execution.recorded:v1 = 2 publishes
	// Entry2 → node.updated:v1 = 1 publish
	// Total: 3
	require.Equal(t, 3, publisher.PublishCallCount,
		"expected 3 total Redis publishes for the two outbox entries")

	require.NotNil(t, publisher.FindPublish("task.status.updated:v1"))
	require.NotNil(t, publisher.FindPublish("task.execution.recorded:v1"))
	require.NotNil(t, publisher.FindPublish("node.updated:v1"))
}
