package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/dependency-controller/adapters/grpc"
	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/domain/event"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/carolsimone/continuo/dependency-controller/service/handlers"
	"github.com/carolsimone/continuo/dependency-controller/test/fakes"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func TestHandleSucceeded_NoDownstream(t *testing.T) {
	// Setup
	ctx := context.Background()
	logger := newTestLogger()

	graphClient := &fakes.FakeGraphNodeClient{}
	stateClient := &fakes.FakeStateClient{}
	uow := fakes.NewFakeUnitOfWork()

	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	scheduleID := uuid.New()
	taskID := uuid.New()

	cmd := command.ProcessNodeStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	// Execute
	err := handler.Handle(ctx, cmd, "test-msg-1")

	// Assert
	require.NoError(t, err)

	// Should have updated graph node status
	assert.Len(t, graphClient.UpdateNodeStatusCalls, 1)
	assert.Equal(t, "SUCCEEDED", graphClient.UpdateNodeStatusCalls[0].Status)
	assert.Equal(t, "users", graphClient.UpdateNodeStatusCalls[0].TableName)

	// Should have queried downstream
	assert.Len(t, graphClient.GetReadyDownstreamCalls, 1)

	// No outbox entries (no downstream nodes)
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 0)

	// Should have recorded message processing
	msgProc, err := uow.MessageProcessingRepo().GetByMessageID(ctx, "test-msg-1")
	require.NoError(t, err)
	assert.Equal(t, "test-msg-1", msgProc.MessageID)
	assert.Equal(t, "update.table:v1", msgProc.StreamName)
	assert.Equal(t, model.MessageProcessingStateCompleted, msgProc.State)
}

func TestHandleSucceeded_WithDownstream(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	existingTaskID := uuid.New()

	graphClient := &fakes.FakeGraphNodeClient{
		GetReadyDownstreamFunc: func(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
			return []model.DownstreamNode{
				{ServiceName: "dbt", Schema: "analytics", TableName: "user_summary", NodeType: "dbt-model"},
				{ServiceName: "dbt", Schema: "analytics", TableName: "user_metrics", NodeType: "dbt-model"},
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId: existingTaskID.String(),
				Status: statev1.TaskStatus_TASK_STATUS_PENDING,
			}, nil
		},
	}

	uow := fakes.NewFakeUnitOfWork()
	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	scheduleID := uuid.New()
	cmd := command.ProcessNodeStatus{
		TaskID:       uuid.New(),
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	err := handler.Handle(ctx, cmd, "test-msg-2")

	require.NoError(t, err)
	assert.Len(t, graphClient.UpdateNodeStatusCalls, 1)
	assert.Len(t, graphClient.GetReadyDownstreamCalls, 1)
	// Two outbox entries — one per downstream node
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 2)
	for _, entry := range uow.OutboxRepository.CreatedEntries {
		assert.Equal(t, "node_ready_for_execution", entry.EventType)
		assert.Equal(t, "query.model:v1", entry.StreamName)
		assert.Equal(t, "dependency", entry.AggregateType)
		assert.Equal(t, string(model.OutboxStatusPending), entry.Status)
		var evt event.NodeReadyForExecution
		require.NoError(t, json.Unmarshal(entry.Payload, &evt))
		assert.Equal(t, scheduleID.String(), evt.ScheduleID)
		assert.Equal(t, "daily", evt.ScheduleName)
		assert.Equal(t, "dbt-model", evt.NodeType)
	}
	assert.True(t, uow.CommittedTx)
}

func TestHandleFailed_NoDownstreamActivation(t *testing.T) {
	// Setup
	ctx := context.Background()
	logger := newTestLogger()

	graphClient := &fakes.FakeGraphNodeClient{}
	stateClient := &fakes.FakeStateClient{}
	uow := fakes.NewFakeUnitOfWork()

	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	scheduleID := uuid.New()
	taskID := uuid.New()

	cmd := command.ProcessNodeStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "FAILED",
	}

	// Execute
	err := handler.Handle(ctx, cmd, "test-msg-3")

	// Assert
	require.NoError(t, err)

	// Should have updated graph node status
	assert.Len(t, graphClient.UpdateNodeStatusCalls, 1)
	assert.Equal(t, "FAILED", graphClient.UpdateNodeStatusCalls[0].Status)

	// Should NOT have queried downstream (failed nodes don't trigger downstream)
	assert.Len(t, graphClient.GetReadyDownstreamCalls, 0)

	// No outbox entries
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 0)
}

func TestHandleSucceeded_ExistingTaskPending(t *testing.T) {
	// Setup: downstream node has an existing task already in pending state
	ctx := context.Background()
	logger := newTestLogger()

	existingTaskID := uuid.New()

	graphClient := &fakes.FakeGraphNodeClient{
		GetReadyDownstreamFunc: func(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
			return []model.DownstreamNode{
				{ServiceName: "dbt", Schema: "analytics", TableName: "user_summary", NodeType: "dbt-model"},
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
			return &statev1.Task{
				TaskId: existingTaskID.String(),
				Status: statev1.TaskStatus_TASK_STATUS_PENDING,
			}, nil
		},
	}

	uow := fakes.NewFakeUnitOfWork()

	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	scheduleID := uuid.New()

	cmd := command.ProcessNodeStatus{
		TaskID:       uuid.New(),
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	// Execute
	err := handler.Handle(ctx, cmd, "test-msg-4")

	// Assert
	require.NoError(t, err)

	// Should have written outbox entry with existing task ID
	require.Len(t, uow.OutboxRepository.CreatedEntries, 1)
	var evt event.NodeReadyForExecution
	err = json.Unmarshal(uow.OutboxRepository.CreatedEntries[0].Payload, &evt)
	require.NoError(t, err)
	assert.Equal(t, existingTaskID.String(), evt.TaskID)
}

// TestHandleSucceeded_WithDownstream_TaskNotFound verifies that ErrTaskNotFound
// from GetTask is treated as a contract violation (hard error), not silently recovered.
func TestHandleSucceeded_WithDownstream_TaskNotFound(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	graphClient := &fakes.FakeGraphNodeClient{
		GetReadyDownstreamFunc: func(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
			return []model.DownstreamNode{
				{ServiceName: "dbt", Schema: "analytics", TableName: "user_summary", NodeType: "dbt-model"},
			}, nil
		},
	}

	stateClient := &fakes.FakeStateClient{
		GetTaskFunc: func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
			return nil, grpc.ErrTaskNotFound
		},
	}

	uow := fakes.NewFakeUnitOfWork()
	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	cmd := command.ProcessNodeStatus{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	err := handler.Handle(ctx, cmd, "test-msg-not-found")

	require.Error(t, err, "ErrTaskNotFound must propagate as a hard error")
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 0)
}
