package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/dependency-controller/domain/command"
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

// TestMessageDeduplication_SameMessageTwice verifies that processing the same message twice
// results in only one outbox entry and the message is marked as completed.
func TestMessageDeduplication_SameMessageTwice(t *testing.T) {
	// Setup
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
			return &statev1.Task{
				TaskId: uuid.New().String(),
				Status: statev1.TaskStatus_TASK_STATUS_PENDING,
			}, nil
		},
	}

	uow := fakes.NewFakeUnitOfWork()
	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	scheduleID := uuid.New()
	taskID := uuid.New()
	messageID := "test-dedup-msg-1"

	cmd := command.ProcessNodeStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	// Execute: Process message first time
	err := handler.Handle(ctx, cmd, messageID)
	require.NoError(t, err)

	// Verify first processing
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 1, "First processing should create 1 outbox entry")

	// Verify message processing state is 'completed'
	msgProc, err := uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
	require.NoError(t, err)
	assert.Equal(t, messageID, msgProc.MessageID)
	assert.Equal(t, "node.updated:v1", msgProc.StreamName)
	assert.Equal(t, model.MessageProcessingStateCompleted, msgProc.State)

	// Reset outbox to track second attempt
	initialOutboxCount := len(uow.OutboxRepository.CreatedEntries)

	// Execute: Process same message second time
	err = handler.Handle(ctx, cmd, messageID)
	require.NoError(t, err)

	// Verify no additional outbox entries created
	assert.Len(t, uow.OutboxRepository.CreatedEntries, initialOutboxCount,
		"Second processing should NOT create additional outbox entries")

	// Verify message processing state is still 'completed'
	msgProc2, err := uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
	require.NoError(t, err)
	assert.Equal(t, model.MessageProcessingStateCompleted, msgProc2.State)
	assert.Equal(t, msgProc.ID, msgProc2.ID, "Should be same message processing record")
}

// TestMessageDeduplication_ConcurrentProcessing simulates concurrent processing by inserting
// a message as 'processing' first, then trying to process the same message.
// Verifies that no outbox entries are created (processing skipped).
func TestMessageDeduplication_ConcurrentProcessing(t *testing.T) {
	// Setup
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
			return &statev1.Task{
				TaskId: uuid.New().String(),
				Status: statev1.TaskStatus_TASK_STATUS_PENDING,
			}, nil
		},
	}

	uow := fakes.NewFakeUnitOfWork()
	handler := handlers.NewProcessStatusHandler(uow, graphClient, stateClient, logger)

	messageID := "test-concurrent-msg-1"

	// Pre-insert message as 'processing' to simulate concurrent processing
	existingMsgProc := &model.MessageProcessing{
		MessageID:  messageID,
		StreamName: "node.updated:v1",
		State:      model.MessageProcessingStateProcessing,
		Payload:    []byte(`{"test": "payload"}`),
	}
	_, _, err := uow.MessageProcessingRepo().InsertIfNotExists(ctx, existingMsgProc)
	require.NoError(t, err)

	// Verify message is in processing state
	msgProc, err := uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
	require.NoError(t, err)
	assert.Equal(t, model.MessageProcessingStateProcessing, msgProc.State)

	// Execute: Try to process the same message
	cmd := command.ProcessNodeStatus{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "dbt",
		Schema:       "public",
		TableName:    "users",
		Status:       "SUCCEEDED",
	}

	err = handler.Handle(ctx, cmd, messageID)
	require.NoError(t, err)

	// Verify no outbox entries created (processing was skipped)
	assert.Len(t, uow.OutboxRepository.CreatedEntries, 0,
		"Concurrent processing should NOT create any outbox entries")

	// Verify message processing state is still 'processing'
	msgProc2, err := uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
	require.NoError(t, err)
	assert.Equal(t, model.MessageProcessingStateProcessing, msgProc2.State,
		"Message state should remain 'processing' (unchanged)")
}
