package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/carolsimone/continuo/dependency-controller/service/handlers"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── mocks ────────────────────────────────────────────────────────────────────

type mockDependencyRepo struct {
	updateNodeStatusFn        func(ctx context.Context, scheduleName, schema, tableName, status, runID string) error
	getReadyDownstreamFn      func(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error)
	checkScheduleCompletionFn func(ctx context.Context, scheduleName, runID string) (bool, bool, error)
	finalizeRunFn             func(ctx context.Context, runID, terminalStatus string) error
}

func (m *mockDependencyRepo) UpdateNodeStatus(ctx context.Context, scheduleName, schema, tableName, status, runID string) error {
	if m.updateNodeStatusFn != nil {
		return m.updateNodeStatusFn(ctx, scheduleName, schema, tableName, status, runID)
	}
	return nil
}

func (m *mockDependencyRepo) GetReadyDownstream(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
	if m.getReadyDownstreamFn != nil {
		return m.getReadyDownstreamFn(ctx, scheduleName, schema, tableName, runID)
	}
	return nil, nil
}

func (m *mockDependencyRepo) CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (bool, bool, error) {
	if m.checkScheduleCompletionFn != nil {
		return m.checkScheduleCompletionFn(ctx, scheduleName, runID)
	}
	return false, false, nil
}

func (m *mockDependencyRepo) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	if m.finalizeRunFn != nil {
		return m.finalizeRunFn(ctx, runID, terminalStatus)
	}
	return nil
}

type mockStateClient struct {
	getTaskFn                func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	updateTaskStatusFn       func(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error
	updateSchedulerStatusFn  func(ctx context.Context, scheduleID uuid.UUID, schedulerStatus statev1.SchedulerStatus) error
	getSchedulerInitStatusFn func(ctx context.Context, scheduleID uuid.UUID) (string, error)

	updateSchedulerCalled   bool
	capturedSchedulerStatus statev1.SchedulerStatus
}

func (m *mockStateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
	if m.getTaskFn != nil {
		return m.getTaskFn(ctx, scheduleID, serviceName, schemaName, tableName)
	}
	return &statev1.Task{
		TaskId: uuid.New().String(),
		Status: statev1.TaskStatus_TASK_STATUS_PENDING,
	}, nil
}

func (m *mockStateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error {
	if m.updateTaskStatusFn != nil {
		return m.updateTaskStatusFn(ctx, taskID, taskStatus)
	}
	return nil
}

func (m *mockStateClient) UpdateSchedulerStatus(ctx context.Context, scheduleID uuid.UUID, schedulerStatus statev1.SchedulerStatus) error {
	m.updateSchedulerCalled = true
	m.capturedSchedulerStatus = schedulerStatus
	if m.updateSchedulerStatusFn != nil {
		return m.updateSchedulerStatusFn(ctx, scheduleID, schedulerStatus)
	}
	return nil
}

func (m *mockStateClient) GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error) {
	if m.getSchedulerInitStatusFn != nil {
		return m.getSchedulerInitStatusFn(ctx, scheduleID)
	}
	return "completed", nil
}

type mockMsgProcessingRepo struct{}

func (m *mockMsgProcessingRepo) InsertIfNotExists(ctx context.Context, msgProc *model.MessageProcessing) (uuid.UUID, bool, error) {
	return uuid.New(), true, nil
}
func (m *mockMsgProcessingRepo) GetByMessageID(ctx context.Context, messageID string) (*model.MessageProcessing, error) {
	return nil, nil
}
func (m *mockMsgProcessingRepo) UpdateState(ctx context.Context, id uuid.UUID, state model.MessageProcessingState) error {
	return nil
}

type mockOutboxRepo struct{}

func (m *mockOutboxRepo) Create(ctx context.Context, entry *model.OutboxEntry) error { return nil }
func (m *mockOutboxRepo) GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	return nil, nil
}
func (m *mockOutboxRepo) MarkProcessed(ctx context.Context, id uuid.UUID) error { return nil }
func (m *mockOutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	return nil
}
func (m *mockOutboxRepo) IncrementRetry(ctx context.Context, id uuid.UUID) error { return nil }
func (m *mockOutboxRepo) UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
	return nil
}

type mockUnitOfWork struct{}

func (m *mockUnitOfWork) OutboxRepo() postgres.OutboxRepository { return &mockOutboxRepo{} }
func (m *mockUnitOfWork) MessageProcessingRepo() postgres.MessageProcessingRepository {
	return &mockMsgProcessingRepo{}
}
func (m *mockUnitOfWork) Begin(ctx context.Context) error { return nil }
func (m *mockUnitOfWork) Commit() error                   { return nil }
func (m *mockUnitOfWork) Rollback() error                 { return nil }

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func baseCmd() command.ProcessNodeStatus {
	return command.ProcessNodeStatus{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "daily",
		ServiceName:  "warehouse",
		Schema:       "public",
		TableName:    "orders",
		Status:       "SUCCEEDED",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// SUCCEEDED node, graph fully drained, no failures → scheduler SUCCEEDED.
func TestHandle_SucceededNode_ScheduleComplete_NoFailures(t *testing.T) {
	stateClient := &mockStateClient{}
	finalizedRunID := ""
	finalizedStatus := ""
	cmd := baseCmd()
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return true, false, nil
		},
		finalizeRunFn: func(_ context.Context, runID, terminalStatus string) error {
			finalizedRunID = runID
			finalizedStatus = terminalStatus
			return nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), cmd, "msg-1"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED, stateClient.capturedSchedulerStatus)
	assert.Equal(t, cmd.ScheduleID.String(), finalizedRunID)
	assert.Equal(t, "SUCCEEDED", finalizedStatus)
}

// SUCCEEDED node, graph drained, one node previously failed → scheduler FAILED.
func TestHandle_SucceededNode_ScheduleComplete_WithFailures(t *testing.T) {
	stateClient := &mockStateClient{}
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return true, true, nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), baseCmd(), "msg-2"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED, stateClient.capturedSchedulerStatus)
}

// SUCCEEDED node, but other nodes still running → no scheduler update.
func TestHandle_SucceededNode_ScheduleNotComplete(t *testing.T) {
	stateClient := &mockStateClient{}
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return false, false, nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	require.NoError(t, h.Handle(context.Background(), baseCmd(), "msg-3"))
	assert.False(t, stateClient.updateSchedulerCalled)
}

// FAILED node, graph fully drained → scheduler FAILED.
func TestHandle_FailedNode_ScheduleComplete(t *testing.T) {
	stateClient := &mockStateClient{}
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return true, true, nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	cmd := baseCmd()
	cmd.Status = "FAILED"
	require.NoError(t, h.Handle(context.Background(), cmd, "msg-4"))
	assert.True(t, stateClient.updateSchedulerCalled)
	assert.Equal(t, statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED, stateClient.capturedSchedulerStatus)
}

// FAILED node, other nodes still running → no scheduler update.
func TestHandle_FailedNode_ScheduleNotComplete(t *testing.T) {
	stateClient := &mockStateClient{}
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			return false, false, nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	cmd := baseCmd()
	cmd.Status = "FAILED"
	require.NoError(t, h.Handle(context.Background(), cmd, "msg-5"))
	assert.False(t, stateClient.updateSchedulerCalled)
}

// RUNNING node → completion check must NOT be called at all.
func TestHandle_RunningNode_NoCompletionCheck(t *testing.T) {
	checkCalled := false
	stateClient := &mockStateClient{}
	neo4jRepo := &mockDependencyRepo{
		checkScheduleCompletionFn: func(_ context.Context, _, _ string) (bool, bool, error) {
			checkCalled = true
			return false, false, nil
		},
	}
	h := handlers.NewProcessStatusHandler(&mockUnitOfWork{}, neo4jRepo, stateClient, newTestLogger())

	cmd := baseCmd()
	cmd.Status = "RUNNING"
	require.NoError(t, h.Handle(context.Background(), cmd, "msg-6"))
	assert.False(t, checkCalled, "CheckScheduleCompletion must NOT be called for non-terminal status")
	assert.False(t, stateClient.updateSchedulerCalled)
}
