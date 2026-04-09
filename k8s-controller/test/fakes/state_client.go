package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/adapters/grpc"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// FakeStateClient is a fake implementation of StateManager for testing
type FakeStateClient struct {
	GetTaskFunc              func(ctx context.Context, taskID uuid.UUID) (*statev1.Task, error)
	UpdateTaskStatusFunc     func(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error
	UpdateTaskWithRetryFunc  func(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus, retryCount int32) error
	CreateTaskExecutionFunc  func(ctx context.Context, params *grpc.TaskExecutionParams) error
	GetTaskCallCount         int
	UpdateStatusCallCount    int
	UpdateWithRetryCallCount int
	CreateExecutionCallCount int
	LastTaskID               uuid.UUID
	LastStatus               statev1.TaskStatus
	LastRetryCount           int32
}

// GetTask implements StateManager
func (f *FakeStateClient) GetTask(ctx context.Context, taskID uuid.UUID) (*statev1.Task, error) {
	f.GetTaskCallCount++
	f.LastTaskID = taskID
	if f.GetTaskFunc != nil {
		return f.GetTaskFunc(ctx, taskID)
	}
	return &statev1.Task{
		TaskId:     taskID.String(),
		RetryCount: 0,
		MaxRetries: 3,
	}, nil
}

// UpdateTaskStatus implements StateManager
func (f *FakeStateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error {
	f.UpdateStatusCallCount++
	f.LastTaskID = taskID
	f.LastStatus = status
	if f.UpdateTaskStatusFunc != nil {
		return f.UpdateTaskStatusFunc(ctx, taskID, status)
	}
	return nil
}

// UpdateTaskWithRetry implements StateManager
func (f *FakeStateClient) UpdateTaskWithRetry(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus, retryCount int32) error {
	f.UpdateWithRetryCallCount++
	f.LastTaskID = taskID
	f.LastStatus = status
	f.LastRetryCount = retryCount
	if f.UpdateTaskWithRetryFunc != nil {
		return f.UpdateTaskWithRetryFunc(ctx, taskID, status, retryCount)
	}
	return nil
}

// CreateTaskExecution implements StateManager
func (f *FakeStateClient) CreateTaskExecution(ctx context.Context, params *grpc.TaskExecutionParams) error {
	f.CreateExecutionCallCount++
	if f.CreateTaskExecutionFunc != nil {
		return f.CreateTaskExecutionFunc(ctx, params)
	}
	return nil
}
