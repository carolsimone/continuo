package fakes

import (
	"context"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// FakeStateClient is a fake implementation of the state gRPC read interface for testing.
// Only read operations remain — write operations are now published as Redis events.
type FakeStateClient struct {
	GetTaskFunc          func(ctx context.Context, taskID uuid.UUID) (*statev1.Task, error)
	UpdateTaskStatusFunc func(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error
	GetTaskCallCount     int
	UpdateStatusCallCount int
	LastTaskID           uuid.UUID
	LastStatus           statev1.TaskStatus
}

// GetTask implements TaskGetter
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

// UpdateTaskStatus implements StateManager (still used by check_status_handler in some paths)
func (f *FakeStateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error {
	f.UpdateStatusCallCount++
	f.LastTaskID = taskID
	f.LastStatus = status
	if f.UpdateTaskStatusFunc != nil {
		return f.UpdateTaskStatusFunc(ctx, taskID, status)
	}
	return nil
}
