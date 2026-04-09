package fakes

import (
	"context"
	"fmt"
	"sync"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// TaskUpdate represents a task status update
type TaskUpdate struct {
	TaskID uuid.UUID
	Status statev1.TaskStatus
}

// FakeStateClient is a fake implementation of StateClient for testing
type FakeStateClient struct {
	mu                  sync.RWMutex
	taskUpdates         []TaskUpdate
	updateTaskStatusErr error
}

// NewFakeStateClient creates a new FakeStateClient
func NewFakeStateClient() *FakeStateClient {
	return &FakeStateClient{
		taskUpdates: make([]TaskUpdate, 0),
	}
}

// SetUpdateTaskStatusError sets an error to be returned by UpdateTaskStatus
func (f *FakeStateClient) SetUpdateTaskStatusError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateTaskStatusErr = err
}

// UpdateTaskStatus records a task status update
func (f *FakeStateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.updateTaskStatusErr != nil {
		return f.updateTaskStatusErr
	}

	f.taskUpdates = append(f.taskUpdates, TaskUpdate{
		TaskID: taskID,
		Status: status,
	})

	return nil
}

// Close closes the fake client (no-op)
func (f *FakeStateClient) Close() error {
	return nil
}

// GetTaskUpdates returns all recorded task updates
func (f *FakeStateClient) GetTaskUpdates() []TaskUpdate {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Return a copy
	updates := make([]TaskUpdate, len(f.taskUpdates))
	copy(updates, f.taskUpdates)
	return updates
}

// GetLastTaskUpdate returns the last task update for a given task ID
func (f *FakeStateClient) GetLastTaskUpdate(taskID uuid.UUID) (statev1.TaskStatus, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for i := len(f.taskUpdates) - 1; i >= 0; i-- {
		if f.taskUpdates[i].TaskID == taskID {
			return f.taskUpdates[i].Status, nil
		}
	}

	return statev1.TaskStatus_TASK_STATUS_UNSPECIFIED, fmt.Errorf("no updates found for task %s", taskID)
}

// Reset clears all recorded updates and errors
func (f *FakeStateClient) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.taskUpdates = make([]TaskUpdate, 0)
	f.updateTaskStatusErr = nil
}
