package fakes

import (
	"context"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
)

// FakeStateClient is a fake implementation of handlers.StateTaskClient for testing
type FakeStateClient struct {
	GetTaskFunc                func(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error)
	UpdateSchedulerStatusFunc  func(ctx context.Context, scheduleID uuid.UUID, schedulerStatus statev1.SchedulerStatus) error
	GetSchedulerInitStatusFunc func(ctx context.Context, scheduleID uuid.UUID) (string, error)
	InitStatus                 string // default "completed" when GetSchedulerInitStatusFunc is nil

	GetTaskCalls               []GetTaskCall
	UpdateSchedulerStatusCalls []UpdateSchedulerStatusCall
}

type UpdateSchedulerStatusCall struct {
	ScheduleID uuid.UUID
	Status     statev1.SchedulerStatus
}

type GetTaskCall struct {
	ScheduleID  uuid.UUID
	ServiceName string
	SchemaName  string
	TableName   string
}

func (f *FakeStateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
	f.GetTaskCalls = append(f.GetTaskCalls, GetTaskCall{
		ScheduleID:  scheduleID,
		ServiceName: serviceName,
		SchemaName:  schemaName,
		TableName:   tableName,
	})
	if f.GetTaskFunc != nil {
		return f.GetTaskFunc(ctx, scheduleID, serviceName, schemaName, tableName)
	}
	return &statev1.Task{
		TaskId: uuid.New().String(),
		Status: statev1.TaskStatus_TASK_STATUS_PENDING,
	}, nil
}

func (f *FakeStateClient) UpdateSchedulerStatus(ctx context.Context, scheduleID uuid.UUID, schedulerStatus statev1.SchedulerStatus) error {
	f.UpdateSchedulerStatusCalls = append(f.UpdateSchedulerStatusCalls, UpdateSchedulerStatusCall{
		ScheduleID: scheduleID,
		Status:     schedulerStatus,
	})
	if f.UpdateSchedulerStatusFunc != nil {
		return f.UpdateSchedulerStatusFunc(ctx, scheduleID, schedulerStatus)
	}
	return nil
}

func (f *FakeStateClient) GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error) {
	if f.GetSchedulerInitStatusFunc != nil {
		return f.GetSchedulerInitStatusFunc(ctx, scheduleID)
	}
	status := f.InitStatus
	if status == "" {
		status = "completed"
	}
	return status, nil
}
