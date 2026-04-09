package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// ErrTaskNotFound is returned when a task is not found
	ErrTaskNotFound = errors.New("task not found")
	// ErrAlreadyCompleted is returned when initialization is already completed
	ErrAlreadyCompleted = errors.New("initialization already completed")
)

// StateClient wraps the gRPC client for state service
type StateClient struct {
	client statev1.StateServiceClient
	conn   *grpc.ClientConn
	logger *slog.Logger
}

// NewStateClient creates a new state service gRPC client
func NewStateClient(addr string, logger *slog.Logger) (*StateClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Failed to connect to state service",
			"addr", addr,
			"error", err,
		)
		return nil, fmt.Errorf("failed to connect to state service: %w", err)
	}

	client := statev1.NewStateServiceClient(conn)

	logger.Info("Connected to state service", "addr", addr)

	return &StateClient{
		client: client,
		conn:   conn,
		logger: logger,
	}, nil
}

// Close closes the gRPC connection
func (c *StateClient) Close() error {
	return c.conn.Close()
}

// outgoingCtx attaches the service's caller identity to an outgoing gRPC call.
func (c *StateClient) outgoingCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-caller-id", "startup-controller")
}

// GetTask retrieves a task by schedule and node information
func (c *StateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*statev1.Task, error) {
	req := &statev1.GetTaskByScheduleAndNodeRequest{
		ScheduleId:  scheduleID.String(),
		ServiceName: serviceName,
		SchemaName:  schemaName,
		TableName:   tableName,
	}

	resp, err := c.client.GetTaskByScheduleAndNode(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			c.logger.Debug("Task not found",
				"schedule_id", scheduleID,
				"service_name", serviceName,
				"table_name", tableName,
			)
			return nil, ErrTaskNotFound
		}
		c.logger.Error("Failed to get task",
			"schedule_id", scheduleID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return resp.Task, nil
}

// CreateTask creates a new task tracker entry
func (c *StateClient) CreateTask(ctx context.Context, taskID, scheduleID uuid.UUID, serviceName, schemaName, tableName string, maxRetries int) error {
	req := &statev1.CreateTaskRequest{
		TaskId:      taskID.String(),
		ScheduleId:  scheduleID.String(),
		ServiceName: serviceName,
		SchemaName:  schemaName,
		TableName:   tableName,
		Status:      statev1.TaskStatus_TASK_STATUS_PENDING,
		MaxRetries:  int32(maxRetries),
	}

	_, err := c.client.CreateTask(ctx, req)
	if err != nil {
		c.logger.Error("Failed to create task",
			"task_id", taskID,
			"schedule_id", scheduleID,
			"error", err,
		)
		return fmt.Errorf("failed to create task: %w", err)
	}

	c.logger.Info("Created task",
		"task_id", taskID,
		"schedule_id", scheduleID,
		"table", tableName,
	)

	return nil
}

// UpdateTaskStatus updates the status of a task
func (c *StateClient) UpdateTaskStatus(ctx context.Context, taskID uuid.UUID, taskStatus statev1.TaskStatus) error {
	req := &statev1.UpdateTaskRequest{
		TaskId: taskID.String(),
		Status: taskStatus,
	}

	_, err := c.client.UpdateTask(c.outgoingCtx(ctx), req)
	if err != nil {
		c.logger.Error("Failed to update task status",
			"task_id", taskID,
			"status", taskStatus,
			"error", err,
		)
		return fmt.Errorf("failed to update task status: %w", err)
	}

	c.logger.Info("Updated task status",
		"task_id", taskID,
		"status", taskStatus,
	)

	return nil
}

// UpdateSchedulerInitStatus updates the initialization status of a scheduler
func (c *StateClient) UpdateSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID, initStatus string) error {
	req := &statev1.UpdateSchedulerInitStatusRequest{
		ScheduleId:           scheduleID.String(),
		InitializationStatus: initStatus,
	}

	resp, err := c.client.UpdateSchedulerInitStatus(ctx, req)
	if err != nil {
		// Check if already completed
		if initStatus == "in_progress" {
			// Try to get the scheduler to check current status
			getReq := &statev1.GetSchedulerRequest{
				ScheduleId: scheduleID.String(),
			}
			getResp, getErr := c.client.GetScheduler(ctx, getReq)
			if getErr == nil && getResp.Scheduler.InitializationStatus == "completed" {
				c.logger.Info("Initialization already completed, skipping",
					"schedule_id", scheduleID,
				)
				return ErrAlreadyCompleted
			}
		}

		c.logger.Error("Failed to update scheduler initialization status",
			"schedule_id", scheduleID,
			"status", initStatus,
			"error", err,
		)
		return fmt.Errorf("failed to update scheduler initialization status: %w", err)
	}

	c.logger.Info("Updated scheduler initialization status",
		"schedule_id", scheduleID,
		"status", resp.Scheduler.InitializationStatus,
	)

	return nil
}

// ResetInProgressInitializations resets all in_progress initializations to pending
func (c *StateClient) ResetInProgressInitializations(ctx context.Context) (int, error) {
	req := &statev1.ResetInProgressInitializationsRequest{}

	resp, err := c.client.ResetInProgressInitializations(ctx, req)
	if err != nil {
		c.logger.Error("Failed to reset in_progress initializations", "error", err)
		return 0, fmt.Errorf("failed to reset in_progress initializations: %w", err)
	}

	if resp.Count > 0 {
		c.logger.Warn("Reset in_progress initializations",
			"count", resp.Count,
			"reason", "service startup recovery",
		)
	}

	return int(resp.Count), nil
}

// ResetTask unconditionally resets a task to PENDING with retry_count=0.
func (c *StateClient) ResetTask(ctx context.Context, taskID uuid.UUID) error {
	_, err := c.client.ResetTask(c.outgoingCtx(ctx), &statev1.ResetTaskRequest{TaskId: taskID.String()})
	if err != nil {
		c.logger.Error("Failed to reset task", "task_id", taskID, "error", err)
		return fmt.Errorf("failed to reset task: %w", err)
	}
	return nil
}

// GetSchedulerInitStatus returns the initialization_status for a scheduler.
func (c *StateClient) GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error) {
	resp, err := c.client.GetSchedulerInitStatus(ctx, &statev1.GetSchedulerInitStatusRequest{ScheduleId: scheduleID.String()})
	if err != nil {
		return "", fmt.Errorf("failed to get scheduler init status: %w", err)
	}
	return resp.InitializationStatus, nil
}

// CreateScheduler creates a new scheduler entry
func (c *StateClient) CreateScheduler(ctx context.Context, scheduleID uuid.UUID, scheduleName string) error {
	req := &statev1.CreateSchedulerRequest{
		ScheduleId:   scheduleID.String(),
		ScheduleName: scheduleName,
	}

	_, err := c.client.CreateScheduler(ctx, req)
	if err != nil {
		c.logger.Error("Failed to create scheduler",
			"schedule_id", scheduleID,
			"schedule_name", scheduleName,
			"error", err,
		)
		return fmt.Errorf("failed to create scheduler: %w", err)
	}

	c.logger.Info("Created scheduler",
		"schedule_id", scheduleID,
		"schedule_name", scheduleName,
	)

	return nil
}
