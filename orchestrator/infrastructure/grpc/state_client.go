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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrTaskNotFound is returned when a task is not found
	ErrTaskNotFound = errors.New("task not found")
)

// StateClient wraps the gRPC client for state service.
// It satisfies the command.StateTaskClient interface.
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

// GetTask retrieves a task by schedule and node information, returning the task ID string.
func (c *StateClient) GetTask(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (string, error) {
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
			return "", ErrTaskNotFound
		}
		c.logger.Error("Failed to get task",
			"schedule_id", scheduleID,
			"error", err,
		)
		return "", fmt.Errorf("failed to get task: %w", err)
	}

	return resp.Task.GetTaskId(), nil
}

// GetSchedulerInitStatus returns the initialization_status for a scheduler.
func (c *StateClient) GetSchedulerInitStatus(ctx context.Context, scheduleID uuid.UUID) (string, error) {
	resp, err := c.client.GetSchedulerInitStatus(ctx, &statev1.GetSchedulerInitStatusRequest{
		ScheduleId: scheduleID.String(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get scheduler init status: %w", err)
	}
	return resp.InitializationStatus, nil
}

// UpdateSchedulerStatus marks the scheduler as SUCCEEDED or FAILED with a completedAt timestamp.
// It accepts a plain string status and converts to the proto enum internally.
func (c *StateClient) UpdateSchedulerStatus(ctx context.Context, scheduleID uuid.UUID, schedulerStatus string) error {
	protoStatus := toProtoSchedulerStatus(schedulerStatus)

	req := &statev1.UpdateSchedulerRequest{
		ScheduleId:  scheduleID.String(),
		Status:      protoStatus,
		CompletedAt: timestamppb.Now(),
	}

	_, err := c.client.UpdateScheduler(ctx, req)
	if err != nil {
		c.logger.Error("Failed to update scheduler status",
			"schedule_id", scheduleID,
			"status", schedulerStatus,
			"error", err,
		)
		return fmt.Errorf("failed to update scheduler status: %w", err)
	}

	c.logger.Info("Scheduler status updated",
		"schedule_id", scheduleID,
		"status", schedulerStatus,
	)

	return nil
}

// toProtoSchedulerStatus converts a plain string status to the proto enum.
func toProtoSchedulerStatus(s string) statev1.SchedulerStatus {
	switch s {
	case "SUCCEEDED":
		return statev1.SchedulerStatus_SCHEDULER_STATUS_SUCCEEDED
	case "FAILED":
		return statev1.SchedulerStatus_SCHEDULER_STATUS_FAILED
	default:
		return statev1.SchedulerStatus_SCHEDULER_STATUS_UNSPECIFIED
	}
}
