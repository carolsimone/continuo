package grpc

import (
	"context"
	"fmt"
	"log/slog"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// StateClient wraps the gRPC client for state service
type StateClient struct {
	client statev1.StateServiceClient
	conn   *grpc.ClientConn
	logger *slog.Logger
}

// NewStateClient creates a new state service gRPC client
func NewStateClient(addr string, logger *slog.Logger) (*StateClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	return metadata.AppendToOutgoingContext(ctx, "x-caller-id", "k8s-controller")
}

// GetTask retrieves task details including retry_count and max_retries
func (c *StateClient) GetTask(ctx context.Context, taskID uuid.UUID) (*statev1.Task, error) {
	req := &statev1.GetTaskRequest{
		TaskId: taskID.String(),
	}

	resp, err := c.client.GetTask(ctx, req)
	if err != nil {
		c.logger.Error("Failed to get task",
			"task_id", taskID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	return resp.Task, nil
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

// CreateScheduler creates a new scheduler (for testing)
func (c *StateClient) CreateScheduler(ctx context.Context, req *statev1.CreateSchedulerRequest) (*statev1.SchedulerResponse, error) {
	resp, err := c.client.CreateScheduler(ctx, req)
	if err != nil {
		c.logger.Error("Failed to create scheduler",
			"schedule_id", req.ScheduleId,
			"error", err,
		)
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}
	return resp, nil
}

// CreateTask creates a new task (for testing)
func (c *StateClient) CreateTask(ctx context.Context, req *statev1.CreateTaskRequest) (*statev1.TaskResponse, error) {
	resp, err := c.client.CreateTask(ctx, req)
	if err != nil {
		c.logger.Error("Failed to create task",
			"task_id", req.TaskId,
			"error", err,
		)
		return nil, fmt.Errorf("failed to create task: %w", err)
	}
	return resp, nil
}

// DeleteTask deletes a task (for testing)
func (c *StateClient) DeleteTask(ctx context.Context, req *statev1.DeleteTaskRequest) (*statev1.DeleteTaskResponse, error) {
	resp, err := c.client.DeleteTask(ctx, req)
	if err != nil {
		c.logger.Error("Failed to delete task",
			"task_id", req.TaskId,
			"error", err,
		)
		return nil, fmt.Errorf("failed to delete task: %w", err)
	}
	return resp, nil
}
