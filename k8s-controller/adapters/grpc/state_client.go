package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// UpdateTaskWithRetry updates task status and retry_count
func (c *StateClient) UpdateTaskWithRetry(ctx context.Context, taskID uuid.UUID, status statev1.TaskStatus, retryCount int32) error {
	req := &statev1.UpdateTaskRequest{
		TaskId:     taskID.String(),
		Status:     status,
		RetryCount: retryCount,
	}

	_, err := c.client.UpdateTask(c.outgoingCtx(ctx), req)
	if err != nil {
		c.logger.Error("Failed to update task with retry count",
			"task_id", taskID,
			"status", status,
			"retry_count", retryCount,
			"error", err,
		)
		return fmt.Errorf("failed to update task with retry count: %w", err)
	}

	c.logger.Info("Updated task with retry count",
		"task_id", taskID,
		"status", status,
		"retry_count", retryCount,
	)

	return nil
}

// TaskExecutionParams contains parameters for creating a task execution
type TaskExecutionParams struct {
	TaskID           string
	StartedAt        *time.Time
	CompletedAt      *time.Time
	ExecutionSeconds float64
	K8sJobName       string
	ErrorMessage     string
	ExecutionID      string // pre-generated UUID; used as PK in task_execution
	LogS3Key         string // S3 object key for full pod log; empty if upload failed
}

// CreateTaskExecution creates a task execution record
func (c *StateClient) CreateTaskExecution(ctx context.Context, params *TaskExecutionParams) error {
	req := &statev1.CreateTaskExecutionRequest{
		Id:                   params.ExecutionID,
		TaskId:               params.TaskID,
		K8SJobName:           params.K8sJobName,
		ExecutionTimeSeconds: params.ExecutionSeconds,
		ErrorMessage:         params.ErrorMessage,
		LogS3Key:             params.LogS3Key,
	}

	if params.StartedAt != nil {
		req.StartedAt = timestamppb.New(*params.StartedAt)
	}

	if params.CompletedAt != nil {
		req.CompletedAt = timestamppb.New(*params.CompletedAt)
	}

	_, err := c.client.CreateTaskExecution(ctx, req)
	if err != nil {
		c.logger.Error("Failed to create task execution",
			"task_id", params.TaskID,
			"job_name", params.K8sJobName,
			"error", err,
		)
		return fmt.Errorf("failed to create task execution: %w", err)
	}

	c.logger.Info("Created task execution",
		"task_id", params.TaskID,
		"job_name", params.K8sJobName,
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
