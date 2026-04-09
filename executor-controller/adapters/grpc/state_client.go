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

// outgoingCtx attaches the service's caller identity to an outgoing gRPC call.
func (c *StateClient) outgoingCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-caller-id", "executor-controller")
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
