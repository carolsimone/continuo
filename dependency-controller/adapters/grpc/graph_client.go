package grpc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GraphClient wraps the gRPC client for the graph service.
type GraphClient struct {
	conn   *grpc.ClientConn
	client graphv1.GraphServiceClient
	logger *slog.Logger
}

// NewGraphClient creates a new graph service gRPC client.
func NewGraphClient(addr string, logger *slog.Logger) (*GraphClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to graph service: %w", err)
	}

	logger.Info("Initialised graph service gRPC client", "addr", addr)

	return &GraphClient{
		conn:   conn,
		client: graphv1.NewGraphServiceClient(conn),
		logger: logger,
	}, nil
}

// Close closes the gRPC connection.
func (c *GraphClient) Close() error {
	return c.conn.Close()
}

// UpdateNodeStatus sets the execution status on a graph node (write-through projection).
func (c *GraphClient) UpdateNodeStatus(ctx context.Context, scheduleName, schema, tableName, status, runID string) error {
	_, err := c.client.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: scheduleName,
		SchemaName:   schema,
		TableName:    tableName,
		Status:       status,
		RunId:        runID,
	})
	if err != nil {
		c.logger.Error("Failed to update node status",
			"schedule_name", scheduleName,
			"table_name", tableName,
			"status", status,
			"error", err,
		)
		return fmt.Errorf("failed to update node status: %w", err)
	}
	return nil
}

// GetReadyDownstream returns downstream nodes whose all upstreams have SUCCEEDED.
// Maps proto TableNode.SchemaName back to model.DownstreamNode.Schema.
func (c *GraphClient) GetReadyDownstream(ctx context.Context, scheduleName, schema, tableName, runID string) ([]model.DownstreamNode, error) {
	resp, err := c.client.GetReadyDownstream(ctx, &graphv1.GetReadyDownstreamRequest{
		ScheduleName: scheduleName,
		SchemaName:   schema,
		TableName:    tableName,
		RunId:        runID,
	})
	if err != nil {
		c.logger.Error("Failed to get ready downstream nodes",
			"schedule_name", scheduleName,
			"table_name", tableName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get ready downstream nodes: %w", err)
	}

	nodes := make([]model.DownstreamNode, len(resp.Nodes))
	for i, n := range resp.Nodes {
		nodes[i] = model.DownstreamNode{
			ServiceName: n.ServiceName,
			Schema:      n.SchemaName, // proto field SchemaName → domain field Schema
			TableName:   n.TableName,
			NodeType:    n.NodeType,
		}
	}
	return nodes, nil
}

// CheckScheduleCompletion reports whether no node in the schedule can change state anymore.
func (c *GraphClient) CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (bool, bool, error) {
	resp, err := c.client.CheckScheduleCompletion(ctx, &graphv1.CheckScheduleCompletionRequest{
		ScheduleName: scheduleName,
		RunId:        runID,
	})
	if err != nil {
		c.logger.Error("Failed to check schedule completion",
			"schedule_name", scheduleName,
			"error", err,
		)
		return false, false, fmt.Errorf("failed to check schedule completion: %w", err)
	}
	return resp.IsComplete, resp.HasFailed, nil
}

func (c *GraphClient) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	_, err := c.client.FinalizeRun(ctx, &graphv1.FinalizeRunRequest{
		RunId:          runID,
		TerminalStatus: terminalStatus,
	})
	if err != nil {
		c.logger.Error("Failed to finalize run",
			"run_id", runID,
			"terminal_status", terminalStatus,
			"error", err,
		)
		return fmt.Errorf("FinalizeRun: %w", err)
	}
	return nil
}
