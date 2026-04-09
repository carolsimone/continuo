package grpc

import (
	"context"
	"fmt"
	"log/slog"

	graphv1 "github.com/carolsimone/continuo/graph/api/graph/v1"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/startup-controller/domain/model"
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
		logger.Error("Failed to connect to graph service", "addr", addr, "error", err)
		return nil, fmt.Errorf("failed to connect to graph service: %w", err)
	}

	logger.Info("Connected to graph service", "addr", addr)

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

func (c *GraphClient) SnapshotGraph(ctx context.Context, runID, scheduleName string) error {
	_, err := c.client.SnapshotGraph(ctx, &graphv1.SnapshotGraphRequest{
		RunId:        runID,
		ScheduleName: scheduleName,
	})
	if err != nil {
		return fmt.Errorf("failed to call SnapshotGraph: %w", err)
	}
	return nil
}

// GetScheduleInitNodes fetches all_nodes, root_nodes, and seed_nodes for a
// schedule from the graph service. Returns an error if any node carries an
// unrecognised node_type — this prevents partially-initialized scheduler state.
func (c *GraphClient) GetScheduleInitNodes(
	ctx context.Context,
	scheduleName string,
	runID string,
) (allNodes []model.NodeInfo, rootNodes []model.NodeInfo, seedNodes []model.NodeInfo, err error) {
	resp, err := c.client.GetScheduleInitNodes(ctx, &graphv1.GetScheduleInitNodesRequest{
		ScheduleName: scheduleName,
		RunId:        runID,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to call GetScheduleInitNodes: %w", err)
	}

	allNodes, err = protoNodesToNodeInfo(resp.AllNodes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("all_nodes mapping failed: %w", err)
	}
	rootNodes, err = protoNodesToNodeInfo(resp.RootNodes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("root_nodes mapping failed: %w", err)
	}
	seedNodes, err = protoNodesToNodeInfo(resp.SeedNodes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("seed_nodes mapping failed: %w", err)
	}

	return allNodes, rootNodes, seedNodes, nil
}

// GetTransitiveDownstream returns all non-SUCCEEDED nodes downstream of the given node.
func (c *GraphClient) GetTransitiveDownstream(ctx context.Context, scheduleName, schemaName, tableName string) ([]*graphv1.TableNode, error) {
	resp, err := c.client.GetTransitiveDownstream(ctx, &graphv1.GetTransitiveDownstreamRequest{
		ScheduleName: scheduleName,
		SchemaName:   schemaName,
		TableName:    tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call GetTransitiveDownstream: %w", err)
	}
	return resp.Nodes, nil
}

// UpdateNodeStatus updates a node's status in the graph service.
func (c *GraphClient) UpdateNodeStatus(ctx context.Context, scheduleName, schemaName, tableName, status, runID string) error {
	_, err := c.client.UpdateNodeStatus(ctx, &graphv1.UpdateNodeStatusRequest{
		ScheduleName: scheduleName,
		SchemaName:   schemaName,
		TableName:    tableName,
		Status:       status,
		RunId:        runID,
	})
	if err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}
	return nil
}

func protoNodesToNodeInfo(nodes []*graphv1.TableNode) ([]model.NodeInfo, error) {
	out := make([]model.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		nodeType, err := pkg_model.ParseNodeType(n.NodeType)
		if err != nil {
			return nil, fmt.Errorf("node %s.%s has invalid node_type %q: %w",
				n.SchemaName, n.TableName, n.NodeType, err)
		}
		out = append(out, model.NodeInfo{
			Schema:      n.SchemaName,
			TableName:   n.TableName,
			ServiceName: n.ServiceName,
			NodeType:    nodeType,
		})
	}
	return out, nil
}
