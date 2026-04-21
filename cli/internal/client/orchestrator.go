// Package client wraps the generated gRPC clients so commands do not import
// google.golang.org/grpc directly.
package client

import (
	"context"

	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// OrchestratorClient is the narrow interface the CLI depends on for orchestrator reads.
type OrchestratorClient interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*orchestratorv1.GetScheduleGraphResponse, error)
	Close() error
}

type orchestratorGRPCClient struct {
	conn   *grpc.ClientConn
	client orchestratorv1.OrchestratorQueryClient
}

// NewOrchestratorClient dials the given endpoint and returns a production OrchestratorClient.
// The returned client must be Closed by the caller.
func NewOrchestratorClient(ctx context.Context, endpoint string) (OrchestratorClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &orchestratorGRPCClient{conn: conn, client: orchestratorv1.NewOrchestratorQueryClient(conn)}, nil
}

func (c *orchestratorGRPCClient) GetScheduleGraph(ctx context.Context, scheduleName string) (*orchestratorv1.GetScheduleGraphResponse, error) {
	return c.client.GetScheduleGraph(ctx, &orchestratorv1.GetScheduleGraphRequest{ScheduleName: scheduleName})
}

func (c *orchestratorGRPCClient) Close() error {
	return c.conn.Close()
}
