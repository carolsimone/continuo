// Package client wraps the generated gRPC clients so commands do not import
// google.golang.org/grpc directly.
package client

import (
	"context"

	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// maxOrchestratorRecvMsgSize matches the server's largest legitimate
// response: GetNodeVersionsResponse with include_code=true and the default
// gRPC-unaware page sizes can approach the library's 4 MiB default receive
// limit (a single VersionView's compiled_code alone can run to 256 KiB), so
// this client dials with a limit sized for that response rather than the
// library default.
const maxOrchestratorRecvMsgSize = 32 * 1024 * 1024

// OrchestratorClient is the narrow interface the CLI depends on for orchestrator reads.
type OrchestratorClient interface {
	GetScheduleGraph(ctx context.Context, scheduleName string) (*orchestratorv1.GetScheduleGraphResponse, error)
	GetNodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) (*orchestratorv1.GetNodeVersionsResponse, error)
	GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*orchestratorv1.GetNodeVersionDiffResponse, error)
	GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since string) (*orchestratorv1.GetUpstreamChangesResponse, error)
	GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) (*orchestratorv1.GetCodeUnitVersionsResponse, error)
	GetNodeRunHistory(ctx context.Context, uniqueID string, limit int32, operation string) (*orchestratorv1.GetNodeRunHistoryResponse, error)
	GetPrecedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) (*orchestratorv1.GetPrecedentsResponse, error)
	Close() error
}

type orchestratorGRPCClient struct {
	conn   *grpc.ClientConn
	client orchestratorv1.OrchestratorQueryClient
}

// NewOrchestratorClient dials the given endpoint and returns a production OrchestratorClient.
// The returned client must be Closed by the caller.
func NewOrchestratorClient(ctx context.Context, endpoint string) (OrchestratorClient, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxOrchestratorRecvMsgSize)),
	)
	if err != nil {
		return nil, err
	}
	return &orchestratorGRPCClient{conn: conn, client: orchestratorv1.NewOrchestratorQueryClient(conn)}, nil
}

func (c *orchestratorGRPCClient) GetScheduleGraph(ctx context.Context, scheduleName string) (*orchestratorv1.GetScheduleGraphResponse, error) {
	return c.client.GetScheduleGraph(ctx, &orchestratorv1.GetScheduleGraphRequest{ScheduleName: scheduleName})
}

func (c *orchestratorGRPCClient) GetNodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) (*orchestratorv1.GetNodeVersionsResponse, error) {
	return c.client.GetNodeVersions(ctx, &orchestratorv1.GetNodeVersionsRequest{UniqueId: uniqueID, Limit: limit, IncludeCode: includeCode})
}

func (c *orchestratorGRPCClient) GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*orchestratorv1.GetNodeVersionDiffResponse, error) {
	return c.client.GetNodeVersionDiff(ctx, &orchestratorv1.GetNodeVersionDiffRequest{UniqueId: uniqueID, FromSeq: fromSeq, ToSeq: toSeq})
}

func (c *orchestratorGRPCClient) GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since string) (*orchestratorv1.GetUpstreamChangesResponse, error) {
	return c.client.GetUpstreamChanges(ctx, &orchestratorv1.GetUpstreamChangesRequest{UniqueId: uniqueID, Depth: depth, Since: since})
}

func (c *orchestratorGRPCClient) GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) (*orchestratorv1.GetCodeUnitVersionsResponse, error) {
	return c.client.GetCodeUnitVersions(ctx, &orchestratorv1.GetCodeUnitVersionsRequest{UnitId: unitID, UniqueId: uniqueID, Limit: limit})
}

func (c *orchestratorGRPCClient) GetNodeRunHistory(ctx context.Context, uniqueID string, limit int32, operation string) (*orchestratorv1.GetNodeRunHistoryResponse, error) {
	return c.client.GetNodeRunHistory(ctx, &orchestratorv1.GetNodeRunHistoryRequest{UniqueId: uniqueID, Limit: limit, Operation: operation})
}

func (c *orchestratorGRPCClient) GetPrecedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) (*orchestratorv1.GetPrecedentsResponse, error) {
	return c.client.GetPrecedents(ctx, &orchestratorv1.GetPrecedentsRequest{
		Signature: signature, Category: category, Reason: reason,
		Limit: limit, IncludeCode: includeCode,
	})
}

func (c *orchestratorGRPCClient) Close() error {
	return c.conn.Close()
}
