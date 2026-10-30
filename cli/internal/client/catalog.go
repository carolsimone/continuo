// Package client wraps the generated gRPC clients so commands do not import
// google.golang.org/grpc directly.
package client

import (
	"context"

	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// maxCatalogRecvMsgSize matches the server's largest legitimate
// response: GetNodeVersionsResponse with include_code=true and the default
// gRPC-unaware page sizes can approach the library's 4 MiB default receive
// limit (a single VersionView's compiled_code alone can run to 256 KiB), so
// this client dials with a limit sized for that response rather than the
// library default.
const maxCatalogRecvMsgSize = 32 * 1024 * 1024

// CatalogClient is the narrow interface the CLI depends on for catalog reads.
type CatalogClient interface {
	GetNodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) (*catalogv1.GetNodeVersionsResponse, error)
	GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*catalogv1.GetNodeVersionDiffResponse, error)
	GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since string) (*catalogv1.GetUpstreamChangesResponse, error)
	GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) (*catalogv1.GetCodeUnitVersionsResponse, error)
	GetPrecedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) (*catalogv1.GetPrecedentsResponse, error)
	Close() error
}

type catalogGRPCClient struct {
	conn   *grpc.ClientConn
	client catalogv1.CatalogQueryClient
}

// NewCatalogClient dials the given endpoint and returns a production CatalogClient.
// The returned client must be Closed by the caller.
func NewCatalogClient(ctx context.Context, endpoint string) (CatalogClient, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxCatalogRecvMsgSize)),
	)
	if err != nil {
		return nil, err
	}
	return &catalogGRPCClient{conn: conn, client: catalogv1.NewCatalogQueryClient(conn)}, nil
}

func (c *catalogGRPCClient) GetNodeVersions(ctx context.Context, uniqueID string, limit int32, includeCode bool) (*catalogv1.GetNodeVersionsResponse, error) {
	return c.client.GetNodeVersions(ctx, &catalogv1.GetNodeVersionsRequest{UniqueId: uniqueID, Limit: limit, IncludeCode: includeCode})
}

func (c *catalogGRPCClient) GetNodeVersionDiff(ctx context.Context, uniqueID string, fromSeq, toSeq int64) (*catalogv1.GetNodeVersionDiffResponse, error) {
	return c.client.GetNodeVersionDiff(ctx, &catalogv1.GetNodeVersionDiffRequest{UniqueId: uniqueID, FromSeq: fromSeq, ToSeq: toSeq})
}

func (c *catalogGRPCClient) GetUpstreamChanges(ctx context.Context, uniqueID string, depth int32, since string) (*catalogv1.GetUpstreamChangesResponse, error) {
	return c.client.GetUpstreamChanges(ctx, &catalogv1.GetUpstreamChangesRequest{UniqueId: uniqueID, Depth: depth, Since: since})
}

func (c *catalogGRPCClient) GetCodeUnitVersions(ctx context.Context, unitID, uniqueID string, limit int32) (*catalogv1.GetCodeUnitVersionsResponse, error) {
	return c.client.GetCodeUnitVersions(ctx, &catalogv1.GetCodeUnitVersionsRequest{UnitId: unitID, UniqueId: uniqueID, Limit: limit})
}

func (c *catalogGRPCClient) GetPrecedents(ctx context.Context, signature, category, reason string, limit int32, includeCode bool) (*catalogv1.GetPrecedentsResponse, error) {
	return c.client.GetPrecedents(ctx, &catalogv1.GetPrecedentsRequest{
		Signature: signature, Category: category, Reason: reason,
		Limit: limit, IncludeCode: includeCode,
	})
}

func (c *catalogGRPCClient) Close() error {
	return c.conn.Close()
}
