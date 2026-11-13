package client

import (
	"context"

	releasev1 "github.com/carolsimone/continuo/cli/proto/release/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ReleaseClient is the narrow interface the release commands depend on.
type ReleaseClient interface {
	GetReleaseNotices(ctx context.Context, releaseID string) (*releasev1.GetReleaseNoticesResponse, error)
	Close() error
}

// NewReleaseClient dials release-controller. The returned client must be
// Closed by the caller.
func NewReleaseClient(_ context.Context, endpoint string) (ReleaseClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(maintenanceInterceptor))
	if err != nil {
		return nil, err
	}
	return &releaseGRPCClient{conn: conn, api: releasev1.NewReleaseQueryClient(conn)}, nil
}

type releaseGRPCClient struct {
	conn *grpc.ClientConn
	api  releasev1.ReleaseQueryClient
}

func (c *releaseGRPCClient) GetReleaseNotices(ctx context.Context, releaseID string) (*releasev1.GetReleaseNoticesResponse, error) {
	return c.api.GetReleaseNotices(ctx, &releasev1.GetReleaseNoticesRequest{ReleaseId: releaseID})
}

func (c *releaseGRPCClient) Close() error { return c.conn.Close() }
