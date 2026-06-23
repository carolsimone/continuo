// Package grpc contains gRPC adapter implementations for external service clients.
package grpc

import (
	"context"
	"time"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var _ ports.AncestryClient = (*AncestryClient)(nil)

// AncestryClient calls the orchestrator's GetNodeAncestry RPC to retrieve
// the upstream dependency chain for a failed node.
type AncestryClient struct {
	client orchestratorv1.OrchestratorQueryClient
}

// NewAncestryClient dials the orchestrator gRPC endpoint and returns an
// AncestryClient. The connection uses insecure credentials (no TLS), matching
// the cluster-internal transport used by all services.
func NewAncestryClient(addr string) (*AncestryClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &AncestryClient{
		client: orchestratorv1.NewOrchestratorQueryClient(conn),
	}, nil
}

// Ancestors returns the upstream ancestry of nodeID ranked by depth. A
// NOT_FOUND response from the orchestrator means the node has no recorded
// ancestry; the handler treats this as a degraded (not error) path and
// proceeds without ancestors. Other gRPC errors are returned to the caller.
func (c *AncestryClient) Ancestors(ctx context.Context, nodeID string) ([]prompt.Ancestor, error) {
	resp, err := c.client.GetNodeAncestry(ctx, &orchestratorv1.GetNodeAncestryRequest{
		NodeUniqueId: nodeID,
		MaxDepth:     0,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return MapAncestors(resp), nil
}

// MapAncestors converts a GetNodeAncestryResponse into prompt.Ancestor values.
// Depth-0 entries represent the queried node itself and are excluded; only
// upstream ancestors (depth >= 1) are returned. LastChangedAt is formatted as
// RFC3339; an unset proto timestamp produces an empty string.
func MapAncestors(resp *orchestratorv1.GetNodeAncestryResponse) []prompt.Ancestor {
	result := make([]prompt.Ancestor, 0, len(resp.Ancestors))
	for _, n := range resp.Ancestors {
		if n.Depth == 0 {
			continue
		}
		var lastChangedAt string
		if n.LastChangedAt != nil {
			lastChangedAt = n.LastChangedAt.AsTime().Format(time.RFC3339)
		}
		result = append(result, prompt.Ancestor{
			NodeID:        n.UniqueId,
			ServiceName:   n.ServiceName,
			LastCommitSHA: n.LastCommitSha,
			FilePath:      n.FilePath,
			LastChangedAt: lastChangedAt,
			Depth:         int(n.Depth),
		})
	}
	return result
}
