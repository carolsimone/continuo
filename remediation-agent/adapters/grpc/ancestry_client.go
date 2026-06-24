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
// the queried node's own file path and its upstream dependency chain.
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

// NodeContext returns the queried node's own file path, service name (depth-0),
// and its upstream ancestors ranked by depth. A NOT_FOUND response from the
// orchestrator means the node has no recorded ancestry; the handler treats
// this as a degraded (not error) path and proceeds without context. Other
// gRPC errors are returned to the caller.
func (c *AncestryClient) NodeContext(ctx context.Context, nodeID string) (string, string, []prompt.Ancestor, error) {
	resp, err := c.client.GetNodeAncestry(ctx, &orchestratorv1.GetNodeAncestryRequest{
		NodeUniqueId: nodeID,
		MaxDepth:     0,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", "", nil, nil
		}
		return "", "", nil, err
	}
	filePath, serviceName, ancestors := MapNodeContext(resp)
	return filePath, serviceName, ancestors, nil
}

// MapNodeContext converts a GetNodeAncestryResponse into the queried node's
// file path, service name, and its upstream prompt.Ancestor values. The
// depth-0 entry represents the queried node itself: its FilePath and
// ServiceName are returned as filePath and serviceName, and it is excluded
// from the ancestors slice. Only depth>0 nodes are mapped to ancestors.
// LastChangedAt is formatted as RFC3339; an unset proto timestamp produces an
// empty string.
func MapNodeContext(resp *orchestratorv1.GetNodeAncestryResponse) (filePath string, serviceName string, ancestors []prompt.Ancestor) {
	result := make([]prompt.Ancestor, 0, len(resp.Ancestors))
	for _, n := range resp.Ancestors {
		if n.Depth == 0 {
			filePath = n.FilePath
			serviceName = n.ServiceName
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
	return filePath, serviceName, result
}
