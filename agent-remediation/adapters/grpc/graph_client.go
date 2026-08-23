package grpc

import (
	"context"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// GraphClient serves agent-remediation's graph read ports over the
// orchestrator's OrchestratorQuery gRPC surface.
type GraphClient struct {
	client orchestratorv1.OrchestratorQueryClient
}

var _ ports.NodeLocator = (*GraphClient)(nil)
var _ ports.UpstreamChangeReader = (*GraphClient)(nil)
var _ ports.VersionReader = (*GraphClient)(nil)
var _ ports.PrecedentReader = (*GraphClient)(nil)

// upstreamDepth is the hop bound requested from GetUpstreamChanges — the
// server's validated maximum, approximating the full ancestry closure.
const upstreamDepth = 10

// NewGraphClient dials the orchestrator gRPC endpoint (insecure transport,
// matching the cluster-internal convention shared by every service client).
func NewGraphClient(addr string) (*GraphClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GraphClient{client: orchestratorv1.NewOrchestratorQueryClient(conn)}, nil
}

// Locate returns the node's project-relative source path and owning service.
// A NOT_FOUND response degrades to ("", "", nil); other gRPC errors are
// returned to the caller.
func (c *GraphClient) Locate(ctx context.Context, uniqueID string) (string, string, error) {
	resp, err := c.client.GetNodeLocation(ctx, &orchestratorv1.GetNodeLocationRequest{UniqueId: uniqueID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", "", nil
		}
		return "", "", err
	}
	return resp.FilePath, resp.ServiceName, nil
}

// UpstreamChanges returns the node's most-recently-changed ancestors with
// their latest code and config diffs. A NOT_FOUND response degrades to
// (nil, nil); other gRPC errors are returned to the caller.
func (c *GraphClient) UpstreamChanges(ctx context.Context, uniqueID string) ([]prompt.UpstreamChange, error) {
	resp, err := c.client.GetUpstreamChanges(ctx, &orchestratorv1.GetUpstreamChangesRequest{
		UniqueId: uniqueID, Depth: upstreamDepth,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	return MapUpstreamChanges(resp), nil
}

// CurrentVersion returns the node's running version. A NOT_FOUND response
// degrades to (zero value, false, nil); other gRPC errors are returned to the
// caller.
func (c *GraphClient) CurrentVersion(ctx context.Context, uniqueID string) (ports.CurrentVersion, bool, error) {
	resp, err := c.client.GetNodeVersions(ctx, &orchestratorv1.GetNodeVersionsRequest{
		UniqueId: uniqueID, CurrentOnly: true, IncludeCode: true,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ports.CurrentVersion{}, false, nil
		}
		return ports.CurrentVersion{}, false, err
	}
	v, ok := MapCurrentVersion(resp)
	return v, ok, nil
}

// Precedents returns past rejections matching q's signature, or the broader
// (category, reason) class.
func (c *GraphClient) Precedents(ctx context.Context, q ports.PrecedentQuery) ([]prompt.Precedent, error) {
	resp, err := c.client.GetPrecedents(ctx, &orchestratorv1.GetPrecedentsRequest{
		Signature: q.Signature, Category: q.Category, Reason: q.Reason, Limit: q.Limit,
	})
	if err != nil {
		return nil, err
	}
	return MapPrecedents(resp), nil
}

// MapUpstreamChanges converts a GetUpstreamChangesResponse to prompt values.
func MapUpstreamChanges(resp *orchestratorv1.GetUpstreamChangesResponse) []prompt.UpstreamChange {
	out := make([]prompt.UpstreamChange, 0, len(resp.Changes))
	for _, ch := range resp.Changes {
		u := prompt.UpstreamChange{NodeID: ch.UniqueId, Depth: int(ch.Depth)}
		if d := ch.Diff; d != nil {
			u.CodeDiff, u.ConfigDiff, u.Truncated = d.RawCodeDiff, d.ConfigDiff, d.Truncated
		}
		out = append(out, u)
	}
	return out
}

// MapCurrentVersion extracts the single current version; ok=false when the
// node has no recorded current version.
func MapCurrentVersion(resp *orchestratorv1.GetNodeVersionsResponse) (ports.CurrentVersion, bool) {
	if len(resp.Versions) == 0 {
		return ports.CurrentVersion{}, false
	}
	v := resp.Versions[0]
	return ports.CurrentVersion{RawCode: v.RawCode, ContentHash: v.ContentHash, PromotedAt: v.PromotedAt}, true
}

// MapPrecedents converts a GetPrecedentsResponse to prompt values; only the
// first proposal's PR URL is carried (one fix PR per rejection in practice).
func MapPrecedents(resp *orchestratorv1.GetPrecedentsResponse) []prompt.Precedent {
	out := make([]prompt.Precedent, 0, len(resp.Precedents))
	for _, p := range resp.Precedents {
		pr := prompt.Precedent{
			ReleaseID: p.ReleaseId, NodeID: p.NodeId, Stage: p.Stage,
			Category: p.Category, Reason: p.Reason, ErrorExcerpt: p.ErrorExcerpt,
			RejectedAt: p.RejectedAt, Resolved: p.Resolved,
			ResolutionDiff: p.ResolutionDiff, DiffTruncated: p.ResolutionDiffTruncated,
		}
		if len(p.Proposals) > 0 {
			pr.PRURL = p.Proposals[0].PrUrl
		}
		out = append(out, pr)
	}
	return out
}
