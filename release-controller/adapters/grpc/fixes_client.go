// Package grpc holds release-controller's gRPC clients to other components and its
// own gRPC server, release.v1.ReleaseQuery.
package grpc

import (
	"context"
	"fmt"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// FixesClient reads a release's fix rows from agent-remediation's
// RemediationFixes service.
type FixesClient struct {
	c remediationv1.RemediationFixesClient
}

var _ ports.FixReader = (*FixesClient)(nil)

// NewFixesClient wraps an already-dialed RemediationFixesClient.
func NewFixesClient(c remediationv1.RemediationFixesClient) *FixesClient {
	return &FixesClient{c: c}
}

// ListFixesForRelease returns every fix row recorded for the release, mapped
// from the wire type to ports.FixSummary. The request
// carries Limit: 0 — the repository's List treats that as unbounded — rather
// than a fixed page size: the list is already scoped to one release_id, and
// bounded in practice by that release's nodes × rounds × tries, but the
// retry decision must see every try to decide correctly. A capped page
// could let an older open PR or proposed try fall off the page and be
// missed, which would let the retry gate approve a round it should have
// refused.
func (c *FixesClient) ListFixesForRelease(ctx context.Context, releaseID string) ([]ports.FixSummary, error) {
	resp, err := c.c.ListFixes(ctx, &remediationv1.ListFixesRequest{ReleaseId: releaseID, Limit: 0})
	if err != nil {
		return nil, fmt.Errorf("list fixes for %s: %w", releaseID, err)
	}
	out := make([]ports.FixSummary, 0, len(resp.Fixes))
	for _, f := range resp.Fixes {
		out = append(out, ports.FixSummary{
			ID: f.Id, NodeID: f.NodeId, Try: int(f.Try), Status: f.Status,
			PullRequests:     mapPullRequests(f.PullRequests),
			PRProjects:       f.PrProjects,
			RemediationRound: int(f.RemediationRound),
		})
	}
	return out, nil
}

// mapPullRequests maps a fix's per-project pull requests from the wire type
// to the port type. Returns nil for a fix that carries none. The wire Fix's
// singular pr_* fields only mirror the first of these, so they are not read.
func mapPullRequests(prs []*remediationv1.PullRequest) []ports.FixPR {
	if len(prs) == 0 {
		return nil
	}
	out := make([]ports.FixPR, 0, len(prs))
	for _, pr := range prs {
		out = append(out, ports.FixPR{Project: pr.Project, PRState: pr.PrState, PRURL: pr.PrUrl})
	}
	return out
}
