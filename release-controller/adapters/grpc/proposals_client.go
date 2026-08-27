// Package grpc holds release-controller's gRPC clients to other services.
package grpc

import (
	"context"
	"fmt"

	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// ProposalsClient reads remediation attempts from agent-remediation's
// RemediationProposals service.
type ProposalsClient struct {
	c remediationv1.RemediationProposalsClient
}

var _ ports.ProposalReader = (*ProposalsClient)(nil)

// NewProposalsClient wraps an already-dialed RemediationProposalsClient.
func NewProposalsClient(c remediationv1.RemediationProposalsClient) *ProposalsClient {
	return &ProposalsClient{c: c}
}

// listLimit bounds one release's attempts: nodes × rounds × attempts stays far
// below it for any real release.
const listLimit = 500

// ListProposalsForRelease returns every remediation attempt recorded for the
// release, mapped from the wire type to ports.ProposalSummary.
func (p *ProposalsClient) ListProposalsForRelease(ctx context.Context, releaseID string) ([]ports.ProposalSummary, error) {
	resp, err := p.c.ListProposals(ctx, &remediationv1.ListProposalsRequest{ReleaseId: releaseID, Limit: listLimit})
	if err != nil {
		return nil, fmt.Errorf("list proposals for %s: %w", releaseID, err)
	}
	out := make([]ports.ProposalSummary, 0, len(resp.Proposals))
	for _, pr := range resp.Proposals {
		out = append(out, ports.ProposalSummary{
			ID: pr.Id, NodeID: pr.NodeId, Attempt: int(pr.Attempt),
			Status: pr.Status, PRState: pr.PrState, PRURL: pr.PrUrl,
		})
	}
	return out, nil
}
