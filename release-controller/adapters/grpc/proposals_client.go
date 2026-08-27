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

// ListProposalsForRelease returns every remediation attempt recorded for the
// release, mapped from the wire type to ports.ProposalSummary. The request
// carries Limit: 0 — the repository's List treats that as unbounded — rather
// than a fixed page size: the list is already scoped to one release_id, and
// bounded in practice by that release's nodes × rounds × attempts, but the
// retry decision must see every attempt to decide correctly. A capped page
// could let an older open PR or proposed attempt fall off the page and be
// missed, which would let the retry gate approve a round it should have
// refused.
func (p *ProposalsClient) ListProposalsForRelease(ctx context.Context, releaseID string) ([]ports.ProposalSummary, error) {
	resp, err := p.c.ListProposals(ctx, &remediationv1.ListProposalsRequest{ReleaseId: releaseID, Limit: 0})
	if err != nil {
		return nil, fmt.Errorf("list proposals for %s: %w", releaseID, err)
	}
	out := make([]ports.ProposalSummary, 0, len(resp.Proposals))
	for _, pr := range resp.Proposals {
		out = append(out, ports.ProposalSummary{
			ID: pr.Id, NodeID: pr.NodeId, Attempt: int(pr.Attempt),
			Status: pr.Status, PRState: pr.PrState, PRURL: pr.PrUrl,
			RemediationRound: int(pr.RemediationRound),
		})
	}
	return out, nil
}
