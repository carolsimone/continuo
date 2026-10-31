package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// PrClosedProvenanceHandler consumes remediation.pr_closed:v1 on the
// provenance group and records each PR's terminal outcome in the message's
// tenant, in one write transaction with the message's processed-message
// marker. A merged outcome draws the case-base provenance edges (RESOLVED_BY
// per resolved node, EDITED per edit); a rejected outcome only stamps the
// PullRequest's terminal state. The handler is deliberately outcome-agnostic
// — the repository decides which edges a given outcome draws.
type PrClosedProvenanceHandler struct {
	uow    uow.UnitOfWork
	logger *slog.Logger
}

// NewPrClosedProvenanceHandler creates the handler.
func NewPrClosedProvenanceHandler(u uow.UnitOfWork, logger *slog.Logger) *PrClosedProvenanceHandler {
	return &PrClosedProvenanceHandler{uow: u, logger: logger}
}

// Handle processes one remediation.pr_closed:v1 message of tenantID on the
// provenance group. Steps:
//  1. Refuse as permanent a message lacking its proposal or release, or a
//     parseable closed_at.
//  2. Look the group's marker up, by messageID or by outboxEntryID; a
//     processed message is acknowledged.
//  3. In one write transaction, record the PR's outcome and the marker.
func (h *PrClosedProvenanceHandler) Handle(
	ctx context.Context,
	tenantID string,
	messageID string,
	outboxEntryID *uuid.UUID,
	in event.PRClosed,
) error {
	if in.ProposalID == "" || in.ReleaseID == "" {
		return fmt.Errorf("%w: remediation.pr_closed message %s lacks proposal_id/release_id",
			pkgevents.ErrPermanent, messageID)
	}
	closedAt, err := time.Parse(time.RFC3339, in.ClosedAt)
	if err != nil {
		return fmt.Errorf("%w: remediation.pr_closed message %s has unparseable closed_at %q: %v",
			pkgevents.ErrPermanent, messageID, in.ClosedAt, err)
	}

	marker := uow.Marker{
		TenantID:      tenantID,
		Group:         streams.CatalogRemediationPrClosedProvenance,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}
	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("remediation.pr_closed already processed by the provenance group; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "proposal_id", in.ProposalID)
		return nil
	}

	// The resolved set and edits pass through verbatim — no NodeID fallback, so
	// a payload with no resolved_node_ids draws no provenance edges. The
	// repository gates edge-drawing on Outcome, so a rejected outcome updates
	// only the PullRequest's terminal state.
	edits := make([]casebase.EditOutcome, 0, len(in.Edits))
	for _, e := range in.Edits {
		edits = append(edits, casebase.EditOutcome{
			Path:         e.Path,
			TargetNodeID: e.TargetNodeID,
			Amended:      e.Amended,
			Diff:         e.Diff,
		})
	}
	outcome := casebase.PullRequestOutcome{
		ProposalID:      in.ProposalID,
		ReleaseID:       in.ReleaseID,
		Service:         in.Service,
		Outcome:         in.Outcome,
		ClosedAt:        closedAt,
		PrURL:           in.PrURL,
		PrNumber:        in.PrNumber,
		ResolvedNodeIDs: in.ResolvedNodeIDs,
		Edits:           edits,
	}
	wrote := false
	err = h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		wrote = true
		return tx.CaseBase().RecordPullRequestOutcome(ctx, tenantID, outcome)
	})
	if err != nil {
		// The outcome and the marker roll back together, so the message
		// replays.
		return fmt.Errorf("remediation.pr_closed of release %s: %w", in.ReleaseID, err)
	}
	if !wrote {
		h.logger.Info("remediation.pr_closed processed by another delivery of the provenance group meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "proposal_id", in.ProposalID)
		return nil
	}

	h.logger.Info("pull request outcome recorded in case base",
		"tenant_id", tenantID, "release_id", in.ReleaseID, "proposal_id", in.ProposalID, "service", in.Service,
		"outcome", in.Outcome, "resolved_nodes", in.ResolvedNodeIDs, "edits", len(edits))
	return nil
}
