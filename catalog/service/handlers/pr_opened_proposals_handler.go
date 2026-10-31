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

// PrOpenedProposalsHandler consumes remediation.pr_opened:v1 on the proposals
// group and records each opened fix PR as a :Proposal of the message's
// tenant, in one write transaction with the message's processed-message
// marker.
type PrOpenedProposalsHandler struct {
	uow    uow.UnitOfWork
	logger *slog.Logger
}

// NewPrOpenedProposalsHandler creates the handler.
func NewPrOpenedProposalsHandler(u uow.UnitOfWork, logger *slog.Logger) *PrOpenedProposalsHandler {
	return &PrOpenedProposalsHandler{uow: u, logger: logger}
}

// Handle processes one remediation.pr_opened:v1 message of tenantID on the
// proposals group. Steps:
//  1. Refuse as permanent a message lacking its proposal, release or node, or
//     a parseable opened_at.
//  2. Look the group's marker up, by messageID or by outboxEntryID; a
//     processed message is acknowledged.
//  3. In one write transaction, record the proposal of every node the PR
//     resolves and the marker.
func (h *PrOpenedProposalsHandler) Handle(
	ctx context.Context,
	tenantID string,
	messageID string,
	outboxEntryID *uuid.UUID,
	in event.PROpened,
) error {
	if in.ProposalID == "" || in.ReleaseID == "" || in.NodeID == "" {
		return fmt.Errorf("%w: remediation.pr_opened message %s lacks proposal_id/release_id/node_id",
			pkgevents.ErrPermanent, messageID)
	}
	openedAt, err := time.Parse(time.RFC3339, in.OpenedAt)
	if err != nil {
		return fmt.Errorf("%w: remediation.pr_opened message %s has unparseable opened_at %q: %v",
			pkgevents.ErrPermanent, messageID, in.OpenedAt, err)
	}

	marker := uow.Marker{
		TenantID:      tenantID,
		Group:         streams.CatalogRemediationPrOpenedProposals,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}
	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("remediation.pr_opened already processed by the proposals group; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "proposal_id", in.ProposalID)
		return nil
	}

	// One PR fixes every node it resolves, so each of those nodes gets its own
	// :Proposal attached to its own rejection. Recording only the representative
	// node would leave the rest of a batched fix's rejections without a PROPOSED
	// edge, and precedent for them would stop reporting the fix PR. The PR facts
	// themselves are the same for every node in the batch — one PR, one
	// service — so the same PullRequest value is recorded on each call.
	pr := casebase.PullRequest{
		ProposalID: in.ProposalID,
		Service:    in.Service,
		PrURL:      in.PrURL,
		PrNumber:   in.PrNumber,
		State:      "open",
		OpenedBy:   in.OpenedBy,
		OpenedAt:   openedAt,
	}
	resolved := in.ResolvedNodes()
	wrote := false
	err = h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		wrote = true
		caseBase := tx.CaseBase()
		for _, nodeID := range resolved {
			p := casebase.Proposal{ProposalID: in.ProposalID, ReleaseID: in.ReleaseID, NodeID: nodeID}
			if err := caseBase.RecordProposal(ctx, tenantID, p, pr); err != nil {
				return fmt.Errorf("node %s: %w", nodeID, err)
			}
		}
		return nil
	})
	if err != nil {
		// The proposals and the marker roll back together, so the message
		// replays.
		return fmt.Errorf("remediation.pr_opened of release %s: %w", in.ReleaseID, err)
	}
	if !wrote {
		h.logger.Info("remediation.pr_opened processed by another delivery of the proposals group meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "proposal_id", in.ProposalID)
		return nil
	}

	h.logger.Info("proposal recorded in case base",
		"tenant_id", tenantID, "release_id", in.ReleaseID, "node_ids", resolved,
		"proposal_id", in.ProposalID, "service", in.Service, "pr_url", in.PrURL)
	return nil
}
