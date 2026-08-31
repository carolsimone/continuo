package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// PrOpenedProposalsHandler consumes remediation.pr_opened:v1 on the case-base
// group and records each opened fix PR as a :Proposal.
type PrOpenedProposalsHandler struct {
	uow      uow.UnitOfWork
	caseBase repository.CaseBaseRepository
	logger   *slog.Logger
}

// NewPrOpenedProposalsHandler creates the handler.
func NewPrOpenedProposalsHandler(
	u uow.UnitOfWork,
	caseBase repository.CaseBaseRepository,
	logger *slog.Logger,
) *PrOpenedProposalsHandler {
	return &PrOpenedProposalsHandler{uow: u, caseBase: caseBase, logger: logger}
}

// Handle processes one remediation.pr_opened:v1 message on the proposals group.
func (h *PrOpenedProposalsHandler) Handle(
	ctx context.Context,
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

	payload, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.OrchestratorRemediationPrOpenedProposals, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
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
	for _, nodeID := range resolved {
		p := casebase.Proposal{
			ProposalID: in.ProposalID,
			ReleaseID:  in.ReleaseID,
			NodeID:     nodeID,
		}
		if err := h.caseBase.RecordProposal(ctx, p, pr); err != nil {
			return fmt.Errorf("record proposal %s/%s: %w", in.ReleaseID, nodeID, err)
		}
	}

	h.logger.Info("proposal recorded in case base",
		"release_id", in.ReleaseID, "node_ids", resolved,
		"proposal_id", in.ProposalID, "pr_url", in.PrURL)
	return h.complete(ctx, msgProcessingID)
}

// complete marks the dedup row processed and commits.
func (h *PrOpenedProposalsHandler) complete(ctx context.Context, msgProcessingID uuid.UUID) error {
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
