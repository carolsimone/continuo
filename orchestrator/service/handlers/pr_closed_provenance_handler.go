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

// PrClosedProvenanceHandler consumes remediation.pr_closed:v1 on the case-base
// provenance group and records each PR's terminal outcome. A merged outcome
// draws the case-base provenance edges (RESOLVED_BY per resolved node, EDITED
// per edit); a rejected outcome only stamps the PullRequest's terminal state.
// The handler is deliberately outcome-agnostic — the repository decides which
// edges a given outcome draws.
type PrClosedProvenanceHandler struct {
	uow      uow.UnitOfWork
	caseBase repository.CaseBaseRepository
	logger   *slog.Logger
}

// NewPrClosedProvenanceHandler creates the handler.
func NewPrClosedProvenanceHandler(
	u uow.UnitOfWork,
	caseBase repository.CaseBaseRepository,
	logger *slog.Logger,
) *PrClosedProvenanceHandler {
	return &PrClosedProvenanceHandler{uow: u, caseBase: caseBase, logger: logger}
}

// Handle processes one remediation.pr_closed:v1 message on the provenance group.
func (h *PrClosedProvenanceHandler) Handle(
	ctx context.Context,
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
		messageID, streams.OrchestratorRemediationPrClosedProvenance, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// The resolved set and edits pass through verbatim — no NodeID fallback, so
	// a legacy payload with no resolved_node_ids draws no provenance edges. The
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
	if err := h.caseBase.RecordPullRequestOutcome(ctx, outcome); err != nil {
		return fmt.Errorf("record pull request outcome %s/%s: %w", in.ReleaseID, in.ProposalID, err)
	}

	h.logger.Info("pull request outcome recorded in case base",
		"release_id", in.ReleaseID, "proposal_id", in.ProposalID, "service", in.Service,
		"outcome", in.Outcome, "resolved_nodes", in.ResolvedNodeIDs, "edits", len(edits))
	return h.complete(ctx, msgProcessingID)
}

// complete marks the dedup row processed and commits.
func (h *PrClosedProvenanceHandler) complete(ctx context.Context, msgProcessingID uuid.UUID) error {
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
