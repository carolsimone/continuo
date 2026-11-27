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

// PrOpenedFixesHandler consumes fix.pr_opened:v1 on the fixes
// group and records each opened fix PR as a :Fix of the message's
// tenant, in one write transaction with the message's processed-message
// marker.
type PrOpenedFixesHandler struct {
	uow    uow.UnitOfWork
	logger *slog.Logger
}

// NewPrOpenedFixesHandler creates the handler.
func NewPrOpenedFixesHandler(u uow.UnitOfWork, logger *slog.Logger) *PrOpenedFixesHandler {
	return &PrOpenedFixesHandler{uow: u, logger: logger}
}

// Handle processes one fix.pr_opened:v1 message of tenantID on the
// fixes group. Steps:
//  1. Refuse as permanent a message lacking its fix, release or node, or
//     a parseable opened_at.
//  2. Look the group's marker up, by messageID or by outboxEntryID; a
//     processed message is acknowledged.
//  3. In one write transaction, record the fix of every node the PR
//     resolves and the marker.
func (h *PrOpenedFixesHandler) Handle(
	ctx context.Context,
	tenantID string,
	messageID string,
	outboxEntryID *uuid.UUID,
	in event.PROpened,
) error {
	if in.FixID == "" || in.ReleaseID == "" || in.NodeID == "" {
		return fmt.Errorf("%w: fix.pr_opened message %s lacks fix_id/release_id/node_id",
			pkgevents.ErrPermanent, messageID)
	}
	openedAt, err := time.Parse(time.RFC3339, in.OpenedAt)
	if err != nil {
		return fmt.Errorf("%w: fix.pr_opened message %s has unparseable opened_at %q: %v",
			pkgevents.ErrPermanent, messageID, in.OpenedAt, err)
	}

	marker := uow.Marker{
		TenantID:      tenantID,
		Group:         streams.CatalogFixPrOpenedFixes,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}
	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("fix.pr_opened already processed by the fixes group; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "fix_id", in.FixID)
		return nil
	}

	// One PR fixes every node it resolves, so each of those nodes gets its own
	// :Fix attached to its own rejection. Recording only the representative
	// node would leave the rest of a batched fix's rejections without a PROPOSED
	// edge, and precedent for them would stop reporting the fix PR. The PR facts
	// themselves are the same for every node in the batch — one PR, one
	// project — so the same PullRequest value is recorded on each call.
	pr := casebase.PullRequest{
		FixID:    in.FixID,
		Project:  in.Project,
		PrURL:    in.PrURL,
		PrNumber: in.PrNumber,
		State:    "open",
		OpenedBy: in.OpenedBy,
		OpenedAt: openedAt,
	}
	resolved := in.ResolvedNodeIDs
	wrote := false
	err = h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		wrote = true
		caseBase := tx.CaseBase()
		for _, nodeID := range resolved {
			p := casebase.Fix{FixID: in.FixID, ReleaseID: in.ReleaseID, NodeID: nodeID}
			if err := caseBase.RecordFix(ctx, tenantID, p, pr); err != nil {
				return fmt.Errorf("node %s: %w", nodeID, err)
			}
		}
		return nil
	})
	if err != nil {
		// The fixes and the marker roll back together, so the message
		// replays.
		return fmt.Errorf("fix.pr_opened of release %s: %w", in.ReleaseID, err)
	}
	if !wrote {
		h.logger.Info("fix.pr_opened processed by another delivery of the fixes group meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "fix_id", in.FixID)
		return nil
	}

	h.logger.Info("fix recorded in case base",
		"tenant_id", tenantID, "release_id", in.ReleaseID, "node_ids", resolved,
		"fix_id", in.FixID, "project", in.Project, "pr_url", in.PrURL)
	return nil
}
