package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// RemediationRequestedRejectionsHandler consumes remediation.requested:v1 on
// the case-base group and records each classified failure as a :Rejection.
//
// The write is code-optional by design: a compile-stage failure has no code
// bundle (parse never ran), and a bundle that turns out unreadable must not
// cost the precedent — the classification carried by the event is recorded
// either way, and only a bundle that may still land (absent from object
// storage) is worth a retry.
type RemediationRequestedRejectionsHandler struct {
	uow      uow.UnitOfWork
	bundles  ports.CodeBundleReader
	caseBase repository.CaseBaseRepository
	logger   *slog.Logger
}

// NewRemediationRequestedRejectionsHandler creates the handler.
func NewRemediationRequestedRejectionsHandler(
	u uow.UnitOfWork,
	bundles ports.CodeBundleReader,
	caseBase repository.CaseBaseRepository,
	logger *slog.Logger,
) *RemediationRequestedRejectionsHandler {
	return &RemediationRequestedRejectionsHandler{uow: u, bundles: bundles, caseBase: caseBase, logger: logger}
}

// Handle processes one remediation.requested:v1 message on the rejections group.
func (h *RemediationRequestedRejectionsHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in event.RemediationRequested,
) error {
	if in.ReleaseID == "" || in.NodeID == "" || in.ErrorSignature == "" {
		return fmt.Errorf("%w: remediation.requested message %s lacks release_id/node_id/error_signature",
			pkgevents.ErrPermanent, messageID)
	}
	at, err := time.Parse(time.RFC3339, in.ClassifiedAt)
	if err != nil {
		return fmt.Errorf("%w: remediation.requested message %s has unparseable classified_at %q: %v",
			pkgevents.ErrPermanent, messageID, in.ClassifiedAt, err)
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
		messageID, streams.OrchestratorRemediationRequestedRejections, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	rawCode, contentHash, err := h.failingCode(ctx, in)
	if err != nil {
		return err // bundle may still land: leave in PEL for redelivery
	}

	rej := casebase.Rejection{
		ReleaseID:    in.ReleaseID,
		NodeID:       in.NodeID,
		Stage:        in.Source,
		Category:     in.Category,
		Reason:       in.Reason,
		Signature:    in.ErrorSignature,
		ErrorExcerpt: sanitize.Text(in.ErrorExcerpt),
		DBTLogURI:    in.DBTLogURI,
		At:           at,
		RawCode:      sanitize.Text(rawCode),
		ContentHash:  contentHash,
	}
	if err := h.caseBase.RecordRejection(ctx, rej); err != nil {
		return fmt.Errorf("record rejection %s/%s: %w", in.ReleaseID, in.NodeID, err)
	}

	h.logger.Info("rejection recorded in case base",
		"release_id", in.ReleaseID, "node_id", in.NodeID,
		"signature", in.ErrorSignature, "has_code", rawCode != "")
	return h.complete(ctx, msgProcessingID)
}

// failingCode reads the failing node's code from the release's bundle.
// Returns ("", "", nil) with a log when there is nothing to read (no URI, node
// absent, unreadable bundle); returns a non-nil error ONLY for a bundle that
// is absent from object storage and may still land (transient, retry).
func (h *RemediationRequestedRejectionsHandler) failingCode(
	ctx context.Context, in event.RemediationRequested,
) (rawCode, contentHash string, err error) {
	if in.CodeBundleURI == "" {
		return "", "", nil // compile-stage failure: no bundle ever existed
	}
	bundle, err := h.bundles.Fetch(ctx, in.CodeBundleURI)
	if err != nil {
		if errors.Is(err, ports.ErrBundleMalformed) || errors.Is(err, ports.ErrBundleTooLarge) {
			// Unreadable forever. Losing the code must not lose the precedent:
			// record without it, loudly.
			h.logger.Error("code bundle unreadable — recording rejection without code",
				"release_id", in.ReleaseID, "node_id", in.NodeID,
				"uri", in.CodeBundleURI, "error", err)
			return "", "", nil
		}
		return "", "", fmt.Errorf("fetch code bundle %s: %w", in.CodeBundleURI, err)
	}
	n, ok := bundle.Nodes[in.NodeID]
	if !ok {
		h.logger.Warn("failing node absent from its release's code bundle — recording without code",
			"release_id", in.ReleaseID, "node_id", in.NodeID, "uri", in.CodeBundleURI)
		return "", "", nil
	}
	return n.RawCode, n.ContentHash, nil
}

// complete marks the dedup row processed and commits.
func (h *RemediationRequestedRejectionsHandler) complete(ctx context.Context, msgProcessingID uuid.UUID) error {
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
