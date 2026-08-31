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
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// RemediationRequestedRejectionsHandler consumes remediation.requested:v2 on
// the case-base group and records each node inside the batch as a :Rejection.
//
// The write is code-optional by design: a compile-stage failure has no code
// bundle (parse never ran), and a bundle that turns out unreadable must not
// cost the precedent — the classification carried by the event is recorded
// either way, and only a bundle that may still land (absent from object
// storage) is worth a retry. One message carries every healable node from a
// rejected release; the bundle is fetched once and every node's :Rejection
// is recorded inside the message's single dedup'd transaction.
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

// Handle processes one remediation.requested:v2 message on the rejections
// group, recording one :Rejection per node the classifier flagged as healable.
func (h *RemediationRequestedRejectionsHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in event.RemediationRequested,
) error {
	if in.ReleaseID == "" || len(in.Nodes) == 0 {
		return fmt.Errorf("%w: remediation.requested message %s lacks release_id/nodes",
			pkgevents.ErrPermanent, messageID)
	}
	for _, n := range in.Nodes {
		if n.NodeID == "" || n.ErrorSignature == "" {
			return fmt.Errorf("%w: remediation.requested message %s has a node lacking node_id/error_signature",
				pkgevents.ErrPermanent, messageID)
		}
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

	bundle, err := h.releaseBundle(ctx, in)
	if err != nil {
		return err // bundle may still land: leave in PEL for redelivery
	}

	for _, n := range in.Nodes {
		rawCode, contentHash := codeFor(bundle, in, n.NodeID, h.logger)
		rej := casebase.Rejection{
			ReleaseID:    in.ReleaseID,
			NodeID:       n.NodeID,
			Stage:        in.Source,
			Category:     n.Category,
			Reason:       n.Reason,
			Signature:    n.ErrorSignature,
			ErrorExcerpt: sanitize.Text(n.ErrorExcerpt),
			DBTLogURI:    n.DBTLogURI,
			At:           at,
			RawCode:      sanitize.Text(rawCode),
			ContentHash:  contentHash,
		}
		if err := h.caseBase.RecordRejection(ctx, rej); err != nil {
			return fmt.Errorf("record rejection %s/%s: %w", in.ReleaseID, n.NodeID, err)
		}
		h.logger.Info("rejection recorded in case base",
			"release_id", in.ReleaseID, "node_id", n.NodeID,
			"signature", n.ErrorSignature, "has_code", rawCode != "")
	}
	return h.complete(ctx, msgProcessingID)
}

// releaseBundle fetches the release's code bundle once for the whole batch.
// Returns (nil, nil) with a log when there is nothing usable to read (no URI,
// unreadable bundle, bundle for another release); returns a non-nil error
// ONLY for a bundle that is absent from object storage and may still land
// (transient, retry).
func (h *RemediationRequestedRejectionsHandler) releaseBundle(
	ctx context.Context, in event.RemediationRequested,
) (*codebundle.Bundle, error) {
	if in.CodeBundleURI == "" {
		return nil, nil // compile-stage failure: no bundle ever existed
	}
	bundle, err := h.bundles.Fetch(ctx, in.CodeBundleURI)
	if err != nil {
		if errors.Is(err, ports.ErrBundleMalformed) || errors.Is(err, ports.ErrBundleTooLarge) {
			// Unreadable forever. Losing the code must not lose the precedent:
			// record every node in the batch without it, loudly.
			h.logger.Error("code bundle unreadable — recording rejections without code",
				"release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
			return nil, nil
		}
		return nil, fmt.Errorf("fetch code bundle %s: %w", in.CodeBundleURI, err)
	}
	if bundle.ReleaseID != in.ReleaseID {
		// The URI resolved to a bundle for a different release. Recording its
		// RawCode/ContentHash would stamp another release's code onto these
		// rejections' failing precedent. Unlike the versions handler (which
		// drops the message — writing the wrong code would corrupt the version
		// graph), losing the code here must not lose the precedent: record every
		// node in the batch without it.
		h.logger.Error("code bundle belongs to a different release — recording rejections without code",
			"release_id", in.ReleaseID, "bundle_release_id", bundle.ReleaseID, "uri", in.CodeBundleURI)
		return nil, nil
	}
	return &bundle, nil
}

// codeFor reads one node's code out of the release's already-fetched bundle.
// A nil bundle means releaseBundle already logged why (or there was never one
// to begin with), so it returns ("", "") silently; a present bundle missing
// this particular node (it never reached parse) is new information worth its
// own warning.
func codeFor(
	bundle *codebundle.Bundle, in event.RemediationRequested, nodeID string, logger *slog.Logger,
) (rawCode, contentHash string) {
	if bundle == nil {
		return "", ""
	}
	n, ok := bundle.Nodes[nodeID]
	if !ok {
		logger.Warn("failing node absent from its release's code bundle — recording without code",
			"release_id", in.ReleaseID, "node_id", nodeID, "uri", in.CodeBundleURI)
		return "", ""
	}
	return n.RawCode, n.ContentHash
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
