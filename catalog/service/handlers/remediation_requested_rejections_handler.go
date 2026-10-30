package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/codebundle"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// RemediationRequestedRejectionsHandler consumes remediation.requested:v2 on
// the rejections group and records each node of the batch as a :Rejection of
// the message's tenant.
//
// The write is code-optional by design: a compile-stage failure has no code
// bundle (parse never ran), and a bundle that turns out unreadable must not
// cost the precedent — the classification carried by the event is recorded
// either way, and only a bundle that may still land (absent from object
// storage) is worth a retry. One message carries every healable node from a
// rejected release; the bundle is fetched once, and every node's :Rejection
// is recorded in one write transaction with the message's processed-message
// marker.
type RemediationRequestedRejectionsHandler struct {
	uow     uow.UnitOfWork
	bundles ports.CodeBundleReader
	logger  *slog.Logger
}

// NewRemediationRequestedRejectionsHandler creates the handler.
func NewRemediationRequestedRejectionsHandler(
	u uow.UnitOfWork,
	bundles ports.CodeBundleReader,
	logger *slog.Logger,
) *RemediationRequestedRejectionsHandler {
	return &RemediationRequestedRejectionsHandler{uow: u, bundles: bundles, logger: logger}
}

// Handle processes one remediation.requested:v2 message of tenantID on the
// rejections group, recording one :Rejection per node the classifier flagged
// as healable. Steps:
//  1. Refuse as permanent a message lacking its release, its nodes, a node's
//     id or error signature, or a parseable classified_at.
//  2. Look the group's marker up, by messageID or by outboxEntryID, which
//     catches a re-XADD of the same upstream outbox row under a fresh Redis
//     message ID. A processed message is acknowledged without reading object
//     storage.
//  3. Read the release's code bundle, outside any transaction.
//  4. In one write transaction, record every node's rejection and the marker.
func (h *RemediationRequestedRejectionsHandler) Handle(
	ctx context.Context,
	tenantID string,
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

	marker := uow.Marker{
		TenantID:      tenantID,
		Group:         streams.CatalogRemediationRequestedRejections,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}
	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("remediation.requested already processed by the rejections group; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "release_id", in.ReleaseID)
		return nil
	}

	bundle, err := h.releaseBundle(ctx, tenantID, in)
	if err != nil {
		return err // bundle may still land: leave in PEL for redelivery
	}

	rejections := make([]casebase.Rejection, 0, len(in.Nodes))
	for _, n := range in.Nodes {
		rawCode, contentHash := codeFor(bundle, tenantID, in, n.NodeID, h.logger)
		rejections = append(rejections, casebase.Rejection{
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
		})
	}

	wrote := false
	err = h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		wrote = true
		caseBase := tx.CaseBase()
		for _, rej := range rejections {
			if err := caseBase.RecordRejection(ctx, tenantID, rej); err != nil {
				return fmt.Errorf("record rejection %s/%s: %w", rej.ReleaseID, rej.NodeID, err)
			}
		}
		return nil
	})
	if err != nil {
		// The rejections and the marker roll back together, so the message
		// replays.
		return fmt.Errorf("record the rejections of release %s: %w", in.ReleaseID, err)
	}
	if !wrote {
		h.logger.Info("remediation.requested processed by another delivery of the rejections group meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", tenantID, "release_id", in.ReleaseID)
		return nil
	}
	for _, rej := range rejections {
		h.logger.Info("rejection recorded in case base",
			"tenant_id", tenantID, "release_id", rej.ReleaseID, "node_id", rej.NodeID,
			"signature", rej.Signature, "has_code", rej.RawCode != "")
	}
	return nil
}

// releaseBundle fetches the release's code bundle once for the whole batch.
// Returns (nil, nil) with a log when there is nothing usable to read (no URI,
// unreadable bundle, bundle for another release); returns a non-nil error
// ONLY for a bundle that is absent from object storage and may still land
// (transient, retry).
func (h *RemediationRequestedRejectionsHandler) releaseBundle(
	ctx context.Context, tenantID string, in event.RemediationRequested,
) (*codebundle.Bundle, error) {
	if in.CodeBundleURI == "" {
		// A parse- or compile-stage failure: both precede the parse that
		// builds the bundle, so no bundle ever existed for this release.
		return nil, nil
	}
	bundle, err := h.bundles.Fetch(ctx, in.CodeBundleURI)
	if err != nil {
		if errors.Is(err, ports.ErrBundleMalformed) || errors.Is(err, ports.ErrBundleTooLarge) {
			// Unreadable forever. Losing the code must not lose the precedent:
			// record every node in the batch without it, loudly.
			h.logger.Error("code bundle unreadable — recording rejections without code",
				"tenant_id", tenantID, "release_id", in.ReleaseID, "uri", in.CodeBundleURI, "error", err)
			return nil, nil
		}
		return nil, fmt.Errorf("fetch code bundle %s: %w", in.CodeBundleURI, err)
	}
	if bundle.ReleaseID != in.ReleaseID {
		// The URI resolved to a bundle for a different release. Recording its
		// RawCode/ContentHash would stamp another release's code onto these
		// rejections' failing precedent. Unlike the versions handler (which
		// fails the message permanently — writing the wrong code would corrupt
		// the version graph), losing the code here must not lose the precedent:
		// record every node in the batch without it.
		h.logger.Error("code bundle belongs to a different release — recording rejections without code",
			"tenant_id", tenantID, "release_id", in.ReleaseID, "bundle_release_id", bundle.ReleaseID, "uri", in.CodeBundleURI)
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
	bundle *codebundle.Bundle, tenantID string, in event.RemediationRequested, nodeID string, logger *slog.Logger,
) (rawCode, contentHash string) {
	if bundle == nil {
		return "", ""
	}
	n, ok := bundle.Nodes[nodeID]
	if !ok {
		logger.Warn("failing node absent from its release's code bundle — recording without code",
			"tenant_id", tenantID, "release_id", in.ReleaseID, "node_id", nodeID, "uri", in.CodeBundleURI)
		return "", ""
	}
	return n.RawCode, n.ContentHash
}
