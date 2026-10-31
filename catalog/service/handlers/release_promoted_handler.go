package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	domainModel "github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// ReleasePromotedHandler consumes release.promoted:v2 on the topology group.
// It reads the promoted release's topology from its artifact and swaps the
// tenant's live Neo4j topology when the promotion is newer than the live one,
// recording the message's processed-message marker in the swap's transaction.
// It writes no event: run-controller reads the schedule names of a release,
// and plans its seed loads, from its own copy of every promoted topology.
//
// A promotion older than the live one arriving late leaves the live topology
// alone.
type ReleasePromotedHandler struct {
	uow       uow.UnitOfWork
	artifacts ports.TopologyArtifactReader
	logger    *slog.Logger
}

// NewReleasePromotedHandler creates a new ReleasePromotedHandler.
func NewReleasePromotedHandler(
	u uow.UnitOfWork,
	artifacts ports.TopologyArtifactReader,
	logger *slog.Logger,
) *ReleasePromotedHandler {
	return &ReleasePromotedHandler{uow: u, artifacts: artifacts, logger: logger}
}

// Handle processes one promotion of in.TenantID. Steps:
//  1. Look the group's marker up, by messageID or by outboxEntryID, which
//     catches a re-XADD of the same upstream outbox row under a fresh Redis
//     message ID. A processed message is acknowledged without reading object
//     storage.
//  2. Read the topology artifact, outside any transaction. A checksum
//     mismatch, an unreadable object or an artifact of another release or
//     another tenant is permanent; a missing object or an outage is retried.
//  3. In one write transaction, swap the topology through PromoteRelease,
//     which decides by promotion seq (a redelivered or stale promotion writes
//     nothing), and record the marker.
func (h *ReleasePromotedHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v2",
		"message_id", messageID,
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
	)
	marker := uow.Marker{
		TenantID:      in.TenantID,
		Group:         streams.CatalogReleaseTopology,
		MessageID:     messageID,
		OutboxEntryID: outboxEntryID,
	}

	processed, err := h.uow.Processed(ctx, marker)
	if err != nil {
		return fmt.Errorf("look up the processed-message marker: %w", err)
	}
	if processed {
		h.logger.Info("release.promoted already processed; acknowledged",
			"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
		return nil
	}

	doc, err := h.artifacts.Load(ctx, in.TopologyURI, in.TopologySHA256)
	if err != nil {
		if errors.Is(err, ports.ErrTopologyArtifactCorrupt) {
			h.logger.Error("topology artifact does not match the promotion — dead-lettering the message",
				"tenant_id", in.TenantID,
				"release_id", in.ReleaseID,
				"promotion_seq", in.PromotionSeq,
				"uri", in.TopologyURI,
				"expected_sha256", in.TopologySHA256,
				"error", err)
			return fmt.Errorf("%w: topology artifact %s: %v", pkgevents.ErrPermanent, in.TopologyURI, err)
		}
		return fmt.Errorf("load topology artifact %s: %w", in.TopologyURI, err)
	}
	if doc.ReleaseID != in.ReleaseID {
		h.logger.Error("topology artifact belongs to another release — dead-lettering the message",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "artifact_release_id", doc.ReleaseID, "uri", in.TopologyURI)
		return fmt.Errorf("%w: topology artifact %s belongs to release %q, the promotion is of %q",
			pkgevents.ErrPermanent, in.TopologyURI, doc.ReleaseID, in.ReleaseID)
	}
	if doc.TenantID != in.TenantID {
		h.logger.Error("topology artifact belongs to another tenant — dead-lettering the message",
			"tenant_id", in.TenantID, "release_id", in.ReleaseID, "artifact_tenant_id", doc.TenantID, "uri", in.TopologyURI)
		return fmt.Errorf("%w: topology artifact %s belongs to tenant %q, the promotion is of %q",
			pkgevents.ErrPermanent, in.TopologyURI, doc.TenantID, in.TenantID)
	}
	nodes := promotedNodes(doc)

	var outcome topology.PromotionOutcome
	err = h.uow.Write(ctx, marker, true, func(ctx context.Context, tx uow.Tx) error {
		var err error
		// time.Now().UTC() at the boundary: the Neo4j Go driver serialises
		// time.Time using its Location().String() as a timezone identifier and
		// rejects "Local".
		outcome, err = tx.Topology().PromoteRelease(ctx, in.TenantID, in.ReleaseID, in.PromotionSeq, nodes, time.Now().UTC())
		return err
	})
	if err != nil {
		// The swap and the marker roll back together, so the message replays.
		return fmt.Errorf("promote release topology: %w", err)
	}

	switch outcome {
	case "":
		h.logger.Info("release.promoted processed by another delivery meanwhile; acknowledged",
			"message_id", messageID, "tenant_id", in.TenantID, "release_id", in.ReleaseID)
		return nil
	case topology.PromotionStale:
		h.logger.Warn("release.promoted is older than the live topology — topology left unchanged",
			"tenant_id", in.TenantID,
			"release_id", in.ReleaseID,
			"promotion_seq", in.PromotionSeq)
	}
	h.logger.Info("Release promotion processing finished",
		"tenant_id", in.TenantID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
		"outcome", string(outcome),
	)
	return nil
}
