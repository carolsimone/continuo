package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
)

// ReleasePromotedHandler consumes release.promoted:v2 on the topology-swap group.
// It reads the promoted release's topology from its artifact and swaps the live
// Neo4j topology when the promotion is newer than the live one, recording the
// message in its Postgres dedup row. It writes no event: state reads the
// schedule names of a release, and plans its seed loads, from its own copy of
// every promoted topology.
//
// A promotion older than the live one arriving late leaves the live topology
// alone.
type ReleasePromotedHandler struct {
	uow       uow.UnitOfWork
	artifacts ports.TopologyArtifactReader
	topology  repository.ReleasePromotionRepository
	logger    *slog.Logger
}

// NewReleasePromotedHandler creates a new ReleasePromotedHandler.
func NewReleasePromotedHandler(
	u uow.UnitOfWork,
	artifacts ports.TopologyArtifactReader,
	topo repository.ReleasePromotionRepository,
	logger *slog.Logger,
) *ReleasePromotedHandler {
	return &ReleasePromotedHandler{uow: u, artifacts: artifacts, topology: topo, logger: logger}
}

// Handle processes one promotion. Steps:
//  1. Begin the Postgres transaction and dedup the message. Several consumer
//     groups read the stream, so the dedup is scoped by this group's name;
//     outboxEntryID catches a re-XADD of the same upstream outbox row under a
//     fresh Redis message ID.
//  2. Read the topology artifact. A checksum mismatch, an unreadable object or
//     an artifact of another release is permanent; a missing object or an
//     outage is retried.
//  3. Swap the topology through PromoteRelease, which decides by promotion seq;
//     a redelivered or stale promotion leaves the graph untouched.
//  4. Mark the dedup row completed and commit.
func (h *ReleasePromotedHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v2",
		"message_id", messageID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
	)

	payload, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.OrchestratorReleasePromoted, payload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	doc, err := h.artifacts.Load(ctx, in.TopologyURI, in.TopologySHA256)
	if err != nil {
		if errors.Is(err, ports.ErrTopologyArtifactCorrupt) {
			h.logger.Error("topology artifact does not match the promotion — dead-lettering the message",
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
			"release_id", in.ReleaseID, "artifact_release_id", doc.ReleaseID, "uri", in.TopologyURI)
		return fmt.Errorf("%w: topology artifact %s belongs to release %q, the promotion is of %q",
			pkgevents.ErrPermanent, in.TopologyURI, doc.ReleaseID, in.ReleaseID)
	}

	// time.Now().UTC() at the boundary: the Neo4j Go driver serialises
	// time.Time using its Location().String() as a timezone identifier and
	// rejects "Local".
	outcome, err := h.topology.PromoteRelease(ctx, in.ReleaseID, in.PromotionSeq, promotedNodes(doc), time.Now().UTC())
	if err != nil {
		// Transient failure: rolling back discards the dedup row so the message replays.
		return fmt.Errorf("failed to promote release topology: %w", err)
	}

	if outcome == topology.PromotionStale {
		h.logger.Warn("release.promoted is older than the live topology — topology left unchanged",
			"release_id", in.ReleaseID,
			"promotion_seq", in.PromotionSeq)
	}
	return h.complete(ctx, msgProcessingID, in.ReleaseID, in.PromotionSeq, outcome)
}

// promotedNodes maps the artifact's nodes onto the nodes the swap writes.
// dbt-test nodes are left out: a test is bind-checked during validation and
// never scheduled, so it has no place in the live graph.
func promotedNodes(doc topologyartifact.Document) []topology.ReleasePromotedTopologyNode {
	out := make([]topology.ReleasePromotedTopologyNode, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		if n.NodeType == string(pkgModel.NodeTypeDbtTest) {
			continue
		}
		out = append(out, topology.ReleasePromotedTopologyNode{
			UniqueID:          n.UniqueID,
			SchemaName:        n.SchemaName,
			TableName:         n.TableName,
			ServiceName:       n.ServiceName,
			NodeType:          n.NodeType,
			ContentHash:       n.ContentHash,
			TestCount:         n.TestCount,
			ImageTag:          n.ImageTag,
			SecretRef:         n.SecretRef,
			Schedule:          n.Schedule,
			UpstreamUniqueIDs: append([]string(nil), n.UpstreamUniqueIDs...),
			OriginalFilePath:  n.OriginalFilePath,
		})
	}
	return out
}

// complete marks the dedup row processed and commits.
func (h *ReleasePromotedHandler) complete(
	ctx context.Context,
	msgProcessingID uuid.UUID,
	releaseID string,
	promotionSeq int64,
	outcome topology.PromotionOutcome,
) error {
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	h.logger.Info("Release promotion processing finished",
		"release_id", releaseID,
		"promotion_seq", promotionSeq,
		"outcome", string(outcome),
	)
	return nil
}
