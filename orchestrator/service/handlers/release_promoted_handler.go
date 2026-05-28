package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// ReleasePromotedHandler consumes release.promoted:v1 messages, atomically
// swaps the Neo4j topology via ReleasePromotionRepository, and emits a
// schedules.loaded:v1 outbox entry so that the state service can refresh its
// schedule projections. The handler is idempotent: if the repository reports
// that the topology was already promoted for the same release_id (changed=false),
// the outbox write is skipped to avoid spurious schedule projection refreshes.
type ReleasePromotedHandler struct {
	uow      uow.UnitOfWork
	topology repository.ReleasePromotionRepository
	logger   *slog.Logger
}

// NewReleasePromotedHandler creates a new ReleasePromotedHandler.
func NewReleasePromotedHandler(
	u uow.UnitOfWork,
	topo repository.ReleasePromotionRepository,
	logger *slog.Logger,
) *ReleasePromotedHandler {
	return &ReleasePromotedHandler{
		uow:      u,
		topology: topo,
		logger:   logger,
	}
}

// Handle processes a release.promoted:v1 message. Steps:
//  1. Begin Postgres transaction.
//  2. Dedup check — if the (messageID, release.promoted:v1) pair is already
//     recorded, commit and return nil (ACK).
//  3. Translate wire nodes to domain nodes and call PromoteRelease on the
//     Neo4j repository. If changed=false, write dedup row, commit, return nil.
//     If err, return err without writing dedup so the message can replay.
//  4. Derive schedule names and service_metadata from the promoted nodes.
//  5. Write a schedules.loaded:v1 outbox entry with release_id as manifest_version.
//  6. Mark dedup row as completed, commit.
func (h *ReleasePromotedHandler) Handle(
	ctx context.Context,
	messageID string,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted:v1",
		"message_id", messageID,
		"release_id", in.ReleaseID,
		"node_count", len(in.Topology),
	)

	payload, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Dedup check using (message_id, release.promoted:v1) namespace — isolated
	// from the manifest.loaded:v1 consumer's dedup namespace so the two paths
	// never collide even if they produce schedules.loaded:v1 for the same topology.
	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.ReleasePromotedV1, payload, nil,
	)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Translate wire-format nodes to domain nodes.
	domainNodes := toDomainNodes(in.Topology)

	// Atomically swap the Neo4j topology. PromoteRelease short-circuits (returns
	// changed=false) when the :Meta singleton already records the same release_id.
	changed, err := h.topology.PromoteRelease(ctx, in.ReleaseID, domainNodes, time.Now())
	if err != nil {
		// Transient failure: do not write dedup so the message can be replayed.
		return fmt.Errorf("failed to promote release topology: %w", err)
	}

	if !changed {
		// The topology was already promoted for this release_id. Write dedup so
		// the message is not replayed, but skip the outbox write to avoid
		// spurious schedules.loaded:v1 emissions.
		if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
			return fmt.Errorf("failed to update message state: %w", err)
		}
		if err := h.uow.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		h.logger.Info("Release already promoted — skipping outbox write",
			"message_id", messageID,
			"release_id", in.ReleaseID,
		)
		return nil
	}

	// Derive sorted-unique schedule names and per-service metadata from the
	// promoted topology. The release_id IS the manifest identity on the candidate
	// path, so it is passed as manifest_version for every service. image_tag comes
	// from the per-node field (first-seen wins for duplicate service entries).
	scheduleNames, serviceMetadata := scheduleAndMetadataFromNodes(
		in.Topology,
		func(n domainEvent.ReleasePromotedNode) (schedule, service, imageTag, manifestVersion string) {
			return n.Schedule, n.ServiceName, n.ImageTag, in.ReleaseID
		},
	)

	// Build schedules.loaded:v1 outbox payload. The shape is identical to the one
	// produced by IngestTopologyHandler so state's ScheduleCatalogHandler can
	// consume from a single stream regardless of which path produced the message.
	outboxPayload, err := json.Marshal(map[string]interface{}{
		"event_id":         uuid.New().String(),
		"schedule_names":   scheduleNames,
		"service_metadata": serviceMetadata,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	outboxEntry := &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "release_promoted",
		Payload:             outboxPayload,
		StreamName:          streams.SchedulesLoadedV1,
		Status:              "pending",
		MaxRetries:          3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
		return fmt.Errorf("failed to write to outbox: %w", err)
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Release promotion processing finished",
		"release_id", in.ReleaseID,
		"node_count", len(in.Topology),
		"schedule_count", len(scheduleNames),
	)

	return nil
}

// toDomainNodes translates wire-format ReleasePromotedNode slices to domain
// ReleasePromotedTopologyNode slices. Both types carry the same fields; the
// wire type has JSON tags for unmarshaling and the domain type is JSON-tag-free
// as required by clean-architecture boundaries.
func toDomainNodes(wire []domainEvent.ReleasePromotedNode) []topology.ReleasePromotedTopologyNode {
	out := make([]topology.ReleasePromotedTopologyNode, 0, len(wire))
	for _, n := range wire {
		out = append(out, topology.ReleasePromotedTopologyNode{
			UniqueID:          n.UniqueID,
			SchemaName:        n.SchemaName,
			TableName:         n.TableName,
			ServiceName:       n.ServiceName,
			ImageTag:          n.ImageTag,
			Schedule:          n.Schedule,
			UpstreamUniqueIDs: append([]string(nil), n.UpstreamUniqueIDs...),
		})
	}
	return out
}
