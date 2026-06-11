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

// releaseSchedulesNamespace seeds the deterministic event_id stamped on
// schedules.loaded:v1 emissions from the release-promoted path. UUID v5
// ensures duplicate emissions for the same release_id resolve to the
// same event_id, so state's ScheduleCatalogHandler dedups them as one.
//
// IMMUTABLE: changing this value re-keys every schedules.loaded:v1
// event_id derived from a release_id and breaks consumer-side dedup for
// any in-flight redeliveries. The integration test mirrors this literal;
// keep them in sync.
var releaseSchedulesNamespace = uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")

// ReleasePromotedHandler consumes release.promoted:v1 messages, atomically
// swaps the Neo4j topology via ReleasePromotionRepository, increments the
// topology_generation counter, updates :TopologyRoot service_metadata in Neo4j,
// and emits a schedules.loaded:v1 outbox entry so that the state service can
// refresh its schedule projections. The handler is idempotent: the outbox row
// is always emitted (even when changed=false) because the deterministic event_id
// (uuid.NewSHA1 of releaseSchedulesNamespace + release_id) allows state's
// ScheduleCatalogHandler to dedup re-emissions at the consumer side.
type ReleasePromotedHandler struct {
	uow               uow.UnitOfWork
	topology          repository.ReleasePromotionRepository
	topologyRepo      repository.TopologyRepository
	topologyStateRepo repository.TopologyStateRepository
	logger            *slog.Logger
}

// NewReleasePromotedHandler creates a new ReleasePromotedHandler.
func NewReleasePromotedHandler(
	u uow.UnitOfWork,
	topo repository.ReleasePromotionRepository,
	topologyRepo repository.TopologyRepository,
	topologyStateRepo repository.TopologyStateRepository,
	logger *slog.Logger,
) *ReleasePromotedHandler {
	return &ReleasePromotedHandler{
		uow:               u,
		topology:          topo,
		topologyRepo:      topologyRepo,
		topologyStateRepo: topologyStateRepo,
		logger:            logger,
	}
}

// Handle processes a release.promoted:v1 message. Steps:
//  1. Begin Postgres transaction.
//  2. Dedup check — if the (messageID, release.promoted:v1) pair is already
//     recorded, commit and return nil (ACK). outboxEntryID is threaded through
//     so a re-XADD of the same upstream outbox row (different Redis message ID,
//     same business event) is caught by the secondary unique index.
//  3. Translate wire nodes to domain nodes and call PromoteRelease on the
//     Neo4j repository.
//  4. Derive schedule names and service_metadata from the promoted nodes.
//  5. If changed=true: increment topology_generation and write :TopologyRoot.
//     If changed=false: read the current generation for the payload.
//  6. Always write a schedules.loaded:v1 outbox entry. The event_id is
//     deterministic (uuid v5 of releaseSchedulesNamespace + release_id), so
//     state's ScheduleCatalogHandler deduplicates re-emissions from idempotent
//     redeliveries.
//  7. Mark dedup row as completed, commit.
func (h *ReleasePromotedHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
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

	// Dedup check keyed on the (message_id, release.promoted:v1) namespace, so
	// this consumer's dedup state is scoped to its own stream.
	// outboxEntryID provides a secondary uniqueness key that catches a re-XADD
	// of the same upstream outbox row under a fresh Redis message ID.
	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.ReleasePromotedV1, payload, outboxEntryID,
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
	// time.Now().UTC() at the boundary: the Neo4j Go driver serialises time.Time
	// using its Location().String() as a timezone identifier and rejects "Local".
	// The adapter also normalises to UTC defensively (see release_promotion_repository.go),
	// but converting here documents the contract and protects future call sites
	// from re-encountering the same bug.
	changed, err := h.topology.PromoteRelease(ctx, in.ReleaseID, domainNodes, time.Now().UTC())
	if err != nil {
		// Transient failure: do not write dedup so the message can be replayed.
		return fmt.Errorf("failed to promote release topology: %w", err)
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

	// Deterministic event_id: uuid v5 of (namespace, release_id) guarantees that
	// every re-emission for the same release carries the identical event_id, so
	// state's ScheduleCatalogHandler deduplicates it as one logical event.
	eventID := uuid.NewSHA1(releaseSchedulesNamespace, []byte(in.ReleaseID))

	// After a successful swap, increment the topology_generation counter and
	// write the updated service_metadata to :TopologyRoot so runs initialised
	// after this promotion inherit the correct generation and metadata.
	// On a no-op (changed=false), read the current generation for the payload
	// without bumping it — the topology has not actually changed.
	//
	// Drift window — known limitation. IncrementGeneration commits its own tx
	// independent of the main UoW (topology_state is shared mutable state; see
	// orchestrator/adapters/postgres/topology_state_repository.go).
	// If attempt 1 commits PromoteRelease in Neo4j and IncrementGeneration in
	// Postgres but the main UoW then rolls back, attempt 2 sees changed=false
	// and reads the already-advanced generation — end state stays consistent.
	// If attempt 1 fails BEFORE IncrementGeneration commits and PromoteRelease
	// already committed, attempt 2 sees changed=false with the OLD generation,
	// and :TopologyRoot stays at the previous generation. Fully closing this
	// requires pulling IncrementGeneration into the main UoW — tracked as a
	// cross-handler refactor in https://github.com/carolsimone/continuo/issues/94.
	// Calling SetServiceMetadata on both branches narrows the window: if
	// attempt 1 incremented but never wrote :TopologyRoot, attempt 2's retry
	// catches up since SetServiceMetadata is an idempotent MERGE.
	var topologyGeneration int64
	if changed {
		topologyGeneration, err = h.topologyStateRepo.IncrementGeneration(ctx)
		if err != nil {
			return fmt.Errorf("increment topology generation: %w", err)
		}
	} else {
		topologyGeneration, err = h.topologyStateRepo.GetGeneration(ctx)
		if err != nil {
			return fmt.Errorf("get current topology generation: %w", err)
		}
		h.logger.Info("Release already promoted — re-emitting schedules.loaded:v1 for idempotency",
			"message_id", messageID,
			"release_id", in.ReleaseID,
			"topology_generation", topologyGeneration,
		)
	}
	// SetServiceMetadata is an idempotent MERGE on :TopologyRoot, so calling
	// it on the changed=false branch catches up any cross-step crash where
	// attempt 1 incremented the counter but never landed :TopologyRoot. On
	// the no-crash redelivery case it re-writes the same values plus an
	// updated_at bump — no functional impact.
	if err := h.topologyRepo.SetServiceMetadata(ctx, serviceMetadata, topologyGeneration); err != nil {
		return fmt.Errorf("set service metadata on :TopologyRoot: %w", err)
	}

	// Build the schedules.loaded:v1 outbox payload consumed by state's
	// ScheduleCatalogHandler to refresh its schedule projections.
	outboxPayload, err := json.Marshal(map[string]interface{}{
		"event_id":            eventID.String(),
		"schedule_names":      scheduleNames,
		"service_metadata":    serviceMetadata,
		"topology_generation": topologyGeneration,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	// AggregateID is derived deterministically from release_id so every
	// outbox row this handler writes for the same release shares the same
	// aggregate identity. That makes audit queries (e.g., "show all outbox
	// rows for release X") trivial and keeps the always-emit-on-redelivery
	// pattern from leaving uncorrelated duplicates in orchestrator_outbox.
	outboxEntry := &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.NewSHA1(releaseSchedulesNamespace, []byte("aggregate:"+in.ReleaseID)),
		EventType:           "release_promoted",
		Payload:             outboxPayload,
		StreamName:          streams.SchedulesLoadedV1,
		Status:              "pending",
		MaxRetries:          pkgoutbox.DefaultMaxRetries,
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
		"topology_generation", topologyGeneration,
		"changed", changed,
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
			NodeType:          n.NodeType,
			ImageTag:          n.ImageTag,
			Schedule:          n.Schedule,
			UpstreamUniqueIDs: append([]string(nil), n.UpstreamUniqueIDs...),
		})
	}
	return out
}
