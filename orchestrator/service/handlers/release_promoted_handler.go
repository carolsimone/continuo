package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
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

// releaseSchedulesNamespace seeds the deterministic ids stamped on the outbox
// rows this handler writes: the schedules.loaded:v1 event_id, derived from the
// release and its promotion seq, and the aggregate ids, derived from the release.
//
// IMMUTABLE: changing this value re-keys every id derived from it. The tests
// mirror this literal; keep them in sync.
var releaseSchedulesNamespace = uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")

// ReleasePromotedHandler consumes release promotions on the topology-swap group.
// It swaps the live Neo4j topology when a promotion is newer than the live one
// and, in the same Postgres transaction as its dedup row, writes the follow-up
// events state needs: schedules.loaded:v1 for an applied or redelivered
// promotion, and release.seeds.pending:v1 for the seeds it changed. A promotion
// older than the live topology changes nothing.
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
	return &ReleasePromotedHandler{uow: u, topology: topo, logger: logger}
}

// Handle processes one promotion. Steps:
//  1. Begin the Postgres transaction and dedup the message. Several consumer
//     groups read the stream, so the dedup is scoped by this group's name;
//     outboxEntryID catches a re-XADD of the same upstream outbox row under a
//     fresh Redis message ID.
//  2. Swap the topology through PromoteRelease, which decides by promotion seq
//     and records the seq and service metadata in the swap's own transaction.
//  3. Applied or redelivered: write schedules.loaded:v1 and the seeds request.
//     A redelivery writes them again — after a crash between the Neo4j commit
//     and this transaction they were never committed — and both are idempotent
//     downstream: the event_id is deterministic per promotion and state derives
//     the seeds run id from the release.
//  4. Stale: write nothing.
//  5. Mark the dedup row completed and commit.
func (h *ReleasePromotedHandler) Handle(
	ctx context.Context,
	messageID string,
	outboxEntryID *uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	h.logger.Info("Processing release.promoted",
		"message_id", messageID,
		"release_id", in.ReleaseID,
		"promotion_seq", in.PromotionSeq,
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

	// Sorted-unique schedule names and per-service metadata; image_tag comes
	// from the per-node field (first-seen wins for duplicate service entries).
	scheduleNames, serviceMetadata := scheduleAndMetadataFromNodes(
		in.Topology,
		func(n domainEvent.ReleasePromotedNode) (schedule, service, imageTag string) {
			return n.Schedule, n.ServiceName, n.ImageTag
		},
	)

	// time.Now().UTC() at the boundary: the Neo4j Go driver serialises
	// time.Time using its Location().String() as a timezone identifier and
	// rejects "Local".
	outcome, err := h.topology.PromoteRelease(ctx, in.ReleaseID, in.PromotionSeq,
		toDomainNodes(in.Topology), serviceMetadata, time.Now().UTC())
	if err != nil {
		// Transient failure: rolling back discards the dedup row so the message replays.
		return fmt.Errorf("failed to promote release topology: %w", err)
	}

	if outcome != topology.PromotionStale {
		if err := h.writeSchedulesLoaded(ctx, msgProcessingID, in.ReleaseID, in.PromotionSeq, scheduleNames, serviceMetadata); err != nil {
			return err
		}
		if err := h.writeSeedsPending(ctx, msgProcessingID, in); err != nil {
			return err
		}
	}
	return h.complete(ctx, msgProcessingID, in.ReleaseID, in.PromotionSeq, outcome)
}

// writeSchedulesLoaded writes the schedules.loaded:v1 row state reconciles its
// schedule catalog from. The promotion seq travels with it so state can ignore
// a catalog older than the one it already applied.
func (h *ReleasePromotedHandler) writeSchedulesLoaded(
	ctx context.Context,
	msgProcessingID uuid.UUID,
	releaseID string,
	promotionSeq int64,
	scheduleNames []string,
	serviceMetadata map[string]map[string]string,
) error {
	body, err := json.Marshal(map[string]any{
		"event_id":         schedulesLoadedEventID(releaseID, promotionSeq).String(),
		"schedule_names":   scheduleNames,
		"service_metadata": serviceMetadata,
		"promotion_seq":    promotionSeq,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}
	// AggregateID is derived from release_id so every row written for one release
	// shares an aggregate identity: audit queries ("all outbox rows for release
	// X") stay trivial and re-emissions on redelivery stay correlated.
	if err := h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.NewSHA1(releaseSchedulesNamespace, []byte("aggregate:"+releaseID)),
		EventType:           domain.EventTypeReleasePromoted,
		Payload:             body,
		StreamName:          streams.SchedulesLoadedV1,
		Status:              "pending",
	}); err != nil {
		return fmt.Errorf("failed to write to outbox: %w", err)
	}
	return nil
}

// schedulesLoadedEventID is the deterministic event_id of the schedules.loaded:v1
// row for one promotion: the same release announced again under a newer seq is a
// new event.
func schedulesLoadedEventID(releaseID string, promotionSeq int64) uuid.UUID {
	return uuid.NewSHA1(releaseSchedulesNamespace, []byte(fmt.Sprintf("%s|%d", releaseID, promotionSeq)))
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

// toDomainNodes translates wire-format ReleasePromotedNode slices to domain
// ReleasePromotedTopologyNode slices.
func toDomainNodes(wire []domainEvent.ReleasePromotedNode) []topology.ReleasePromotedTopologyNode {
	out := make([]topology.ReleasePromotedTopologyNode, 0, len(wire))
	for _, n := range wire {
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

// writeSeedsPending writes the release.seeds.pending:v1 outbox row listing the
// seeds this release changed, or nothing when it changed none.
//
// An unchanged seed is skipped: its data is already materialised and its content
// hash has not moved, so rebuilding it would cost a Job for no effect.
func (h *ReleasePromotedHandler) writeSeedsPending(
	ctx context.Context,
	msgProcessingID uuid.UUID,
	in domainModel.PromoteReleaseInput,
) error {
	seeds := make([]map[string]string, 0, len(in.Topology))
	for _, n := range in.Topology {
		if !n.Changed || n.NodeType != nodeTypeDBTSeed {
			continue
		}
		seeds = append(seeds, map[string]string{
			"service_name": n.ServiceName,
			"schema_name":  n.SchemaName,
			"table_name":   n.TableName,
			"node_type":    n.NodeType,
			"image_tag":    n.ImageTag,
		})
	}
	if len(seeds) == 0 {
		return nil
	}

	payload, err := json.Marshal(map[string]any{
		"release_id": in.ReleaseID,
		"nodes":      seeds,
	})
	if err != nil {
		return fmt.Errorf("marshal release.seeds.pending payload: %w", err)
	}
	return h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.NewSHA1(releaseSchedulesNamespace, []byte("seeds-pending:"+in.ReleaseID)),
		EventType:           domain.EventTypeReleaseSeedsPending,
		Payload:             payload,
		StreamName:          streams.ReleaseSeedsPendingV1,
		Status:              "pending",
	})
}

// nodeTypeDBTSeed is the node_type a dbt seed carries in a promoted topology.
const nodeTypeDBTSeed = "dbt-seed"
