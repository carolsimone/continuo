package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
)

// releaseSchedulesNamespace seeds the deterministic ids stamped on the outbox
// rows this handler writes: the schedules.loaded:v1 event_id, derived from the
// release and its promotion seq, and the aggregate ids, derived from the release.
//
// IMMUTABLE: changing this value re-keys every id derived from it. The tests
// mirror this literal; keep them in sync.
var releaseSchedulesNamespace = uuid.MustParse("f0d20655-ae9f-4dc9-a512-99f7ce3955c8")

// ReleasePromotedHandler consumes release.promoted:v2 on the topology-swap group.
// It reads the promoted release's topology from its artifact, swaps the live
// Neo4j topology when the promotion is newer than the live one, and writes the
// follow-up events state needs in the same Postgres transaction as its dedup
// row: schedules.loaded:v1 for an applied (or redelivered) promotion and
// release.seeds.pending:v1 for the seeds it changed.
//
// A promotion older than the live one arriving late leaves the live topology
// and the schedule catalog alone. The seeds it changed are still requested when
// the live release carries the same seed content, built at the live image: no
// seed build is lost to reordering, and older data never overwrites newer data.
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
//  3. Swap the topology through PromoteRelease, which decides by promotion seq.
//  4. Applied or redelivered: write schedules.loaded:v1 and the request for the
//     changed seeds. A redelivery writes them again — after a crash between the
//     Neo4j commit and this transaction they were never committed — and both
//     are idempotent downstream.
//  5. Stale: request only the changed seeds the live release still wants.
//  6. Mark the dedup row completed and commit.
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
		messageID, streams.OrchestratorReleasePromotedV2, payload, outboxEntryID,
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

	nodes := promotedNodes(doc)
	scheduleNames, serviceMetadata := scheduleAndMetadataFromNodes(
		nodes,
		func(n topology.ReleasePromotedTopologyNode) (schedule, service, imageTag string) {
			return n.Schedule, n.ServiceName, n.ImageTag
		},
	)

	// time.Now().UTC() at the boundary: the Neo4j Go driver serialises
	// time.Time using its Location().String() as a timezone identifier and
	// rejects "Local".
	outcome, err := h.topology.PromoteRelease(ctx, in.ReleaseID, in.PromotionSeq, nodes, serviceMetadata, time.Now().UTC())
	if err != nil {
		// Transient failure: rolling back discards the dedup row so the message replays.
		return fmt.Errorf("failed to promote release topology: %w", err)
	}

	seeds := changedSeeds(nodes, in.ChangedNodeIDs)
	if outcome == topology.PromotionStale {
		if len(seeds) > 0 {
			seeds, err = h.topology.StillDesiredSeeds(ctx, seeds)
			if err != nil {
				return fmt.Errorf("match the late release's seeds against the live topology: %w", err)
			}
		}
		h.logger.Warn("release.promoted is older than the live topology — topology and schedule catalog left unchanged",
			"release_id", in.ReleaseID,
			"promotion_seq", in.PromotionSeq,
			"seeds_still_wanted", len(seeds))
	} else {
		if err := h.writeSchedulesLoaded(ctx, msgProcessingID, in.ReleaseID, in.PromotionSeq, scheduleNames, serviceMetadata); err != nil {
			return err
		}
	}
	if err := h.writeSeedsPending(ctx, msgProcessingID, in.ReleaseID, seeds); err != nil {
		return err
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

// changedSeeds returns the dbt-seed nodes named in changedNodeIDs, each carrying
// this release's image tag. An unchanged seed is left out: its data is already
// materialised and its content hash has not moved, so rebuilding it would cost
// a Job for no effect.
func changedSeeds(nodes []topology.ReleasePromotedTopologyNode, changedNodeIDs []string) []topology.ReleasePromotedTopologyNode {
	changed := make(map[string]bool, len(changedNodeIDs))
	for _, id := range changedNodeIDs {
		changed[id] = true
	}
	var out []topology.ReleasePromotedTopologyNode
	for _, n := range nodes {
		if n.NodeType == string(pkgModel.NodeTypeDbtSeed) && changed[n.UniqueID] {
			out = append(out, n)
		}
	}
	return out
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

// writeSeedsPending writes the release.seeds.pending:v1 row asking state to
// build seeds, or nothing when seeds is empty — state would otherwise mint a
// task-less run that could never finish. The row belongs to releaseID; state
// derives the seeds run id from it, so a redelivery creates no second run.
func (h *ReleasePromotedHandler) writeSeedsPending(
	ctx context.Context,
	msgProcessingID uuid.UUID,
	releaseID string,
	seeds []topology.ReleasePromotedTopologyNode,
) error {
	if len(seeds) == 0 {
		return nil
	}
	nodes := make([]map[string]string, 0, len(seeds))
	for _, n := range seeds {
		nodes = append(nodes, map[string]string{
			"service_name": n.ServiceName,
			"schema_name":  n.SchemaName,
			"table_name":   n.TableName,
			"node_type":    n.NodeType,
			"image_tag":    n.ImageTag,
		})
	}
	payload, err := json.Marshal(map[string]any{
		"release_id": releaseID,
		"nodes":      nodes,
	})
	if err != nil {
		return fmt.Errorf("marshal release.seeds.pending payload: %w", err)
	}
	return h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.NewSHA1(releaseSchedulesNamespace, []byte("seeds-pending:"+releaseID)),
		EventType:           domain.EventTypeReleaseSeedsPending,
		Payload:             payload,
		StreamName:          streams.ReleaseSeedsPendingV1,
		Status:              "pending",
	})
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
