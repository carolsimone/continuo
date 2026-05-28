package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// IngestTopologyHandler handles the topology-ingestion input.
type IngestTopologyHandler struct {
	uow               uow.UnitOfWork
	topologyRepo      repository.TopologyRepository
	topologyStateRepo repository.TopologyStateRepository
	rejectedRepo      repository.RejectedTopologyRepository
	logger            *slog.Logger
}

// NewIngestTopologyHandler creates a new IngestTopologyHandler.
func NewIngestTopologyHandler(
	u uow.UnitOfWork,
	topologyRepo repository.TopologyRepository,
	topologyStateRepo repository.TopologyStateRepository,
	rejectedRepo repository.RejectedTopologyRepository,
	logger *slog.Logger,
) *IngestTopologyHandler {
	return &IngestTopologyHandler{
		uow:               u,
		topologyRepo:      topologyRepo,
		topologyStateRepo: topologyStateRepo,
		rejectedRepo:      rejectedRepo,
		logger:            logger,
	}
}

// Handle processes the topology-ingestion input.
func (h *IngestTopologyHandler) Handle(ctx context.Context, cmd domainModel.IngestTopologyInput, messageID string, outboxEntryID *uuid.UUID) error {
	h.logger.Info("Processing topology ingestion",
		"message_id", messageID,
		"node_count", len(cmd.Nodes),
	)

	// Validate before any side effect. A permanent error here means the
	// payload is deterministically bad; we record forensics and return
	// the wrapped sentinel so the consumer ACKs.
	if validationErr := validateTopologyNodes(cmd.Nodes); validationErr != nil {
		h.logger.Error("Topology ingestion rejected",
			"message_id", messageID,
			"reason", validationErr,
			"node_count", len(cmd.Nodes),
		)
		// Forensics is best-effort: a failed insert MUST NOT turn a
		// permanent error into a transient one, so we ignore the error.
		// json.Marshal cannot fail on IngestTopologyInput today (all fields
		// are JSON-safe primitives) — if a future field changes that, the
		// forensics row stores nil payload while we still return ErrPermanent.
		rawPayload, _ := json.Marshal(cmd)
		if insertErr := h.rejectedRepo.Insert(ctx, messageID, validationErr.Error(), rawPayload); insertErr != nil {
			h.logger.Error("Failed to record rejected_topology_messages forensics row — proceeding with ErrPermanent return so consumer ACKs",
				"message_id", messageID,
				"insert_error", insertErr,
			)
		}
		return validationErr
	}

	// Marshal input payload for message_processing record.
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}

	// Begin transaction.
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Check for duplicate message.
	msgProcessingID, shouldSkip, err := h.handleTopologyDedup(ctx, messageID, payload, outboxEntryID)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Apply the full manifest snapshot outside the Postgres transaction. The
	// payload is authoritative for the current topology, so missing nodes must
	// be retired as part of the same Neo4j write pass.

	// Warn when multiple nodes from the same service carry different image_tags
	// so operators can detect misconfigured manifests. The first-seen image_tag
	// wins in both this pre-pass and the helper below.
	firstSeenTag := make(map[string]string)
	for _, n := range cmd.Nodes {
		if n.ServiceName == "" || n.ManifestVersion == "" {
			continue
		}
		if existing, seen := firstSeenTag[n.ServiceName]; seen {
			if existing != n.ImageTag {
				h.logger.Warn("Multiple nodes from same service carry different image_tag — using first seen",
					"service", n.ServiceName,
					"first_tag", existing,
					"conflict_tag", n.ImageTag,
				)
			}
		} else {
			firstSeenTag[n.ServiceName] = n.ImageTag
		}
	}

	// Derive sorted unique schedule names and per-service metadata via the
	// shared helper so the schedules.loaded:v1 outbox payload shape is
	// identical across the manifest.loaded:v1 and release.promoted:v1 paths.
	scheduleNames, serviceMetadata := scheduleAndMetadataFromNodes(
		cmd.Nodes,
		func(n domainEvent.ManifestLoadedNode) (string, string, string, string) {
			return n.ScheduleName, n.ServiceName, n.ImageTag, n.ManifestVersion
		},
	)

	// Build the topology node slice for the snapshot writer.
	topologyNodes := make([]*topology.TopologyNode, 0, len(cmd.Nodes))
	for _, n := range cmd.Nodes {
		topologyNodes = append(topologyNodes, toTopologyNode(n))
	}

	// Increment the monotonic topology_generation counter.
	topologyGeneration, err := h.topologyStateRepo.IncrementGeneration(ctx)
	if err != nil {
		return fmt.Errorf("increment topology_generation: %w", err)
	}

	if err := h.topologyRepo.ApplySnapshot(ctx, topologyNodes, topologyGeneration); err != nil {
		return fmt.Errorf("failed to apply topology snapshot: %w", err)
	}

	if err := h.topologyRepo.SetServiceMetadata(ctx, serviceMetadata, topologyGeneration); err != nil {
		return fmt.Errorf("failed to set service_metadata: %w", err)
	}

	// Build outbox payload.
	// event_id is required by the state service's ScheduleCatalogHandler for
	// deduplication — without it the message is discarded on consumption.
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
		EventType:           "topology_ingested",
		Payload:             outboxPayload,
		StreamName:          streams.SchedulesLoadedV1,
		Status:              "pending",
		MaxRetries:          3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
		return fmt.Errorf("failed to write to outbox: %w", err)
	}

	// Mark message processing as completed.
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}

	// Commit the Postgres transaction.
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Topology ingestion processing finished",
		"node_count", len(cmd.Nodes),
		"schedule_count", len(scheduleNames),
	)

	return nil
}

// handleTopologyDedup checks if message was already processed. manifest.loaded:v1
// originates from manifest-controller (Python; not pkg/outbox), so the
// outboxEntryID parameter is normally nil and the dedup falls back to the
// (message_id, stream_name) primary key. Wired through anyway so the
// signature stays consistent with other handlers and future producers can
// opt in.
func (h *IngestTopologyHandler) handleTopologyDedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
	outboxEntryID *uuid.UUID,
) (uuid.UUID, bool, error) {
	return messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, "manifest.loaded:v1", messagePayload, outboxEntryID,
	)
}

// maxOffendersInError caps the number of offending node triples shown in
// the validateTopologyNodes error message. The cap is a log-readability
// bound, not a correctness constraint — the full count appears as the
// "for N node(s)" prefix and the persisted forensics row carries the
// raw payload. Mirror this number in manifest-controller's validator
// to keep error shapes symmetric across services.
const maxOffendersInError = 10

// validateTopologyNodes returns an ErrPermanent-wrapped error if any node
// has an empty ImageTag. The offender list is sorted, capped at
// maxOffendersInError, and followed by "...and N more" when truncated.
// Returns nil for empty input — an empty topology is a valid (if degenerate)
// snapshot.
func validateTopologyNodes(nodes []domainEvent.ManifestLoadedNode) error {
	// nil-slice — first append allocates only when a violation is found.
	var bad []string
	for _, n := range nodes {
		if n.ImageTag == "" {
			bad = append(bad, fmt.Sprintf("%s/%s/%s",
				n.ServiceName, n.SchemaName, n.TableName))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	// Sort the full list before slicing so the truncated head is the
	// lex-first maxOffendersInError, not the insertion-first ones.
	sort.Strings(bad)
	detail := strings.Join(bad, ", ")
	if len(bad) > maxOffendersInError {
		detail = strings.Join(bad[:maxOffendersInError], ", ") +
			fmt.Sprintf(", ...and %d more", len(bad)-maxOffendersInError)
	}
	return fmt.Errorf("%w: image_tag empty for %d node(s): %s",
		events.ErrPermanent, len(bad), detail)
}

// toTopologyNode converts a domainEvent.ManifestLoadedNode to a topology.TopologyNode.
func toTopologyNode(p domainEvent.ManifestLoadedNode) *topology.TopologyNode {
	deps := make([]topology.UpstreamDependency, 0, len(p.Dependencies))
	for _, d := range p.Dependencies {
		deps = append(deps, topology.UpstreamDependency{
			ServiceName: d.ServiceName,
			SchemaName:  d.SchemaName,
			TableName:   d.TableName,
		})
	}

	return &topology.TopologyNode{
		ServiceName:     p.ServiceName,
		SchemaName:      p.SchemaName,
		TableName:       p.TableName,
		Owner:           p.Owner,
		ScheduleName:    p.ScheduleName,
		Criticality:     p.Criticality,
		NodeType:        p.NodeType,
		ManifestVersion: p.ManifestVersion,
		ImageTag:        p.ImageTag,
		Dependencies:    deps,
	}
}
