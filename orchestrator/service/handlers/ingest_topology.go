package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// IngestTopologyHandler handles the IngestTopology command.
type IngestTopologyHandler struct {
	uow               uow.UnitOfWork
	topologyRepo      topology.Repository
	topologyStateRepo topology.TopologyStateRepository
	logger            *slog.Logger
}

// NewIngestTopologyHandler creates a new IngestTopologyHandler.
func NewIngestTopologyHandler(
	u uow.UnitOfWork,
	topologyRepo topology.Repository,
	topologyStateRepo topology.TopologyStateRepository,
	logger *slog.Logger,
) *IngestTopologyHandler {
	return &IngestTopologyHandler{
		uow:               u,
		topologyRepo:      topologyRepo,
		topologyStateRepo: topologyStateRepo,
		logger:            logger,
	}
}

// Handle processes the IngestTopology command.
func (h *IngestTopologyHandler) Handle(ctx context.Context, cmd domainCmd.IngestTopologyCmd, messageID string) error {
	h.logger.Info("Processing topology ingestion",
		"message_id", messageID,
		"node_count", len(cmd.Nodes),
	)

	// Marshal command payload for message_processing record.
	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	// Begin transaction.
	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	// Check for duplicate message.
	msgProcessingID, shouldSkip, err := h.handleTopologyDedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Apply the full manifest snapshot outside the Postgres transaction. The
	// payload is authoritative for the current topology, so missing nodes must
	// be retired as part of the same Neo4j write pass.
	//
	// Collect unique schedule names and service metadata while iterating.
	scheduleNamesSet := make(map[string]struct{})
	serviceMetadata := make(map[string]map[string]string)
	topologyNodes := make([]*topology.TopologyNode, 0, len(cmd.Nodes))

	for _, n := range cmd.Nodes {
		node := toTopologyNode(n)
		topologyNodes = append(topologyNodes, node)

		if n.ScheduleName != "" {
			scheduleNamesSet[n.ScheduleName] = struct{}{}
		}
		if n.ServiceName != "" && n.ManifestVersion != "" {
			if existing, seen := serviceMetadata[n.ServiceName]; seen {
				if existing["image_tag"] != n.ImageTag {
					h.logger.Warn("Multiple nodes from same service carry different image_tag — using first seen",
						"service", n.ServiceName,
						"first_tag", existing["image_tag"],
						"conflict_tag", n.ImageTag,
					)
				}
			} else {
				serviceMetadata[n.ServiceName] = map[string]string{
					"manifest_version": n.ManifestVersion,
					"image_tag":        n.ImageTag,
				}
			}
		}
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

	// Build unique sorted schedule names slice.
	scheduleNames := make([]string, 0, len(scheduleNamesSet))
	for name := range scheduleNamesSet {
		scheduleNames = append(scheduleNames, name)
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

	outboxEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "topology_ingested",
		Payload:             outboxPayload,
		StreamName:          "schedules.loaded:v1",
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

// handleTopologyDedup checks if message was already processed.
// Returns (messageProcessingID, shouldSkip, error).
func (h *IngestTopologyHandler) handleTopologyDedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "manifest.loaded:v1",
		State:      "processing",
		Payload:    messagePayload,
	}

	id, inserted, err := h.uow.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to insert message processing: %w", err)
	}

	if !inserted {
		existing, err := h.uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("failed to get existing message: %w", err)
		}

		if existing.State == "completed" || existing.State == "acked" {
			h.logger.Info("Message already processed, skipping",
				"message_id", messageID,
				"state", existing.State,
			)
			return existing.ID, true, nil
		}

		h.logger.Warn("Message being processed by another instance",
			"message_id", messageID,
		)
		return existing.ID, true, nil
	}

	return id, false, nil
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
func validateTopologyNodes(nodes []domainCmd.TopologyNodePayload) error {
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

// toTopologyNode converts a domainCmd.TopologyNodePayload to a topology.TopologyNode.
func toTopologyNode(p domainCmd.TopologyNodePayload) *topology.TopologyNode {
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
