package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleRerunHandler handles the HandleRerun command.
type HandleRerunHandler struct {
	uow     uow.UnitOfWork
	runRepo run.Repository
	logger  *slog.Logger
}

// NewHandleRerunHandler creates a new HandleRerunHandler.
func NewHandleRerunHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	logger *slog.Logger,
) *HandleRerunHandler {
	return &HandleRerunHandler{
		uow:     u,
		runRepo: runRepo,
		logger:  logger,
	}
}

// Handle processes the HandleRerun command.
func (h *HandleRerunHandler) Handle(ctx context.Context, cmd domainCmd.HandleRerunCmd, messageID string) error {
	h.logger.Info("Processing rerun",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"schedule_name", cmd.ScheduleName,
		"schema", cmd.SchemaName,
		"table_name", cmd.TableName,
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
	msgProcessingID, shouldSkip, err := h.handleRerunDedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Get transitive downstream nodes from the rerun target.
	downstream, err := h.runRepo.GetTransitiveDownstream(ctx, cmd.ScheduleName, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get transitive downstream: %w", err)
	}

	// Reset the rerun target node to PENDING.
	if err := h.runRepo.UpdateNodeStatus(ctx, cmd.RunID, cmd.ScheduleName, cmd.SchemaName, cmd.TableName, "PENDING"); err != nil {
		return fmt.Errorf("failed to reset rerun target node status: %w", err)
	}

	// Reset any FAILED downstream nodes to PENDING.
	for _, node := range downstream {
		if node.Status == string(domain.NodeStatusFailed) {
			if err := h.runRepo.UpdateNodeStatus(ctx, cmd.RunID, cmd.ScheduleName, node.SchemaName, node.TableName, "PENDING"); err != nil {
				return fmt.Errorf("failed to reset downstream node %s.%s status: %w", node.SchemaName, node.TableName, err)
			}
		}
	}

	// Look up the rerun target's node_type and service_name from the graph
	// (the graph reflects the current state after any fixes).
	targetNodeType, err := h.runRepo.GetNodeType(ctx, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get node_type for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}
	targetServiceName, err := h.runRepo.GetNodeServiceName(ctx, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get service_name for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	// Build target nodes list for the outbox payload (target + FAILED downstream).
	targetNodes := buildTargetNodePayloads(cmd, targetNodeType, targetServiceName, downstream)

	// Build outbox payload.
	outboxPayload, err := json.Marshal(map[string]interface{}{
		"run_id":       cmd.RunID,
		"target_nodes": targetNodes,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	outboxEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "rerun_ready",
		Payload:             outboxPayload,
		StreamName:          "rerun.ready:v1",
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

	h.logger.Info("Rerun processing finished",
		"run_id", cmd.RunID,
		"target", fmt.Sprintf("%s.%s", cmd.SchemaName, cmd.TableName),
	)

	return nil
}

// handleRerunDedup checks if message was already processed.
// Returns (messageProcessingID, shouldSkip, error).
func (h *HandleRerunHandler) handleRerunDedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "initialize.run:v1",
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

// buildTargetNodePayloads builds the list of target nodes (rerun target + FAILED downstream).
func buildTargetNodePayloads(cmd domainCmd.HandleRerunCmd, targetNodeType, targetServiceName string, downstream []*domain.TableNode) []domainCmd.NodePayload {
	// Start with the rerun target itself.
	// ServiceName comes from the graph (current state) — this is the Docker
	// image the K8s job will run.
	targets := []domainCmd.NodePayload{
		{
			TableName:    cmd.TableName,
			SchemaName:   cmd.SchemaName,
			ServiceName:  targetServiceName,
			ScheduleName: cmd.ScheduleName,
			NodeType:     targetNodeType,
		},
	}

	// Add FAILED downstream nodes.
	for _, node := range downstream {
		if node.Status == string(domain.NodeStatusFailed) {
			targets = append(targets, domainCmd.NodePayload{
				TableName:    node.TableName,
				SchemaName:   node.SchemaName,
				ServiceName:  node.ServiceName,
				Owner:        node.Owner,
				ScheduleName: node.ScheduleName,
				Criticality:  string(node.Criticality),
				NodeType:     node.NodeType,
				Status:       node.Status,
			})
		}
	}

	return targets
}
