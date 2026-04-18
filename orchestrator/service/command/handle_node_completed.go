package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleNodeCompletedHandler handles the HandleNodeCompleted command.
type HandleNodeCompletedHandler struct {
	uow     uow.UnitOfWork
	runRepo run.Repository
	logger  *slog.Logger
}

// NewHandleNodeCompletedHandler creates a new HandleNodeCompletedHandler.
func NewHandleNodeCompletedHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	logger *slog.Logger,
) *HandleNodeCompletedHandler {
	return &HandleNodeCompletedHandler{
		uow:     u,
		runRepo: runRepo,
		logger:  logger,
	}
}

// Handle processes the HandleNodeCompleted command.
func (h *HandleNodeCompletedHandler) Handle(ctx context.Context, cmd domainCmd.HandleNodeCompletedCmd, messageID string) error {
	h.logger.Info("Processing node completed",
		"message_id", messageID,
		"task_id", cmd.TaskID,
		"schedule_id", cmd.ScheduleID,
		"schedule_name", cmd.ScheduleName,
		"schema", cmd.SchemaName,
		"table_name", cmd.TableName,
		"status", cmd.Status,
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
	msgProcessingID, shouldSkip, err := h.handleMessageDeduplication(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Update the node status in the graph (outside transaction, idempotent via MERGE).
	if err := h.runRepo.UpdateNodeStatus(
		ctx,
		cmd.ScheduleID.String(),
		cmd.ScheduleName,
		cmd.SchemaName,
		cmd.TableName,
		cmd.Status,
	); err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	// If the node succeeded, find and enqueue ready downstream nodes.
	if cmd.Status == "SUCCEEDED" {
		downstreamNodes, err := h.runRepo.GetReadyDownstream(
			ctx,
			cmd.ScheduleID.String(),
			cmd.ScheduleName,
			cmd.SchemaName,
			cmd.TableName,
		)
		if err != nil {
			return fmt.Errorf("failed to query ready downstream nodes: %w", err)
		}

		h.logger.Info("Found downstream nodes ready for execution",
			"count", len(downstreamNodes),
			"trigger_table", cmd.TableName,
		)

		for _, node := range downstreamNodes {
			nodeType, err := pkgModel.ParseNodeType(node.NodeType)
			if err != nil {
				h.logger.Error("Skipping downstream node with invalid node_type",
					"table", node.TableName, "node_type", node.NodeType, "error", err)
				continue
			}

			taskID, err := h.runRepo.GetTaskIDForNode(ctx, cmd.ScheduleID.String(), node.ServiceName, node.SchemaName, node.TableName)
			if err != nil {
				return fmt.Errorf("failed to get task for %s.%s: %w", node.SchemaName, node.TableName, err)
			}

			jobName, err := pkgDomain.ComputeJobName(node.ServiceName, node.SchemaName, node.TableName)
			if err != nil {
				return fmt.Errorf("failed to compute job_name for %s.%s: %w", node.SchemaName, node.TableName, err)
			}

			evt := domain.NodeReadyForExecution{
				ScheduleID:   cmd.ScheduleID.String(),
				ScheduleName: node.ScheduleName,
				ServiceName:  node.ServiceName,
				SchemaName:   node.SchemaName,
				TableName:    node.TableName,
				TaskID:       taskID,
				JobName:      jobName,
				NodeType:     string(nodeType),
			}

			evtPayload, err := json.Marshal(evt)
			if err != nil {
				return fmt.Errorf("failed to marshal event: %w", err)
			}

			outboxEntry := &domain.OutboxEntry{
				ID:                  uuid.New(),
				MessageProcessingID: &msgProcessingID,
				AggregateType:       "orchestrator",
				AggregateID:         cmd.ScheduleID,
				EventType:           "node_ready_for_execution",
				Payload:             evtPayload,
				StreamName:          "query.model:v1",
				Status:              "pending",
				MaxRetries:          3,
			}

			if err := h.uow.OutboxRepo().Create(ctx, outboxEntry); err != nil {
				return fmt.Errorf("failed to write to outbox: %w", err)
			}

			h.logger.Debug("Wrote event to outbox",
				"outbox_id", outboxEntry.ID,
				"downstream_table", node.TableName,
			)
		}
	} else {
		h.logger.Info("Node did not succeed, skipping downstream check",
			"table_name", cmd.TableName,
			"status", cmd.Status,
		)
	}

	// Mark message processing as completed.
	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("failed to update message state: %w", err)
	}

	// Commit the Postgres transaction.
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	h.logger.Info("Node completed processing finished", "trigger_table", cmd.TableName)

	return nil
}

// handleMessageDeduplication checks if message was already processed.
// Returns (messageProcessingID, shouldSkip, error).
func (h *HandleNodeCompletedHandler) handleMessageDeduplication(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "node.updated:v1",
		State:      "processing",
		Payload:    messagePayload,
	}

	id, inserted, err := h.uow.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to insert message processing: %w", err)
	}

	if !inserted {
		// Message already being processed or completed.
		existing, err := h.uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("failed to get existing message: %w", err)
		}

		// If already completed or acked, skip processing.
		if existing.State == "completed" || existing.State == "acked" {
			h.logger.Info("Message already processed, skipping",
				"message_id", messageID,
				"state", existing.State,
			)
			return existing.ID, true, nil
		}

		// Still processing (another instance), abort.
		h.logger.Warn("Message being processed by another instance",
			"message_id", messageID,
		)
		return existing.ID, true, nil
	}

	return id, false, nil
}
