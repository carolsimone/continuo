package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// InitializeRunHandler handles the initialize-run input.
type InitializeRunHandler struct {
	uow         uow.UnitOfWork
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

// NewInitializeRunHandler creates a new InitializeRunHandler.
func NewInitializeRunHandler(
	u uow.UnitOfWork,
	snapshotSvc SnapshotService,
	logger *slog.Logger,
) *InitializeRunHandler {
	return &InitializeRunHandler{
		uow:         u,
		snapshotSvc: snapshotSvc,
		logger:      logger,
	}
}

// Handle processes the initialize-run input.
func (h *InitializeRunHandler) Handle(ctx context.Context, cmd domainModel.InitializeRunInput, messageID string) error {
	h.logger.Info("Processing run initialization",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"schedule_name", cmd.ScheduleName,
	)

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
	msgProcessingID, shouldSkip, err := h.handleInitRunDedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Snapshot the graph via the unified Snapshot+LatestFullDAG routine.
	// InitializeRunInput carries no kind/sourceRunID; default to "cron" / nil.
	projection, err := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "cron",
		Selector:     snapshot.LatestFullDAG{},
	})
	if err != nil {
		if errors.Is(err, snapshot.ErrEmptyProjection) {
			h.logger.Warn("Snapshot: no nodes for schedule, run will have no EXECUTES edges",
				"schedule_name", cmd.ScheduleName, "run_id", cmd.RunID)
			// Continue and write run.initialized:v1 with empty node lists rather
			// than failing the run; downstream consumers handle empty payloads.
			projection = nil
		} else {
			return fmt.Errorf("failed to snapshot graph: %w", err)
		}
	}

	// Build outbox payload from snapshot projection.
	outboxPayload, err := buildRunInitializedPayloadFromProjection(cmd.RunID, projection)
	if err != nil {
		return fmt.Errorf("failed to build outbox payload: %w", err)
	}

	outboxEntry := &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "run_initialized",
		Payload:             outboxPayload,
		StreamName:          streams.RunInitializedV1,
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

	h.logger.Info("Run initialization processing finished",
		"run_id", cmd.RunID,
		"schedule_name", cmd.ScheduleName,
	)

	return nil
}

// handleInitRunDedup checks if message was already processed.
// Returns (messageProcessingID, shouldSkip, error).
func (h *InitializeRunHandler) handleInitRunDedup(
	ctx context.Context,
	messageID string,
	messagePayload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &messageprocessing.MessageProcessing{
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
		existing, err := h.uow.MessageProcessingRepo().GetByMessageIDAndStream(ctx, messageID, "initialize.run:v1")
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

// buildRunInitializedPayloadFromProjection serializes task projection into the outbox JSON payload.
func buildRunInitializedPayloadFromProjection(runID string, projection []snapshot.TaskProjection) ([]byte, error) {
	allNodes := make([]domainEvent.RunInitializedNode, 0, len(projection))
	for _, t := range projection {
		allNodes = append(allNodes, domainEvent.RunInitializedNode{
			TableName:    t.TableName,
			SchemaName:   t.SchemaName,
			ServiceName:  t.ServiceName,
			ScheduleName: t.ScheduleName,
			NodeType:     t.NodeType,
			Status:       t.InitialStatus,
			TaskID:       t.TaskID.String(),
		})
	}

	return json.Marshal(map[string]interface{}{
		"run_id":     runID,
		"all_nodes":  allNodes,
		"root_nodes": []domainEvent.RunInitializedNode{},
		"seed_nodes": []domainEvent.RunInitializedNode{},
	})
}
