package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// InitializeRunHandler handles the initialize-run input.
type InitializeRunHandler struct {
	uow           uow.UnitOfWork
	runRepo       run.Repository
	rerunHandler  *HandleRerunHandler
	logger        *slog.Logger
}

// NewInitializeRunHandler creates a new InitializeRunHandler.
func NewInitializeRunHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	logger *slog.Logger,
) *InitializeRunHandler {
	return &InitializeRunHandler{
		uow:          u,
		runRepo:      runRepo,
		rerunHandler: NewHandleRerunHandler(u, runRepo, logger),
		logger:       logger,
	}
}

// Handle processes the initialize-run input.
func (h *InitializeRunHandler) Handle(ctx context.Context, cmd domainModel.InitializeRunInput, messageID string) error {
	h.logger.Info("Processing run initialization",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"schedule_name", cmd.ScheduleName,
	)

	// Delegate to rerun handler if a rerun target is provided.
	if cmd.RerunTarget != nil {
		return h.rerunHandler.Handle(ctx, domainModel.RerunInput{
			ScheduleName: cmd.ScheduleName,
			RunID:        cmd.RunID,
			ServiceName:  cmd.RerunTarget.ServiceName,
			SchemaName:   cmd.RerunTarget.SchemaName,
			TableName:    cmd.RerunTarget.TableName,
		}, messageID)
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
	msgProcessingID, shouldSkip, err := h.handleInitRunDedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("message deduplication failed: %w", err)
	}
	if shouldSkip {
		return nil
	}

	// Snapshot the graph via the unified Snapshot+LatestFullDAG routine.
	// InitializeRunInput carries no kind/sourceRunID; default to "cron" / nil.
	if _, err := h.runRepo.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "cron",
		Selector:     snapshot.LatestFullDAG{},
	}); err != nil {
		if errors.Is(err, snapshot.ErrEmptyProjection) {
			h.logger.Warn("Snapshot: no nodes for schedule, run will have no EXECUTES edges",
				"schedule_name", cmd.ScheduleName, "run_id", cmd.RunID)
			// Continue and write run.initialized:v1 with empty node lists rather
			// than failing the run; downstream consumers handle empty payloads.
		} else {
			return fmt.Errorf("failed to snapshot graph: %w", err)
		}
	}

	// Get all/root/seed initialization nodes for the schedule.
	initNodes, err := h.runRepo.GetScheduleInitNodes(ctx, cmd.ScheduleName, cmd.RunID)
	if err != nil {
		return fmt.Errorf("failed to get schedule init nodes: %w", err)
	}

	// Build outbox payload with serialized node lists.
	outboxPayload, err := buildRunInitializedPayload(cmd.RunID, initNodes)
	if err != nil {
		return fmt.Errorf("failed to build outbox payload: %w", err)
	}

	outboxEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "run_initialized",
		Payload:             outboxPayload,
		StreamName:          "run.initialized:v1",
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

// buildRunInitializedPayload serializes run init nodes into the outbox JSON payload.
func buildRunInitializedPayload(runID string, initNodes *run.ScheduleInitNodes) ([]byte, error) {
	toNodePayloads := func(nodes []*domain.TableNode) []domainEvent.RunInitializedNode {
		out := make([]domainEvent.RunInitializedNode, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, domainEvent.RunInitializedNode{
				TableName:    n.TableName,
				SchemaName:   n.SchemaName,
				ServiceName:  n.ServiceName,
				Owner:        n.Owner,
				ScheduleName: n.ScheduleName,
				Criticality:  string(n.Criticality),
				NodeType:     n.NodeType,
				Status:       n.Status,
				TaskID:       n.TaskID,
			})
		}
		return out
	}

	var allNodes, rootNodes, seedNodes []domainEvent.RunInitializedNode
	if initNodes != nil {
		allNodes = toNodePayloads(initNodes.AllNodes)
		rootNodes = toNodePayloads(initNodes.RootNodes)
		seedNodes = toNodePayloads(initNodes.SeedNodes)
	}

	return json.Marshal(map[string]interface{}{
		"run_id":     runID,
		"all_nodes":  allNodes,
		"root_nodes": rootNodes,
		"seed_nodes": seedNodes,
	})
}
