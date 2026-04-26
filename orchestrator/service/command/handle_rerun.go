package command

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"

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
//
// It emits two outbox entries:
//  1. run.rerun.dispatched:v1 — consumed by state to reset task counters
//  2. query.model:v1          — triggers execution of the entry (target) node
//
// State gRPC is NOT called; task resets are handled by the state service
// when it consumes run.rerun.dispatched:v1.
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

	// ── Resolve target task ID from Neo4j ────────────────────────────────────
	targetTaskID, err := h.runRepo.GetTaskIDForNode(ctx, cmd.RunID, cmd.ServiceName, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get task_id for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	// ── Resolve skipped downstream task IDs ──────────────────────────────────
	downstreamTaskIDs, err := h.runRepo.GetSkippedDownstreamTaskIDs(ctx, cmd.RunID, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get skipped downstream task IDs for %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	// Reset cascade-skipped downstream EXECUTES edges back to PENDING so that
	// when the target node succeeds, GetReadyDownstream can dispatch them.
	if err := h.runRepo.ResetSkippedDownstreamToPending(ctx, cmd.RunID, cmd.SchemaName, cmd.TableName); err != nil {
		return fmt.Errorf("failed to reset cascade-skipped downstream nodes for %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	tasksToReset := append([]string{targetTaskID}, downstreamTaskIDs...)

	// ── Outbox entry 1: run.rerun.dispatched:v1 ──────────────────────────────
	dispatchedEvt := pkgEvents.RunRerunDispatched{
		ScheduleID:   cmd.RunID, // RunID == ScheduleID.String() in this domain
		ScheduleName: cmd.ScheduleName,
		TasksToReset: tasksToReset,
		EntryTaskID:  targetTaskID,
	}
	dispatchedPayload, err := json.Marshal(dispatchedEvt)
	if err != nil {
		return fmt.Errorf("failed to marshal run.rerun.dispatched payload: %w", err)
	}

	dispatchedEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         uuid.New(),
		EventType:           "run_rerun_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          "run.rerun.dispatched:v1",
		Status:              "pending",
		MaxRetries:          3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, dispatchedEntry); err != nil {
		return fmt.Errorf("failed to write run.rerun.dispatched to outbox: %w", err)
	}

	// ── Outbox entry 2: query.model:v1 for the target node ───────────────────
	targetNodeType, err := h.runRepo.GetNodeType(ctx, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get node_type for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	parsedNodeType, err := pkgModel.ParseNodeType(targetNodeType)
	if err != nil {
		return fmt.Errorf("invalid node_type %q for rerun target %s.%s: %w", targetNodeType, cmd.SchemaName, cmd.TableName, err)
	}

	targetServiceName, err := h.runRepo.GetNodeServiceName(ctx, cmd.SchemaName, cmd.TableName)
	if err != nil {
		return fmt.Errorf("failed to get service_name for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	jobName, err := pkgDomain.ComputeJobName(targetServiceName, cmd.SchemaName, cmd.TableName, cmd.RunID)
	if err != nil {
		return fmt.Errorf("failed to compute job_name for rerun target %s.%s: %w", cmd.SchemaName, cmd.TableName, err)
	}

	queryModelEvt := domain.NodeReadyForExecution{
		ScheduleID:   cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		ServiceName:  targetServiceName,
		SchemaName:   cmd.SchemaName,
		TableName:    cmd.TableName,
		TaskID:       targetTaskID,
		JobName:      jobName,
		NodeType:     string(parsedNodeType),
	}
	queryModelPayload, err := json.Marshal(queryModelEvt)
	if err != nil {
		return fmt.Errorf("failed to marshal query.model payload: %w", err)
	}

	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	queryModelEntry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "node_ready_for_execution",
		Payload:             queryModelPayload,
		StreamName:          "query.model:v1",
		Status:              "pending",
		MaxRetries:          3,
	}

	if err := h.uow.OutboxRepo().Create(ctx, queryModelEntry); err != nil {
		return fmt.Errorf("failed to write query.model to outbox: %w", err)
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
		"target_task_id", targetTaskID,
		"tasks_to_reset", len(tasksToReset),
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
		StreamName: "rerun.ready:v1",
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
