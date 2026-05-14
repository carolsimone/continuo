package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"

	postgresadapter "github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleNodeCompletedHandler processes node-completed messages using the Run aggregate.
type HandleNodeCompletedHandler struct {
	uow                uow.UnitOfWork
	runs               run.AggregateRepository
	cancelledSchedules postgresadapter.CancelledSchedulesRepository
	logger             *slog.Logger
}

// NewHandleNodeCompletedHandler creates a new HandleNodeCompletedHandler.
func NewHandleNodeCompletedHandler(
	u uow.UnitOfWork,
	runs run.AggregateRepository,
	cancelledSchedules postgresadapter.CancelledSchedulesRepository,
	logger *slog.Logger,
) *HandleNodeCompletedHandler {
	return &HandleNodeCompletedHandler{
		uow:                u,
		runs:               runs,
		cancelledSchedules: cancelledSchedules,
		logger:             logger,
	}
}

// Handle processes a node-completed input by loading the Run aggregate,
// applying CompleteNode, persisting, and dispatching domain events as outbox entries.
func (h *HandleNodeCompletedHandler) Handle(ctx context.Context, cmd domainModel.NodeCompletedInput, messageID string) error {
	h.logger.Info("Processing node completed",
		"message_id", messageID,
		"task_id", cmd.TaskID,
		"schedule_id", cmd.ScheduleID,
		"schema", cmd.SchemaName,
		"table_name", cmd.TableName,
		"status", cmd.Status,
	)

	payload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := h.handleDedup(ctx, messageID, payload)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	cancelled, err := h.cancelledSchedules.Exists(ctx, cmd.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled schedules check: %w", err)
	}
	if cancelled {
		h.logger.Info("Schedule is cancelled — suppressing aggregate mutation",
			"schedule_id", cmd.ScheduleID, "table", cmd.TableName)
		if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
			return fmt.Errorf("update message state: %w", err)
		}
		return h.uow.Commit()
	}

	nodeKey := run.NodeKey{
		ServiceName: cmd.ServiceName,
		SchemaName:  cmd.SchemaName,
		TableName:   cmd.TableName,
	}

	for {
		agg, err := h.runs.Rehydrate(ctx, cmd.ScheduleID.String(),
			run.ScopeNodeCompletion{Key: nodeKey, Status: cmd.Status})
		if err != nil {
			return fmt.Errorf("rehydrate aggregate: %w", err)
		}

		events, err := agg.CompleteNode(nodeKey, cmd.Status)
		if err != nil {
			if errors.Is(err, run.ErrNodeAlreadyTerminal) {
				// Neo4j already reflects the terminal transition (likely from a
				// prior attempt whose Postgres tx was rolled back). Re-derive
				// the side-effect events from the loaded subgraph so the new
				// Postgres tx re-creates the missing outbox entries.
				h.logger.Info("Node already terminal — re-deriving effects for outbox",
					"schema", cmd.SchemaName, "table", cmd.TableName)
				redoEvents, derr := agg.EffectsForTerminal(nodeKey)
				if derr != nil {
					return fmt.Errorf("EffectsForTerminal: %w", derr)
				}
				if werr := h.writeOutboxEntries(ctx, cmd, msgProcessingID, redoEvents); werr != nil {
					return werr
				}
				if uerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); uerr != nil {
					return fmt.Errorf("update message state: %w", uerr)
				}
				return h.uow.Commit()
			}
			return fmt.Errorf("CompleteNode: %w", err)
		}

		if err := h.runs.Save(ctx, agg); err != nil {
			if errors.Is(err, run.ErrVersionConflict) {
				h.logger.Info("Version conflict — retrying", "run_id", cmd.ScheduleID)
				continue
			}
			return fmt.Errorf("save aggregate: %w", err)
		}

		if err := h.writeOutboxEntries(ctx, cmd, msgProcessingID, events); err != nil {
			return err
		}
		break
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("update message state: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	h.logger.Info("Node completed processing finished", "trigger_table", cmd.TableName)
	return nil
}

func (h *HandleNodeCompletedHandler) writeOutboxEntries(
	ctx context.Context,
	cmd domainModel.NodeCompletedInput,
	msgProcessingID uuid.UUID,
	events []run.DomainEvent,
) error {
	for _, evt := range events {
		switch e := evt.(type) {
		case run.NodeUnblocked:
			if err := h.writeNodeUnblockedEntry(ctx, cmd, msgProcessingID, e); err != nil {
				return err
			}
		case run.NodeCascadeSkipped:
			if err := h.writeCascadeSkippedEntry(ctx, cmd, msgProcessingID, e); err != nil {
				return err
			}
		case run.RunFinalized:
			h.logger.Info("Run finalized", "run_id", e.RunID, "status", e.TerminalStatus)
			// Neo4j terminal_status and completed_at are written by AggregateRepository.Save.
			// Downstream notification, if needed, can be added here later.
		}
	}
	return nil
}

func (h *HandleNodeCompletedHandler) writeNodeUnblockedEntry(
	ctx context.Context,
	cmd domainModel.NodeCompletedInput,
	msgProcessingID uuid.UUID,
	e run.NodeUnblocked,
) error {
	nodeType, err := pkgModel.ParseNodeType(e.NodeType)
	if err != nil {
		h.logger.Error("Skipping unblocked node with invalid node_type",
			"table", e.Key.TableName, "node_type", e.NodeType, "error", err)
		return nil
	}

	jobName, err := pkgDomain.ComputeJobName(e.Key.ServiceName, e.Key.SchemaName, e.Key.TableName, cmd.ScheduleID.String())
	if err != nil {
		return fmt.Errorf("compute job_name for %s.%s: %w", e.Key.SchemaName, e.Key.TableName, err)
	}

	evt := domain.NodeReadyForExecution{
		ScheduleID:      cmd.ScheduleID.String(),
		ScheduleName:    e.ScheduleName,
		ServiceName:     e.Key.ServiceName,
		SchemaName:      e.Key.SchemaName,
		TableName:       e.Key.TableName,
		TaskID:          e.TaskID.String(),
		JobName:         jobName,
		NodeType:        string(nodeType),
		ManifestVersion: e.ManifestVersion,
		ImageTag:        e.ImageTag,
	}
	evtPayload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal NodeReadyForExecution: %w", err)
	}

	entry := &domain.OutboxEntry{
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
	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("write node_ready_for_execution outbox: %w", err)
	}
	return nil
}

func (h *HandleNodeCompletedHandler) writeCascadeSkippedEntry(
	ctx context.Context,
	cmd domainModel.NodeCompletedInput,
	msgProcessingID uuid.UUID,
	e run.NodeCascadeSkipped,
) error {
	if e.TaskID == uuid.Nil {
		h.logger.Warn("Cascade-skipped node has no task_id — skipping outbox entry",
			"schema", e.Key.SchemaName, "table", e.Key.TableName)
		return nil
	}

	evtPayload, err := json.Marshal(domain.CascadeTaskSkipped{
		TaskID:     e.TaskID.String(),
		ScheduleID: cmd.ScheduleID.String(),
	})
	if err != nil {
		return fmt.Errorf("marshal CascadeTaskSkipped: %w", err)
	}

	entry := &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         cmd.ScheduleID,
		EventType:           "cascade_task_skipped",
		Payload:             evtPayload,
		StreamName:          "task.status.updated:v1",
		Status:              "pending",
		MaxRetries:          3,
	}
	if err := h.uow.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("write cascade_task_skipped outbox: %w", err)
	}
	return nil
}

func (h *HandleNodeCompletedHandler) handleDedup(
	ctx context.Context,
	messageID string,
	payload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &messageprocessing.MessageProcessing{
		MessageID:  messageID,
		StreamName: "node.updated:v1",
		State:      "processing",
		Payload:    payload,
	}
	id, inserted, err := h.uow.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	if !inserted {
		existing, err := h.uow.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("get existing message: %w", err)
		}
		if existing.State == "completed" || existing.State == "acked" {
			h.logger.Info("Message already processed", "message_id", messageID)
			return existing.ID, true, nil
		}
		h.logger.Warn("Message in-flight on another instance", "message_id", messageID)
		return existing.ID, true, nil
	}
	return id, false, nil
}
