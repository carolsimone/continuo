package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainCmd "github.com/carolsimone/continuo/orchestrator/domain/command"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleSingleNodeRunHandler consumes trigger.single_node_run:v1 messages.
type HandleSingleNodeRunHandler struct {
	uow     uow.UnitOfWork
	runRepo run.Repository
	logger  *slog.Logger
}

// NewHandleSingleNodeRunHandler creates a new HandleSingleNodeRunHandler.
func NewHandleSingleNodeRunHandler(u uow.UnitOfWork, runRepo run.Repository, logger *slog.Logger) *HandleSingleNodeRunHandler {
	return &HandleSingleNodeRunHandler{uow: u, runRepo: runRepo, logger: logger}
}

// Handle processes the HandleSingleNodeRunCmd command.
//
// It runs the dedup→snapshot→outbox flow:
//  1. Marshal cmd for dedup record.
//  2. Begin transaction; defer Rollback.
//  3. Dedup on messageID with stream "trigger.single_node_run:v1".
//  4. Parse cmd.SourceRunID → *uuid.UUID (nil if empty).
//  5. Call runRepo.SnapshotSingleNodeRun.
//     - ErrTargetNotFound: emit run.failed:v1, mark dedup completed, commit, return nil.
//     - Other errors: return wrapped error (triggers retry).
//  6. Emit run.initialized:v1.
//  7. Emit query.model:v1 with NodeReadyForExecution payload.
//  8. Mark dedup state="completed".
//  9. Commit.
func (h *HandleSingleNodeRunHandler) Handle(ctx context.Context, cmd domainCmd.HandleSingleNodeRunCmd, messageID string) error {
	h.logger.Info("Processing single-node run",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"target", fmt.Sprintf("%s.%s.%s", cmd.ServiceName, cmd.SchemaName, cmd.TableName),
		"metadata_source", cmd.MetadataSource,
	)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := h.dedup(ctx, messageID, cmdPayload)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	var sourceRunUUID *uuid.UUID
	if cmd.SourceRunID != "" {
		parsed, perr := uuid.Parse(cmd.SourceRunID)
		if perr != nil {
			return fmt.Errorf("invalid source_run_id %q: %w", cmd.SourceRunID, perr)
		}
		sourceRunUUID = &parsed
	}

	taskID, imageTag, manifestVersion, nodeType, snapErr := h.runRepo.SnapshotSingleNodeRun(
		ctx,
		cmd.RunID, cmd.ScheduleName,
		sourceRunUUID,
		cmd.ServiceName, cmd.SchemaName, cmd.TableName,
		cmd.MetadataSource,
	)
	if snapErr != nil {
		if errors.Is(snapErr, run.ErrTargetNotFound) {
			if ferr := h.emitRunFailed(ctx, cmd, msgProcessingID, "target_not_found"); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed after target_not_found: %w", cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot single-node run: %w", snapErr)
	}

	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	// Outbox 1: run.initialized:v1
	initEvt := pkgEvents.RunInitialized{
		ScheduleID:   cmd.RunID,
		ScheduleName: cmd.ScheduleName,
	}
	initPayload, err := json.Marshal(initEvt)
	if err != nil {
		return fmt.Errorf("marshal run.initialized: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_initialized",
		Payload:             initPayload,
		StreamName:          "run.initialized:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.initialized to outbox: %w", err)
	}

	// Outbox 2: query.model:v1
	jobName, err := pkgDomain.ComputeJobName(cmd.ServiceName, cmd.SchemaName, cmd.TableName, cmd.RunID)
	if err != nil {
		return fmt.Errorf("compute job name: %w", err)
	}
	queryEvt := domain.NodeReadyForExecution{
		ScheduleID:      cmd.RunID,
		ScheduleName:    cmd.ScheduleName,
		ServiceName:     cmd.ServiceName,
		SchemaName:      cmd.SchemaName,
		TableName:       cmd.TableName,
		TaskID:          taskID,
		JobName:         jobName,
		NodeType:        nodeType,
		ManifestVersion: manifestVersion,
		ImageTag:        imageTag,
	}
	queryPayload, err := json.Marshal(queryEvt)
	if err != nil {
		return fmt.Errorf("marshal query.model: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "node_ready_for_execution",
		Payload:             queryPayload,
		StreamName:          "query.model:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write query.model to outbox: %w", err)
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	h.logger.Info("Single-node run processing finished",
		"run_id", cmd.RunID,
		"target", fmt.Sprintf("%s.%s.%s", cmd.ServiceName, cmd.SchemaName, cmd.TableName),
		"task_id", taskID,
	)

	return nil
}

// dedup checks for duplicate delivery. Returns (msgProcessingID, shouldSkip, err).
func (h *HandleSingleNodeRunHandler) dedup(
	ctx context.Context,
	messageID string,
	payload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "trigger.single_node_run:v1",
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

// emitRunFailed writes a run.failed:v1 outbox entry.
func (h *HandleSingleNodeRunHandler) emitRunFailed(
	ctx context.Context,
	cmd domainCmd.HandleSingleNodeRunCmd,
	msgProcessingID uuid.UUID,
	reason string,
) error {
	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	failedPayload, err := json.Marshal(map[string]string{
		"schedule_id":   cmd.RunID,
		"schedule_name": cmd.ScheduleName,
		"reason":        reason,
	})
	if err != nil {
		return fmt.Errorf("marshal run.failed payload: %w", err)
	}

	if err := h.uow.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_failed",
		Payload:             failedPayload,
		StreamName:          "run.failed:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.failed to outbox: %w", err)
	}

	h.logger.Info("Emitted run.failed:v1",
		"run_id", cmd.RunID,
		"reason", reason,
	)
	return nil
}
