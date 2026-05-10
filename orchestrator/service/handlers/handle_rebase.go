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
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleRebaseHandler consumes trigger.rebase:v1 messages.
//
// The cmd carries the SOURCE run's schedule_id (SourceRunID) and the NEW
// run's schedule_id (RunID — minted by state's TriggerRebase). Unlike
// rerun, no target identity is carried — the rebase partition is computed
// deterministically from source state + latest topology. This handler:
//  1. Dedups on Redis message ID.
//  2. Calls Snapshot(RebasePartition{}) — projects the rebase partition:
//     non-SUCCEEDED source tasks (+ descendants in latest topology + new
//     arrivals) flipped to PENDING; SUCCEEDED tasks inherited at source's
//     pinned (image_tag, manifest_version).
//  3. On ErrEmptyProjection: emits run.entries.dispatch_failed:v1, marks
//     completed, commits. The new run goes to FAILED via state's
//     dispatch_failed handler.
//  4. On success: emits ONE run.entries.dispatched:v1 covering the full
//     projection (per-task Status: "pending" for rebased, "succeeded" for
//     inherited; InheritedFromTaskID populated for inherited rows), then
//     N × query.model:v1 for the PENDING (rebased) rows only.
type HandleRebaseHandler struct {
	uow     uow.UnitOfWork
	runRepo run.Repository
	logger  *slog.Logger
}

// NewHandleRebaseHandler creates a new HandleRebaseHandler.
func NewHandleRebaseHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	logger *slog.Logger,
) *HandleRebaseHandler {
	return &HandleRebaseHandler{
		uow:     u,
		runRepo: runRepo,
		logger:  logger,
	}
}

// Handle processes a RebaseInput derived from a trigger.rebase:v1 message.
func (h *HandleRebaseHandler) Handle(ctx context.Context, cmd domainModel.RebaseInput, messageID string) error {
	h.logger.Info("Processing rebase",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"source_run_id", cmd.SourceRunID,
	)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
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

	sourceRunUUID, err := uuid.Parse(cmd.SourceRunID)
	if err != nil {
		return fmt.Errorf("invalid source_run_id %q: %w", cmd.SourceRunID, err)
	}
	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	projection, snapErr := h.runRepo.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "rebase",
		SourceRunID:  &sourceRunUUID,
		Selector:     snapshot.RebasePartition{},
	})
	if snapErr != nil {
		if errors.Is(snapErr, snapshot.ErrEmptyProjection) {
			if ferr := h.emitRebaseDispatchFailed(ctx, cmd, msgProcessingID, "rebase_yielded_empty_projection"); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed after empty projection: %w", cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot rebase: %w", snapErr)
	}

	// Build run.entries.dispatched:v1 covering ALL projection rows.
	// Per-task Status drives state's task_tracker.status: "pending" for rebased,
	// "succeeded" for inherited (auto-rollup eligible if every row is terminal).
	allTasks := make([]pkgEvents.DispatchedTask, 0, len(projection))
	queryModelTasks := make([]snapshot.TaskProjection, 0)
	for _, t := range projection {
		statusLower := "pending"
		if t.InitialStatus == "SUCCEEDED" {
			statusLower = "succeeded"
		}
		inheritedStr := ""
		if t.InheritedFromTaskID != nil {
			inheritedStr = t.InheritedFromTaskID.String()
		}
		allTasks = append(allTasks, pkgEvents.DispatchedTask{
			TaskID:              t.TaskID.String(),
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			NodeType:            t.NodeType,
			MaxRetries:          t.MaxRetries,
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			Status:              statusLower,
			InheritedFromTaskID: inheritedStr,
		})
		// Only PENDING (rebased) rows need dispatch.
		if t.InitialStatus == "PENDING" {
			queryModelTasks = append(queryModelTasks, t)
		}
	}

	dispatchedEvt := pkgEvents.RunEntriesDispatched{
		ScheduleID:     cmd.RunID,
		ScheduleName:   cmd.ScheduleName,
		AllTasks:       allTasks,
		TotalTaskCount: int32(len(allTasks)),
	}
	dispatchedPayload, err := json.Marshal(dispatchedEvt)
	if err != nil {
		return fmt.Errorf("marshal run.entries.dispatched: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatched",
		Payload:             dispatchedPayload,
		StreamName:          "run.entries.dispatched:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatched to outbox: %w", err)
	}

	// Per-rebased-task query.model:v1 dispatch.
	for _, t := range queryModelTasks {
		jobName, err := pkgDomain.ComputeJobName(t.ServiceName, t.SchemaName, t.TableName, cmd.RunID)
		if err != nil {
			return fmt.Errorf("compute job name for %s.%s: %w", t.SchemaName, t.TableName, err)
		}
		queryEvt := domain.NodeReadyForExecution{
			ScheduleID:      cmd.RunID,
			ScheduleName:    cmd.ScheduleName,
			ServiceName:     t.ServiceName,
			SchemaName:      t.SchemaName,
			TableName:       t.TableName,
			TaskID:          t.TaskID.String(),
			JobName:         jobName,
			NodeType:        t.NodeType,
			ManifestVersion: t.ManifestVersion,
			ImageTag:        t.ImageTag,
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
			return fmt.Errorf("write query.model for %s.%s: %w", t.SchemaName, t.TableName, err)
		}
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	h.logger.Info("Rebase processing finished",
		"run_id", cmd.RunID,
		"source_run_id", cmd.SourceRunID,
		"total_tasks", len(allTasks),
		"dispatched_tasks", len(queryModelTasks),
	)
	return nil
}

// dedup mirrors handle_rerun.dedup, scoped to trigger.rebase:v1.
func (h *HandleRebaseHandler) dedup(
	ctx context.Context,
	messageID string,
	payload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "trigger.rebase:v1",
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
				"message_id", messageID, "state", existing.State)
			return existing.ID, true, nil
		}
		h.logger.Warn("Message being processed by another instance",
			"message_id", messageID)
		return existing.ID, true, nil
	}
	return id, false, nil
}

// emitRebaseDispatchFailed writes a run.entries.dispatch_failed:v1 outbox entry.
// Mirrors handle_rerun.emitRerunDispatchFailed.
func (h *HandleRebaseHandler) emitRebaseDispatchFailed(
	ctx context.Context,
	cmd domainModel.RebaseInput,
	msgProcessingID uuid.UUID,
	reason string,
) error {
	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}
	evt := pkgEvents.RunEntriesDispatchFailed{
		ScheduleID:   cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Reason:       reason,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &domain.OutboxEntry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           "run_entries_dispatch_failed",
		Payload:             payload,
		StreamName:          "run.entries.dispatch_failed:v1",
		Status:              "pending",
		MaxRetries:          3,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatch_failed to outbox: %w", err)
	}
	h.logger.Info("Emitted run.entries.dispatch_failed:v1",
		"run_id", cmd.RunID,
		"reason", reason,
	)
	return nil
}
