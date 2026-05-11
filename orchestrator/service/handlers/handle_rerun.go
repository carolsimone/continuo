package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleRerunHandler consumes trigger.rerun:v1 messages.
//
// The cmd carries the SOURCE run's schedule_id (SourceRunID) and the NEW
// run's schedule_id (RunID — minted by state's TriggerRerun). This handler
// dedups on Redis message ID, calls Snapshot(SourcePinnedDAG{}) which projects
// non-SUCCEEDED source tasks + their pinned-DAG descendants as PENDING and the
// rest as SUCCEEDED inherits, then emits one run.entries.dispatched:v1 + N ×
// query.model:v1 (PENDING rows only). Empty projection → run.entries.dispatch_failed:v1.
type HandleRerunHandler struct {
	uow         uow.UnitOfWork
	runRepo     run.Repository
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

func NewHandleRerunHandler(
	u uow.UnitOfWork,
	runRepo run.Repository,
	snapshotSvc SnapshotService,
	logger *slog.Logger,
) *HandleRerunHandler {
	return &HandleRerunHandler{uow: u, runRepo: runRepo, snapshotSvc: snapshotSvc, logger: logger}
}

func (h *HandleRerunHandler) Handle(ctx context.Context, cmd domainModel.RerunInput, messageID string) error {
	h.logger.Info("Processing rerun",
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

	projection, snapErr := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "rerun",
		SourceRunID:  &sourceRunUUID,
		Selector:     snapshot.SourcePinnedDAG{},
	})
	if snapErr != nil {
		if errors.Is(snapErr, snapshot.ErrEmptyProjection) {
			if ferr := h.emitRerunDispatchFailed(ctx, cmd, msgProcessingID, "rerun_yielded_empty_projection"); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed after empty projection: %w", cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot rerun: %w", snapErr)
	}

	allTasks := make([]pkgEvents.DispatchedTask, 0, len(projection))
	queryModelTasks := make([]snapshot.TaskProjection, 0)
	for _, t := range projection {
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
			Status:              projectionStatusLower(t.InitialStatus),
			InheritedFromTaskID: inheritedStr,
		})
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
		return fmt.Errorf("write run.entries.dispatched: %w", err)
	}

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

	h.logger.Info("Rerun processing finished",
		"run_id", cmd.RunID,
		"source_run_id", cmd.SourceRunID,
		"total_tasks", len(allTasks),
		"dispatched_tasks", len(queryModelTasks),
	)
	return nil
}

// projectionStatusLower maps a TaskProjection InitialStatus to its wire-format
// (lowercased) string. Inherited terminal rows (FAILED/CANCELLED/SKIPPED)
// MUST round-trip verbatim — coercing them to "pending" would create
// task_tracker rows the executor never runs, blocking run finalize.
func projectionStatusLower(initialStatus string) string {
	switch initialStatus {
	case "PENDING":
		return "pending"
	case "SUCCEEDED":
		return "succeeded"
	case "FAILED":
		return "failed"
	case "CANCELLED":
		return "cancelled"
	case "SKIPPED":
		return "skipped"
	default:
		return strings.ToLower(initialStatus)
	}
}

func (h *HandleRerunHandler) dedup(ctx context.Context, messageID string, payload []byte) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: "trigger.rerun:v1",
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
		h.logger.Warn("Message being processed by another instance", "message_id", messageID)
		return existing.ID, true, nil
	}
	return id, false, nil
}

func (h *HandleRerunHandler) emitRerunDispatchFailed(
	ctx context.Context,
	cmd domainModel.RerunInput,
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
		return fmt.Errorf("write run.entries.dispatch_failed: %w", err)
	}
	h.logger.Info("Emitted run.entries.dispatch_failed:v1", "run_id", cmd.RunID, "reason", reason)
	return nil
}
