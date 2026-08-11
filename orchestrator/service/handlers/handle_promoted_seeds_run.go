package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	pkgDomain "github.com/carolsimone/continuo/pkg/domain"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// HandlePromotedSeedsRunHandler consumes trigger.promoted_seeds:v1 messages and
// projects the run state created for a promoted release.
//
// state creates the run; this projects its tasks onto it. That is the same split
// every other run uses, and it is what puts a production seed build inside the
// standard lifecycle: because the run exists, the terminal announcements have
// somewhere to land, so a failed seed build is retried on its task's retry
// budget, recorded as an execution, and visible as a failed run.
type HandlePromotedSeedsRunHandler struct {
	uow         uow.UnitOfWork
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

// NewHandlePromotedSeedsRunHandler creates a HandlePromotedSeedsRunHandler.
func NewHandlePromotedSeedsRunHandler(u uow.UnitOfWork, snapshotSvc SnapshotService, logger *slog.Logger) *HandlePromotedSeedsRunHandler {
	return &HandlePromotedSeedsRunHandler{uow: u, snapshotSvc: snapshotSvc, logger: logger}
}

// Handle projects the run's tasks and dispatches them.
//
// It runs the dedup → snapshot → outbox flow:
//  1. Dedup on messageID scoped to trigger.promoted_seeds:v1.
//  2. Snapshot the run with a NodeSet selector over the seeds on the message.
//     A target missing from the topology, or an empty projection, emits
//     run.entries.dispatch_failed:v1 and completes rather than retrying.
//  3. Emit one run.entries.dispatched:v1 carrying every task, so state creates
//     the task rows and marks the run's initialization complete.
//  4. Emit one query.model:v1 per task for the executor to dispatch.
func (h *HandlePromotedSeedsRunHandler) Handle(ctx context.Context, cmd domainModel.PromotedSeedsRunInput, messageID string, outboxEntryID *uuid.UUID) error {
	h.logger.Info("Processing promoted-seeds run",
		"message_id", messageID,
		"run_id", cmd.RunID,
		"release_id", cmd.ReleaseID,
		"seed_count", len(cmd.Nodes),
	)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, h.uow.MessageProcessingRepo(), h.logger,
		messageID, streams.TriggerPromotedSeedsV1, cmdPayload, outboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	nodes := make([]snapshot.FQN, 0, len(cmd.Nodes))
	for _, n := range cmd.Nodes {
		nodes = append(nodes, snapshot.FQN{Service: n.ServiceName, Schema: n.SchemaName, Table: n.TableName})
	}

	projection, snapErr := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         "promote_seed",
		InitiatedBy:  cmd.InitiatedBy,
		Selector:     snapshot.NodeSet{Nodes: nodes},
	})
	if snapErr != nil {
		if reason, ok := dispatchFailedReason(snapErr); ok {
			if ferr := EmitDispatchFailed(ctx, h.uow, h.logger, DispatchFailedParams{
				RunID:               cmd.RunID,
				ScheduleName:        cmd.ScheduleName,
				MessageProcessingID: msgProcessingID,
				Reason:              reason,
			}); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed after %s: %w", reason, cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot promoted-seeds run: %w", snapErr)
	}

	scheduleUUID, err := uuid.Parse(cmd.RunID)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", cmd.RunID, err)
	}

	// Outbox 1: run.entries.dispatched:v1 — state creates one task row per entry,
	// sets total_task_count, and marks initialization completed, which the run's
	// finalization guard requires before the run can reach a terminal state.
	allTasks := make([]pkgEvents.DispatchedTask, 0, len(projection))
	for _, task := range projection {
		allTasks = append(allTasks, pkgEvents.DispatchedTask{
			TaskID:          task.TaskID.String(),
			ServiceName:     task.ServiceName,
			SchemaName:      task.SchemaName,
			TableName:       task.TableName,
			NodeType:        task.NodeType,
			MaxRetries:      task.MaxRetries,
			ManifestVersion: task.ManifestVersion,
			ImageTag:        task.ImageTag,
		})
	}
	dispatchedPayload, err := json.Marshal(pkgEvents.RunEntriesDispatched{
		ScheduleID:     cmd.RunID,
		ScheduleName:   cmd.ScheduleName,
		AllTasks:       allTasks,
		TotalTaskCount: int32(len(allTasks)), //nolint:gosec // bounded by the seed count of one release
	})
	if err != nil {
		return fmt.Errorf("marshal run.entries.dispatched: %w", err)
	}
	if err := h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:                  uuid.New(),
		MessageProcessingID: &msgProcessingID,
		AggregateType:       "orchestrator",
		AggregateID:         scheduleUUID,
		EventType:           domain.EventTypeRunEntriesDispatched,
		Payload:             dispatchedPayload,
		StreamName:          streams.RunEntriesDispatchedV1,
		Status:              "pending",
		MaxRetries:          pkgoutbox.DefaultMaxRetries,
	}); err != nil {
		return fmt.Errorf("write run.entries.dispatched to outbox: %w", err)
	}

	// Outbox 2..N+1: one query.model:v1 per task.
	for _, task := range projection {
		jobName, err := pkgDomain.ComputeJobName(task.ServiceName, task.SchemaName, task.TableName, cmd.RunID)
		if err != nil {
			return fmt.Errorf("compute job name for %s.%s: %w", task.SchemaName, task.TableName, err)
		}
		queryPayload, err := json.Marshal(domain.NodeReadyForExecution{
			ScheduleID:      cmd.RunID,
			ScheduleName:    cmd.ScheduleName,
			ServiceName:     task.ServiceName,
			SchemaName:      task.SchemaName,
			TableName:       task.TableName,
			TaskID:          task.TaskID.String(),
			JobName:         jobName,
			NodeType:        task.NodeType,
			ManifestVersion: task.ManifestVersion,
			ImageTag:        task.ImageTag,
		})
		if err != nil {
			return fmt.Errorf("marshal query.model for %s.%s: %w", task.SchemaName, task.TableName, err)
		}
		if err := h.uow.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
			ID:                  uuid.New(),
			MessageProcessingID: &msgProcessingID,
			AggregateType:       "orchestrator",
			AggregateID:         scheduleUUID,
			EventType:           domain.EventTypeNodeReadyForExecution,
			Payload:             queryPayload,
			StreamName:          streams.QueryModelV1,
			Status:              "pending",
			MaxRetries:          pkgoutbox.DefaultMaxRetries,
		}); err != nil {
			return fmt.Errorf("write query.model to outbox for %s.%s: %w", task.SchemaName, task.TableName, err)
		}
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := h.uow.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	h.logger.Info("Promoted-seeds run processing finished",
		"run_id", cmd.RunID,
		"release_id", cmd.ReleaseID,
		"task_count", len(projection),
	)
	return nil
}
