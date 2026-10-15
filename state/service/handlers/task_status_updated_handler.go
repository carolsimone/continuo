package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TaskStatusUpdatedHandler processes task.status.updated:v1 events.
// Loads the Run aggregate, invokes RecordTaskStatus (which owns the
// finalization state machine — terminal-count increment/decrement on
// retries, defer finalize when retryable failed tasks remain, emit
// RunFinalized with the correct outcome — and, on a run state planned, the
// dispatch of the tasks a success makes ready and the skip of the tasks a
// final failure blocks), persists via SaveRun, publishes emitted events. When
// the run applies the success of a task of a run state planned with an
// operation that writes the table, it also records the version the task loaded
// into the warehouse, which the seed reconcile compares with the live release.
// A success the run ignores (the run is over, the attempt was superseded, the
// task already finished that attempt) records no version either, so a stale
// delivery cannot overwrite what the applied one recorded.
type TaskStatusUpdatedHandler struct {
	tenantID string
	logger   *slog.Logger
}

// NewTaskStatusUpdatedHandler constructs the handler for the runs of
// tenantID.
func NewTaskStatusUpdatedHandler(tenantID string, logger *slog.Logger) *TaskStatusUpdatedHandler {
	return &TaskStatusUpdatedHandler{tenantID: tenantID, logger: logger}
}

// Handle orchestrates the task-status-updated flow.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback. A transient error (e.g. run.ErrTaskRowNotProjected) propagates
// so the consumer's reclaim loop retries.
func (h *TaskStatusUpdatedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.TaskStatusUpdated,
	msgProcID uuid.UUID,
) error {
	r, err := u.Run().LoadRunForUpdate(ctx, evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	domainEvents, succeededApplied, err := r.RecordTaskStatus(ctx, u.TaskCollection(), u.TaskGraph(h.tenantID), evt.TaskID, evt.Status, evt.RetryCount, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("record task status: %w", err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	if succeededApplied && recordsBuiltVersions(r) {
		if err := h.recordLoaded(ctx, u, r, evt); err != nil {
			return err
		}
	}
	h.logger.Info("task.status.updated: processed",
		"task_id", evt.TaskID,
		"schedule_id", evt.ScheduleID,
		"status", evt.Status,
	)
	return nil
}

// recordsBuiltVersions reports whether a succeeded task of r loaded its node's
// version of r's release into the warehouse: state chose r's tasks from that
// release, and r's operation writes the table. A test only reads it.
func recordsBuiltVersions(r *run.Run) bool {
	return r.PlannedAt() != nil && r.ReleaseID() != "" && r.Operation() != model.OperationTest
}

// recordLoaded records the version a succeeded task of a run that names a
// release loaded into the warehouse: its node's content hash in that release,
// the image the task ran, and the run and task. A task whose row is gone, or
// whose node the release lacks or holds without a content hash, records
// nothing.
func (h *TaskStatusUpdatedHandler) recordLoaded(ctx context.Context, u uow.UnitOfWork, r *run.Run, evt events.TaskStatusUpdated) error {
	task, err := u.TaskCollection().GetByID(ctx, evt.TaskID)
	if errors.Is(err, run.ErrTaskNotFound) {
		h.logger.Warn("task.status.updated: task row not found; no loaded version recorded",
			"task_id", evt.TaskID, "schedule_id", evt.ScheduleID, "release_id", r.ReleaseID())
		return nil
	}
	if err != nil {
		return fmt.Errorf("read task %s: %w", evt.TaskID, err)
	}
	key := topology.KeyOf(task.SchemaName, task.TableName)
	hash, found, err := u.Topology().NodeContentHash(ctx, h.tenantID, r.ReleaseID(), key)
	if err != nil {
		return fmt.Errorf("read the content hash of %s in release %s: %w", key, r.ReleaseID(), err)
	}
	if !found || hash == "" {
		h.logger.Info("task.status.updated: node has no content hash in the run's release; no loaded version recorded",
			"task_id", evt.TaskID, "node_key", key, "release_id", r.ReleaseID(), "in_release", found)
		return nil
	}
	if err := u.Materializations().RecordBuilt(ctx, h.tenantID, seeds.Built{
		Key:         key,
		ContentHash: hash,
		ImageTag:    task.ImageTag,
		ReleaseID:   r.ReleaseID(),
		RunID:       r.ScheduleID(),
		TaskID:      evt.TaskID,
		At:          u.Clock().Now(),
		Source:      seeds.SourceBuild,
	}); err != nil {
		return fmt.Errorf("record the loaded version of %s: %w", key, err)
	}
	h.logger.Info("task.status.updated: loaded version recorded",
		"task_id", evt.TaskID, "node_key", key, "release_id", r.ReleaseID(), "content_hash", hash)
	return nil
}
