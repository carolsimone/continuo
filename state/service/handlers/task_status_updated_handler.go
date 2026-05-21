package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TaskStatusUpdatedHandler processes task.status.updated:v1 events.
// Loads the Run aggregate, invokes RecordTaskStatus (which owns the
// finalization state machine — terminal-count increment/decrement on
// retries, defer finalize when retryable failed tasks remain, emit
// RunFinalized with the correct outcome), persists via SaveRun, publishes
// emitted events.
type TaskStatusUpdatedHandler struct {
	logger *slog.Logger
}

// NewTaskStatusUpdatedHandler constructs the handler.
func NewTaskStatusUpdatedHandler(logger *slog.Logger) *TaskStatusUpdatedHandler {
	return &TaskStatusUpdatedHandler{logger: logger}
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
	domainEvents, err := r.RecordTaskStatus(ctx, u.TaskCollection(), evt.TaskID, evt.Status, evt.RetryCount)
	if err != nil {
		return fmt.Errorf("record task status: %w", err)
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	h.logger.Info("task.status.updated: processed",
		"task_id", evt.TaskID,
		"schedule_id", evt.ScheduleID,
		"status", evt.Status,
	)
	return nil
}
