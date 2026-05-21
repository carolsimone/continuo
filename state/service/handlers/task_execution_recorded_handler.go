package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TaskExecutionRecordedHandler persists a task_execution row for each
// task.execution.recorded:v1 event.
type TaskExecutionRecordedHandler struct {
	logger *slog.Logger
}

// NewTaskExecutionRecordedHandler constructs the handler.
func NewTaskExecutionRecordedHandler(logger *slog.Logger) *TaskExecutionRecordedHandler {
	return &TaskExecutionRecordedHandler{logger: logger}
}

// Handle inserts a TaskExecution row inside the caller-managed transaction.
// The msgProcID parameter is unused — this handler writes no outbox entries
// and therefore has no need to thread message_processing provenance through
// to a child row. It is accepted to keep a uniform Handle signature across
// every state stream handler.
//
// Caller contract: u.Begin(ctx) has been invoked; the handler MUST NOT call
// Commit or Rollback. The binding owns transaction lifecycle.
func (h *TaskExecutionRecordedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.TaskExecutionRecorded,
	_ uuid.UUID,
) error {
	if err := u.TaskExecutions().CreateRecordTx(ctx, u.Tx(), evt); err != nil {
		return fmt.Errorf("create task_execution: %w", err)
	}
	h.logger.Info("task.execution.recorded: processed",
		"execution_id", evt.ExecutionID,
		"task_id", evt.TaskID,
	)
	return nil
}
