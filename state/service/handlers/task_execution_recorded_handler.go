package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TaskExecutionRecordedHandler applies each task.execution.recorded:v1 event to
// the attempt of the task it describes.
type TaskExecutionRecordedHandler struct {
	logger *slog.Logger
}

// NewTaskExecutionRecordedHandler constructs the handler.
func NewTaskExecutionRecordedHandler(logger *slog.Logger) *TaskExecutionRecordedHandler {
	return &TaskExecutionRecordedHandler{logger: logger}
}

// Handle records the execution on the attempt its Job name names, through the
// run's tasks, inside the caller-managed transaction. run.ErrTaskNotHeld
// propagates as it is; the stream binding marks it permanent. The msgProcID
// parameter is unused — this handler writes no outbox entries and therefore
// has no need to thread message_processing provenance through to a child row.
// It is accepted to keep a uniform Handle signature across every state stream
// handler.
//
// Caller contract: u.Begin(ctx) has been invoked; the handler MUST NOT call
// Commit or Rollback. The binding owns transaction lifecycle.
func (h *TaskExecutionRecordedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.TaskExecutionRecorded,
	_ uuid.UUID,
) error {
	if err := run.RecordTaskExecution(ctx, u.TaskCollection(), evt.Record()); err != nil {
		return fmt.Errorf("record execution of %s: %w", evt.JobName, err)
	}
	h.logger.Info("task.execution.recorded: processed",
		"execution_id", evt.ExecutionID,
		"task_id", evt.TaskID,
		"job_name", evt.JobName,
	)
	return nil
}
