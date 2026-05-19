// executor-controller/service/handlers/retry_task_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// RetryTaskHandler processes retry.task:v1 events. Same shape as
// QueryModelHandler but the outbox row carries the inbound
// task_retry_count and max_retries instead of defaulting to (0, 2).
type RetryTaskHandler struct {
	logger *slog.Logger
}

// NewRetryTaskHandler constructs the handler.
func NewRetryTaskHandler(logger *slog.Logger) *RetryTaskHandler {
	return &RetryTaskHandler{logger: logger}
}

// Handle orchestrates a single retry.task:v1 message.
func (h *RetryTaskHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RetryTask,
	_ uuid.UUID,
) error {
	cancelled, err := u.CancelledSchedulesRepo().Exists(ctx, evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled-schedule check: %w", err)
	}
	if cancelled {
		h.logger.Info("schedule cancelled — dropping retry.task message",
			"schedule_id", evt.ScheduleID, "task_id", evt.TaskID)
		return nil
	}
	return createDeploymentOutboxEntry(ctx, u, evt.QueryModel, evt.TaskRetryCount, evt.MaxRetries)
}
