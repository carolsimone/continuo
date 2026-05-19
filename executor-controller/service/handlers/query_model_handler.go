// executor-controller/service/handlers/query_model_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// QueryModelHandler processes query.model:v1 events by writing a row to
// the executor's executor_outbox table. The cancelled-schedule guard
// runs through the UoW-bound CancelledSchedulesRepo so it shares the
// same snapshot as the dedup row that the binding inserts immediately
// before the handler runs.
type QueryModelHandler struct {
	logger *slog.Logger
}

// NewQueryModelHandler constructs the handler.
func NewQueryModelHandler(logger *slog.Logger) *QueryModelHandler {
	return &QueryModelHandler{logger: logger}
}

// Handle orchestrates a single query.model:v1 message. The handler is
// pure orchestration: it takes a UnitOfWork and a typed event, and
// never parses JSON, manages a transaction, or runs dedup itself.
//
// msgProcID is the dedup row's UUID from message_processing; it is
// stored as message_processing_id on the outbox row for provenance.
func (h *QueryModelHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.QueryModel,
	_ uuid.UUID,
) error {
	cancelled, err := u.CancelledSchedulesRepo().Exists(ctx, evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("cancelled-schedule check: %w", err)
	}
	if cancelled {
		h.logger.Info("schedule cancelled — dropping query.model message",
			"schedule_id", evt.ScheduleID, "task_id", evt.TaskID)
		return nil
	}
	return createDeploymentOutboxEntry(ctx, u, evt, 0, 0)
}
