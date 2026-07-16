// executor-controller/service/handlers/query_model_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// QueryModelHandler processes query.model:v1 events by writing a pending row to
// the executor_deployments command queue. The cancelled-schedule guard
// runs through the UoW-bound CancelledSchedulesRepo so it shares the
// same snapshot as the dedup row that the binding inserts immediately
// before the handler runs.
type QueryModelHandler struct {
	policy routing.Policy
	logger *slog.Logger
}

// NewQueryModelHandler constructs the handler. policy decides whether each
// record gets its own Kubernetes Job or waits to be claimed by a worker pool.
func NewQueryModelHandler(policy routing.Policy, logger *slog.Logger) *QueryModelHandler {
	return &QueryModelHandler{policy: policy, logger: logger}
}

// Handle orchestrates a single query.model:v1 message. The handler is
// pure orchestration: it takes a UnitOfWork and a typed event, and
// never parses JSON, manages a transaction, or runs dedup itself.
//
// msgProcID is the dedup row's UUID from message_processing; it is
// stored as message_processing_id on the deployment row for provenance.
func (h *QueryModelHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.QueryModel,
	msgProcID uuid.UUID,
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
	return createDeployment(ctx, u, h.policy, h.logger, evt, msgProcID, 0, 0)
}
