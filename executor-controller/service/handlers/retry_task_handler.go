// executor-controller/service/handlers/retry_task_handler.go
package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// RetryTaskHandler processes retry.task:v1 events. Same shape as
// QueryModelHandler but the deployment row carries the inbound
// task_retry_count and max_retries instead of defaulting to (0, 2).
type RetryTaskHandler struct {
	policy routing.Policy
	logger *slog.Logger
}

// NewRetryTaskHandler constructs the handler. policy decides the path of the
// deployments this handler enqueues; a retry that names an existing row
// re-attempts that row on the path it already has.
func NewRetryTaskHandler(policy routing.Policy, logger *slog.Logger) *RetryTaskHandler {
	return &RetryTaskHandler{policy: policy, logger: logger}
}

// Handle orchestrates a single retry.task:v1 message.
func (h *RetryTaskHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RetryTask,
	msgProcID uuid.UUID,
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
	if evt.ExecutorDeploymentID != uuid.Nil {
		return h.requeue(ctx, u, evt)
	}
	return createDeployment(ctx, u, h.policy, h.logger,
		evt.QueryModel, msgProcID, evt.TaskRetryCount, evt.MaxRetries)
}

// requeue re-attempts an existing deployment in place. A worker task's lease
// history and attempt counter live on its row, so its retry returns that row to
// the pending queue rather than enqueueing a second one that would compete with
// it for the same task. The aggregate rejects a row that is not parked awaiting
// retry, so a retry can neither resurrect terminal work nor displace a task a
// worker currently holds.
func (h *RetryTaskHandler) requeue(ctx context.Context, u uow.UnitOfWork, evt events.RetryTask) error {
	dep, err := u.DeploymentsRepo().GetByID(ctx, evt.ExecutorDeploymentID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: executor deployment %s does not exist", pkgevents.ErrPermanent, evt.ExecutorDeploymentID)
	}
	if err != nil {
		return fmt.Errorf("load executor deployment %s: %w", evt.ExecutorDeploymentID, err)
	}
	// The aggregate accepts a requeue only from retry_pending. Any other status
	// means this retry lost a race — the row is already running under a newer
	// lease, or terminal — and redelivering the message cannot change that.
	if err := dep.Requeue(evt.TaskRetryCount, evt.JobName, time.Now()); err != nil {
		return fmt.Errorf("%w: requeue executor deployment %s: %v",
			pkgevents.ErrPermanent, evt.ExecutorDeploymentID, err)
	}
	if err := u.DeploymentsRepo().Save(ctx, dep); err != nil {
		return fmt.Errorf("save requeued executor deployment %s: %w", evt.ExecutorDeploymentID, err)
	}
	h.logger.Info("requeued worker task for its next claim",
		"deployment_id", dep.ID(), "task_id", evt.TaskID,
		"task_retry_count", evt.TaskRetryCount, "attempt", dep.Attempt())
	return nil
}
