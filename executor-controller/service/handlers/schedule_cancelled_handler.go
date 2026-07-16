// executor-controller/service/handlers/schedule_cancelled_handler.go
package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// cancelReason is the error message stamped on a deployment cancelled because
// its schedule was.
const cancelReason = "schedule cancelled"

// ScheduleCancelledHandler settles a cancelled schedule inside the executor.
//
// It inserts a row into cancelled_schedules so the deploy bindings drop
// subsequent query.model / retry.task messages whose schedule_id matches, and
// cancels the deployments the schedule already has in flight. Cancelling them is
// what returns their execution slots: a deployment holds its slot until a
// transition releases it, and a cancelled schedule's Kubernetes Job result is
// absorbed without ever reporting the Job terminal, so no later event would free
// them. The insert is idempotent (UPSERT semantics) and Cancel only ever runs on
// rows that are still non-terminal, so a redelivery is a no-op.
//
// It takes the schedule's lock before either step, because the scan alone would
// not reach a deployment being enqueued concurrently: an enqueue whose guard has
// read 'not cancelled' but has not yet committed its row is invisible here, and
// that row would survive the cancellation, dispatch, and have its terminal
// absorbed — holding its execution slot with nothing left to release it. Taking
// the same lock the enqueue guard holds orders the two, so every deployment of
// the schedule is either cancelled here or never created.
type ScheduleCancelledHandler struct {
	logger *slog.Logger
}

// NewScheduleCancelledHandler constructs the handler.
func NewScheduleCancelledHandler(logger *slog.Logger) *ScheduleCancelledHandler {
	return &ScheduleCancelledHandler{logger: logger}
}

// Handle records the cancelled schedule and cancels its in-flight deployments.
// The msgProcID parameter is accepted to keep handler signatures uniform across
// streams but is not used here (no dedup row exists for schedule.cancelled).
func (h *ScheduleCancelledHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ScheduleCancelled,
	_ uuid.UUID,
) error {
	cancelledRepo := u.CancelledSchedulesRepo()
	if err := cancelledRepo.LockSchedule(ctx, evt.ScheduleID); err != nil {
		return fmt.Errorf("lock schedule %s: %w", evt.ScheduleID, err)
	}
	if err := cancelledRepo.Insert(ctx, evt.ScheduleID); err != nil {
		return fmt.Errorf("record cancelled schedule %s: %w", evt.ScheduleID, err)
	}

	repo := u.DeploymentsRepo()
	deps, err := repo.GetNonTerminalByScheduleForUpdate(ctx, evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("load in-flight deployments of schedule %s: %w", evt.ScheduleID, err)
	}

	now := time.Now()
	for _, dep := range deps {
		if err := dep.Cancel(cancelReason, now); err != nil {
			return fmt.Errorf("cancel deployment %s: %w", dep.ID(), err)
		}
		if err := repo.Save(ctx, dep); err != nil {
			return fmt.Errorf("save cancelled deployment %s: %w", dep.ID(), err)
		}
	}

	if len(deps) > 0 {
		h.logger.Info("Cancelled a schedule's in-flight deployments and released their execution slots",
			"schedule_id", evt.ScheduleID, "deployments", len(deps))
	}
	return nil
}
