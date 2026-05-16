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

// RunEntriesDispatchedHandler processes run.entries.dispatched:v1 events.
// Loads the Run aggregate, invokes AcceptDispatch (which bulk-creates child
// tasks via the TaskCollection port, seeds counters, transitions
// init_status=completed, and either rolls up to terminal or transitions to
// RUNNING based on the projection), persists via SaveRun, and publishes
// emitted domain events via OutboxPublisher.
type RunEntriesDispatchedHandler struct {
	logger *slog.Logger
}

// NewRunEntriesDispatchedHandler constructs the handler.
func NewRunEntriesDispatchedHandler(logger *slog.Logger) *RunEntriesDispatchedHandler {
	return &RunEntriesDispatchedHandler{logger: logger}
}

// Handle orchestrates the dispatched flow.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback. msgProcID is stored on the outbox entry (auto-rollup branch) for
// provenance back to the inbound message.
func (h *RunEntriesDispatchedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RunEntriesDispatched,
	msgProcID uuid.UUID,
) error {
	r, err := u.Run().LoadRunForUpdate(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	projection := toDispatchedTasks(evt)
	domainEvents, err := r.AcceptDispatch(ctx, u.TaskCollection(), projection, u.Clock().Now())
	if err != nil {
		return fmt.Errorf("accept dispatch: %w", err)
	}
	if err := u.Run().SaveRun(ctx, u.Tx(), r); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, u.Tx(), domainEvents, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	h.logger.Info("run.entries.dispatched: processed",
		"schedule_id", evt.ScheduleID,
		"task_count", len(projection),
		"total_task_count", evt.TotalTaskCount,
	)
	return nil
}

// toDispatchedTasks converts event task entries to the aggregate's DispatchedTask projection.
func toDispatchedTasks(evt events.RunEntriesDispatched) []run.DispatchedTask {
	out := make([]run.DispatchedTask, 0, len(evt.AllTasks))
	for _, t := range evt.AllTasks {
		out = append(out, run.DispatchedTask{
			TaskID:              t.TaskID,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			Status:              t.Status,
			MaxRetries:          t.MaxRetries,
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: t.InheritedFromTaskID,
		})
	}
	return out
}
