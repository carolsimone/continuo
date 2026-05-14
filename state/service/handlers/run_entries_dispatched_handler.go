package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// RunEntriesDispatchedHandler processes run.entries.dispatched:v1 events.
// On success it bulk-creates task_tracker rows, sets total_task_count and
// terminal_task_count, transitions initialization_status to "completed",
// and either auto-rolls-up the run to a terminal state (when every projected
// task is already terminal at creation) or transitions the scheduler to
// running. Auto-rollup also emits a run.finalized:v1 outbox entry carrying
// the originating message_processing_id.
//
// The handler is pure orchestration: it takes a UnitOfWork and a typed event,
// and never parses JSON, manages a transaction, or runs dedup itself — those
// responsibilities live in the adapter binding that wires the parser, dedup,
// and UoW lifecycle around this call.
type RunEntriesDispatchedHandler struct {
	logger *slog.Logger
}

// NewRunEntriesDispatchedHandler constructs the handler.
func NewRunEntriesDispatchedHandler(logger *slog.Logger) *RunEntriesDispatchedHandler {
	return &RunEntriesDispatchedHandler{logger: logger}
}

// Handle orchestrates the dispatched flow.
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers rollback.
// msgProcID is stored on the outbox entry (auto-rollup branch) for provenance
// back to the inbound message.
func (h *RunEntriesDispatchedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.RunEntriesDispatched,
	msgProcID uuid.UUID,
) error {
	scheduler, err := u.SchedulerRepo().GetByIDForUpdateTx(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("lock scheduler row: %w", err)
	}
	if isTerminalSchedulerStatus(scheduler.Status) {
		h.logger.Info("run.entries.dispatched: scheduler already terminal — skipping",
			"schedule_id", evt.ScheduleID, "status", scheduler.Status)
		return nil
	}

	now := time.Now()
	tasks := make([]*model.TaskTracker, 0, len(evt.AllTasks))
	for _, t := range evt.AllTasks {
		jobName, err := domain.ComputeJobName(t.ServiceName, t.SchemaName, t.TableName, evt.ScheduleID.String())
		if err != nil {
			return fmt.Errorf("%w: compute job_name: %v", pkgevents.ErrPermanent, err)
		}
		tasks = append(tasks, &model.TaskTracker{
			TaskID:              t.TaskID,
			ScheduleID:          evt.ScheduleID,
			CreatedAt:           now,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			JobName:             jobName,
			Status:              t.Status,
			MaxRetries:          int(t.MaxRetries),
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: t.InheritedFromTaskID,
		})
	}

	if err := u.TaskRepo().BulkCreateTx(ctx, u.Tx(), tasks); err != nil {
		return fmt.Errorf("bulk create tasks: %w", err)
	}
	if err := u.SchedulerRepo().SetTotalTaskCountTx(ctx, u.Tx(), evt.ScheduleID, evt.TotalTaskCount); err != nil {
		return fmt.Errorf("set total_task_count: %w", err)
	}
	if err := u.SchedulerRepo().UpdateInitializationStatusTx(ctx, u.Tx(), evt.ScheduleID, "completed"); err != nil {
		return fmt.Errorf("update initialization_status: %w", err)
	}

	// Auto-rollup: if every projected task is already terminal, the run is born
	// complete and transitions straight to a terminal status (with completed_at)
	// rather than going through RUNNING. Mixed-terminal (some failed/cancelled)
	// is treated as FAILED conservatively. Any PENDING/RUNNING in the projection
	// keeps the standard cron/single-node-run/rerun behaviour: go to RUNNING.
	allTerminal := true
	allSucceeded := true
	terminalCount := int32(0)
	for _, task := range tasks {
		if task.Status == model.TaskStatusPending || task.Status == model.TaskStatusRunning {
			allTerminal = false
			allSucceeded = false
			continue
		}
		terminalCount++
		if task.Status != model.TaskStatusSucceeded {
			allSucceeded = false
		}
	}

	if allTerminal {
		terminal := string(model.SchedulerStatusFailed)
		if allSucceeded {
			terminal = string(model.SchedulerStatusSucceeded)
		}
		if err := u.SchedulerRepo().FinalizeRunTx(ctx, u.Tx(), evt.ScheduleID, terminal); err != nil {
			return fmt.Errorf("finalize scheduler to %s: %w", terminal, err)
		}
		// Seed terminal_task_count for observability — FinalizeRunTx only updates
		// status + completed_at, so without this the rolled-up row carries
		// terminal_task_count=0 despite all tasks being terminal.
		if terminalCount > 0 {
			if err := u.SchedulerRepo().SetTerminalTaskCountTx(ctx, u.Tx(), evt.ScheduleID, terminalCount); err != nil {
				return fmt.Errorf("seed terminal_task_count on rollup: %w", err)
			}
		}

		// Emit run.finalized:v1 in the same tx so orchestrator finalizes the
		// Neo4j :Run. Without this, the auto-rollup run stays "active" in the
		// orchestrator's projection forever.
		payload, err := json.Marshal(pkgevents.RunFinalized{
			ScheduleID:   evt.ScheduleID.String(),
			ScheduleName: scheduler.ScheduleName,
			Status:       terminal,
		})
		if err != nil {
			return fmt.Errorf("marshal run.finalized payload: %w", err)
		}
		if err := u.OutboxRepo().Create(ctx, u.Tx(), &postgres.OutboxEntry{
			ID:                  uuid.New(),
			MessageProcessingID: &msgProcID,
			AggregateType:       "scheduler_tracker",
			AggregateID:         evt.ScheduleID,
			EventType:           "run.finalized:v1",
			Payload:             payload,
			StreamName:          "run.finalized:v1",
			Status:              "pending",
			MaxRetries:          5,
			RetryCount:          0,
			CreatedAt:           time.Now(),
		}); err != nil {
			return fmt.Errorf("create outbox entry for run.finalized: %w", err)
		}
	} else {
		// Seed terminal_task_count from inherited terminal rows so the run can
		// finalize when the remaining pending tasks reach terminal status.
		if terminalCount > 0 {
			if err := u.SchedulerRepo().SetTerminalTaskCountTx(ctx, u.Tx(), evt.ScheduleID, terminalCount); err != nil {
				return fmt.Errorf("seed terminal_task_count: %w", err)
			}
		}
		if err := u.SchedulerRepo().UpdateStatusTx(ctx, u.Tx(), evt.ScheduleID, string(model.SchedulerStatusRunning)); err != nil {
			return fmt.Errorf("update status to running: %w", err)
		}
	}

	h.logger.Info("run.entries.dispatched: processed",
		"schedule_id", evt.ScheduleID,
		"task_count", len(tasks),
		"total_task_count", evt.TotalTaskCount,
	)
	return nil
}
