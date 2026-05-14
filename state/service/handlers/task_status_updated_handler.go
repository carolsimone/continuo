package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/finalization"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// TaskStatusUpdatedHandler processes task.status.updated:v1 events.
//
// Behaviour:
//   - Row-locks the scheduler_tracker row to serialize concurrent
//     finalization decisions.
//   - No-op when the scheduler is already terminal (cancelled/failed/
//     succeeded): late retry events must not regress the row.
//   - Updates task_tracker.status when it actually differs; replays (status
//     unchanged) commit dedup and exit. A missing task_tracker row is
//     surfaced as a transient error so the binding rolls back and the
//     StreamConsumer redelivers once run.entries.dispatched:v1 has caught up.
//   - On non-terminal transitions away from a previously-terminal status
//     (FAILED→RUNNING on k8s retry) decrements terminal_task_count so the
//     counter stays accurate across retries.
//   - On terminal transitions increments terminal_task_count and — when
//     terminal == total AND no failed task is still retryable — finalizes
//     the run via finalization.Decide and writes a run.finalized:v1 outbox
//     entry carrying the originating message_processing_id.
//
// The handler is pure orchestration: it takes a UnitOfWork and a typed
// event, and never parses JSON, manages a transaction, or runs dedup
// itself — those responsibilities live in the adapter binding that wires
// parser, dedup, and UoW lifecycle around this call.
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
// rollback. A transient error (e.g. task_tracker row not yet created) is
// returned as a plain error — the binding rolls back so the message stays
// in PEL and the consumer's reclaim loop retries it later. msgProcID is
// stored on the outbox entry (finalize branch) for provenance back to the
// inbound message.
func (h *TaskStatusUpdatedHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.TaskStatusUpdated,
	msgProcID uuid.UUID,
) error {
	scheduler, err := u.SchedulerRepo().GetByIDForUpdateTx(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("get scheduler for update: %w", err)
	}

	// Stale events arriving after the run has finalised must not mutate
	// task_tracker or terminal_task_count. Without this guard, a delayed
	// FAILED→RUNNING retry would drive terminal_task_count below
	// total_task_count after finalize, breaking the B1 invariant
	// (terminal == total after finalize).
	if isTerminalSchedulerStatus(scheduler.Status) {
		h.logger.Info("task.status.updated: scheduler already terminal — skipping",
			"schedule_id", evt.ScheduleID, "scheduler_status", scheduler.Status,
			"task_id", evt.TaskID, "incoming_status", evt.Status)
		return nil
	}

	// Capture previous status so we can detect bidirectional terminal
	// transitions (e.g. FAILED→RUNNING on k8s retry must decrement the
	// terminal counter). GetStatusTx returns "" when the row is missing,
	// matching the legacy inline behaviour.
	prevTaskStatus, err := u.TaskRepo().GetStatusTx(ctx, u.Tx(), evt.TaskID)
	if err != nil {
		// Preserve legacy lenient behaviour: silently treat read failure as
		// "no previous status". The UpdateStatusIfChangedTx + ExistsTx pair
		// below distinguishes missing-row from no-op-update.
		prevTaskStatus = ""
	}

	affected, err := u.TaskRepo().UpdateStatusIfChangedTx(ctx, u.Tx(), evt.TaskID, string(evt.Status), evt.RetryCount)
	if err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	if affected == 0 {
		exists, err := u.TaskRepo().ExistsTx(ctx, u.Tx(), evt.TaskID)
		if err != nil {
			return fmt.Errorf("exists check: %w", err)
		}
		if !exists {
			// Transient: task row not yet projected. Plain error → binding
			// rolls back and the StreamConsumer reclaim loop retries.
			return fmt.Errorf("task_tracker row not found for task_id %s — will retry", evt.TaskID)
		}
		// Row exists but status is unchanged (replay). Commit dedup + exit.
		h.logger.Info("task.status.updated: status unchanged (replay) — skipping increment",
			"task_id", evt.TaskID, "status", evt.Status)
		return nil
	}

	isTerminal := evt.Status == model.TaskStatusSucceeded ||
		evt.Status == model.TaskStatusFailed ||
		evt.Status == model.TaskStatusSkipped
	prevWasTerminal := prevTaskStatus == string(model.TaskStatusSucceeded) ||
		prevTaskStatus == string(model.TaskStatusFailed) ||
		prevTaskStatus == string(model.TaskStatusSkipped)

	if !isTerminal {
		// Non-terminal update (e.g. RUNNING on retry). If the task was
		// previously terminal, decrement the counter so it stays accurate
		// across k8s retries (FAILED→RUNNING un-fills the slot).
		if prevWasTerminal {
			if err := u.SchedulerRepo().DecrementTerminalCountTx(ctx, u.Tx(), evt.ScheduleID, 1); err != nil {
				return fmt.Errorf("decrement terminal count on retry: %w", err)
			}
			h.logger.Info("task.status.updated: retry detected — decremented terminal count",
				"task_id", evt.TaskID, "prev_status", prevTaskStatus, "new_status", evt.Status)
		}
		h.logger.Info("task.status.updated: non-terminal update recorded",
			"task_id", evt.TaskID, "status", evt.Status)
		return nil
	}

	// Atomically increment terminal_task_count and read back (terminal, total).
	terminal, total, err := u.SchedulerRepo().IncrementTerminalCountTx(ctx, u.Tx(), evt.ScheduleID)
	if err != nil {
		return fmt.Errorf("increment terminal count: %w", err)
	}

	initCompleted := scheduler.InitializationStatus == "completed"
	var anyFailed, skipFinalize bool
	if terminal == total {
		// If any failed task still has retries left, the RUNNING retry
		// event will decrement terminal_count later — don't finalize yet.
		hasRetryable, err := u.TaskRepo().HasRetryableFailedTaskTx(ctx, u.Tx(), evt.ScheduleID)
		if err != nil {
			return fmt.Errorf("has retryable failed task check: %w", err)
		}
		if hasRetryable {
			skipFinalize = true
		} else {
			anyFailed, err = u.TaskRepo().HasFailedTaskTx(ctx, u.Tx(), evt.ScheduleID)
			if err != nil {
				return fmt.Errorf("has failed task check: %w", err)
			}
		}
	}

	var outcome string
	if !skipFinalize {
		outcome = finalization.Decide(terminal, total, anyFailed, initCompleted, string(scheduler.Status))
	}
	if outcome != "" {
		if err := u.SchedulerRepo().FinalizeRunTx(ctx, u.Tx(), evt.ScheduleID, outcome); err != nil {
			return fmt.Errorf("finalize scheduler status to %s: %w", outcome, err)
		}
		if err := emitRunFinalized(ctx, u, evt.ScheduleID, scheduler.ScheduleName, outcome, msgProcID); err != nil {
			return fmt.Errorf("create outbox entry for run.finalized: %w", err)
		}
		h.logger.Info("task.status.updated: run finalized",
			"schedule_id", evt.ScheduleID, "outcome", outcome,
			"terminal", terminal, "total", total)
	}

	h.logger.Info("task.status.updated: processed",
		"task_id", evt.TaskID, "schedule_id", evt.ScheduleID,
		"status", evt.Status, "terminal", terminal, "total", total)
	return nil
}
