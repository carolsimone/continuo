package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/internal/finalization"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskStatusUpdatedHandler processes task.status.updated:v1 events.
// On success it updates the task_tracker row, increments terminal_task_count when the
// status is terminal, and — when all tasks have reached a terminal state — finalizes
// the run by updating scheduler_tracker.status and writing a run.finalized:v1 outbox entry.
type TaskStatusUpdatedHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewTaskStatusUpdatedHandler creates a TaskStatusUpdatedHandler.
func NewTaskStatusUpdatedHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *TaskStatusUpdatedHandler {
	return &TaskStatusUpdatedHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		outboxRepo:    outboxRepo,
		logger:        logger,
	}
}

// Handle processes flat-field values from a task.status.updated:v1 Redis stream message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *TaskStatusUpdatedHandler) Handle(ctx context.Context, messageID string, values map[string]interface{}) (shouldACK bool, err error) {
	taskIDStr, _ := values["task_id"].(string)
	scheduleIDStr, _ := values["schedule_id"].(string)
	statusStr, _ := values["status"].(string)
	retryCountStr, _ := values["retry_count"].(string)

	if taskIDStr == "" || scheduleIDStr == "" || statusStr == "" {
		h.logger.Error("task.status.updated: missing required fields — discarding",
			"task_id", taskIDStr, "schedule_id", scheduleIDStr, "status", statusStr)
		return true, nil
	}

	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		h.logger.Error("task.status.updated: invalid task_id UUID — discarding", "error", err)
		return true, nil
	}

	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		h.logger.Error("task.status.updated: invalid schedule_id UUID — discarding", "error", err)
		return true, nil
	}

	var retryCount int32
	if retryCountStr != "" {
		n, parseErr := strconv.ParseInt(retryCountStr, 10, 32)
		if parseErr == nil {
			retryCount = int32(n)
		}
	}

	// Normalize status to lowercase to match TaskStatus constants ("succeeded", "failed", "running").
	normalizedStatus := strings.ToLower(statusStr)

	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Deduplication via processed_events.
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("task.status.updated:"+messageID))
	var already bool
	if dbErr := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		dedupID,
	).Scan(&already); dbErr != nil {
		return false, fmt.Errorf("dedup check: %w", dbErr)
	}
	if already {
		h.logger.Info("task.status.updated: duplicate message — skipping", "message_id", messageID)
		_ = tx.Commit()
		return true, nil
	}

	// Row-lock the scheduler row to serialize concurrent finalization decisions.
	scheduler, txErr := h.schedulerRepo.GetByIDForUpdateTx(ctx, tx, scheduleID)
	if txErr != nil {
		return false, fmt.Errorf("get scheduler for update: %w", txErr)
	}

	// Capture the previous task status before updating so we can track bidirectional
	// terminal transitions (e.g. FAILED→RUNNING on retry must decrement terminal_count).
	var prevTaskStatus string
	_ = tx.QueryRowContext(ctx,
		`SELECT COALESCE(status, '') FROM task_tracker WHERE task_id = $1`,
		taskID,
	).Scan(&prevTaskStatus)

	// Update the task status only when something actually changed.
	affected, txErr := h.taskRepo.UpdateStatusIfChangedTx(ctx, tx, taskID, normalizedStatus, retryCount)
	if txErr != nil {
		return false, fmt.Errorf("update task status: %w", txErr)
	}
	if affected == 0 {
		// Either the task row does not exist, or status+retry_count are already at the target.
		exists, txErr := h.taskRepo.ExistsTx(ctx, tx, taskID)
		if txErr != nil {
			return false, fmt.Errorf("exists check: %w", txErr)
		}
		if !exists {
			// The task row has not been created yet — return an error so the message is
			// redelivered and retried once the run.entries.dispatched:v1 consumer has caught up.
			return false, fmt.Errorf("task_tracker row not found for task_id %s — will retry", taskID)
		}
		// Row exists but status is unchanged (replay). Record dedup and commit.
		if _, dbErr := tx.ExecContext(ctx,
			`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
			dedupID,
		); dbErr != nil {
			return false, fmt.Errorf("record processed event (replay): %w", dbErr)
		}
		if txErr := tx.Commit(); txErr != nil {
			return false, fmt.Errorf("commit tx (replay): %w", txErr)
		}
		h.logger.Info("task.status.updated: status unchanged (replay) — skipping increment",
			"task_id", taskID, "status", normalizedStatus)
		return true, nil
	}

	// Only terminal statuses count towards run finalization.
	isTerminal := normalizedStatus == "succeeded" || normalizedStatus == "failed" || normalizedStatus == "skipped"
	prevWasTerminal := prevTaskStatus == "succeeded" || prevTaskStatus == "failed" || prevTaskStatus == "skipped"

	if !isTerminal {
		// Non-terminal update (e.g. RUNNING on retry).
		// If the task was previously terminal, decrement the counter so terminal_count
		// stays accurate across k8s retries (FAILED→RUNNING must un-fill the slot).
		if prevWasTerminal {
			if txErr := h.schedulerRepo.DecrementTerminalCountTx(ctx, tx, scheduleID, 1); txErr != nil {
				return false, fmt.Errorf("decrement terminal count on retry: %w", txErr)
			}
			h.logger.Info("task.status.updated: retry detected — decremented terminal count",
				"task_id", taskID, "prev_status", prevTaskStatus, "new_status", normalizedStatus)
		}
		if _, dbErr := tx.ExecContext(ctx,
			`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
			dedupID,
		); dbErr != nil {
			return false, fmt.Errorf("record processed event: %w", dbErr)
		}
		if txErr := tx.Commit(); txErr != nil {
			return false, fmt.Errorf("commit tx: %w", txErr)
		}
		h.logger.Info("task.status.updated: non-terminal update recorded",
			"task_id", taskID, "status", normalizedStatus)
		return true, nil
	}

	// Atomically increment terminal_task_count and read back (terminal, total).
	terminal, total, txErr := h.schedulerRepo.IncrementTerminalCountTx(ctx, tx, scheduleID)
	if txErr != nil {
		return false, fmt.Errorf("increment terminal count: %w", txErr)
	}

	// Decide whether to finalize the run.
	initCompleted := scheduler.InitializationStatus == "completed"
	var anyFailed bool
	// anyFailed stays false when skipFinalize=true; finalization.Decide is skipped in that case.
	var skipFinalize bool
	if terminal == total {
		// If any failed task still has retries left, the RUNNING retry event will
		// decrement terminal_count later — don't finalize yet.
		hasRetryable, txErr := h.taskRepo.HasRetryableFailedTaskTx(ctx, tx, scheduleID)
		if txErr != nil {
			return false, fmt.Errorf("has retryable failed task check: %w", txErr)
		}
		if hasRetryable {
			skipFinalize = true
		} else {
			anyFailed, txErr = h.taskRepo.HasFailedTaskTx(ctx, tx, scheduleID)
			if txErr != nil {
				return false, fmt.Errorf("has failed task check: %w", txErr)
			}
		}
	}

	var outcome string
	if !skipFinalize {
		outcome = finalization.Decide(terminal, total, anyFailed, initCompleted, string(scheduler.Status))
	}
	if outcome != "" {
		if txErr := h.schedulerRepo.FinalizeRunTx(ctx, tx, scheduleID, outcome); txErr != nil {
			return false, fmt.Errorf("finalize scheduler status to %s: %w", outcome, txErr)
		}

		finalizedEvt := events.RunFinalized{
			ScheduleID:   scheduleID.String(),
			ScheduleName: scheduler.ScheduleName,
			Status:       outcome,
		}
		payload, marshalErr := json.Marshal(finalizedEvt)
		if marshalErr != nil {
			return false, fmt.Errorf("marshal run.finalized payload: %w", marshalErr)
		}

		outboxEntry := &postgres.OutboxEntry{
			ID:            uuid.New(),
			AggregateType: "scheduler_tracker",
			AggregateID:   scheduleID,
			EventType:     "run.finalized:v1",
			Payload:       payload,
			StreamName:    "run.finalized:v1",
			Status:        "pending",
			MaxRetries:    5,
			RetryCount:    0,
			CreatedAt:     time.Now(),
		}
		if txErr := h.outboxRepo.Create(ctx, tx, outboxEntry); txErr != nil {
			return false, fmt.Errorf("create outbox entry for run.finalized: %w", txErr)
		}

		h.logger.Info("task.status.updated: run finalized",
			"schedule_id", scheduleID,
			"outcome", outcome,
			"terminal", terminal,
			"total", total,
		)
	}

	// Record processed message for dedup.
	if _, dbErr := tx.ExecContext(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		dedupID,
	); dbErr != nil {
		return false, fmt.Errorf("record processed event: %w", dbErr)
	}

	if txErr := tx.Commit(); txErr != nil {
		return false, fmt.Errorf("commit tx: %w", txErr)
	}

	h.logger.Info("task.status.updated: processed",
		"task_id", taskID,
		"schedule_id", scheduleID,
		"status", normalizedStatus,
		"terminal", terminal,
		"total", total,
	)
	return true, nil
}
