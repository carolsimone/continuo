package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// Handle processes a raw payload string from a task.status.updated:v1 Redis message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *TaskStatusUpdatedHandler) Handle(ctx context.Context, messageID, payloadStr string) (shouldACK bool, err error) {
	if payloadStr == "" {
		h.logger.Error("task.status.updated: missing payload field — discarding")
		return true, nil
	}

	var evt events.TaskStatusUpdated
	if err := json.Unmarshal([]byte(payloadStr), &evt); err != nil {
		h.logger.Error("task.status.updated: invalid JSON payload — discarding", "error", err)
		return true, nil
	}

	taskID, err := uuid.Parse(evt.TaskID)
	if err != nil {
		h.logger.Error("task.status.updated: invalid task_id UUID — discarding", "error", err)
		return true, nil
	}

	scheduleID, err := uuid.Parse(evt.ScheduleID)
	if err != nil {
		h.logger.Error("task.status.updated: invalid schedule_id UUID — discarding", "error", err)
		return true, nil
	}

	// Normalize status to lowercase to match TaskStatus constants ("succeeded", "failed", "running").
	normalizedStatus := strings.ToLower(evt.Status)

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

	// Update the task status only when something actually changed.
	affected, txErr := h.taskRepo.UpdateStatusIfChangedTx(ctx, tx, taskID, normalizedStatus, evt.RetryCount)
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
	isTerminal := normalizedStatus == "succeeded" || normalizedStatus == "failed"
	if !isTerminal {
		// Non-terminal update (e.g. RUNNING) — just record dedup and commit.
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
	if terminal == total {
		anyFailed, txErr = h.taskRepo.HasFailedTaskTx(ctx, tx, scheduleID)
		if txErr != nil {
			return false, fmt.Errorf("has failed task check: %w", txErr)
		}
	}

	outcome := finalization.Decide(terminal, total, anyFailed, initCompleted, string(scheduler.Status))
	if outcome != "" {
		if txErr := h.schedulerRepo.UpdateStatusTx(ctx, tx, scheduleID, outcome); txErr != nil {
			return false, fmt.Errorf("update scheduler status to %s: %w", outcome, txErr)
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
