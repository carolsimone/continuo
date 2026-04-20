package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RunRerunDispatchedHandler processes run.rerun.dispatched:v1 events.
// On success it resets the listed tasks to PENDING, decrements terminal_task_count,
// and transitions the run back to RUNNING.
type RunRerunDispatchedHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	logger        *slog.Logger
}

// NewRunRerunDispatchedHandler creates a RunRerunDispatchedHandler.
func NewRunRerunDispatchedHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	logger *slog.Logger,
) *RunRerunDispatchedHandler {
	return &RunRerunDispatchedHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		logger:        logger,
	}
}

// Handle processes a raw payload string from a run.rerun.dispatched:v1 Redis message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *RunRerunDispatchedHandler) Handle(ctx context.Context, messageID, payloadStr string) (shouldACK bool, err error) {
	if payloadStr == "" {
		h.logger.Error("run.rerun.dispatched: missing payload field — discarding")
		return true, nil
	}

	var evt events.RunRerunDispatched
	if err := json.Unmarshal([]byte(payloadStr), &evt); err != nil {
		h.logger.Error("run.rerun.dispatched: invalid JSON payload — discarding", "error", err)
		return true, nil
	}

	scheduleID, err := uuid.Parse(evt.ScheduleID)
	if err != nil {
		h.logger.Error("run.rerun.dispatched: invalid schedule_id UUID — discarding", "error", err)
		return true, nil
	}

	taskUUIDs := make([]uuid.UUID, 0, len(evt.TasksToReset))
	for _, taskIDStr := range evt.TasksToReset {
		taskID, parseErr := uuid.Parse(taskIDStr)
		if parseErr != nil {
			h.logger.Error("run.rerun.dispatched: invalid task_id UUID — discarding message",
				"task_id", taskIDStr, "error", parseErr)
			return true, nil
		}
		taskUUIDs = append(taskUUIDs, taskID)
	}

	tx, err := h.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Deduplication via processed_events (keyed on a deterministic UUID derived from
	// the Redis stream message ID so we don't require UUID format from Redis).
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.rerun.dispatched:"+messageID))
	var already bool
	if dbErr := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		dedupID,
	).Scan(&already); dbErr != nil {
		return false, fmt.Errorf("dedup check: %w", dbErr)
	}
	if already {
		h.logger.Info("run.rerun.dispatched: duplicate message — skipping", "message_id", messageID)
		_ = tx.Commit()
		return true, nil
	}

	// Reset tasks to PENDING and get the count of actually modified rows.
	resetCount, txErr := h.taskRepo.ResetTasksTx(ctx, tx, taskUUIDs)
	if txErr != nil {
		return false, fmt.Errorf("reset tasks: %w", txErr)
	}

	// Decrement terminal_task_count only for rows that were actually changed.
	if resetCount > 0 {
		if txErr := h.schedulerRepo.DecrementTerminalCountTx(ctx, tx, scheduleID, resetCount); txErr != nil {
			return false, fmt.Errorf("decrement terminal count: %w", txErr)
		}
	}

	// Revive the run to RUNNING.
	if txErr := h.schedulerRepo.UpdateStatusTx(ctx, tx, scheduleID, string(model.SchedulerStatusRunning)); txErr != nil {
		return false, fmt.Errorf("update status to running: %w", txErr)
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

	h.logger.Info("run.rerun.dispatched: processed",
		"schedule_id", scheduleID,
		"tasks_reset", resetCount,
		"entry_task_id", evt.EntryTaskID,
	)
	return true, nil
}
