package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/domain"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RunEntriesDispatchedHandler processes run.entries.dispatched:v1 events.
// On success it bulk-creates task_tracker rows, sets total_task_count, transitions
// initialization_status → "completed", and transitions status → "running".
type RunEntriesDispatchedHandler struct {
	db            *sqlx.DB
	schedulerRepo postgres.SchedulerTrackerRepository
	taskRepo      postgres.TaskTrackerRepository
	logger        *slog.Logger
}

// NewRunEntriesDispatchedHandler creates a RunEntriesDispatchedHandler.
func NewRunEntriesDispatchedHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	logger *slog.Logger,
) *RunEntriesDispatchedHandler {
	return &RunEntriesDispatchedHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		logger:        logger,
	}
}

// Handle processes a raw payload string from a run.entries.dispatched:v1 Redis message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *RunEntriesDispatchedHandler) Handle(ctx context.Context, messageID, payloadStr string) (shouldACK bool, err error) {
	if payloadStr == "" {
		h.logger.Error("run.entries.dispatched: missing payload field — discarding")
		return true, nil
	}

	var evt events.RunEntriesDispatched
	if err := json.Unmarshal([]byte(payloadStr), &evt); err != nil {
		h.logger.Error("run.entries.dispatched: invalid JSON payload — discarding", "error", err)
		return true, nil
	}

	scheduleID, err := uuid.Parse(evt.ScheduleID)
	if err != nil {
		h.logger.Error("run.entries.dispatched: invalid schedule_id UUID — discarding", "error", err)
		return true, nil
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
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.entries.dispatched:"+messageID))
	var already bool
	if dbErr := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		dedupID,
	).Scan(&already); dbErr != nil {
		return false, fmt.Errorf("dedup check: %w", dbErr)
	}
	if already {
		h.logger.Info("run.entries.dispatched: duplicate message — skipping", "message_id", messageID)
		_ = tx.Commit()
		return true, nil
	}

	// Guard: row-lock the scheduler before inserting tasks so a concurrent CancelSchedule
	// cannot win the race and leave a cancelled run in "running" with fresh pending tasks.
	scheduler, dbErr := h.schedulerRepo.GetByIDForUpdateTx(ctx, tx, scheduleID)
	if dbErr != nil {
		return false, fmt.Errorf("lock scheduler row: %w", dbErr)
	}
	if scheduler.Status == model.SchedulerStatusCancelled {
		h.logger.Info("run.entries.dispatched: scheduler already cancelled — skipping",
			"schedule_id", scheduleID)
		_ = tx.Commit()
		return true, nil
	}

	// Build task rows.
	now := time.Now()
	tasks := make([]*model.TaskTracker, 0, len(evt.AllTasks))
	for _, t := range evt.AllTasks {
		taskID, parseErr := uuid.Parse(t.TaskID)
		if parseErr != nil {
			h.logger.Error("run.entries.dispatched: invalid task_id UUID — discarding message",
				"task_id", t.TaskID, "error", parseErr)
			_ = tx.Rollback()
			return true, nil
		}
		jobName, parseErr := domain.ComputeJobName(t.ServiceName, t.SchemaName, t.TableName, evt.ScheduleID)
		if parseErr != nil {
			h.logger.Error("run.entries.dispatched: invalid task names — discarding message",
				"service", t.ServiceName, "schema", t.SchemaName, "table", t.TableName, "error", parseErr)
			_ = tx.Rollback()
			return true, nil
		}
			tasks = append(tasks, &model.TaskTracker{
				TaskID:          taskID,
				ScheduleID:      scheduleID,
				CreatedAt:       now,
				ServiceName:     t.ServiceName,
				SchemaName:      t.SchemaName,
				TableName:       t.TableName,
				JobName:         jobName,
				Status:          model.TaskStatusPending,
				MaxRetries:      int(t.MaxRetries),
				ManifestVersion: t.ManifestVersion,
			})
	}

	if txErr := h.taskRepo.BulkCreateTx(ctx, tx, tasks); txErr != nil {
		return false, fmt.Errorf("bulk create tasks: %w", txErr)
	}

	if txErr := h.schedulerRepo.SetTotalTaskCountTx(ctx, tx, scheduleID, evt.TotalTaskCount); txErr != nil {
		return false, fmt.Errorf("set total_task_count: %w", txErr)
	}

	if txErr := h.schedulerRepo.UpdateInitializationStatusTx(ctx, tx, scheduleID, "completed"); txErr != nil {
		return false, fmt.Errorf("update initialization_status: %w", txErr)
	}

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

	h.logger.Info("run.entries.dispatched: processed",
		"schedule_id", scheduleID,
		"task_count", len(tasks),
		"total_task_count", evt.TotalTaskCount,
	)
	return true, nil
}
