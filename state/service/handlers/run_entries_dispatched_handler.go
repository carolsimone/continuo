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
	outboxRepo    postgres.OutboxRepository
	logger        *slog.Logger
}

// NewRunEntriesDispatchedHandler creates a RunEntriesDispatchedHandler.
func NewRunEntriesDispatchedHandler(
	db *sqlx.DB,
	schedulerRepo postgres.SchedulerTrackerRepository,
	taskRepo postgres.TaskTrackerRepository,
	outboxRepo postgres.OutboxRepository,
	logger *slog.Logger,
) *RunEntriesDispatchedHandler {
	return &RunEntriesDispatchedHandler{
		db:            db,
		schedulerRepo: schedulerRepo,
		taskRepo:      taskRepo,
		outboxRepo:    outboxRepo,
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

		// Per-task Status: rebased tasks arrive as "pending"; tasks inherited
		// from a previous successful run arrive as "succeeded" (carrying their
		// original task_id via InheritedFromTaskID). Empty defaults to "pending".
		domainStatus := model.TaskStatusPending
		if t.Status != "" {
			s, statusErr := model.ParseTaskStatus(t.Status)
			if statusErr != nil {
				h.logger.Error("run.entries.dispatched: invalid per-task status — discarding message",
					"task_id", t.TaskID, "status", t.Status, "error", statusErr)
				_ = tx.Rollback()
				return true, nil
			}
			domainStatus = s
		}

		// Optional lineage pointer to the root task this row inherits from.
		var inheritedFrom *uuid.UUID
		if t.InheritedFromTaskID != "" {
			parsed, perr := uuid.Parse(t.InheritedFromTaskID)
			if perr != nil {
				h.logger.Error("run.entries.dispatched: invalid inherited_from_task_id — discarding message",
					"task_id", t.TaskID, "inherited_from", t.InheritedFromTaskID, "error", perr)
				_ = tx.Rollback()
				return true, nil
			}
			inheritedFrom = &parsed
		}

		tasks = append(tasks, &model.TaskTracker{
			TaskID:              taskID,
			ScheduleID:          scheduleID,
			CreatedAt:           now,
			ServiceName:         t.ServiceName,
			SchemaName:          t.SchemaName,
			TableName:           t.TableName,
			JobName:             jobName,
			Status:              domainStatus,
			MaxRetries:          int(t.MaxRetries),
			ManifestVersion:     t.ManifestVersion,
			ImageTag:            t.ImageTag,
			InheritedFromTaskID: inheritedFrom,
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

	// Auto-rollup: if every projected task is already terminal, the run is born
	// complete and transitions straight to a terminal status (with completed_at)
	// rather than going through RUNNING. Mixed-terminal (some failed/cancelled)
	// is treated as FAILED conservatively. Any PENDING in the projection keeps
	// the standard cron/single-node-run/rerun behaviour: go to RUNNING.
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
		if txErr := h.schedulerRepo.FinalizeRunTx(ctx, tx, scheduleID, terminal); txErr != nil {
			return false, fmt.Errorf("finalize scheduler to %s: %w", terminal, txErr)
		}
		// Seed terminal_task_count for observability — FinalizeRunTx only updates
		// status + completed_at, so without this the rolled-up row carries
		// terminal_task_count=0 despite all tasks being terminal.
		if terminalCount > 0 {
			if txErr := h.schedulerRepo.SetTerminalTaskCountTx(ctx, tx, scheduleID, terminalCount); txErr != nil {
				return false, fmt.Errorf("seed terminal_task_count on rollup: %w", txErr)
			}
		}

		// Emit run.finalized:v1 in the same tx so orchestrator finalizes the
		// Neo4j :Run. Without this, the auto-rollup run stays "active" in the
		// orchestrator's projection forever.
		finalizedEvt := events.RunFinalized{
			ScheduleID:   scheduleID.String(),
			ScheduleName: scheduler.ScheduleName,
			Status:       terminal,
		}
		finalizedPayload, marshalErr := json.Marshal(finalizedEvt)
		if marshalErr != nil {
			return false, fmt.Errorf("marshal run.finalized payload: %w", marshalErr)
		}
		outboxEntry := &postgres.OutboxEntry{
			ID:            uuid.New(),
			AggregateType: "scheduler_tracker",
			AggregateID:   scheduleID,
			EventType:     "run.finalized:v1",
			Payload:       finalizedPayload,
			StreamName:    "run.finalized:v1",
			Status:        "pending",
			MaxRetries:    5,
			RetryCount:    0,
			CreatedAt:     time.Now(),
		}
		if txErr := h.outboxRepo.Create(ctx, tx, outboxEntry); txErr != nil {
			return false, fmt.Errorf("create outbox entry for run.finalized: %w", txErr)
		}
	} else {
		// Seed terminal_task_count from inherited terminal rows so the run can
		// finalize when the remaining pending tasks reach terminal status.
		if terminalCount > 0 {
			if txErr := h.schedulerRepo.SetTerminalTaskCountTx(ctx, tx, scheduleID, terminalCount); txErr != nil {
				return false, fmt.Errorf("seed terminal_task_count: %w", txErr)
			}
		}
		if txErr := h.schedulerRepo.UpdateStatusTx(ctx, tx, scheduleID, string(model.SchedulerStatusRunning)); txErr != nil {
			return false, fmt.Errorf("update status to running: %w", txErr)
		}
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
