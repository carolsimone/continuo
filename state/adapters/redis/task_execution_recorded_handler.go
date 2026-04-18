package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskExecutionRecordedHandler processes task.execution.recorded:v1 events.
// On success it persists a task_execution record.
type TaskExecutionRecordedHandler struct {
	db       *sqlx.DB
	execRepo postgres.TaskExecutionRepository
	logger   *slog.Logger
}

// NewTaskExecutionRecordedHandler creates a TaskExecutionRecordedHandler.
func NewTaskExecutionRecordedHandler(
	db *sqlx.DB,
	execRepo postgres.TaskExecutionRepository,
	logger *slog.Logger,
) *TaskExecutionRecordedHandler {
	return &TaskExecutionRecordedHandler{
		db:       db,
		execRepo: execRepo,
		logger:   logger,
	}
}

// Handle processes flat-field values from a task.execution.recorded:v1 Redis stream message.
// Returns (shouldACK bool, err error).
// shouldACK=true means ACK regardless of err (permanent/unprocessable message).
// shouldACK=false + err means transient failure — do NOT ACK.
func (h *TaskExecutionRecordedHandler) Handle(ctx context.Context, messageID string, values map[string]interface{}) (shouldACK bool, err error) {
	executionIDStr, _ := values["execution_id"].(string)
	taskIDStr, _ := values["task_id"].(string)
	jobNameStr, _ := values["job_name"].(string)
	startedAtStr, _ := values["started_at"].(string)
	completedAtStr, _ := values["completed_at"].(string)
	execSecondsStr, _ := values["execution_seconds"].(string)
	errorMessageStr, _ := values["error_message"].(string)
	logS3KeyStr, _ := values["log_s3_key"].(string)

	if executionIDStr == "" || taskIDStr == "" {
		h.logger.Error("task.execution.recorded: missing required fields — discarding",
			"execution_id", executionIDStr, "task_id", taskIDStr)
		return true, nil
	}

	executionID, err := uuid.Parse(executionIDStr)
	if err != nil {
		h.logger.Error("task.execution.recorded: invalid execution_id UUID — discarding", "error", err)
		return true, nil
	}

	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		h.logger.Error("task.execution.recorded: invalid task_id UUID — discarding", "error", err)
		return true, nil
	}

	var startedAt *time.Time
	if startedAtStr != "" {
		t, parseErr := time.Parse(time.RFC3339, startedAtStr)
		if parseErr != nil {
			h.logger.Error("task.execution.recorded: invalid started_at — discarding", "error", parseErr)
			return true, nil
		}
		startedAt = &t
	}

	var completedAt *time.Time
	if completedAtStr != "" {
		t, parseErr := time.Parse(time.RFC3339, completedAtStr)
		if parseErr != nil {
			h.logger.Error("task.execution.recorded: invalid completed_at — discarding", "error", parseErr)
			return true, nil
		}
		completedAt = &t
	}

	var execSeconds *float64
	if execSecondsStr != "" {
		v, parseErr := strconv.ParseFloat(execSecondsStr, 64)
		if parseErr == nil && v > 0 {
			execSeconds = &v
		}
	}

	var jobName *string
	if jobNameStr != "" {
		jobName = &jobNameStr
	}

	var errMsg *string
	if errorMessageStr != "" {
		errMsg = &errorMessageStr
	}

	var logS3Key *string
	if logS3KeyStr != "" {
		logS3Key = &logS3KeyStr
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

	// Deduplication via processed_events.
	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("task.execution.recorded:"+messageID))
	var already bool
	if dbErr := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
		dedupID,
	).Scan(&already); dbErr != nil {
		return false, fmt.Errorf("dedup check: %w", dbErr)
	}
	if already {
		h.logger.Info("task.execution.recorded: duplicate message — skipping", "message_id", messageID)
		_ = tx.Commit()
		return true, nil
	}

	execution := &model.TaskExecution{
		ID:                   executionID,
		TaskID:               taskID,
		CreatedAt:            time.Now(),
		StartedAt:            startedAt,
		CompletedAt:          completedAt,
		ExecutionTimeSeconds: execSeconds,
		K8sJobName:           jobName,
		ErrorMessage:         errMsg,
		LogS3Key:             logS3Key,
		// ExecutorID is intentionally not set: events.TaskExecutionRecorded does not
		// carry an executor identifier. If a future event version adds this field,
		// add it to the events struct and map it here.
	}

	if txErr := h.execRepo.CreateTx(ctx, tx, execution); txErr != nil {
		return false, fmt.Errorf("create task_execution: %w", txErr)
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

	h.logger.Info("task.execution.recorded: processed",
		"execution_id", executionID,
		"task_id", taskID,
	)
	return true, nil
}
