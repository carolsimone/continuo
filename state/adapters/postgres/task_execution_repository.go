package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/domain/events"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskExecutionRepository defines all operations for task_execution table
type TaskExecutionRepository interface {
	Create(ctx context.Context, execution *TaskExecution) error
	CreateTx(ctx context.Context, tx *sqlx.Tx, execution *TaskExecution) error
	GetByID(ctx context.Context, id uuid.UUID) (*TaskExecution, error)
	ListByScheduleID(ctx context.Context, scheduleID string, pageSize, pageOffset int) ([]*TaskExecution, int, error)
}

type taskExecutionRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewTaskExecutionRepository creates a new TaskExecutionRepository
func NewTaskExecutionRepository(db *sqlx.DB, logger *slog.Logger) TaskExecutionRepository {
	return &taskExecutionRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new task_execution record
func (r *taskExecutionRepository) Create(ctx context.Context, execution *TaskExecution) error {
	query := `
		INSERT INTO task_execution (
			id, task_id, created_at, started_at, completed_at,
			execution_time_seconds, executor_id, k8s_job_name,
			error_message, cancelled_at, cancelled_by, cancellation_reason,
			log_s3_key
		) VALUES (
			:id, :task_id, :created_at, :started_at, :completed_at,
			:execution_time_seconds, :executor_id, :k8s_job_name,
			:error_message, :cancelled_at, :cancelled_by, :cancellation_reason,
			:log_s3_key
		)
	`

	_, err := r.db.NamedExecContext(ctx, query, execution)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
			r.logger.Warn("Duplicate key violation when creating task_execution",
				"id", execution.ID,
				"task_id", execution.TaskID,
			)
			return ErrDuplicateKey
		}
		// Check for foreign key violation (task_id doesn't exist)
		if strings.Contains(err.Error(), "foreign key") || strings.Contains(err.Error(), "23503") {
			r.logger.Warn("Foreign key violation: task_id not found",
				"task_id", execution.TaskID,
			)
			return fmt.Errorf("task_id not found: %w", ErrNotFound)
		}
		r.logger.Error("Failed to create task_execution",
			"id", execution.ID,
			"task_id", execution.TaskID,
			"error", err,
		)
		return fmt.Errorf("failed to create task_execution: %w", err)
	}

	r.logger.Info("Created task_execution",
		"id", execution.ID,
		"task_id", execution.TaskID,
	)

	return nil
}

// CreateTx inserts a new task_execution record within an existing transaction.
// Uses ON CONFLICT (id) DO NOTHING for idempotency.
func (r *taskExecutionRepository) CreateTx(ctx context.Context, tx *sqlx.Tx, execution *TaskExecution) error {
	_, err := tx.NamedExecContext(ctx, `
		INSERT INTO task_execution (
			id, task_id, created_at, started_at, completed_at,
			execution_time_seconds, executor_id, k8s_job_name,
			error_message, cancelled_at, cancelled_by, cancellation_reason,
			log_s3_key
		) VALUES (
			:id, :task_id, :created_at, :started_at, :completed_at,
			:execution_time_seconds, :executor_id, :k8s_job_name,
			:error_message, :cancelled_at, :cancelled_by, :cancellation_reason,
			:log_s3_key
		)
		ON CONFLICT (id) DO NOTHING
	`, execution)
	if err != nil {
		r.logger.Error("Failed to create task_execution in tx",
			"id", execution.ID,
			"task_id", execution.TaskID,
			"error", err,
		)
		return fmt.Errorf("failed to create task_execution in tx: %w", err)
	}
	r.logger.Info("Created task_execution in tx",
		"id", execution.ID,
		"task_id", execution.TaskID,
	)
	return nil
}

// rowFromEvent maps a recorded-execution domain event to its storage row.
// ExecutorID is intentionally left nil: the event carries no executor id.
func rowFromEvent(evt events.TaskExecutionRecorded) *TaskExecution {
	return &TaskExecution{
		ID:                   evt.ExecutionID,
		TaskID:               evt.TaskID,
		CreatedAt:            time.Now(),
		StartedAt:            evt.StartedAt,
		CompletedAt:          evt.CompletedAt,
		ExecutionTimeSeconds: evt.ExecutionTimeSeconds,
		K8sJobName:           evt.JobName,
		ErrorMessage:         evt.ErrorMessage,
		LogS3Key:             evt.LogS3Key,
	}
}

// TaskExecutionWriterAdapter binds a TaskExecutionRepository to a transaction
// and satisfies repository.TaskExecutionWriter. The UnitOfWork constructs one
// per call so CreateRecord always writes inside the active transaction.
type TaskExecutionWriterAdapter struct {
	repo TaskExecutionRepository
	tx   *sqlx.Tx
}

// NewTaskExecutionWriter binds repo to tx for recorded-execution writes.
func NewTaskExecutionWriter(repo TaskExecutionRepository, tx *sqlx.Tx) *TaskExecutionWriterAdapter {
	return &TaskExecutionWriterAdapter{repo: repo, tx: tx}
}

// CreateRecord maps a recorded-execution domain event to the storage row and
// inserts it inside the bound transaction.
func (w *TaskExecutionWriterAdapter) CreateRecord(ctx context.Context, evt events.TaskExecutionRecorded) error {
	if w.tx == nil {
		return fmt.Errorf("CreateRecord requires an active transaction")
	}
	return w.repo.CreateTx(ctx, w.tx, rowFromEvent(evt))
}

var _ repository.TaskExecutionWriter = (*TaskExecutionWriterAdapter)(nil)

// GetByID retrieves a task_execution by id
func (r *taskExecutionRepository) GetByID(ctx context.Context, id uuid.UUID) (*TaskExecution, error) {
	query := `
		SELECT
			id, task_id, created_at, started_at, completed_at,
			execution_time_seconds, executor_id, k8s_job_name,
			error_message, cancelled_at, cancelled_by, cancellation_reason,
			log_s3_key
		FROM task_execution
		WHERE id = $1
	`

	var execution TaskExecution
	err := r.db.GetContext(ctx, &execution, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Task execution not found",
				"id", id,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get task_execution",
			"id", id,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task_execution: %w", err)
	}

	r.logger.Debug("Retrieved task_execution",
		"id", id,
		"task_id", execution.TaskID,
	)

	return &execution, nil
}

// ListByScheduleID retrieves all task executions for a given schedule, paginated
func (r *taskExecutionRepository) ListByScheduleID(ctx context.Context, scheduleID string, pageSize, pageOffset int) ([]*TaskExecution, int, error) {
	countQuery := `
		SELECT COUNT(*)
		FROM task_execution te
		JOIN task_tracker t ON t.task_id = te.task_id
		WHERE t.schedule_id = $1
	`
	var total int
	if err := r.db.GetContext(ctx, &total, countQuery, scheduleID); err != nil {
		return nil, 0, fmt.Errorf("failed to count task executions: %w", err)
	}

	query := `
		SELECT te.id, te.task_id, te.created_at, te.started_at, te.completed_at,
		       te.execution_time_seconds, te.executor_id, te.k8s_job_name,
		       te.error_message, te.cancelled_at, te.cancelled_by, te.cancellation_reason,
		       te.log_s3_key
		FROM task_execution te
		JOIN task_tracker t ON t.task_id = te.task_id
		WHERE t.schedule_id = $1
		ORDER BY te.created_at DESC
		LIMIT $2 OFFSET $3
	`

	var executions []*TaskExecution
	if err := r.db.SelectContext(ctx, &executions, query, scheduleID, pageSize, pageOffset); err != nil {
		return nil, 0, fmt.Errorf("failed to list task executions: %w", err)
	}

	r.logger.Debug("Listed task executions by schedule",
		"schedule_id", scheduleID,
		"count", len(executions),
		"total", total,
	)

	return executions, total, nil
}
