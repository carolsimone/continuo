package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// OutboxRepository defines operations for the k8s_status_outbox table
type OutboxRepository interface {
	Create(ctx context.Context, entry *model.K8sStatusOutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*model.K8sStatusOutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
	GetStuckEntries(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*model.K8sStatusOutboxEntry, error)
	ForceMarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
}

// Executor abstracts sqlx.DB and sqlx.Tx for database operations
type Executor interface {
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type outboxRepository struct {
	executor Executor
	logger   *slog.Logger
}

// NewOutboxRepository creates a new OutboxRepository with a *sqlx.DB
func NewOutboxRepository(db *sqlx.DB, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		executor: db,
		logger:   logger,
	}
}

// NewOutboxRepositoryWithTx creates a new OutboxRepository with a *sqlx.Tx
func NewOutboxRepositoryWithTx(tx *sqlx.Tx, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		executor: tx,
		logger:   logger,
	}
}

// Create inserts a new k8s status outbox entry
func (r *outboxRepository) Create(ctx context.Context, entry *model.K8sStatusOutboxEntry) error {
	query := `
		INSERT INTO k8s_status_outbox (
			id, event_type, stream_name,
			task_id, schedule_id, schedule_name, service_name, schema_name,
			table_name, job_name, node_type, image_tag,
			error_message, task_retry_count, task_max_retries, check_after,
			update_task_status, new_task_status, new_retry_count,
			create_execution, execution_started_at, execution_completed_at, execution_seconds,
			execution_id, log_s3_key,
			status, created_at, outbox_retry_count, max_retries
		) VALUES (
			:id, :event_type, :stream_name,
			:task_id, :schedule_id, :schedule_name, :service_name, :schema_name,
			:table_name, :job_name, :node_type, :image_tag,
			:error_message, :task_retry_count, :task_max_retries, :check_after,
			:update_task_status, :new_task_status, :new_retry_count,
			:create_execution, :execution_started_at, :execution_completed_at, :execution_seconds,
			:execution_id, :log_s3_key,
			:status, :created_at, :outbox_retry_count, :max_retries
		)
	`

	// Set defaults if not provided
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.Status == "" {
		entry.Status = model.OutboxStatusPending
	}
	if entry.MaxRetries == 0 {
		entry.MaxRetries = 3
	}

	_, err := r.executor.NamedExecContext(ctx, query, entry)
	if err != nil {
		r.logger.Error("Failed to create k8s status outbox entry",
			"task_id", entry.TaskID,
			"event_type", entry.EventType,
			"error", err,
		)
		return fmt.Errorf("failed to create k8s status outbox entry: %w", err)
	}

	r.logger.Debug("Created k8s status outbox entry",
		"id", entry.ID,
		"task_id", entry.TaskID,
		"event_type", entry.EventType,
	)

	return nil
}

// GetPendingBatch retrieves a batch of pending k8s status outbox entries
// Uses FOR UPDATE SKIP LOCKED for safe concurrent processing
func (r *outboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*model.K8sStatusOutboxEntry, error) {
	query := `
		SELECT id, event_type, stream_name,
		       task_id, schedule_id, schedule_name, service_name, schema_name,
		       table_name, job_name, COALESCE(node_type, 'dbt-model') as node_type,
		       COALESCE(image_tag, '') as image_tag,
		       COALESCE(error_message, '') as error_message,
		       COALESCE(task_retry_count, 0) as task_retry_count,
		       COALESCE(task_max_retries, 2) as task_max_retries,
		       COALESCE(check_after, 0) as check_after,
		       COALESCE(update_task_status, false) as update_task_status,
		       COALESCE(new_task_status, '') as new_task_status,
		       COALESCE(new_retry_count, 0) as new_retry_count,
		       COALESCE(create_execution, false) as create_execution,
		       execution_started_at, execution_completed_at,
		       COALESCE(execution_seconds, 0) as execution_seconds,
		       COALESCE(execution_id, '00000000-0000-0000-0000-000000000000'::uuid) as execution_id,
		       COALESCE(log_s3_key, '') as log_s3_key,
		       status, created_at, processed_at,
		       outbox_retry_count, max_retries,
		       COALESCE(outbox_error_message, '') as outbox_error_message
		FROM k8s_status_outbox
		WHERE status = $1
		  AND outbox_retry_count < max_retries
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var entries []*model.K8sStatusOutboxEntry
	err := r.executor.SelectContext(ctx, &entries, query, string(model.OutboxStatusPending), limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get pending k8s status outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get pending k8s status outbox entries: %w", err)
	}

	r.logger.Debug("Retrieved pending k8s status outbox entries", "count", len(entries))

	return entries, nil
}

// MarkProcessed marks a k8s status outbox entry as processed
func (r *outboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE k8s_status_outbox
		SET status = $1, processed_at = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, string(model.OutboxStatusProcessed), time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to mark k8s status outbox entry as processed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark k8s status outbox entry as processed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("K8s status outbox entry not found", "id", id)
		return fmt.Errorf("k8s status outbox entry not found")
	}

	r.logger.Debug("Marked k8s status outbox entry as processed", "id", id)

	return nil
}

// MarkFailed marks a k8s status outbox entry as failed
func (r *outboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := `
		UPDATE k8s_status_outbox
		SET status = $1, outbox_error_message = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, string(model.OutboxStatusFailed), errorMessage, id)
	if err != nil {
		r.logger.Error("Failed to mark k8s status outbox entry as failed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark k8s status outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("K8s status outbox entry not found", "id", id)
		return fmt.Errorf("k8s status outbox entry not found")
	}

	r.logger.Warn("Marked k8s status outbox entry as failed",
		"id", id,
		"error_message", errorMessage,
	)

	return nil
}

// IncrementRetry increments the retry count for a k8s status outbox entry
func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE k8s_status_outbox
		SET outbox_retry_count = outbox_retry_count + 1
		WHERE id = $1
	`

	result, err := r.executor.ExecContext(ctx, query, id)
	if err != nil {
		r.logger.Error("Failed to increment outbox retry count",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to increment outbox retry count: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("K8s status outbox entry not found", "id", id)
		return fmt.Errorf("k8s status outbox entry not found")
	}

	r.logger.Debug("Incremented outbox retry count", "id", id)

	return nil
}

// GetStuckEntries retrieves entries stuck in pending state with retry_count >= max_retries
// Uses FOR UPDATE SKIP LOCKED for safe concurrent processing
func (r *outboxRepository) GetStuckEntries(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*model.K8sStatusOutboxEntry, error) {
	query := `
		SELECT id, event_type, stream_name,
		       task_id, schedule_id, schedule_name, service_name, schema_name,
		       table_name, job_name, COALESCE(node_type, 'dbt-model') as node_type,
		       COALESCE(image_tag, '') as image_tag,
		       COALESCE(error_message, '') as error_message,
		       COALESCE(task_retry_count, 0) as task_retry_count,
		       COALESCE(task_max_retries, 2) as task_max_retries,
		       COALESCE(check_after, 0) as check_after,
		       COALESCE(update_task_status, false) as update_task_status,
		       COALESCE(new_task_status, '') as new_task_status,
		       COALESCE(new_retry_count, 0) as new_retry_count,
		       COALESCE(create_execution, false) as create_execution,
		       execution_started_at, execution_completed_at,
		       COALESCE(execution_seconds, 0) as execution_seconds,
		       COALESCE(execution_id, '00000000-0000-0000-0000-000000000000'::uuid) as execution_id,
		       COALESCE(log_s3_key, '') as log_s3_key,
		       status, created_at, processed_at,
		       outbox_retry_count, max_retries,
		       COALESCE(outbox_error_message, '') as outbox_error_message
		FROM k8s_status_outbox
		WHERE status = $1
		  AND outbox_retry_count >= max_retries
		  AND created_at < NOW() - ($2 || ' seconds')::INTERVAL
		ORDER BY created_at ASC
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`

	var entries []*model.K8sStatusOutboxEntry
	err := r.executor.SelectContext(ctx, &entries, query, string(model.OutboxStatusPending), stuckThresholdSeconds, limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get stuck k8s status outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get stuck k8s status outbox entries: %w", err)
	}

	r.logger.Debug("Retrieved stuck k8s status outbox entries", "count", len(entries))

	return entries, nil
}

// ProcessedEventsRepository defines operations for the processed_events table.
type ProcessedEventsRepository interface {
	// TryMarkProcessed atomically tries to insert outbox_entry_id into
	// processed_events using INSERT ... ON CONFLICT DO NOTHING.
	// Returns (true, nil)  — row already existed; caller should skip processing.
	// Returns (false, nil) — row was newly inserted; caller should proceed.
	// Must be called inside a transaction so a later rollback undoes the insert.
	TryMarkProcessed(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
}

type processedEventsRepository struct {
	executor Executor
	logger   *slog.Logger
}

// NewProcessedEventsRepository creates a ProcessedEventsRepository backed by db.
func NewProcessedEventsRepository(db *sqlx.DB, logger *slog.Logger) ProcessedEventsRepository {
	return &processedEventsRepository{executor: db, logger: logger}
}

// NewProcessedEventsRepositoryWithTx creates a ProcessedEventsRepository backed by tx.
func NewProcessedEventsRepositoryWithTx(tx *sqlx.Tx, logger *slog.Logger) ProcessedEventsRepository {
	return &processedEventsRepository{executor: tx, logger: logger}
}

func (r *processedEventsRepository) TryMarkProcessed(ctx context.Context, outboxEntryID uuid.UUID) (bool, error) {
	res, err := r.executor.ExecContext(ctx,
		`INSERT INTO processed_events (outbox_entry_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		outboxEntryID)
	if err != nil {
		return false, fmt.Errorf("processed_events TryMarkProcessed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("processed_events TryMarkProcessed rows affected: %w", err)
	}
	return n == 0, nil // n==0 → conflict → already processed
}

// ForceMarkFailed attempts to mark an entry as failed with aggressive retry logic
// Unlike MarkFailed, this includes processed_at timestamp and is designed for stuck entry resolution
func (r *outboxRepository) ForceMarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	// Create a context with 5-second timeout for aggressive retry
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE k8s_status_outbox
		SET status = $1,
		    outbox_error_message = $2,
		    processed_at = $3
		WHERE id = $4
	`

	result, err := r.executor.ExecContext(ctxWithTimeout, query, string(model.OutboxStatusFailed), errorMessage, time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to force mark k8s status outbox entry as failed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to force mark k8s status outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("K8s status outbox entry not found for force mark failed", "id", id)
		return fmt.Errorf("k8s status outbox entry not found")
	}

	r.logger.Warn("Force marked k8s status outbox entry as failed",
		"id", id,
		"error_message", errorMessage,
	)

	return nil
}
