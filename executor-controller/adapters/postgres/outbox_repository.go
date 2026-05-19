package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
)

// OutboxRepository defines operations for the deployment_outbox table
type OutboxRepository interface {
	Create(ctx context.Context, entry *model.DeploymentOutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*model.DeploymentOutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}

// Executor is implemented by both sqlx.DB and sqlx.Tx.
type Executor interface {
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type outboxRepository struct {
	executor Executor
	logger   *slog.Logger
}

// NewOutboxRepository creates an OutboxRepository against any sqlx executor
// (either *sqlx.DB for autocommit work, or *sqlx.Tx when bound to an active
// transaction).
func NewOutboxRepository(executor Executor, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		executor: executor,
		logger:   logger,
	}
}

// Create inserts a new deployment outbox entry.
//
// When entry.OutboxEntryID is set, the INSERT uses ON CONFLICT against
// the partial unique index deployment_outbox_outbox_entry_id_key (see
// migration V10) and silently no-ops on conflict. This is the
// application-level idempotency layer that catches publisher retries
// producing a new Redis msg.ID for the same orchestrator outbox row.
// Conflict → 0 rows affected; the caller's transaction commits the
// dedup row in message_processing and the message ACKs.
//
// When entry.OutboxEntryID is nil (retry.task:v1 does not carry one on
// the wire today), the partial index does not apply and dedup degrades
// to the shared (msg.ID, stream_name) layer in message_processing.
func (r *outboxRepository) Create(ctx context.Context, entry *model.DeploymentOutboxEntry) error {
	query := `
		INSERT INTO deployment_outbox (
			id, outbox_entry_id, task_id, schedule_id, schedule_name, service_name, schema_name,
			table_name, job_name, node_type, image_tag, task_retry_count, task_max_retries,
			status, created_at, retry_count, max_retries
		) VALUES (
			:id, :outbox_entry_id, :task_id, :schedule_id, :schedule_name, :service_name, :schema_name,
			:table_name, :job_name, :node_type, :image_tag, :task_retry_count, :task_max_retries,
			:status, :created_at, :retry_count, :max_retries
		)
		ON CONFLICT (outbox_entry_id) WHERE outbox_entry_id IS NOT NULL DO NOTHING
	`

	// Set defaults if not provided
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.Status == "" {
		entry.Status = string(model.OutboxStatusPending)
	}
	if entry.OutboxMaxRetries == 0 {
		entry.OutboxMaxRetries = 3
	}

	_, err := r.executor.NamedExecContext(ctx, query, entry)
	if err != nil {
		r.logger.Error("Failed to create deployment outbox entry",
			"task_id", entry.TaskID,
			"job_name", entry.JobName,
			"error", err,
		)
		return fmt.Errorf("failed to create deployment outbox entry: %w", err)
	}

	r.logger.Debug("Created deployment outbox entry",
		"id", entry.ID,
		"task_id", entry.TaskID,
		"job_name", entry.JobName,
	)

	return nil
}

// GetPendingBatch retrieves a batch of pending deployment outbox entries
// Uses FOR UPDATE SKIP LOCKED for safe concurrent processing
func (r *outboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*model.DeploymentOutboxEntry, error) {
	query := `
		SELECT id, outbox_entry_id, task_id, schedule_id, schedule_name, service_name, schema_name,
		       table_name, job_name, node_type, image_tag,
		       COALESCE(task_retry_count, 0) as task_retry_count,
		       COALESCE(task_max_retries, 2) as task_max_retries,
		       status, created_at, processed_at,
		       retry_count, max_retries, error_message
		FROM deployment_outbox
		WHERE status = $1
		  AND retry_count < max_retries
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var entries []*model.DeploymentOutboxEntry
	err := r.executor.SelectContext(ctx, &entries, query, string(model.OutboxStatusPending), limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get pending deployment outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get pending deployment outbox entries: %w", err)
	}

	r.logger.Debug("Retrieved pending deployment outbox entries", "count", len(entries))

	return entries, nil
}

// MarkProcessed marks a deployment outbox entry as processed
func (r *outboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE deployment_outbox
		SET status = $1, processed_at = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, string(model.OutboxStatusProcessed), time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to mark deployment outbox entry as processed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark deployment outbox entry as processed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Deployment outbox entry not found", "id", id)
		return fmt.Errorf("deployment outbox entry not found")
	}

	r.logger.Debug("Marked deployment outbox entry as processed", "id", id)

	return nil
}

// MarkFailed marks a deployment outbox entry as failed
func (r *outboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := `
		UPDATE deployment_outbox
		SET status = $1, error_message = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, string(model.OutboxStatusFailed), errorMessage, id)
	if err != nil {
		r.logger.Error("Failed to mark deployment outbox entry as failed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark deployment outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Deployment outbox entry not found", "id", id)
		return fmt.Errorf("deployment outbox entry not found")
	}

	r.logger.Warn("Marked deployment outbox entry as failed",
		"id", id,
		"error_message", errorMessage,
	)

	return nil
}

// IncrementRetry increments the retry count for a deployment outbox entry
func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE deployment_outbox
		SET retry_count = retry_count + 1
		WHERE id = $1
	`

	result, err := r.executor.ExecContext(ctx, query, id)
	if err != nil {
		r.logger.Error("Failed to increment retry count",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to increment retry count: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Deployment outbox entry not found", "id", id)
		return fmt.Errorf("deployment outbox entry not found")
	}

	r.logger.Debug("Incremented retry count", "id", id)

	return nil
}
