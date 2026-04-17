package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// OutboxRepository defines operations for the outbox table
type OutboxRepository interface {
	Create(ctx context.Context, entry *domain.OutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
	UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error
}

// OutboxExecutor abstracts sqlx.DB and sqlx.Tx for database operations
type OutboxExecutor interface {
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type outboxRepository struct {
	executor OutboxExecutor
	logger   *slog.Logger
}

// NewOutboxRepository creates a new OutboxRepository
func NewOutboxRepository(db *sqlx.DB, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		executor: db,
		logger:   logger,
	}
}

// NewOutboxRepositoryWithExecutor creates a new OutboxRepository with a custom executor
func NewOutboxRepositoryWithExecutor(executor OutboxExecutor, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		executor: executor,
		logger:   logger,
	}
}

// Create inserts a new outbox entry
func (r *outboxRepository) Create(ctx context.Context, entry *domain.OutboxEntry) error {
	query := `
		INSERT INTO outbox (
			id, message_processing_id, aggregate_type, aggregate_id, event_type, payload,
			stream_name, created_at, status, retry_count, max_retries
		) VALUES (
			:id, :message_processing_id, :aggregate_type, :aggregate_id, :event_type, :payload,
			:stream_name, :created_at, :status, :retry_count, :max_retries
		)
	`

	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.Status == "" {
		entry.Status = "pending"
	}
	if entry.MaxRetries == 0 {
		entry.MaxRetries = 3
	}

	_, err := r.executor.NamedExecContext(ctx, query, entry)
	if err != nil {
		r.logger.Error("Failed to create outbox entry",
			"aggregate_id", entry.AggregateID,
			"event_type", entry.EventType,
			"error", err,
		)
		return fmt.Errorf("failed to create outbox entry: %w", err)
	}

	r.logger.Debug("Created outbox entry",
		"id", entry.ID,
		"aggregate_id", entry.AggregateID,
		"event_type", entry.EventType,
	)

	return nil
}

// GetPendingBatch retrieves a batch of pending outbox entries
func (r *outboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error) {
	query := `
		SELECT id, aggregate_type, aggregate_id, event_type, payload,
		       stream_name, created_at, processed_at, status,
		       retry_count, max_retries, error_message
		FROM outbox
		WHERE status = $1
		  AND retry_count < max_retries
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var entries []*domain.OutboxEntry
	err := r.executor.SelectContext(ctx, &entries, query, "pending", limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get pending outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get pending outbox entries: %w", err)
	}

	r.logger.Debug("Retrieved pending outbox entries", "count", len(entries))

	return entries, nil
}

// MarkProcessed marks an outbox entry as processed
func (r *outboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox
		SET status = $1, processed_at = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, "processed", time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to mark outbox entry as processed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark outbox entry as processed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Outbox entry not found", "id", id)
		return fmt.Errorf("outbox entry not found")
	}

	r.logger.Debug("Marked outbox entry as processed", "id", id)

	return nil
}

// MarkFailed marks an outbox entry as failed
func (r *outboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := `
		UPDATE outbox
		SET status = $1, error_message = $2
		WHERE id = $3
	`

	result, err := r.executor.ExecContext(ctx, query, "failed", errorMessage, id)
	if err != nil {
		r.logger.Error("Failed to mark outbox entry as failed",
			"id", id,
			"error", err,
		)
		return fmt.Errorf("failed to mark outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Outbox entry not found", "id", id)
		return fmt.Errorf("outbox entry not found")
	}

	r.logger.Warn("Marked outbox entry as failed",
		"id", id,
		"error_message", errorMessage,
	)

	return nil
}

// IncrementRetry increments the retry count for an outbox entry and resets status to pending
// so that entries stuck in 'publishing' are picked up again on the next processor tick.
func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox
		SET retry_count = retry_count + 1,
		    status      = 'pending'
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
		r.logger.Warn("Outbox entry not found", "id", id)
		return fmt.Errorf("outbox entry not found")
	}

	r.logger.Debug("Incremented retry count and reset status to pending", "id", id)

	return nil
}

// UpdateStatus updates the status if it matches expected status (optimistic lock)
func (r *outboxRepository) UpdateStatus(
	ctx context.Context,
	id uuid.UUID,
	newStatus, expectedStatus string,
) error {
	query := `
		UPDATE outbox
		SET status = $1
		WHERE id = $2 AND status = $3
	`

	result, err := r.executor.ExecContext(ctx, query, newStatus, id, expectedStatus)
	if err != nil {
		r.logger.Error("Failed to update status",
			"id", id,
			"new_status", newStatus,
			"expected_status", expectedStatus,
			"error", err,
		)
		return fmt.Errorf("failed to update status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("status mismatch or entry not found")
	}

	r.logger.Debug("Updated status",
		"id", id,
		"new_status", newStatus,
	)

	return nil
}
