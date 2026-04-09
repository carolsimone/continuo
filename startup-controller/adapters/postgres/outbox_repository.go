package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/startup-controller/domain/model"
	"github.com/google/uuid"
)

// OutboxRepository defines operations for the outbox table
type OutboxRepository interface {
	Create(ctx context.Context, entry *model.OutboxEntry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}

// DBExecutor is a common interface for *sqlx.DB and *sqlx.Tx
// Both types satisfy this interface, allowing us to use either in transactions
type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

type outboxRepository struct {
	db     DBExecutor
	logger *slog.Logger
}

// NewOutboxRepository creates a new OutboxRepository
func NewOutboxRepository(db DBExecutor, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new outbox entry
func (r *outboxRepository) Create(ctx context.Context, entry *model.OutboxEntry) error {
	query := `
		INSERT INTO startup_outbox (
			id, aggregate_type, aggregate_id, event_type, payload,
			stream_name, created_at, status, retry_count, max_retries
		) VALUES (
			:id, :aggregate_type, :aggregate_id, :event_type, :payload,
			:stream_name, :created_at, :status, :retry_count, :max_retries
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
		entry.Status = string(model.OutboxStatusPending)
	}
	if entry.MaxRetries == 0 {
		entry.MaxRetries = 3
	}

	_, err := r.db.NamedExecContext(ctx, query, entry)
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
func (r *outboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*model.OutboxEntry, error) {
	query := `
		SELECT id, aggregate_type, aggregate_id, event_type, payload,
		       stream_name, created_at, processed_at, status,
		       retry_count, max_retries, error_message
		FROM startup_outbox
		WHERE status = $1
		  AND retry_count < max_retries
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var entries []*model.OutboxEntry
	err := r.db.SelectContext(ctx, &entries, query, string(model.OutboxStatusPending), limit)
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
		UPDATE startup_outbox
		SET status = $1, processed_at = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, string(model.OutboxStatusProcessed), time.Now(), id)
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
		UPDATE startup_outbox
		SET status = $1, error_message = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctx, query, string(model.OutboxStatusFailed), errorMessage, id)
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

// IncrementRetry increments the retry count for an outbox entry
func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE startup_outbox
		SET retry_count = retry_count + 1
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id)
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

	r.logger.Debug("Incremented retry count", "id", id)

	return nil
}
