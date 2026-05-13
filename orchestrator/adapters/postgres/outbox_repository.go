package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// outboxExecutor abstracts sqlx.DB and sqlx.Tx for outbox database operations.
type outboxExecutor interface {
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// outboxEntryRow is the adapter-internal scan struct for SELECT queries against the outbox table.
type outboxEntryRow struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"`
	StreamName          string     `db:"stream_name"`
	CreatedAt           time.Time  `db:"created_at"`
	ProcessedAt         *time.Time `db:"processed_at"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	ErrorMessage        *string    `db:"error_message"`
}

func domainFromOutboxRow(r *outboxEntryRow) *domain.OutboxEntry {
	return &domain.OutboxEntry{
		ID:                  r.ID,
		MessageProcessingID: r.MessageProcessingID,
		AggregateType:       r.AggregateType,
		AggregateID:         r.AggregateID,
		EventType:           r.EventType,
		Payload:             r.Payload,
		StreamName:          r.StreamName,
		CreatedAt:           r.CreatedAt,
		ProcessedAt:         r.ProcessedAt,
		Status:              r.Status,
		RetryCount:          r.RetryCount,
		MaxRetries:          r.MaxRetries,
		ErrorMessage:        r.ErrorMessage,
	}
}

// compile-time interface check
var _ repository.OutboxRepository = (*outboxRepository)(nil)

type outboxRepository struct {
	executor outboxExecutor
	logger   *slog.Logger
}

// NewOutboxRepository creates a new OutboxRepository backed by *sqlx.DB.
func NewOutboxRepository(db *sqlx.DB, logger *slog.Logger) repository.OutboxRepository {
	return &outboxRepository{executor: db, logger: logger}
}

// NewOutboxRepositoryWithExecutor creates a new OutboxRepository with a custom executor (e.g. *sqlx.Tx).
func NewOutboxRepositoryWithExecutor(executor outboxExecutor, logger *slog.Logger) repository.OutboxRepository {
	return &outboxRepository{executor: executor, logger: logger}
}

// Create inserts a new outbox entry using positional parameters.
func (r *outboxRepository) Create(ctx context.Context, entry *domain.OutboxEntry) error {
	query := `
		INSERT INTO outbox (
			id, message_processing_id, aggregate_type, aggregate_id, event_type, payload,
			stream_name, created_at, status, retry_count, max_retries
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
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

	_, err := r.executor.ExecContext(ctx, query,
		entry.ID,
		entry.MessageProcessingID,
		entry.AggregateType,
		entry.AggregateID,
		entry.EventType,
		entry.Payload,
		entry.StreamName,
		entry.CreatedAt,
		entry.Status,
		entry.RetryCount,
		entry.MaxRetries,
	)
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

// GetPendingBatch retrieves a batch of pending outbox entries.
func (r *outboxRepository) GetPendingBatch(ctx context.Context, limit int) ([]*domain.OutboxEntry, error) {
	query := `
		SELECT id, message_processing_id, aggregate_type, aggregate_id, event_type, payload,
		       stream_name, created_at, processed_at, status,
		       retry_count, max_retries, error_message
		FROM outbox
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var rows []*outboxEntryRow
	err := r.executor.SelectContext(ctx, &rows, query, "pending", limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get pending outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get pending outbox entries: %w", err)
	}

	entries := make([]*domain.OutboxEntry, len(rows))
	for i, row := range rows {
		entries[i] = domainFromOutboxRow(row)
	}

	r.logger.Debug("Retrieved pending outbox entries", "count", len(entries))
	return entries, nil
}

// MarkProcessed marks an outbox entry as processed.
func (r *outboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE outbox SET status = $1, processed_at = $2 WHERE id = $3`

	result, err := r.executor.ExecContext(ctx, query, "processed", time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to mark outbox entry as processed", "id", id, "error", err)
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

// MarkFailed marks an outbox entry as failed.
func (r *outboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := `UPDATE outbox SET status = $1, error_message = $2 WHERE id = $3`

	result, err := r.executor.ExecContext(ctx, query, "failed", errorMessage, id)
	if err != nil {
		r.logger.Error("Failed to mark outbox entry as failed", "id", id, "error", err)
		return fmt.Errorf("failed to mark outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Outbox entry not found", "id", id)
		return fmt.Errorf("outbox entry not found")
	}

	r.logger.Warn("Marked outbox entry as failed", "id", id, "error_message", errorMessage)
	return nil
}

// IncrementRetry increments the retry count and resets status to pending.
func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := `UPDATE outbox SET retry_count = retry_count + 1, status = 'pending' WHERE id = $1`

	result, err := r.executor.ExecContext(ctx, query, id)
	if err != nil {
		r.logger.Error("Failed to increment retry count", "id", id, "error", err)
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

// UpdateStatus updates the status if it matches expectedStatus (optimistic lock).
func (r *outboxRepository) UpdateStatus(ctx context.Context, id uuid.UUID, newStatus, expectedStatus string) error {
	query := `UPDATE outbox SET status = $1 WHERE id = $2 AND status = $3`

	result, err := r.executor.ExecContext(ctx, query, newStatus, id, expectedStatus)
	if err != nil {
		r.logger.Error("Failed to update status",
			"id", id, "new_status", newStatus, "expected_status", expectedStatus, "error", err)
		return fmt.Errorf("failed to update status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("status mismatch or entry not found")
	}

	r.logger.Debug("Updated status", "id", id, "new_status", newStatus)
	return nil
}
