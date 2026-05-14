package postgres

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type OutboxEntry struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"`
	StreamName          string     `db:"stream_name"`
	Status              string     `db:"status"`
	MaxRetries          int        `db:"max_retries"`
	RetryCount          int        `db:"retry_count"`
	CreatedAt           time.Time  `db:"created_at"`
}

type OutboxRepository interface {
	Create(ctx context.Context, tx *sqlx.Tx, entry *OutboxEntry) error
	ListPending(ctx context.Context, limit int) ([]*OutboxEntry, error)
	MarkPublished(ctx context.Context, id uuid.UUID) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}

type outboxRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

func NewOutboxRepository(db *sqlx.DB, logger *slog.Logger) OutboxRepository {
	return &outboxRepository{db: db, logger: logger}
}

func (r *outboxRepository) Create(ctx context.Context, tx *sqlx.Tx, entry *OutboxEntry) error {
	_, err := tx.NamedExecContext(ctx, `
		INSERT INTO state_outbox
			(id, message_processing_id, aggregate_type, aggregate_id, event_type, payload, stream_name, status, max_retries, retry_count, created_at)
		VALUES
			(:id, :message_processing_id, :aggregate_type, :aggregate_id, :event_type, :payload, :stream_name, :status, :max_retries, :retry_count, :created_at)
	`, entry)
	return err
}

func (r *outboxRepository) ListPending(ctx context.Context, limit int) ([]*OutboxEntry, error) {
	var entries []*OutboxEntry
	err := r.db.SelectContext(ctx, &entries,
		`SELECT * FROM state_outbox WHERE status = 'pending' AND retry_count < max_retries ORDER BY created_at LIMIT $1`, limit)
	return entries, err
}

func (r *outboxRepository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `UPDATE state_outbox SET status = 'published' WHERE id = $1`, id)
	return err
}

func (r *outboxRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `UPDATE state_outbox SET retry_count = retry_count + 1 WHERE id = $1`, id)
	return err
}
