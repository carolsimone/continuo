// pkg/outbox/postgres.go
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// outboxRow is the adapter-internal scan struct.
type outboxRow struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"`
	StreamName          string     `db:"stream_name"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	CreatedAt           time.Time  `db:"created_at"`
	ProcessedAt         *time.Time `db:"processed_at"`
	ErrorMessage        *string    `db:"error_message"`
}

func entryFromRow(r *outboxRow) *Entry {
	return &Entry{
		ID:                  r.ID,
		MessageProcessingID: r.MessageProcessingID,
		AggregateType:       r.AggregateType,
		AggregateID:         r.AggregateID,
		EventType:           r.EventType,
		Payload:             r.Payload,
		StreamName:          r.StreamName,
		Status:              r.Status,
		RetryCount:          r.RetryCount,
		MaxRetries:          r.MaxRetries,
		CreatedAt:           r.CreatedAt,
		ProcessedAt:         r.ProcessedAt,
		ErrorMessage:        r.ErrorMessage,
	}
}

type postgresRepository struct {
	exec      Executor
	tableName string
	logger    *slog.Logger
}

// NewPostgresRepository constructs a Repository bound to a specific physical
// table. Pass *sqlx.DB for autocommit operations (the Processor's GetPendingBatch
// holds its own tx) or *sqlx.Tx for transactional writes (the writer's Create
// must run inside the UoW transaction).
func NewPostgresRepository(exec Executor, tableName string, logger *slog.Logger) Repository {
	return &postgresRepository{exec: exec, tableName: tableName, logger: logger}
}

func (r *postgresRepository) Create(ctx context.Context, entry *Entry) error {
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

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, message_processing_id, aggregate_type, aggregate_id,
			event_type, payload, stream_name,
			status, retry_count, max_retries, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, r.tableName)

	_, err := r.exec.ExecContext(ctx, query,
		entry.ID, entry.MessageProcessingID, entry.AggregateType, entry.AggregateID,
		entry.EventType, entry.Payload, entry.StreamName,
		entry.Status, entry.RetryCount, entry.MaxRetries, entry.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create outbox entry in %s: %w", r.tableName, err)
	}
	return nil
}

func (r *postgresRepository) GetPendingBatch(ctx context.Context, limit int) ([]*Entry, error) {
	query := fmt.Sprintf(`
		SELECT id, message_processing_id, aggregate_type, aggregate_id,
		       event_type, payload, stream_name,
		       status, retry_count, max_retries,
		       created_at, processed_at, error_message
		FROM %s
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.tableName)

	var rows []*outboxRow
	if err := r.exec.SelectContext(ctx, &rows, query, limit); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get pending batch from %s: %w", r.tableName, err)
	}

	entries := make([]*Entry, len(rows))
	for i, row := range rows {
		entries[i] = entryFromRow(row)
	}
	return entries, nil
}

func (r *postgresRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'processed', processed_at = $1 WHERE id = $2`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("mark processed in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}

func (r *postgresRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'failed', error_message = $1 WHERE id = $2`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, errorMessage, id)
	if err != nil {
		return fmt.Errorf("mark failed in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}

func (r *postgresRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s SET retry_count = retry_count + 1 WHERE id = $1`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("increment retry in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}
