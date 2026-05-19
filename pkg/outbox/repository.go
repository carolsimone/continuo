// pkg/outbox/repository.go
package outbox

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

// Executor abstracts *sqlx.DB and *sqlx.Tx so the same Postgres impl works
// inside or outside a transaction. Mirrors pkg/messageprocessing's pattern.
type Executor interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Repository defines operations on a service's outbox table.
// Implementations operate against a table conforming to the canonical schema:
//   id (uuid PK), message_processing_id (uuid nullable FK), aggregate_type (text),
//   aggregate_id (uuid), event_type (text), payload (jsonb), stream_name (text),
//   status (text, CHECK pending|processed|failed), retry_count (int), max_retries (int),
//   created_at (timestamptz), processed_at (timestamptz nullable),
//   error_message (text nullable).
//
// GetPendingBatch MUST be called inside a transaction held by the caller until
// MarkProcessed / IncrementRetry / MarkFailed completes, because the batch uses
// FOR UPDATE SKIP LOCKED.
type Repository interface {
	Create(ctx context.Context, entry *Entry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*Entry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}
