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
//
//	id (uuid PK), message_processing_id (uuid nullable FK), aggregate_type (text),
//	aggregate_id (uuid), event_type (text), payload (jsonb), stream_name (text),
//	status (text, CHECK pending|scheduled|processed|failed), retry_count (int), max_retries (int),
//	created_at (timestamptz), processed_at (timestamptz nullable),
//	error_message (text nullable), next_attempt_at (timestamptz nullable).
//
// Create writes MaxAttempts into max_retries; no operation reads the column.
// Create stamps created_at, and GetPendingBatch returns rows in (created_at,
// id) order, never a row ahead of an older open row of its aggregate that it
// can see. That publishes the rows of one aggregate in the order they were
// created, within three limits: created_at comes from the clock of the process
// that wrote the row, so rows written by two processes are ordered only as
// closely as their clocks agree; a row becomes visible when its transaction
// commits, so a row whose transaction commits after a younger row was
// published publishes after it; and on the pipelined publish path
// (BatchPublisher) a younger row sent in the same pipeline as an older row
// that failed can reach its stream before the older row's retry.
//
// GetPendingBatch MUST be called inside a transaction held by the caller until
// the follow-up write that resolves each claimed row — marking it processed,
// marking it failed, or rescheduling it for a later attempt — completes,
// because the batch uses FOR UPDATE SKIP LOCKED to hold the rows for the life
// of that transaction.
type Repository interface {
	Create(ctx context.Context, entry *Entry) error
	GetPendingBatch(ctx context.Context, limit int) ([]*Entry, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	// MarkProcessedBatch flips every id to status='processed' with a single
	// UPDATE … WHERE id = ANY($1), avoiding one round trip per row. ids may be
	// empty (no-op). processed_at is stamped from the DB clock (NOW()).
	MarkProcessedBatch(ctx context.Context, ids []uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}
