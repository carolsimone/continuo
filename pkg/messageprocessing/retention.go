package messageprocessing

import (
	"context"
	"log/slog"
	"time"
)

// Pruner deletes message_processing dedup rows older than a retention window,
// whatever their state. It is the narrow capability the shared retention
// sweeper needs from this package, satisfied by the Postgres repository.
//
// A row's state does not decide whether it may go. Every consumer inserts its
// dedup row in the transaction that commits the handler's database writes, so
// a committed row, 'processing' or 'completed', stands for a message that was
// handled; an attempt that failed rolled its row back with everything else. A
// redelivery is recognised by the row existing, not by its state. What keeps a
// row long enough is its age: the sweeper's message_processing target sets
// MinRetention to model.ReplayHorizon, the longest a message can still be
// redelivered or redriven, so a row is deleted only once nothing that could
// match it can still arrive.
type Pruner interface {
	DeleteOlderThan(ctx context.Context, retention time.Duration, limit int) (int64, error)
}

// NewPruner constructs a Pruner over the message_processing table for the
// given executor (*sqlx.DB for autocommit sweeps). outboxTable is the caller's
// outbox table (e.g. "orchestrator_outbox", "state_outbox"): a
// message_processing row still referenced by a row in that table — pending,
// scheduled, processed-but-within-its-own-retention-window, or permanently
// dead-lettered as 'failed' — is never selected for deletion, so the delete
// can never violate the outbox table's message_processing_id foreign key.
func NewPruner(exec executor, outboxTable string, logger *slog.Logger) Pruner {
	return &postgresRepository{exec: exec, outboxTable: outboxTable, logger: logger}
}
