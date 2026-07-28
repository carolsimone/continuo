package messageprocessing

import (
	"context"
	"log/slog"
	"time"
)

// Pruner deletes terminal dedup rows older than a retention window. It is the
// narrow capability the shared retention sweeper needs from this package,
// satisfied by the Postgres repository.
type Pruner interface {
	DeleteTerminalOlderThan(ctx context.Context, retention time.Duration, limit int) (int64, error)
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
