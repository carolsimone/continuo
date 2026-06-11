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

// NewPruner constructs a Pruner over the message_processing table for the given
// executor (*sqlx.DB for autocommit sweeps).
func NewPruner(exec executor, logger *slog.Logger) Pruner {
	return &postgresRepository{exec: exec, logger: logger}
}
