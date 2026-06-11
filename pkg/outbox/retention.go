// pkg/outbox/retention.go
package outbox

import (
	"context"
	"log/slog"
	"time"
)

// PruneFunc deletes at most limit rows that are older than retention, evaluated
// against the DB clock, and returns how many it removed. The retention sweeper
// calls it in a LIMIT loop until a pass removes fewer than limit rows (backlog
// drained) so each statement holds only a small lock.
type PruneFunc func(ctx context.Context, retention time.Duration, limit int) (int64, error)

// RetentionTarget names one table-and-prune pair the sweeper should keep
// bounded. Name is used only in log lines.
type RetentionTarget struct {
	Name  string
	Prune PruneFunc
}

// RetentionConfig groups the sweeper's timing knobs. All have safe defaults so
// no operator configuration is required.
type RetentionConfig struct {
	// Retention is how long a processed/terminal row is kept before it is
	// eligible for deletion. Default 7 days.
	Retention time.Duration
	// Interval is how often the sweep runs. Default 1 hour.
	Interval time.Duration
	// BatchLimit is the maximum rows deleted per statement; the sweeper loops
	// until a pass deletes fewer than this. Default 1000.
	BatchLimit int
	// MaxBatchesPerSweep caps how many LIMIT iterations one sweep performs, so a
	// huge backlog cannot monopolize the connection indefinitely; the remainder
	// is picked up on the next interval. Default 100.
	MaxBatchesPerSweep int
}

func (c RetentionConfig) withDefaults() RetentionConfig {
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.BatchLimit <= 0 {
		c.BatchLimit = 1000
	}
	if c.MaxBatchesPerSweep <= 0 {
		c.MaxBatchesPerSweep = 100
	}
	return c
}

// RetentionSweeper periodically prunes rows that have aged past the retention
// window from one or more tables. It is the shared cleanup component for the
// transactional outbox and the message_processing dedup table, both of which
// otherwise grow unbounded: every dispatched outbox row and every consumed
// message leaves a permanent row (the latter carrying its full payload).
type RetentionSweeper struct {
	targets []RetentionTarget
	cfg     RetentionConfig
	logger  *slog.Logger
}

// NewRetentionSweeper builds a sweeper over the given targets. Zero-value config
// fields fall back to safe defaults (7-day retention, hourly sweep), so callers
// may pass RetentionConfig{} to accept them wholesale.
func NewRetentionSweeper(targets []RetentionTarget, cfg RetentionConfig, logger *slog.Logger) *RetentionSweeper {
	return &RetentionSweeper{
		targets: targets,
		cfg:     cfg.withDefaults(),
		logger:  logger,
	}
}

// Run blocks until ctx is cancelled, sweeping every target on each tick.
func (s *RetentionSweeper) Run(ctx context.Context) {
	s.logger.Info("Starting retention sweeper",
		"retention", s.cfg.Retention, "interval", s.cfg.Interval, "targets", len(s.targets))
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Retention sweeper stopped")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

// sweep prunes every target once, each in its own bounded LIMIT loop.
func (s *RetentionSweeper) sweep(ctx context.Context) {
	for _, target := range s.targets {
		deleted, err := s.pruneTarget(ctx, target)
		if err != nil {
			s.logger.Error("Retention sweep failed", "target", target.Name, "error", err)
			continue
		}
		if deleted > 0 {
			s.logger.Info("Retention sweep deleted rows", "target", target.Name, "count", deleted)
		}
	}
}

// pruneTarget runs the target's PruneFunc in a LIMIT loop until a pass removes
// fewer rows than the batch limit (backlog cleared) or the per-sweep batch cap
// is reached. Returns the total deleted across the loop.
func (s *RetentionSweeper) pruneTarget(ctx context.Context, target RetentionTarget) (int64, error) {
	var total int64
	for i := 0; i < s.cfg.MaxBatchesPerSweep; i++ {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := target.Prune(ctx, s.cfg.Retention, s.cfg.BatchLimit)
		if err != nil {
			return total, err
		}
		total += n
		if int(n) < s.cfg.BatchLimit {
			break
		}
	}
	return total, nil
}

// OutboxRetentionTarget returns a RetentionTarget that prunes processed rows
// from the given outbox table via the package's Postgres repository.
func OutboxRetentionTarget(exec Executor, tableName string, logger *slog.Logger) RetentionTarget {
	repo := &postgresRepository{exec: exec, tableName: tableName, logger: logger}
	return RetentionTarget{
		Name:  tableName,
		Prune: repo.DeleteProcessedOlderThan,
	}
}
