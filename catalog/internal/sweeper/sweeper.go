package sweeper

import (
	"context"
	"log/slog"
	"time"
)

// RunCleaner is the minimal interface that the sweeper needs.
type RunCleaner interface {
	DeleteExpiredRuns(ctx context.Context, retentionDays int) error
}

// RunSweeper periodically deletes expired Run nodes from Neo4j.
type RunSweeper struct {
	repo          RunCleaner
	retentionDays int
	interval      time.Duration
	logger        *slog.Logger
}

// New creates a new RunSweeper.
func New(repo RunCleaner, retentionDays, intervalMinutes int, logger *slog.Logger) *RunSweeper {
	return &RunSweeper{
		repo:          repo,
		retentionDays: retentionDays,
		interval:      time.Duration(intervalMinutes) * time.Minute,
		logger:        logger,
	}
}

// Start begins the periodic sweep loop until the context is cancelled.
func (s *RunSweeper) Start(ctx context.Context) {
	s.logger.Info("RunSweeper started", "retention_days", s.retentionDays, "interval", s.interval)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.repo.DeleteExpiredRuns(ctx, s.retentionDays); err != nil {
				s.logger.Error("RunSweeper: failed to delete expired runs", "error", err)
			}
		case <-ctx.Done():
			s.logger.Info("RunSweeper stopped")
			return
		}
	}
}
