package sweeper

import (
	"context"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/graph/adapters/neo4j"
)

// RunSweeper periodically deletes expired Run nodes from Neo4j.
type RunSweeper struct {
	repo          neo4j.GraphRepository
	retentionDays int
	interval      time.Duration
	logger        *slog.Logger
}

func New(repo neo4j.GraphRepository, retentionDays, intervalMinutes int, logger *slog.Logger) *RunSweeper {
	return &RunSweeper{
		repo:          repo,
		retentionDays: retentionDays,
		interval:      time.Duration(intervalMinutes) * time.Minute,
		logger:        logger,
	}
}

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
