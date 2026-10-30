package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/internal/sweeper"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RunHistoryRepository deletes the :Run history the graph still holds. Nothing
// writes a :Run; the run sweeper calls DeleteExpiredRuns until the history the
// graph holds is gone.
type RunHistoryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ sweeper.RunCleaner = (*RunHistoryRepository)(nil)

// NewRunHistoryRepository constructs a RunHistoryRepository.
func NewRunHistoryRepository(client Neo4jClient, logger *slog.Logger) *RunHistoryRepository {
	return &RunHistoryRepository{client: client, logger: logger}
}

// DeleteExpiredRuns deletes every :Run, with its :EXECUTES edges, whose
// completed_at, or created_at when it never completed, is more than
// retentionDays old, and every :Run that carries neither. Nothing finalizes a
// :Run, so a run left unfinished would otherwise stay for ever and keep the
// :Table nodes it executed from the promotion's orphan cleanup.
func (r *RunHistoryRepository) DeleteExpiredRuns(ctx context.Context, retentionDays int) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	result, err := session.Run(ctx, `
		MATCH (run:Run)
		WITH run, coalesce(run.completed_at, run.created_at) AS stamped
		WHERE stamped IS NULL OR stamped < datetime() - duration({days: $retention_days})
		DETACH DELETE run
	`, map[string]any{"retention_days": retentionDays})
	if err != nil {
		return fmt.Errorf("DeleteExpiredRuns: %w", err)
	}
	summary, err := result.Consume(ctx)
	if err != nil {
		return fmt.Errorf("DeleteExpiredRuns consume: %w", err)
	}
	if deleted := summary.Counters().NodesDeleted(); deleted > 0 {
		r.logger.Info("Swept expired run nodes", "deleted", deleted, "retention_days", retentionDays)
	}
	return nil
}
