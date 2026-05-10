// Package snapshotsvc owns the application-layer composition of snapshot
// selection and materialisation. It is named snapshotsvc rather than snapshot
// to avoid an import-name collision with the domain package of the same name.
package snapshotsvc

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
)

// Service composes snapshot.Selector → snapshot.SnapshotWriter inside a single
// Neo4j write transaction (provided by snapshot.TxRunner).
type Service struct {
	runner snapshot.TxRunner
	logger *slog.Logger
}

// NewService constructs a Service.
func NewService(runner snapshot.TxRunner, logger *slog.Logger) *Service {
	return &Service{runner: runner, logger: logger}
}

// Snapshot runs the full selector → write pipeline. Returns the projection so
// handlers can build outbox events without re-reading Neo4j. Sentinel errors
// (snapshot.ErrEmptyProjection, snapshot.ErrTargetNotFound) propagate unchanged.
func (s *Service) Snapshot(ctx context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
	var projection []snapshot.TaskProjection
	err := s.runner.Run(ctx, func(r snapshot.TopologyReader, w snapshot.SnapshotWriter) error {
		sel, err := p.Selector.SelectTasks(ctx, r, p)
		if err != nil {
			return err
		}
		if len(sel) == 0 {
			return snapshot.ErrEmptyProjection
		}
		projection = sel
		return w.WriteRunAndExecutesEdges(ctx, p, sel)
	})
	if err != nil {
		return nil, err
	}
	s.logger.Info("Snapshot created",
		"run_id", p.RunID,
		"schedule_name", p.ScheduleName,
		"kind", p.Kind,
		"tasks", len(projection),
	)
	return projection, nil
}
