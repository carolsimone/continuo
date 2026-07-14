// Package snapshotsvc owns the application-layer composition of snapshot
// selection and materialisation. It is named snapshotsvc rather than snapshot
// to avoid an import-name collision with the domain package of the same name.
package snapshotsvc

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
)

// Service composes snapshot.Selector → snapshot.SnapshotWriter inside a single
// Neo4j write transaction (provided by snapshot.TxRunner).
type Service struct {
	runner        snapshot.TxRunner
	cancelledRepo repository.CancelledSchedulesRepository
	logger        *slog.Logger
}

// NewService constructs a Service. cancelledRepo lets the snapshot stamp the
// :Run terminal on create when the schedule was already cancelled (closing the
// cancel-before-snapshot race).
func NewService(runner snapshot.TxRunner, cancelledRepo repository.CancelledSchedulesRepository, logger *slog.Logger) *Service {
	return &Service{runner: runner, cancelledRepo: cancelledRepo, logger: logger}
}

// Snapshot runs the full selector → write pipeline. Returns the projection so
// handlers can build outbox events without re-reading Neo4j. Sentinel errors
// (snapshot.ErrEmptyProjection, snapshot.ErrTargetNotFound) propagate unchanged.
func (s *Service) Snapshot(ctx context.Context, p snapshot.Params) ([]snapshot.TaskProjection, error) {
	var projection []snapshot.TaskProjection
	err := s.runner.Run(ctx, func(r snapshot.TopologyReader, w snapshot.SnapshotWriter) error {
		// Derived runs (rerun/rebase) inherit their operation from the source
		// :Run so a re-run of a build re-runs `dbt build` (models + tests),
		// not a bare `dbt run`. Fresh runs and single-node runs carry their own
		// Operation in Params already, so they are excluded by the Kind guard.
		if p.SourceRunID != nil && (p.Kind == "rerun" || p.Kind == "rebase") {
			op, opErr := r.SourceRunOperation(ctx, p.SourceRunID.String())
			if opErr != nil {
				return fmt.Errorf("snapshot: source operation: %w", opErr)
			}
			p.Operation = op
		}
		sel, err := p.Selector.SelectTasks(ctx, r, p)
		if err != nil {
			return err
		}
		if len(sel) == 0 {
			return snapshot.ErrEmptyProjection
		}
		projection = sel
		cancelled, err := s.scheduleCancelled(ctx, p.RunID)
		if err != nil {
			return err
		}
		p.Cancelled = cancelled
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

// scheduleCancelled reports whether the run's schedule is already in the
// cancelled_schedules guard. run_id is the schedule_id; when it is set, the
// snapshot stamps the :Run terminal on create so a quick-cancelled run never
// enters the active set even if its run.finalized:v1 projection raced ahead of
// this snapshot and found no :Run to finalize.
func (s *Service) scheduleCancelled(ctx context.Context, runID string) (bool, error) {
	scheduleID, err := uuid.Parse(runID)
	if err != nil {
		return false, fmt.Errorf("snapshotsvc: parse run_id %q: %w", runID, err)
	}
	return s.cancelledRepo.Exists(ctx, scheduleID)
}
