// Package reconciler converges the Neo4j :Run projection to state's
// authoritative run status. It is the ordering-independent backstop for
// finalizations that were missed or raced ahead of a run's snapshot: any active
// :Run whose state row is already terminal is finalized on the next tick.
package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// ActiveRunLister returns in-flight runs (Neo4j :Run with completed_at IS NULL).
type ActiveRunLister interface {
	ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error)
}

// RunStatusReader reads a run's terminal status from the state service.
type RunStatusReader interface {
	GetTerminalStatus(ctx context.Context, runID string) (status string, terminal bool, err error)
}

// RunFinalizer projects a terminal status onto the Neo4j :Run node.
type RunFinalizer interface {
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
}

// Reconciler periodically finalizes any active :Run that state already
// considers terminal. It only acts on runs that exist in Neo4j and are terminal
// in state, so retention-deleted or never-snapshotted runs are never touched.
type Reconciler struct {
	runs      ActiveRunLister
	status    RunStatusReader
	finalizer RunFinalizer
	interval  time.Duration
	logger    *slog.Logger
}

// New constructs a Reconciler. intervalSecs is the sweep cadence in seconds.
func New(runs ActiveRunLister, status RunStatusReader, finalizer RunFinalizer, intervalSecs int, logger *slog.Logger) *Reconciler {
	return &Reconciler{
		runs:      runs,
		status:    status,
		finalizer: finalizer,
		interval:  time.Duration(intervalSecs) * time.Second,
		logger:    logger,
	}
}

// Start runs the reconcile loop until ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	r.logger.Info("Reconciler started", "interval", r.interval)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			tickCtx, cancel := context.WithTimeout(ctx, r.tickTimeout())
			r.tick(tickCtx)
			cancel()
		case <-ctx.Done():
			r.logger.Info("Reconciler stopped")
			return
		}
	}
}

// tickTimeout bounds a single sweep so a slow state service never stalls the
// reconcile loop across ticks. It is the interval less a small margin, capped
// so the deadline is always positive.
func (r *Reconciler) tickTimeout() time.Duration {
	d := r.interval - time.Second
	if d <= 0 {
		d = r.interval
	}
	if d <= 0 {
		d = 5 * time.Second
	}
	return d
}

// tick finalizes every active :Run that state already considers terminal.
// Per-run errors are logged and skipped so one bad run never aborts the sweep.
func (r *Reconciler) tick(ctx context.Context) {
	active, err := r.runs.ListActiveRuns(ctx)
	if err != nil {
		r.logger.Error("Reconciler: list active runs failed", "error", err)
		return
	}
	reconciled := 0
	for _, run := range active {
		st, terminal, err := r.status.GetTerminalStatus(ctx, run.RunID)
		if err != nil {
			r.logger.Warn("Reconciler: state status read failed", "run_id", run.RunID, "error", err)
			continue
		}
		if !terminal {
			continue
		}
		if err := r.finalizer.FinalizeRun(ctx, run.RunID, st); err != nil {
			r.logger.Error("Reconciler: finalize failed", "run_id", run.RunID, "status", st, "error", err)
			continue
		}
		reconciled++
	}
	if reconciled > 0 {
		r.logger.Info("Reconciler: converged active runs to state-terminal status", "count", reconciled)
	}
}
