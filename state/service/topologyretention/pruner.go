// Package topologyretention deletes the promoted topologies state no longer
// needs.
package topologyretention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/state/service/uow"
)

// Pruner deletes old promoted releases under the topology lock.
type Pruner struct {
	uowFactory func() uow.UnitOfWork
	logger     *slog.Logger
}

// NewPruner builds the pruner.
func NewPruner(uowFactory func() uow.UnitOfWork, logger *slog.Logger) *Pruner {
	return &Pruner{uowFactory: uowFactory, logger: logger}
}

// Prune deletes, with their nodes and edges, the releases loaded more than
// retention ago that are neither live nor named by a pending or running run,
// and returns how many it deleted. It has the shape of a pkg/outbox retention
// target's prune function; limit is not used, because one statement deletes a
// release with its rows and releases are few. It holds the topology lock in a
// transaction of its own, so it never interleaves with a promotion moving the
// live release or with a seed reconcile pass.
func (p *Pruner) Prune(ctx context.Context, retention time.Duration, _ int) (int64, error) {
	u := p.uowFactory()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin the topology prune: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	if err := u.Topology().LockTopology(ctx); err != nil {
		return 0, fmt.Errorf("lock the topology: %w", err)
	}
	cutoff := u.Clock().Now().Add(-retention)
	n, err := u.Topology().PruneReleases(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune the releases loaded before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	if err := u.Commit(); err != nil {
		return 0, fmt.Errorf("commit the topology prune: %w", err)
	}
	committed = true
	return n, nil
}
