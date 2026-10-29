package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/run-controller/service/uow"
)

// Pruner deletes the command requests too old to answer a retry.
type Pruner struct {
	newUoW func() uow.UnitOfWork
}

// NewPruner builds the pruner.
func NewPruner(newUoW func() uow.UnitOfWork) *Pruner { return &Pruner{newUoW: newUoW} }

// Prune deletes the command requests of every tenant created more than
// retention ago and returns how many it deleted. It has the shape of a
// pkg/outbox retention target's prune function; limit is not used, because the
// delete is one statement over an index on the age and the rows are small.
func (p *Pruner) Prune(ctx context.Context, retention time.Duration, _ int) (int64, error) {
	n, err := p.newUoW().CommandRequests().DeleteOlderThan(ctx, retention)
	if err != nil {
		return 0, fmt.Errorf("prune the command requests: %w", err)
	}
	return n, nil
}
