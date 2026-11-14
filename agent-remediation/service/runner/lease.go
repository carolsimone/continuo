package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// leaseKeeper keeps a remediation's lease while work on it runs: the worker's
// drive and the gate poller's poll. The remediation was claimed under owner,
// the name the runner's commits hold the lease as, and repo is the remediation
// repository bound to the database.
type leaseKeeper struct {
	repo      func() repository.RemediationRepository
	owner     string
	clock     ports.Clock
	heartbeat time.Duration
	beat      func()
	logger    *slog.Logger
}

// holdWhile runs fn on the remediation id and holds its lease again every
// heartbeat while fn runs, so work that outlasts one lease keeps it. A lease
// another process took ends fn's context at once, so a model call or a status
// read returns, and is never held again: lost is then true. A hold that fails
// for another reason is tried again at the next heartbeat, with fn left
// running. beat is called at every heartbeat. holdWhile returns once fn has
// returned and the holds have stopped, with fn's error.
func (k leaseKeeper) holdWhile(ctx context.Context, id uuid.UUID, fn func(ctx context.Context) error) (lost bool, err error) {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lostLease atomic.Bool
	held := make(chan struct{})
	go func() {
		defer close(held)
		k.renew(wctx, cancel, id, &lostLease)
	}()
	err = fn(wctx)
	cancel()
	<-held
	return lostLease.Load(), err
}

// renew holds the lease every heartbeat until ctx ends. A lease another
// process took is recorded in lost and ends ctx through cancel, and renew
// stops.
func (k leaseKeeper) renew(ctx context.Context, cancel context.CancelFunc, id uuid.UUID, lost *atomic.Bool) {
	ticker := time.NewTicker(k.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		k.beat()
		err := k.repo().Hold(ctx, id, k.owner, k.clock.Now(), leaseDuration)
		switch {
		case errors.Is(err, repository.ErrLeaseLost):
			k.logger.Warn("remediation lease lost while work on it runs; stopping the work", "remediation", id, "owner", k.owner)
			lost.Store(true)
			cancel()
			return
		case err != nil && ctx.Err() == nil:
			k.logger.Warn("could not renew a remediation's lease; trying again at the next beat", "remediation", id, "error", err)
		}
	}
}
