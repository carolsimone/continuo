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

const (
	// workerIdleWait is how long a worker with nothing to claim waits before it
	// looks again, unless it is woken.
	workerIdleWait = 2 * time.Second
	// ParkDuration is how long a remediation whose drive ended in an error is
	// kept from every worker: the worker holds its lease that long instead of
	// letting it go. The failures a step counts are what end a remediation, so
	// a failing step must not use them up in a burst, and the oldest
	// remediation must not be claimed again at once.
	ParkDuration = 2 * time.Minute
	// releaseTimeout bounds the call that lets go of a lease, which runs even
	// when the worker is shutting down.
	releaseTimeout = 5 * time.Second
)

// Worker claims running remediations one at a time and drives each of them. It
// holds a remediation's lease for the whole drive, renewing it while a long
// step runs, and lets it go when the drive ends. A drive that ends in an error
// first checks whether the remediation can go no further, and ends it if so;
// otherwise the worker parks it, holding the lease for two more minutes.
// Remediations whose try is verifying belong to the gate's poller and are never
// claimed.
type Worker struct {
	r      *Runner
	repo   func() repository.RemediationRepository
	owner  string
	clock  ports.Clock
	logger *slog.Logger
	wake   chan struct{}

	// drive is what runs a claimed remediation: the runner's Drive. stuck ends
	// the remediation when it can go no further, and reports whether it did.
	drive     func(ctx context.Context, id uuid.UUID) error
	stuck     func(ctx context.Context, id uuid.UUID) (bool, error)
	heartbeat time.Duration
	idleWait  time.Duration
	beat      func()
}

// NewWorker builds a worker over r. repo returns the remediation repository
// bound to the database, and owner is the name r holds its leases under: the
// same one, since r's commits hold the lease the worker claims.
func NewWorker(r *Runner, repo func() repository.RemediationRepository, owner string, clock ports.Clock, logger *slog.Logger) *Worker {
	if clock == nil {
		clock = ports.SystemClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		r:      r,
		repo:   repo,
		owner:  owner,
		clock:  clock,
		logger: logger,
		wake:   make(chan struct{}, 1),
		drive:  r.Drive,
		stuck: func(ctx context.Context, id uuid.UUID) (bool, error) {
			return stuckEnder{newUoW: r.d.NewUoW, clock: clock, logger: logger}.endIfStuck(ctx, id, owner)
		},
		heartbeat: leaseDuration / 3,
		idleWait:  workerIdleWait,
		beat:      func() {},
	}
}

// SetHeartbeat sets the function the worker calls at every iteration of its
// loop and every time it renews a lease, for a liveness probe to read.
func (w *Worker) SetHeartbeat(beat func()) { w.beat = beat }

// Wake ends the wait of an idle worker, so it looks for a remediation now. It
// never blocks.
func (w *Worker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run claims and drives remediations until ctx ends: it claims the oldest
// running remediation whose lease is free, drives it, lets go of the lease and
// claims again. With nothing to claim it waits for Wake or a short time.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		w.beat()
		id, found, err := w.repo().ClaimNext(ctx, w.owner, w.clock.Now(), leaseDuration)
		if err != nil {
			if ctx.Err() == nil {
				w.logger.Error("claim a remediation", "owner", w.owner, "error", err)
			}
			w.idle(ctx)
			continue
		}
		if !found {
			w.idle(ctx)
			continue
		}
		if err := w.driveClaimed(ctx, id); err != nil {
			w.logger.Error("remediation drive failed", "remediation", id, "owner", w.owner, "error", err)
		}
	}
}

// driveClaimed drives the remediation the worker has claimed, renewing its
// lease while it runs, and settles the lease. It returns the drive's error, and
// nil when the drive ended because the lease was lost or ctx ended, since
// neither says anything about the remediation.
//
// After a drive that ended in an error the worker still holds the lease, and
// checks whether the remediation can go no further: a step that failed five
// times, a remediation that failed five times before a step could run, or a
// try past its deadline. If so it is ended and the lease is let go.
// If not, the remediation is parked: the worker holds the lease for two more
// minutes, so no worker claims it before then and a failing step is run again
// at that pace.
func (w *Worker) driveClaimed(ctx context.Context, id uuid.UUID) error {
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		w.renew(dctx, cancel, id, &lost)
	}()
	err := w.drive(dctx, id)
	cancel()
	<-renewed

	rctx, done := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer done()
	if lost.Load() || ctx.Err() != nil {
		w.release(rctx, id)
		return nil
	}
	if err == nil {
		w.release(rctx, id)
		return nil
	}
	ended, serr := w.stuck(rctx, id)
	switch {
	case serr != nil:
		w.logger.Warn("could not check whether a failed remediation can go on; parking it", "remediation", id, "error", serr)
	case ended:
		w.release(rctx, id)
		return err
	}
	if perr := w.repo().Hold(rctx, id, w.owner, w.clock.Now(), ParkDuration); perr != nil {
		w.logger.Warn("could not park a failed remediation", "remediation", id, "error", perr)
	}
	return err
}

// release lets go of the lease on the remediation.
func (w *Worker) release(ctx context.Context, id uuid.UUID) {
	if err := w.repo().ReleaseLease(ctx, id, w.owner); err != nil {
		w.logger.Warn("could not let go of a remediation's lease; it ends by itself", "remediation", id, "error", err)
	}
}

// renew holds the lease every third of its duration, until ctx ends. A lease
// another worker took stops the drive: cancel ends its context, so a step that
// is calling the model returns at once, and the worker never renews again.
func (w *Worker) renew(ctx context.Context, cancel context.CancelFunc, id uuid.UUID, lost *atomic.Bool) {
	ticker := time.NewTicker(w.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		w.beat()
		err := w.repo().Hold(ctx, id, w.owner, w.clock.Now(), leaseDuration)
		switch {
		case errors.Is(err, repository.ErrLeaseLost):
			w.logger.Warn("remediation lease lost while a drive runs; stopping the drive", "remediation", id, "owner", w.owner)
			lost.Store(true)
			cancel()
			return
		case err != nil && ctx.Err() == nil:
			w.logger.Warn("could not renew a remediation's lease; trying again at the next beat", "remediation", id, "error", err)
		}
	}
}

// idle waits until Wake, the idle time or ctx ends.
func (w *Worker) idle(ctx context.Context) {
	t := time.NewTimer(w.idleWait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-w.wake:
	case <-t.C:
	}
}
