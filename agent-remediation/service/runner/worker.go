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
	// workerRetryDelay is how long a worker waits after a drive that ended in
	// an error, before it claims again. The failures a step counts are what end
	// a remediation, so a failing step must not use them up in a burst.
	workerRetryDelay = 10 * time.Second
	// releaseTimeout bounds the call that lets go of a lease, which runs even
	// when the worker is shutting down.
	releaseTimeout = 5 * time.Second
)

// Worker claims running remediations one at a time and drives each of them. It
// holds a remediation's lease for the whole drive, renewing it while a long
// step runs, and lets it go when the drive ends. Remediations whose try is
// verifying belong to the gate's poller and are never claimed.
type Worker struct {
	r      *Runner
	repo   func() repository.RemediationRepository
	owner  string
	clock  ports.Clock
	logger *slog.Logger
	wake   chan struct{}

	// drive is what runs a claimed remediation: the runner's Drive.
	drive      func(ctx context.Context, id uuid.UUID) error
	heartbeat  time.Duration
	idleWait   time.Duration
	retryDelay time.Duration
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
		r:          r,
		repo:       repo,
		owner:      owner,
		clock:      clock,
		logger:     logger,
		wake:       make(chan struct{}, 1),
		drive:      r.Drive,
		heartbeat:  leaseDuration / 3,
		idleWait:   workerIdleWait,
		retryDelay: workerRetryDelay,
	}
}

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
			w.logger.Error("remediation drive failed; it is run again after a pause", "remediation", id, "owner", w.owner, "error", err)
			sleep(ctx, w.retryDelay)
		}
	}
}

// driveClaimed drives the remediation the worker has claimed, renewing its
// lease while it runs, and lets go of the lease. It returns the drive's error,
// and nil when the drive ended because the lease was lost or ctx ended, since
// neither says anything about the remediation.
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

	rctx, release := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer release()
	if rerr := w.repo().ReleaseLease(rctx, id, w.owner); rerr != nil {
		w.logger.Warn("could not let go of a remediation's lease; it ends by itself", "remediation", id, "error", rerr)
	}
	if lost.Load() || ctx.Err() != nil {
		return nil
	}
	return err
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

// sleep waits for d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
