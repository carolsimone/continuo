package runner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

const (
	// pollBatch is how many verifying remediations one pass of the gate poller
	// reads.
	pollBatch = 20
	// defaultPollInterval paces the gate poller when it is given no interval.
	defaultPollInterval = 15 * time.Second
)

// GatePoller reads the gate's verdict on every remediation whose try is
// verifying. It takes a remediation's lease before it polls, so it never
// polls one a worker is on, holds it again every third of its duration while
// the poll runs, as the worker does while it drives, and lets the lease go
// afterwards.
type GatePoller struct {
	r        *Runner
	repo     func() repository.RemediationRepository
	owner    string
	interval time.Duration
	wake     func()
	clock    ports.Clock
	logger   *slog.Logger
	beat     func()
	// heartbeat is how often the lease of the remediation being polled is
	// held again.
	heartbeat time.Duration
}

// NewGatePoller builds a poller over r. repo returns the remediation
// repository bound to the database, and owner is the name r holds its leases
// under. wake is called after each remediation it polled, since a failed
// verification opens the next try for the worker.
func NewGatePoller(r *Runner, repo func() repository.RemediationRepository, owner string, interval time.Duration, wake func(), clock ports.Clock, logger *slog.Logger) *GatePoller {
	if interval <= 0 {
		interval = defaultPollInterval
	}
	if wake == nil {
		wake = func() {}
	}
	if clock == nil {
		clock = ports.SystemClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &GatePoller{r: r, repo: repo, owner: owner, interval: interval, wake: wake, clock: clock, logger: logger, beat: func() {}, heartbeat: leaseDuration / 3}
}

// SetHeartbeat sets the function the poller calls at every pass, for every
// remediation it polls and every time it renews a lease, for a liveness probe
// to read.
func (p *GatePoller) SetHeartbeat(beat func()) { p.beat = beat }

// Run polls at once and then every interval, until ctx ends.
func (p *GatePoller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		if err := p.Poll(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("poll the gates of the verifying remediations", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Poll makes one pass: it lists the oldest verifying remediations, and for each
// one it can claim it polls the gate while it keeps the lease, lets go of the
// lease and wakes the worker. A remediation another process holds is left to
// it, and a poll whose lease another process takes is stopped at once with
// nothing written. An error from one remediation is logged, and the pass goes
// on to the next; the returned error is the listing's.
func (p *GatePoller) Poll(ctx context.Context) error {
	p.beat()
	snaps, err := p.repo().ListVerifying(ctx, pollBatch)
	if err != nil {
		return fmt.Errorf("list the verifying remediations: %w", err)
	}
	for _, snap := range snaps {
		if ctx.Err() != nil {
			return nil
		}
		p.beat()
		id := snap.Remediation.ID
		claimed, err := p.repo().Claim(ctx, id, p.owner, p.clock.Now(), leaseDuration)
		if err != nil {
			p.logger.Error("claim a verifying remediation", "remediation", id, "error", err)
			continue
		}
		if !claimed {
			continue
		}
		lost, err := p.lease().holdWhile(ctx, id, func(pctx context.Context) error {
			return p.r.PollGate(pctx, snap)
		})
		if err != nil && !lost {
			p.logger.Error("poll a remediation's gate", "remediation", id, "error", err)
		}
		rctx, release := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		if err := p.repo().ReleaseLease(rctx, id, p.owner); err != nil {
			p.logger.Warn("could not let go of a remediation's lease; it ends by itself", "remediation", id, "error", err)
		}
		release()
		p.wake()
	}
	return nil
}

// lease is what keeps the lease of the remediation the poller polls: held
// again every heartbeat, with a beat each time.
func (p *GatePoller) lease() leaseKeeper {
	return leaseKeeper{repo: p.repo, owner: p.owner, clock: p.clock, heartbeat: p.heartbeat, beat: p.beat, logger: p.logger}
}
