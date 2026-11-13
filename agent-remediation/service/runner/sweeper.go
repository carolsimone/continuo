package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

const (
	// sweepInterval is how often the sweeper looks for remediations to end.
	sweepInterval = time.Minute
	// sweepBatch is how many remediations one list of a pass reads.
	sweepBatch = 50
	// stepFailureLimit is how many times a step, or a remediation before any
	// step could run, may fail before the remediation is ended.
	stepFailureLimit = 5
)

// Sweeper ends the remediations that can make no more progress and that no
// worker is driving: one with a step that failed five times, one that failed
// five times in a row before a step could run, and one whose try is still
// drafting or checking past its deadline. It acts on a remediation only after
// it claimed it, so one that a worker or the gate poller is on is left to
// them. A worker ends the remediation of a drive that failed itself, by the
// same rules.
type Sweeper struct {
	newUoW   func() uow.UnitOfWork
	repo     func() repository.RemediationRepository
	owner    string
	clock    ports.Clock
	logger   *slog.Logger
	interval time.Duration
}

// NewSweeper builds a sweeper. repo returns the remediation repository bound to
// the database, newUoW opens the transaction that ends a remediation, and owner
// names the sweeper's lease.
func NewSweeper(newUoW func() uow.UnitOfWork, repo func() repository.RemediationRepository, owner string, clock ports.Clock, logger *slog.Logger) *Sweeper {
	if clock == nil {
		clock = ports.SystemClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Sweeper{newUoW: newUoW, repo: repo, owner: owner, clock: clock, logger: logger, interval: sweepInterval}
}

// Run sweeps at once and then every minute, until ctx ends.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("sweep the remediations", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Sweep makes one pass over the remediations that have a step, or whose own
// row has failures, past the failure limit and those whose try is past its
// deadline. It tries each of them even when one fails, and returns every error
// joined.
func (s *Sweeper) Sweep(ctx context.Context) error {
	now := s.clock.Now()
	repo := s.repo()
	failing, err := repo.ListFailingSteps(ctx, stepFailureLimit, sweepBatch)
	if err != nil {
		return fmt.Errorf("list the remediations with failing steps: %w", err)
	}
	late, err := repo.ListPastDeadline(ctx, now, sweepBatch)
	if err != nil {
		return fmt.Errorf("list the remediations past their deadline: %w", err)
	}
	var errs []error
	seen := map[uuid.UUID]bool{}
	for _, id := range append(failing, late...) {
		if seen[id] {
			continue
		}
		seen[id] = true
		if ctx.Err() != nil {
			break
		}
		if err := s.sweep(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("remediation %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// sweep ends the remediation when it can claim it and, read again under the
// claim, it still has a reason to end.
func (s *Sweeper) sweep(ctx context.Context, id uuid.UUID) error {
	claimed, err := s.repo().Claim(ctx, id, s.owner, s.clock.Now(), leaseDuration)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		return nil
	}
	defer func() {
		rctx, release := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer release()
		if err := s.repo().ReleaseLease(rctx, id, s.owner); err != nil {
			s.logger.Warn("could not let go of a remediation's lease; it ends by itself", "remediation", id, "error", err)
		}
	}()
	_, err = stuckEnder{newUoW: s.newUoW, clock: s.clock, logger: s.logger}.endIfStuck(ctx, id, s.owner)
	return err
}
