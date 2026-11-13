package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

const (
	// sweepInterval is how often the sweeper looks for remediations to end.
	sweepInterval = time.Minute
	// sweepBatch is how many remediations one list of a pass reads.
	sweepBatch = 50
	// stepFailureLimit is how many times a step may fail before the sweeper ends
	// its remediation.
	stepFailureLimit = 5
)

// Sweeper ends the remediations that can make no more progress: one with a step
// that failed five times, and one whose try is still drafting or checking past
// its deadline. It acts on a remediation only after it claimed it, so one that
// a worker or the gate poller is on is left to them. A remediation it ends is
// recorded as a system failure with the reason, and the in-flight attempt row
// of its release is failed with the same reason, in one transaction.
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

// Sweep makes one pass over the remediations that have a step past its failure
// limit and those whose try is past its deadline. It tries each of them even
// when one fails, and returns every error joined.
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
	return s.endIfStuck(ctx, id)
}

// endIfStuck, in one transaction after holding the lease, reads the remediation
// and, when it is running and stuck, records it as a system failure, fails its
// current try and fails the in-flight attempt row of its release.
func (s *Sweeper) endIfStuck(ctx context.Context, id uuid.UUID) error {
	u := s.newUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	now := s.clock.Now()
	repo := u.RemediationRepo()
	if err := repo.Hold(ctx, id, s.owner, now, leaseDuration); err != nil {
		return err
	}
	snap, err := repo.Load(ctx, id)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	rem := snap.Remediation
	if rem.Status != remediation.StatusRunning {
		return nil
	}
	reason := stuckReason(snap, now)
	if reason == "" {
		return nil
	}
	if !rem.Status.CanMoveTo(remediation.StatusSystemFailure) {
		return fmt.Errorf("remediation cannot move from %s to %s", rem.Status, remediation.StatusSystemFailure)
	}
	rem.Status = remediation.StatusSystemFailure
	rem.Failure = reason
	rem.UpdatedAt = now
	if err := repo.SaveRemediation(ctx, rem); err != nil {
		return fmt.Errorf("save remediation: %w", err)
	}
	if t := snap.Try; t != nil && t.Status.CanMoveTo(remediation.TryFailed) {
		failed := *t
		failed.Status = remediation.TryFailed
		failed.UpdatedAt = now
		if err := repo.SaveTry(ctx, failed); err != nil {
			return fmt.Errorf("save try: %w", err)
		}
	}
	if _, err := u.ProposalRepo().FailGenerating(ctx, rem.ReleaseID, reason); err != nil {
		return fmt.Errorf("fail the in-flight attempt row: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	s.logger.Error("remediation ended as a system failure", "remediation", id, "release", rem.ReleaseID, "reason", reason)
	return nil
}

// stuckReason says why the remediation can make no more progress, or "" when it
// can: a step of its current try that has not finished and failed at least
// stepFailureLimit times, or a drafting or checking try past its deadline.
func stuckReason(snap remediation.Snapshot, now time.Time) string {
	for _, st := range snap.Steps {
		if st.Status == remediation.StepDone || st.Failures < stepFailureLimit {
			continue
		}
		reason := fmt.Sprintf("step %q failed %d times", st.Step, st.Failures)
		if st.LastError != "" {
			reason += ", the last time with: " + st.LastError
		}
		return reason
	}
	t := snap.Try
	if t == nil || !t.DeadlineAt.Before(now) {
		return ""
	}
	switch t.Status {
	case remediation.TryDrafting, remediation.TryChecking:
		return fmt.Sprintf("try %d was still %s at its deadline, %s", t.Try, t.Status, t.DeadlineAt.UTC().Format(time.RFC3339))
	}
	return ""
}
