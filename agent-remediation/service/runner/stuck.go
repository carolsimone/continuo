package runner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// stuckEnder ends a remediation that can go no further. The runner uses it
// before every step it starts, the worker on the remediation of a drive that
// failed, and the sweeper on the remediations nobody drives.
type stuckEnder struct {
	newUoW func() uow.UnitOfWork
	clock  ports.Clock
	logger *slog.Logger
}

// endIfStuck, in one transaction after holding the lease as owner, reads the
// remediation and, when it is running and stuck, records it as a system
// failure, fails its current try and fails the attempt row of that try. ended
// reports whether it did.
func (e stuckEnder) endIfStuck(ctx context.Context, id uuid.UUID, owner string) (ended bool, err error) {
	u := e.newUoW()
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()
	now := e.clock.Now()
	repo := u.RemediationRepo()
	if err := repo.Hold(ctx, id, owner, now, leaseDuration); err != nil {
		return false, err
	}
	snap, err := repo.Load(ctx, id)
	if err != nil {
		return false, fmt.Errorf("load: %w", err)
	}
	rem := snap.Remediation
	if rem.Status != remediation.StatusRunning {
		return false, nil
	}
	reason := stuckReason(snap, now)
	if reason == "" {
		return false, nil
	}
	if !rem.Status.CanMoveTo(remediation.StatusSystemFailure) {
		return false, fmt.Errorf("remediation cannot move from %s to %s", rem.Status, remediation.StatusSystemFailure)
	}
	rem.Status = remediation.StatusSystemFailure
	rem.Failure = reason
	rem.UpdatedAt = now
	if err := repo.SaveRemediation(ctx, rem); err != nil {
		return false, fmt.Errorf("save remediation: %w", err)
	}
	if t := snap.Try; t != nil && t.Status.CanMoveTo(remediation.TryFailed) {
		failed := *t
		failed.Status = remediation.TryFailed
		failed.UpdatedAt = now
		if err := repo.SaveTry(ctx, failed); err != nil {
			return false, fmt.Errorf("save try: %w", err)
		}
	}
	if err := failAttemptRow(ctx, u.ProposalRepo(), snap, reason); err != nil {
		return false, err
	}
	if err := u.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	e.logger.Error("remediation ended as a system failure", "remediation", id, "release", rem.ReleaseID, "reason", reason)
	return true, nil
}

// endIfStuck applies the stuck rules to run, which the runner read holding the
// lease, before it starts a step on the remediation or moves its try on. When
// one holds at the runner's clock, the remediation is ended through the path
// the worker and the sweeper use: one transaction that holds the lease, reads
// the remediation again and, when it is still stuck, records the system
// failure, fails the try and fails the attempt's row. ended reports whether it
// was ended. A lost lease is repository.ErrLeaseLost, with nothing written.
func (r *Runner) endIfStuck(ctx context.Context, run tryRun) (ended bool, err error) {
	if stuckReason(run.snap, r.clock.Now()) == "" {
		return false, nil
	}
	return stuckEnder{newUoW: r.d.NewUoW, clock: r.clock, logger: r.logger}.endIfStuck(ctx, run.snap.Remediation.ID, r.d.Owner)
}

// failAttemptRow fails the proposal row of the current try's attempt with
// reason, by what the row is: an attempt still generating is failed, and an
// attempt whose runs are still being judged is failed as a verification that
// did not pass, with the runs it recorded. A try with no row of its own leaves
// the release's in-flight row failed, if it has one. Any other row already
// says how its attempt ended.
func failAttemptRow(ctx context.Context, repo repository.ProposalRepository, snap remediation.Snapshot, reason string) error {
	releaseID := snap.Remediation.ReleaseID
	if snap.Try != nil {
		views, err := repo.List(ctx, repository.ProposalFilter{ReleaseID: releaseID})
		if err != nil {
			return fmt.Errorf("list the proposals of release %s: %w", releaseID, err)
		}
		for _, v := range views {
			if v.Attempt != snap.Try.Attempt {
				continue
			}
			switch v.Status {
			case proposal.StatusVerifying:
				if _, err := repo.MarkVerifyFailed(ctx, v.ID, reason, v.Verifications); err != nil {
					return fmt.Errorf("fail the verifying attempt row: %w", err)
				}
				return nil
			case proposal.StatusGenerating:
				if _, err := repo.FailGenerating(ctx, releaseID, reason); err != nil {
					return fmt.Errorf("fail the in-flight attempt row: %w", err)
				}
				return nil
			}
			return nil
		}
	}
	if _, err := repo.FailGenerating(ctx, releaseID, reason); err != nil {
		return fmt.Errorf("fail the in-flight attempt row: %w", err)
	}
	return nil
}

// stuckReason says why the remediation can make no more progress, or "" when it
// can: a step of its current try that has not finished and failed at least
// stepFailureLimit times, a remediation that failed that many times in a row
// before a step could run (it could not be loaded, or its next try could not
// open), or a drafting or checking try past its deadline.
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
	if rem := snap.Remediation; rem.Failures >= stepFailureLimit {
		reason := fmt.Sprintf("the remediation failed %d times in a row before a step could run", rem.Failures)
		if rem.LastError != "" {
			reason += ", the last time with: " + rem.LastError
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
