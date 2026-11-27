package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// acceptExisting decides, in u's transaction, what a trigger for a release
// and round that already has a remediation does. It locks the remediation's
// row first, so two redrives of one dead letter never both resume it: the
// second reads what the first committed.
//   - A remediation that runs or is done is left as it is: the message is a
//     duplicate.
//   - A system failure whose release has a remediation of a later round is
//     left as it is: an operator already started the next round.
//   - Any other system failure is resumed on the message (resume).
//
// It reports whether it resumed the remediation.
func (r *Runner) acceptExisting(ctx context.Context, u uow.UnitOfWork, t remediation.Trigger, a Accepted) (bool, error) {
	repo := u.RemediationRepo()
	snap, found, err := repo.LockByRelease(ctx, t.TenantID, t.ReleaseID, t.Round)
	if err != nil {
		return false, fmt.Errorf("lock the remediation of release %s round %d: %w", t.ReleaseID, t.Round, err)
	}
	if !found {
		return false, fmt.Errorf("release %s round %d has a remediation that could not be found", t.ReleaseID, t.Round)
	}
	rem := snap.Remediation
	if rem.Status != remediation.StatusSystemFailure {
		r.logger.Info("heal.triggered trigger for a release round that already has a remediation — nothing to start",
			"message_id", a.MessageID, "remediation", rem.ID, "release", t.ReleaseID, "round", t.Round, "status", rem.Status)
		return false, nil
	}
	later, err := repo.HasLaterRound(ctx, t.TenantID, t.ReleaseID, t.Round)
	if err != nil {
		return false, fmt.Errorf("look for a later round of release %s: %w", t.ReleaseID, err)
	}
	if later {
		r.logger.Info("redriven heal.triggered trigger for a round a later round replaced — nothing resumed",
			"message_id", a.MessageID, "remediation", rem.ID, "release", t.ReleaseID, "round", t.Round)
		return false, nil
	}
	if err := r.resume(ctx, u, snap, a); err != nil {
		return false, fmt.Errorf("resume remediation %s: %w", rem.ID, err)
	}
	return true, nil
}

// resume moves a remediation that ended as a system failure back to running,
// in u's transaction, answering to the redriven message a. The remediation
// loses its failure and the failures its own row counted, and records a's
// message id and stream fields as its trigger's. Its current try, when it has
// one still open, goes on where it stopped: every step row that has not
// finished has its failures reset, a drafting or checking try gets its
// status's full time again from now, and the try's fix row goes back in
// flight (reopenFixRow). The try keeps every row it committed, so the next
// drive or poll continues from its first step with no done row, and no step
// that committed runs again. A remediation with no try, or whose try failed,
// has nothing else to change: the next drive opens its next try.
func (r *Runner) resume(ctx context.Context, u uow.UnitOfWork, snap remediation.Snapshot, a Accepted) error {
	now := r.clock.Now()
	repo := u.RemediationRepo()
	rem := snap.Remediation
	if !rem.Status.CanMoveTo(remediation.StatusRunning) {
		return fmt.Errorf("remediation cannot move from %s to %s", rem.Status, remediation.StatusRunning)
	}
	rem.Status = remediation.StatusRunning
	rem.Failure = ""
	rem.TriggerMessageID = a.MessageID
	rem.TriggerFields = a.Fields
	rem.UpdatedAt = now
	if err := repo.SaveRemediation(ctx, rem); err != nil {
		return fmt.Errorf("save remediation: %w", err)
	}
	if err := repo.ClearFailures(ctx, rem.ID); err != nil {
		return fmt.Errorf("clear the failures of the remediation: %w", err)
	}
	t := snap.Try
	if t == nil || t.Status == remediation.TryPassed || t.Status == remediation.TryFailed {
		return nil
	}
	for _, st := range snap.Steps {
		if st.Status == remediation.StepDone || (st.Failures == 0 && st.LastError == "") {
			continue
		}
		st.Failures, st.LastError = 0, ""
		if err := repo.SaveStep(ctx, st); err != nil {
			return fmt.Errorf("reset the failures of step %s: %w", st.Step, err)
		}
	}
	reopenTo := fix.StatusGenerating
	switch t.Status {
	case remediation.TryDrafting, remediation.TryChecking:
		restarted := *t
		restarted.DeadlineAt = now.Add(tryOpenTime(t.Status))
		restarted.UpdatedAt = now
		if err := repo.SaveTry(ctx, restarted); err != nil {
			return fmt.Errorf("restart the deadline of try %d: %w", t.Try, err)
		}
	case remediation.TryVerifying:
		reopenTo = fix.StatusVerifying
	}
	return r.reopenFixRow(ctx, u, snap, reopenTo)
}

// tryOpenTime is how long a try may stay drafting or checking.
func tryOpenTime(status remediation.TryStatus) time.Duration {
	if status == remediation.TryChecking {
		return tryCheckingTime
	}
	return tryDraftingTime
}

// reopenFixRow moves the fix row of snap's try, which the system failure
// failed, back to status to, in u's transaction: a row reopened generating
// takes back the per-node outcomes the failure rewrote, which the fix
// repository undoes, and a row reopened verifying the outcomes the try's
// clusters give (nodeOutcomes), which the clusters still give, since nothing
// changes them once the try is verifying, the message of each coded outcome
// cleaned as every row the runner writes has it. A row that is not failed already
// says how its try ended and is left as it is: the drive or the poll that
// follows acts on it as it finds it.
func (r *Runner) reopenFixRow(ctx context.Context, u uow.UnitOfWork, snap remediation.Snapshot, to fix.Status) error {
	rem := snap.Remediation
	try := snap.Try.Try
	var outcomes map[string]fix.NodeOutcome
	if to == fix.StatusVerifying {
		outcomes = fix.CleanedOutcomes(nodeOutcomes(snap.Clusters))
	}
	reopened, err := u.FixRepo().ReopenFixRow(ctx, rem.ID, try, to, outcomes)
	if err != nil {
		return fmt.Errorf("reopen the row of try %d: %w", try, err)
	}
	if !reopened {
		r.logger.Warn("the resumed try's fix row is not failed; it is left as it is",
			"remediation", rem.ID, "release", rem.ReleaseID, "round", rem.Round, "try", try)
	}
	return nil
}
