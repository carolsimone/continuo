package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// gateStep returns the flow's gate: one long check, which starts the
// verification of a try and answers later. The name of the gate's first step
// is returned with any error, for the failure to be counted on its row.
func (r *Runner) gateStep(flow workflow.Flow) (workflow.StepName, step.LongCheck, error) {
	if len(flow.Gate) == 0 {
		return "", nil, fmt.Errorf("flow %q has no gate", flow.Name)
	}
	name := flow.Gate[0].Name
	if len(flow.Gate) > 1 {
		return name, nil, fmt.Errorf("flow %q has %d gate steps, and the runner runs a gate of one long check", flow.Name, len(flow.Gate))
	}
	g, err := lookup[step.LongCheck](r.d.Registry, name, step.KindCheck)
	return name, g, err
}

// gateInput is what the gate reads for the try: its drafts and the proposal
// row of its attempt. submittedAt is the start of the gate step's run, zero
// for the submit itself.
func gateInput(run tryRun, drafts []step.ClusterDraft, proposalID string, submittedAt time.Time) step.GateInput {
	return step.GateInput{
		Remediation: run.snap.Remediation,
		Try:         *run.snap.Try,
		Trigger:     run.trigger,
		Drafts:      drafts,
		ProposalID:  proposalID,
		SubmittedAt: submittedAt,
	}
}

// submitGate submits the try's drafts to the flow's gate and commits its
// answer with the gate's row. A gate that started its runs leaves the try
// verifying: the attempt's proposal row records the runs, the edits and the
// composed rationale, and the gate's row waits on the runs. A gate that
// refused the drafts fails the attempt with its first finding and ends the
// remediation.
func (r *Runner) submitGate(ctx context.Context, run tryRun, checked []checkedCluster) error {
	name, gate, err := r.gateStep(run.flow)
	row := r.newRow(run, flowCluster, string(name), string(step.KindCheck), flowRound, run.flow.Revision)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	drafts := draftsOf(checked)
	if row.InputDigest, err = draftsDigest(drafts); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	var attempt proposal.View
	if err := r.read(ctx, func(u uow.UnitOfWork) error {
		var err error
		attempt, err = attemptRow(ctx, u.ProposalRepo(), run)
		return err
	}); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	verdict, err := gate.Submit(ctx, gateInput(run, drafts, attempt.ID, time.Time{}))
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", name, err))
	}
	if row.Output, err = json.Marshal(checkRecord{Result: verdict.Result}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", name, err))
	}
	if !verdict.Result.Waiting {
		f, refused := firstFinding([]remediation.CheckResult{verdict.Result})
		if !refused {
			return r.recordFailure(ctx, row, fmt.Errorf("step %s neither started a verification run nor refused the try", name))
		}
		row.Findings = verdict.Result.Findings
		r.finish(&row)
		return r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
			return r.refuseAttempt(ctx, u, run, f.Message)
		})
	}
	row.Status = remediation.StepWaiting
	row.WaitingOn = verdict.Result.RunIDs
	p := verifyingAttempt(run, checked, verdict.Verifications)
	deadline := r.clock.Now().Add(r.d.VerificationTimeout + tryVerifyingSlack)
	if err := r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
		if err := r.recordAttempt(ctx, u, run, run.snap.Try.Attempt, p); err != nil {
			return err
		}
		return r.saveTry(ctx, u, *run.snap.Try, remediation.TryVerifying, deadline)
	}); err != nil {
		return err
	}
	r.logger.Info("the try's drafts are submitted for verification", "remediation", run.snap.Remediation.ID,
		"release", run.trigger.ReleaseID, "try", run.snap.Try.Try, "attempt", run.snap.Try.Attempt, "runs", row.WaitingOn)
	return nil
}

// PollGate reads the gate's verdict on the remediation's verifying try and
// acts on it. The caller has claimed the remediation, which snap names; it is
// loaded again, since a snapshot listed without the lease may be torn. PollGate
// holds the lease, then polls the gate once with the drafts it was submitted,
// rebuilt from the journal, counting unreadable runs from the start of the
// gate step's run.
//
// While the gate waits, each run whose phase changed is written to the
// attempt's proposal row, and nothing else. A passed gate runs the flow's pass
// output, passes the try and ends the remediation, in one transaction. A failed
// gate fails the attempt with the gate's verify error and fails the try, in
// one transaction, and then opens the next try, which hands the failure over
// once the tries are spent. A proposal row that is no longer verifying already
// says how the attempt ended: the try fails and the remediation ends with
// nothing emitted. An error is counted on the gate's row, which keeps waiting,
// and a lost lease returns nil with nothing written.
func (r *Runner) PollGate(ctx context.Context, snap remediation.Snapshot) error {
	id := snap.Remediation.ID
	sctx, cancel := r.stepContext(ctx)
	defer cancel()
	err := r.pollGate(sctx, id)
	if errors.Is(err, repository.ErrLeaseLost) {
		r.logger.Info("remediation lease lost; the gate is polled later", "remediation", id, "owner", r.d.Owner)
		return nil
	}
	return err
}

func (r *Runner) pollGate(ctx context.Context, id uuid.UUID) error {
	run, running, err := r.load(ctx, id)
	if err != nil || !running {
		return err
	}
	if run.snap.Try == nil || run.snap.Try.Status != remediation.TryVerifying {
		return nil
	}
	name, gate, err := r.gateStep(run.flow)
	if err != nil {
		return r.countGateFailure(ctx, run, name, err)
	}
	row, ok := run.journal.row(flowCluster, string(name), flowRound)
	if !ok {
		return r.countGateFailure(ctx, run, name, fmt.Errorf("remediation %s: try %d is verifying with no %s row", id, run.snap.Try.Try, name))
	}
	var attempt proposal.View
	if err := r.commit(ctx, id, func(u uow.UnitOfWork) error {
		var err error
		attempt, err = attemptRow(ctx, u.ProposalRepo(), run)
		return err
	}); err != nil {
		return r.countFailure(ctx, row, err)
	}
	checked, err := r.checkedClusters(run)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	waiting := remediation.CheckResult{Check: string(name), Waiting: true, RunIDs: row.WaitingOn}
	verdict, err := gate.Poll(ctx, gateInput(run, draftsOf(checked), attempt.ID, row.StartedAt), waiting)
	if err != nil {
		return r.countFailure(ctx, row, fmt.Errorf("step %s: %w", name, err))
	}
	switch {
	case verdict.Result.Waiting:
		return r.writePhases(ctx, run, row, attempt, verdict.Phases)
	case verdict.Result.Passed():
		return r.passGate(ctx, run, row, attempt, checked, verdict)
	}
	failed, err := r.failGate(ctx, run, row, attempt, verdict)
	if err != nil || !failed {
		return err
	}
	return r.NextTry(ctx, id)
}

// countGateFailure counts an error that stopped a poll before the gate was
// polled on the gate step's row, which the failures sweep reads. A gate with
// no row yet gets a failed one, and a gate with a row keeps its status. A flow
// with no gate step has no name to count on, so the row is the flow's
// "gate". The error is returned.
func (r *Runner) countGateFailure(ctx context.Context, run tryRun, name workflow.StepName, cause error) error {
	if name == "" {
		name = flowGateRowName
	}
	if row, ok := run.journal.row(flowCluster, string(name), flowRound); ok {
		return r.countFailure(ctx, row, cause)
	}
	row := r.newRow(run, flowCluster, string(name), string(step.KindCheck), flowRound, run.flow.Revision)
	return r.recordFailure(ctx, row, cause)
}

// attemptRow returns the proposal row of the try's attempt.
func attemptRow(ctx context.Context, repo repository.ProposalRepository, run tryRun) (proposal.View, error) {
	views, err := repo.List(ctx, repository.ProposalFilter{ReleaseID: run.trigger.ReleaseID})
	if err != nil {
		return proposal.View{}, fmt.Errorf("list the proposals of release %s: %w", run.trigger.ReleaseID, err)
	}
	for _, v := range views {
		if v.Attempt == run.snap.Try.Attempt {
			return v, nil
		}
	}
	return proposal.View{}, fmt.Errorf("release %s has no proposal row for attempt %d", run.trigger.ReleaseID, run.snap.Try.Attempt)
}

// writePhases records what a poll that read the gate's runs found: each run's
// phase that differs from what the attempt's proposal row records, and the gate
// row's failures and the remediation's own, which a poll that got through
// resets to zero so that only consecutive failed polls add up. It writes in one
// transaction, and writes nothing when no phase differs and neither the row
// nor the remediation has failures.
func (r *Runner) writePhases(ctx context.Context, run tryRun, row remediation.StepRun, attempt proposal.View, phases []proposal.Verification) error {
	changed := changedPhases(attempt.Verifications, phases)
	reset := row.Failures > 0
	if len(changed) == 0 && !reset && !failuresToClear(run) {
		return nil
	}
	cleared := row
	cleared.Failures, cleared.LastError = 0, ""
	err := r.commit(ctx, row.RemediationID, func(u uow.UnitOfWork) error {
		if err := r.clearFailures(ctx, u, run); err != nil {
			return err
		}
		if reset {
			if err := u.RemediationRepo().SaveStep(ctx, cleared); err != nil {
				return fmt.Errorf("save step %s: %w", cleared.Step, err)
			}
		}
		for _, p := range changed {
			if err := u.ProposalRepo().UpdateVerificationPhase(ctx, attempt.ID, p.RunID, p.Phase, p.ActivatedAt); err != nil {
				return fmt.Errorf("record the phase of run %s: %w", p.RunID, err)
			}
		}
		return nil
	})
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	return nil
}

// changedPhases is the runs read whose phase or activation time is not the one
// stored for them.
func changedPhases(stored, read []proposal.Verification) []proposal.Verification {
	byRun := make(map[string]proposal.Verification, len(stored))
	for _, v := range stored {
		byRun[v.RunID] = v
	}
	var out []proposal.Verification
	for _, v := range read {
		s, ok := byRun[v.RunID]
		if ok && s.Phase == v.Phase && sameTime(s.ActivatedAt, v.ActivatedAt) {
			continue
		}
		out = append(out, v)
	}
	return out
}

// sameTime reports whether two optional times are both absent or equal.
func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// mergeVerifications is the stored runs with each run read in this poll in
// place of its stored entry, so a run the poll could not read keeps what was
// recorded for it. A run read that is not stored is added after them.
func mergeVerifications(stored, read []proposal.Verification) []proposal.Verification {
	byRun := make(map[string]proposal.Verification, len(read))
	for _, v := range read {
		byRun[v.RunID] = v
	}
	out := make([]proposal.Verification, 0, len(stored))
	for _, v := range stored {
		if got, ok := byRun[v.RunID]; ok {
			v = got
			delete(byRun, v.RunID)
		}
		out = append(out, v)
	}
	for _, v := range read {
		if _, ok := byRun[v.RunID]; ok {
			out = append(out, v)
		}
	}
	return out
}

// concluded is the gate's row once the gate gave its verdict res.
func (r *Runner) concluded(row remediation.StepRun, res remediation.CheckResult) (remediation.StepRun, error) {
	out, err := json.Marshal(checkRecord{Result: res})
	if err != nil {
		return row, fmt.Errorf("encode the output of step %s: %w", row.Step, err)
	}
	row.Output = out
	row.Findings = res.Findings
	r.finish(&row)
	return row, nil
}

// passGate runs the flow's pass output on the verified drafts, passes the try
// and ends the remediation, in one transaction with the gate's row. When the
// attempt's proposal row is no longer verifying, the output does not run: the
// try fails and the remediation ends.
func (r *Runner) passGate(ctx context.Context, run tryRun, row remediation.StepRun, attempt proposal.View, checked []checkedCluster, verdict step.GateVerdict) error {
	outName := workflow.StepName(run.flow.OnPass.Action)
	out, err := lookup[step.PassOutput](r.d.Registry, outName, step.KindOutput)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	v, err := verified(attempt.ID, checked, verdict.Result, mergeVerifications(attempt.Verifications, verdict.Verifications))
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	gateRow, err := r.concluded(row, verdict.Result)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	outRow := r.newRow(run, flowCluster, string(outName), kindOutput, flowRound, run.flow.Revision)
	r.finish(&outRow)
	moved := false
	err = r.commit(ctx, row.RemediationID, func(u uow.UnitOfWork) error {
		repo := u.RemediationRepo()
		if err := repo.SaveStep(ctx, gateRow); err != nil {
			return fmt.Errorf("save step %s: %w", gateRow.Step, err)
		}
		current, err := u.ProposalRepo().Get(ctx, attempt.ID)
		if err != nil {
			return fmt.Errorf("read proposal %s: %w", attempt.ID, err)
		}
		if current.Status != proposal.StatusVerifying {
			moved = true
			return r.endTry(ctx, u, run, remediation.TryFailed)
		}
		in := step.OutputInput{Remediation: run.snap.Remediation, Try: *run.snap.Try, Trigger: run.trigger, Snapshot: run.snap}
		if err := out.Pass(ctx, u, in, v); err != nil {
			return fmt.Errorf("step %s: %w", outName, err)
		}
		if err := repo.SaveStep(ctx, outRow); err != nil {
			return fmt.Errorf("save step %s: %w", outRow.Step, err)
		}
		return r.endTry(ctx, u, run, remediation.TryPassed)
	})
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	if moved {
		r.logger.Warn("the attempt's proposal row is no longer verifying; the remediation ends with nothing emitted",
			"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "attempt", run.snap.Try.Attempt)
		return nil
	}
	r.logger.Info("every verification run passed; the fix is handed over for review",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "attempt", run.snap.Try.Attempt, "output", outName)
	return nil
}

// verified is the try's checked drafts that have an edit, verified by the
// gate's result and the runs that judged them.
func verified(proposalID string, checked []checkedCluster, gate remediation.CheckResult, vs []proposal.Verification) (remediation.Verified, error) {
	drafts := make([]remediation.Checked, 0, len(checked))
	for _, cc := range checked {
		if len(cc.draft.Draft.Edits) == 0 {
			continue
		}
		c, err := remediation.NewChecked(cc.draft.Draft, cc.results)
		if err != nil {
			return remediation.Verified{}, fmt.Errorf("cluster %d: %w", cc.draft.Cluster.Cluster, err)
		}
		drafts = append(drafts, c)
	}
	return remediation.NewVerified(proposalID, drafts, []remediation.CheckResult{gate}, vs)
}

// failGate fails the attempt's proposal row with the gate's verify error and
// fails the try, in one transaction with the gate's row. failed is false when
// the proposal row was no longer verifying: it already says how the attempt
// ended, and the remediation ends with the try.
func (r *Runner) failGate(ctx context.Context, run tryRun, row remediation.StepRun, attempt proposal.View, verdict step.GateVerdict) (failed bool, err error) {
	gateRow, err := r.concluded(row, verdict.Result)
	if err != nil {
		return false, r.countFailure(ctx, row, err)
	}
	final := mergeVerifications(attempt.Verifications, verdict.Verifications)
	err = r.commit(ctx, row.RemediationID, func(u uow.UnitOfWork) error {
		if err := r.clearFailures(ctx, u, run); err != nil {
			return err
		}
		if err := u.RemediationRepo().SaveStep(ctx, gateRow); err != nil {
			return fmt.Errorf("save step %s: %w", gateRow.Step, err)
		}
		hit, err := u.ProposalRepo().MarkVerifyFailed(ctx, attempt.ID, verdict.VerifyError, final)
		if err != nil {
			return fmt.Errorf("mark verify failed: %w", err)
		}
		failed = hit
		if !hit {
			return r.endTry(ctx, u, run, remediation.TryFailed)
		}
		return r.saveTry(ctx, u, *run.snap.Try, remediation.TryFailed, time.Time{})
	})
	if err != nil {
		return false, r.countFailure(ctx, row, err)
	}
	if !failed {
		r.logger.Warn("the attempt's proposal row is no longer verifying; the remediation ends with nothing emitted",
			"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "attempt", run.snap.Try.Attempt)
		return false, nil
	}
	r.logger.Info("a verification run did not pass the fix; the next try starts",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "attempt", run.snap.Try.Attempt,
		"verify_error", verdict.VerifyError)
	return true, nil
}
