package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// gateStep returns the flow's gate: one long check, which starts the
// release checks of a try and answers later. The name of the gate's first step
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

// gateInput is what the gate reads for the try: its drafts and the fix
// row of its try. submittedAt is the start of the gate step's run, zero
// for the submit itself.
func gateInput(run tryRun, drafts []step.ClusterDraft, fixID string, submittedAt time.Time) step.GateInput {
	return step.GateInput{
		Remediation: run.snap.Remediation,
		Try:         *run.snap.Try,
		Trigger:     run.trigger,
		Drafts:      drafts,
		FixID:       fixID,
		SubmittedAt: submittedAt,
	}
}

// submitGate submits the try's drafts to the flow's gate and commits its
// answer with the gate's row. A gate that started its release checks leaves
// the try verifying: the try's fix row records the checks, the edits
// and the composed rationale, and the gate's row waits on the checks. A gate
// that refused the drafts without starting a check fails the try with its findings
// and fails the try, in one transaction with the gate's row, as a cross-cluster
// finding does (failTryOnFindings): the next try opens in the drive's next
// step, or the failing set is handed over once the tries are spent.
func (r *Runner) submitGate(ctx context.Context, run tryRun, checked []checkedCluster) error {
	name, gate, err := r.gateStep(run.flow)
	row := r.newRow(run, flowCluster, string(name), string(step.KindCheck), flowDraft, run.flow.Revision)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	drafts := draftsOf(checked)
	if row.InputDigest, err = draftsDigest(drafts); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	var try fix.View
	if err := r.read(ctx, func(u uow.UnitOfWork) error {
		var err error
		try, err = fixRowOf(ctx, u.FixRepo(), run)
		return err
	}); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	verdict, err := gate.Submit(ctx, gateInput(run, drafts, try.ID, time.Time{}))
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", name, err))
	}
	if row.Output, err = serialization.EncodeRecord(serialization.CheckRecord{Result: verdict.Result, Gate: true}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", name, err))
	}
	if !verdict.Result.Waiting {
		if _, refused := firstFinding([]remediation.CheckResult{verdict.Result}); !refused {
			return r.recordFailure(ctx, row, fmt.Errorf("step %s neither started a release check nor refused the try", name))
		}
		row.Findings = verdict.Result.Findings
		r.finish(&row)
		run.countStage("", ports.StageGate, row)
		return r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
			return r.failTryOnFindings(ctx, u, run, checked, row.Step, row.Findings)
		})
	}
	row.Status = remediation.StepWaiting
	row.WaitingOn = verdict.Result.CheckIDs
	p := verifyingTry(run, checked, verdict.ReleaseChecks)
	deadline := r.clock.Now().Add(r.d.ReleaseCheckTimeout + tryVerifyingSlack)
	if err := r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
		if err := r.recordTry(ctx, u, run, run.snap.Try.Try, p); err != nil {
			return err
		}
		return r.saveTry(ctx, u, *run.snap.Try, remediation.TryVerifying, deadline)
	}); err != nil {
		return err
	}
	r.logger.Info("the try's drafts are submitted to their release checks", "remediation", run.snap.Remediation.ID,
		"release", run.trigger.ReleaseID, "round", run.trigger.Round, "try", run.snap.Try.Try, "checks", row.WaitingOn)
	return nil
}

// PollGate reads the gate's verdict on the remediation's verifying try and
// acts on it. The caller has claimed the remediation, which snap names; it is
// read again, since a snapshot listed without the lease may be torn. PollGate
// holds the lease, then polls the gate once with the drafts it was submitted,
// rebuilt from the journal, counting unreadable checks from the start of the
// gate step's run. Before it polls, the stuck rules are applied as before
// every step of a drive: a verifying try has no deadline that ends it, but a
// gate row that failed five times in a row, or a remediation that did, ends
// the remediation as a system failure, with the try's row failed and the
// gate not read.
//
// While the gate waits, each release check whose phase changed is written to the
// try's fix row, and nothing else. A passed gate runs the flow's pass
// output, hands over the nodes the fix leaves to a human, passes the try and
// ends the remediation, in one transaction. A failed gate fails the try
// with the gate's verify error and fails the try, in one transaction, and then
// opens the next try, which hands the failure over once the tries are spent. A
// fix row that is no longer verifying already says how the try ended:
// the try fails and the remediation ends with nothing emitted. An error is
// counted on the gate's row, which keeps waiting, and a lost lease returns nil
// with nothing written.
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
	if ended, err := r.endIfStuck(ctx, run); err != nil || ended {
		return err
	}
	name, gate, err := r.gateStep(run.flow)
	if err != nil {
		return r.countGateFailure(ctx, run, name, err)
	}
	row, ok := run.journal.row(flowCluster, string(name), flowDraft)
	if !ok {
		return r.countGateFailure(ctx, run, name, fmt.Errorf("remediation %s: try %d is verifying with no %s row", id, run.snap.Try.Try, name))
	}
	var try fix.View
	if err := r.commit(ctx, id, func(u uow.UnitOfWork) error {
		var err error
		try, err = fixRowOf(ctx, u.FixRepo(), run)
		return err
	}); err != nil {
		return r.countFailure(ctx, row, err)
	}
	checked, err := r.checkedClusters(run)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	waiting := remediation.CheckResult{Check: string(name), Waiting: true, CheckIDs: row.WaitingOn}
	verdict, err := gate.Poll(ctx, gateInput(run, draftsOf(checked), try.ID, row.StartedAt), waiting)
	if err != nil {
		return r.countFailure(ctx, row, fmt.Errorf("step %s: %w", name, err))
	}
	switch {
	case verdict.Result.Waiting:
		return r.writePhases(ctx, run, row, try, verdict.Phases)
	case verdict.Result.Passed():
		return r.passGate(ctx, run, row, try, checked, verdict)
	}
	failed, err := r.failGate(ctx, run, row, try, verdict)
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
	if row, ok := run.journal.row(flowCluster, string(name), flowDraft); ok {
		return r.countFailure(ctx, row, cause)
	}
	row := r.newRow(run, flowCluster, string(name), string(step.KindCheck), flowDraft, run.flow.Revision)
	return r.recordFailure(ctx, row, cause)
}

// fixRowOf returns the fix row of the run's current try: the row of its
// number in the trigger's round.
func fixRowOf(ctx context.Context, repo repository.FixRepository, run tryRun) (fix.View, error) {
	views, err := repo.List(ctx, repository.FixFilter{ReleaseID: run.trigger.ReleaseID})
	if err != nil {
		return fix.View{}, fmt.Errorf("list the fixes of release %s: %w", run.trigger.ReleaseID, err)
	}
	for _, v := range views {
		if v.Round == run.trigger.Round && v.Try == run.snap.Try.Try {
			return v, nil
		}
	}
	return fix.View{}, fmt.Errorf("release %s has no fix row for round %d, try %d",
		run.trigger.ReleaseID, run.trigger.Round, run.snap.Try.Try)
}

// writePhases records what a poll that read the gate's release checks found: each check's
// phase that differs from what the try's fix row records, and the gate
// row's failures and the remediation's own, which a poll that got through
// resets to zero so that only consecutive failed polls add up. It writes in one
// transaction, and writes nothing when no phase differs and neither the row
// nor the remediation has failures.
func (r *Runner) writePhases(ctx context.Context, run tryRun, row remediation.StepRun, try fix.View, phases []fix.ReleaseCheck) error {
	changed := changedPhases(try.ReleaseChecks, phases)
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
			if err := u.FixRepo().UpdateReleaseCheckPhase(ctx, try.ID, p.CheckID, p.Phase, p.ActivatedAt); err != nil {
				return fmt.Errorf("record the phase of release check %s: %w", p.CheckID, err)
			}
		}
		return nil
	})
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	return nil
}

// changedPhases is the checks read whose phase or activation time is not the one
// stored for them.
func changedPhases(stored, read []fix.ReleaseCheck) []fix.ReleaseCheck {
	byCheck := make(map[string]fix.ReleaseCheck, len(stored))
	for _, v := range stored {
		byCheck[v.CheckID] = v
	}
	var out []fix.ReleaseCheck
	for _, v := range read {
		s, ok := byCheck[v.CheckID]
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

// cleanedError is the check with its error cleaned (fix.CleanedReason): the error
// is what a release check's nodes printed, and the row shows it.
func cleanedError(v fix.ReleaseCheck) fix.ReleaseCheck {
	v.Error = fix.CleanedReason(v.Error)
	return v
}

// mergeReleaseChecks is the stored checks with each check read in this poll in
// place of its stored entry, so a check the poll could not read keeps what was
// recorded for it. A check read that is not stored is added after them. The error of
// each check is cleaned as the row records it.
func mergeReleaseChecks(stored, read []fix.ReleaseCheck) []fix.ReleaseCheck {
	byCheck := make(map[string]fix.ReleaseCheck, len(read))
	for _, v := range read {
		byCheck[v.CheckID] = v
	}
	out := make([]fix.ReleaseCheck, 0, len(stored))
	for _, v := range stored {
		if got, ok := byCheck[v.CheckID]; ok {
			v = got
			delete(byCheck, v.CheckID)
		}
		out = append(out, cleanedError(v))
	}
	for _, v := range read {
		if _, ok := byCheck[v.CheckID]; ok {
			out = append(out, cleanedError(v))
		}
	}
	return out
}

// concluded is the gate's row once the gate gave its verdict res.
func (r *Runner) concluded(row remediation.StepRun, res remediation.CheckResult) (remediation.StepRun, error) {
	out, err := serialization.EncodeRecord(serialization.CheckRecord{Result: res, Gate: true})
	if err != nil {
		return row, fmt.Errorf("encode the output of step %s: %w", row.Step, err)
	}
	row.Output = out
	row.Findings = res.Findings
	r.finish(&row)
	return row, nil
}

// passGate runs the flow's approver for the nodes the verified drafts cover and
// its pass output on those drafts, handed the approvers, hands over the nodes
// the try's final row leaves to a human (handOver), passes the try and
// ends the remediation, in one transaction with the gate's row. When the
// try's fix row is no longer verifying, no output runs: the try fails
// and the remediation ends.
func (r *Runner) passGate(ctx context.Context, run tryRun, row remediation.StepRun, try fix.View, checked []checkedCluster, verdict step.GateVerdict) error {
	outName := workflow.StepName(run.flow.OnPass.Action)
	out, err := lookup[step.PassOutput](r.d.Registry, outName, step.KindOutput)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	v, err := verified(try.ID, checked, verdict.Result, mergeReleaseChecks(try.ReleaseChecks, verdict.ReleaseChecks))
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	gateRow, err := r.concluded(row, verdict.Result)
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	moved := false
	err = r.commitRun(ctx, run, func(u uow.UnitOfWork) error {
		repo := u.RemediationRepo()
		if err := repo.SaveStep(ctx, gateRow); err != nil {
			return fmt.Errorf("save step %s: %w", gateRow.Step, err)
		}
		run.countStage("", ports.StageGate, gateRow)
		current, err := u.FixRepo().Get(ctx, try.ID)
		if err != nil {
			return fmt.Errorf("read fix %s: %w", try.ID, err)
		}
		if current.Status != fix.StatusVerifying {
			moved = true
			if err := r.endTry(ctx, u, run, remediation.TryFailed); err != nil {
				return err
			}
			run.countOutcomes(current.NodeOutcomes)
			return nil
		}
		fixed := draftsOf(checked)
		approvers, err := r.approversFor(ctx, run, fixed, coveredNodes(fixed))
		if err != nil {
			return err
		}
		in := step.OutputInput{Remediation: run.snap.Remediation, Try: *run.snap.Try, Trigger: run.trigger, Snapshot: run.snap, Approvers: approvers}
		outRow := r.newRow(run, flowCluster, string(outName), kindOutput, flowDraft, run.flow.Revision)
		if err := out.Pass(ctx, u, in, v); err != nil {
			return fmt.Errorf("step %s: %w", outName, err)
		}
		r.finish(&outRow)
		if err := repo.SaveStep(ctx, outRow); err != nil {
			return fmt.Errorf("save step %s: %w", outRow.Step, err)
		}
		run.countStage("", ports.StageOutput, outRow)
		final, err := u.FixRepo().Get(ctx, try.ID)
		if err != nil {
			return fmt.Errorf("read fix %s: %w", try.ID, err)
		}
		if err := r.handOver(ctx, u, run, final.Try, final.NodeOutcomes); err != nil {
			return err
		}
		if err := r.endTry(ctx, u, run, remediation.TryPassed); err != nil {
			return err
		}
		run.countTries(ports.TryPassed)
		run.countOutcomes(final.NodeOutcomes)
		return nil
	})
	if err != nil {
		return r.countFailure(ctx, row, err)
	}
	if moved {
		r.logger.Warn("the try's fix row is no longer verifying; the remediation ends with nothing emitted",
			"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "try", run.snap.Try.Try)
		return nil
	}
	r.logger.Info("every release check passed; the fix is handed over for review",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "try", run.snap.Try.Try, "output", outName)
	return nil
}

// verified is the try's checked drafts that have an edit, verified by the
// gate's result and the release checks that judged them.
func verified(fixID string, checked []checkedCluster, gate remediation.CheckResult, vs []fix.ReleaseCheck) (remediation.Verified, error) {
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
	return remediation.NewVerified(fixID, drafts, []remediation.CheckResult{gate}, vs)
}

// failGate fails the try's fix row with the gate's verify error and
// fails the try, in one transaction with the gate's row. failed is false when
// the fix row was no longer verifying: it already says how the try
// ended, and the remediation ends with the try.
func (r *Runner) failGate(ctx context.Context, run tryRun, row remediation.StepRun, try fix.View, verdict step.GateVerdict) (failed bool, err error) {
	gateRow, err := r.concluded(row, verdict.Result)
	if err != nil {
		return false, r.countFailure(ctx, row, err)
	}
	final := mergeReleaseChecks(try.ReleaseChecks, verdict.ReleaseChecks)
	err = r.commitRun(ctx, run, func(u uow.UnitOfWork) error {
		if err := r.clearFailures(ctx, u, run); err != nil {
			return err
		}
		if err := u.RemediationRepo().SaveStep(ctx, gateRow); err != nil {
			return fmt.Errorf("save step %s: %w", gateRow.Step, err)
		}
		run.countStage("", ports.StageGate, gateRow)
		hit, err := u.FixRepo().MarkVerifyFailed(ctx, try.ID, fix.CleanedReason(verdict.VerifyError), final)
		if err != nil {
			return fmt.Errorf("mark verify failed: %w", err)
		}
		failed = hit
		if !hit {
			current, err := u.FixRepo().Get(ctx, try.ID)
			if err != nil {
				return fmt.Errorf("read fix %s: %w", try.ID, err)
			}
			if err := r.endTry(ctx, u, run, remediation.TryFailed); err != nil {
				return err
			}
			run.countOutcomes(current.NodeOutcomes)
			return nil
		}
		if err := r.saveTry(ctx, u, *run.snap.Try, remediation.TryFailed, time.Time{}); err != nil {
			return err
		}
		run.countTries(ports.TryGateFailed)
		return nil
	})
	if err != nil {
		return false, r.countFailure(ctx, row, err)
	}
	if !failed {
		r.logger.Warn("the try's fix row is no longer verifying; the remediation ends with nothing emitted",
			"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "try", run.snap.Try.Try)
		return false, nil
	}
	r.logger.Info("a release check did not pass the fix; the next try starts",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "try", run.snap.Try.Try,
		"verify_error", verdict.VerifyError)
	return true, nil
}
