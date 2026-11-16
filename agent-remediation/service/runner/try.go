package runner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/typology"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// How long a try may stay in each open status: its deadline is set when it
// enters the status, and a drafting or checking try past it is ended, by the
// runner before the try's next step or by the sweeper when nobody drives it.
// A verifying try's deadline is informational: it records when the gate's
// runs should have answered, an hour past the verification timeout, and
// nothing acts on it, since the gate's own timeouts end a verification that
// never answers.
const (
	tryDraftingTime   = time.Hour
	tryCheckingTime   = 10 * time.Minute
	tryVerifyingSlack = time.Hour
)

// The cluster and round of a flow-level step's journal row: a cross-cluster
// check, the gate or an output.
const (
	flowCluster = remediation.FlowCluster
	flowRound   = 1
	// flowGateRowName is the step name of the gate's row for a flow that names
	// no gate step.
	flowGateRowName = "gate"
)

// The reasons a node is recorded with when no fix is attempted for it.
const (
	// maintenanceSkipReason closes a try that came due in maintenance mode, so
	// the release can be retried once maintenance is off.
	maintenanceSkipReason = "not attempted: continuo was in maintenance mode"
	// dbtTestSkipReason: a test is validation evidence, never a fix target. A
	// model fix that makes it bind again resolves it in verification, and a
	// test that is itself wrong is a human's edit.
	dbtTestSkipReason = "dbt tests are not fix targets; fix the model or edit the test by hand"
	// noTypeSkipReason is for a cluster no type of the flow handles.
	noTypeSkipReason = "no remediation type matches this failure"
)

// NextTry opens the remediation's next try from its stored trigger, as Drive
// also does when it finds the remediation with no try or with a failed one; a
// try therefore opens even when the process stopped between failing a try and
// calling NextTry. It opens one only while the remediation runs and has no try
// or its try failed, so calling it again after it succeeded opens nothing. As
// before every step of a drive, the stuck rules are applied first, and a
// remediation that can go no further is ended instead of opening a try. The
// caller holds the lease; a lost lease returns nil with nothing written. A try
// that cannot open has the error counted on the remediation's own row.
func (r *Runner) NextTry(ctx context.Context, id uuid.UUID) error {
	run, running, err := r.load(ctx, id)
	if err != nil || !running {
		return err
	}
	if run.snap.Try != nil && run.snap.Try.Status != remediation.TryFailed {
		return nil
	}
	ended, err := r.endIfStuck(ctx, run)
	if err == nil && !ended {
		err = r.openTry(ctx, run)
		if err != nil {
			err = r.countRemediationFailure(ctx, id, err)
		}
	}
	if errors.Is(err, repository.ErrLeaseLost) {
		r.logger.Info("remediation lease lost; no try opened", "remediation", id, "owner", r.d.Owner)
		return nil
	}
	return err
}

// openTry opens the remediation's next try in one transaction, checking in
// order:
//   - the terminal attempts of the trigger's round reached the flow's tries:
//     the failing set is handed over (giveUp) and the remediation is done;
//   - maintenance mode: the attempt is recorded skipped for every node, with
//     no code, and the remediation is done;
//   - the trigger names no repository: the attempt is recorded failed, every
//     node with no_repository, since no pull request could ever be opened, and
//     the remediation is done;
//   - otherwise the try opens drafting, with the release's next attempt
//     number, the trigger's failing set grouped into clusters each matched to
//     a type, and the attempt's in-flight proposal row.
//
// In both attempts recorded without a try, a node the classifier marked
// unfixable keeps the outcome the classifier gave it, and the nodes whose
// outcome carries a code are handed over (handOver): none in maintenance mode
// but the unfixable ones.
func (r *Runner) openTry(ctx context.Context, run tryRun) error {
	t := run.trigger
	rem := run.snap.Remediation
	return r.commit(ctx, rem.ID, func(u uow.UnitOfWork) error {
		inRound, total, err := countAttempts(ctx, u.ProposalRepo(), t)
		if err != nil {
			return err
		}
		attempt := total + 1
		switch {
		case inRound >= run.flow.Tries:
			return r.giveUp(ctx, u, run, attempt, inRound)
		case r.d.Maintenance:
			r.logger.Info("remediation try skipped: maintenance mode is on",
				"remediation", rem.ID, "release", t.ReleaseID, "round", t.Round, "attempt", attempt)
			return r.endWithAttempt(ctx, u, run, attempt, proposal.Proposal{
				Status:       proposal.StatusSkipped,
				NodeOutcomes: attemptOutcomes(t, proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: maintenanceSkipReason}),
				Rationale:    maintenanceSkipReason,
			})
		case t.Repo == "":
			reason := fmt.Sprintf("the trigger carried no repository for %s", t.ReleaseID)
			r.logger.Warn("remediation.requested trigger names no repository; recording the attempt failed",
				"remediation", rem.ID, "release", t.ReleaseID, "attempt", attempt)
			return r.endWithAttempt(ctx, u, run, attempt, proposal.Proposal{
				Status: proposal.StatusFailed,
				NodeOutcomes: attemptOutcomes(t, proposal.NodeOutcome{
					Status: proposal.StatusFailed, Reason: reason, Code: pkg_model.HandoverReasonNoRepository,
				}),
				Rationale: reason,
			})
		}
		return r.startTry(ctx, u, run, attempt)
	})
}

// startTry writes the remediation's next try, its clusters and the attempt's
// in-flight proposal row in u's transaction, and makes it the current try. The
// failures counted on the remediation's own row are cleared: the try opened.
//
// Each node settled for good (settledNodes) comes first, as a single cluster of
// its own already handed over with the outcome it was settled with; it runs no
// step. Only the other nodes are grouped into clusters to work. The in-flight
// row names every cluster handed over at once with its outcome, and every
// other node as being worked on.
func (r *Runner) startTry(ctx context.Context, u uow.UnitOfWork, run tryRun, attempt int) error {
	t := run.trigger
	rem := run.snap.Remediation
	repo := u.RemediationRepo()
	now := r.clock.Now()
	try := remediation.Try{
		RemediationID: rem.ID,
		Try:           rem.CurrentTry + 1,
		Attempt:       attempt,
		Status:        remediation.TryDrafting,
		FileRevisions: fileRevisions(r.d.Files, run.flow),
		DeadlineAt:    now.Add(tryDraftingTime),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := repo.SaveTry(ctx, try); err != nil {
		return fmt.Errorf("save try: %w", err)
	}
	settled := settledNodes(run)
	clusters := make([]remediation.Cluster, 0, len(t.Nodes))
	open := make([]remediation.TriggerNode, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		if _, ok := settled[n.NodeID]; !ok {
			open = append(open, n)
		}
	}
	for _, id := range sortedKeys(settled) {
		clusters = append(clusters, r.settledCluster(rem, try, len(clusters)+1, id, settled[id]))
	}
	groups := groupClusters(open)
	for _, g := range groups {
		clusters = append(clusters, r.newCluster(rem, try, run.flow, t, len(clusters)+1, clusterKind(g.Kind), g.TargetNodeID, g.Members))
	}
	for _, c := range clusters {
		if err := repo.SaveCluster(ctx, c); err != nil {
			return fmt.Errorf("save cluster %d: %w", c.Cluster, err)
		}
	}
	if err := u.ProposalRepo().InsertGenerating(ctx, generatingProposal(rem.ID, t, attempt, now, clusters)); err != nil {
		return fmt.Errorf("mark generating: %w", err)
	}
	rem.CurrentTry = try.Try
	rem.UpdatedAt = now
	if err := repo.SaveRemediation(ctx, rem); err != nil {
		return fmt.Errorf("save remediation: %w", err)
	}
	if err := repo.ClearFailures(ctx, rem.ID); err != nil {
		return fmt.Errorf("clear the failures of remediation %s: %w", rem.ID, err)
	}
	r.logger.Info("remediation try opened", "remediation", rem.ID, "release", t.ReleaseID,
		"try", try.Try, "attempt", attempt, "clusters", len(clusters), "settled", len(settled))
	return nil
}

// settledNodes is every node of the remediation settled for good, with the
// outcome it was settled with: each trigger node the classifier marked
// unfixable, with the classifier's outcome, and, when the try being replaced
// failed, each member of one of its clusters whose outcome carries a settled
// code (settledCode). The clusters a try carries are themselves handed over
// with their code, so a node once settled stays settled in every later try. A
// node handed over because its pre-flight rounds are spent is not settled, and
// is grouped again.
func settledNodes(run tryRun) map[string]proposal.NodeOutcome {
	out := map[string]proposal.NodeOutcome{}
	for _, n := range run.trigger.Nodes {
		if n.Unfixable != nil {
			out[n.NodeID] = unfixableOutcome(*n.Unfixable)
		}
	}
	if run.snap.Try == nil || run.snap.Try.Status != remediation.TryFailed {
		return out
	}
	for _, c := range run.snap.Clusters {
		if c.Outcome == nil || !settledCode(c.Outcome.Code) {
			continue
		}
		for _, m := range c.Members {
			if _, ok := out[m]; !ok {
				out[m] = *c.Outcome
			}
		}
	}
	return out
}

// settledCode reports whether a node handed over with code is settled for good:
// no later try of the remediation can reach it. A step declined it, no type
// matches it, or the classifier marked it unfixable.
func settledCode(code pkg_model.HandoverReason) bool {
	switch code {
	case pkg_model.HandoverReasonDeclined, pkg_model.HandoverReasonNoType,
		pkg_model.HandoverReasonClassifierInfra, pkg_model.HandoverReasonClassifierUnhealableParse:
		return true
	}
	return false
}

// unfixableOutcome is how a node the classifier marked unfixable ends: skipped,
// with the classifier's reason as its code and its detail as the reason.
func unfixableOutcome(u remediation.Unfixable) proposal.NodeOutcome {
	return proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: u.Detail, Code: u.Reason}
}

// attemptOutcomes stamps o on every node of the trigger but the ones the
// classifier marked unfixable, which keep the classifier's outcome.
func attemptOutcomes(t remediation.Trigger, o proposal.NodeOutcome) map[string]proposal.NodeOutcome {
	out := make(map[string]proposal.NodeOutcome, len(t.Nodes))
	for _, n := range t.Nodes {
		if n.Unfixable != nil {
			out[n.NodeID] = unfixableOutcome(*n.Unfixable)
			continue
		}
		out[n.NodeID] = o
	}
	return out
}

// settledCluster is cluster n of a try for the node settled for good with
// outcome o: a single cluster targeting the node, already handed over, with no
// type.
func (r *Runner) settledCluster(rem remediation.Remediation, try remediation.Try, n int, nodeID string, o proposal.NodeOutcome) remediation.Cluster {
	return remediation.Cluster{
		RemediationID: rem.ID,
		Try:           try.Try,
		Cluster:       n,
		Kind:          workflow.ClusterSingle,
		TargetNodeID:  nodeID,
		Members:       []string{nodeID},
		Status:        remediation.ClusterHandedOver,
		Round:         1,
		Outcome:       &o,
		UpdatedAt:     r.clock.Now(),
	}
}

// giveUp hands the failing set over once the tries of the trigger's round are
// spent, after tried terminal attempts, in u's transaction: the attempt is
// recorded escalated with how each node ends (escalatedOutcomes), the nodes are
// handed over and the remediation is done. The give-up output therefore never
// runs twice: a remediation that is done is never driven again.
func (r *Runner) giveUp(ctx context.Context, u uow.UnitOfWork, run tryRun, attempt, tried int) error {
	outcomes, err := escalatedOutcomes(ctx, u, run, tried)
	if err != nil {
		return err
	}
	r.logger.Info("remediation tries spent; handing over", "remediation", run.snap.Remediation.ID,
		"release", run.trigger.ReleaseID, "round", run.trigger.Round, "attempt", attempt)
	return r.endWithAttempt(ctx, u, run, attempt, proposal.Proposal{Status: proposal.StatusEscalated, NodeOutcomes: outcomes})
}

// escalatedOutcomes is how each failing node ends once the tries of the
// trigger's round are spent, after tried terminal attempts. A node the
// classifier marked unfixable keeps the classifier's outcome. Every other node
// is judged by the outcome the remediation's last try recorded for it
// (lastTry): one handed over with a settled code (settledCode), or because its
// pre-flight rounds were spent, keeps that outcome; a drafted member of a try a
// check refused is escalated with unverifiable_edits; and every other node,
// one a failed verification judged among them, is escalated with tries_spent.
// Both escalated codes carry the message triedMessage builds.
func escalatedOutcomes(ctx context.Context, u uow.UnitOfWork, run tryRun, tried int) (map[string]proposal.NodeOutcome, error) {
	last, why, err := lastTry(ctx, u, run)
	if err != nil {
		return nil, err
	}
	message := triedMessage(tried, why)
	out := make(map[string]proposal.NodeOutcome, len(run.trigger.Nodes))
	for _, n := range run.trigger.Nodes {
		if n.Unfixable != nil {
			out[n.NodeID] = unfixableOutcome(*n.Unfixable)
			continue
		}
		o := last[n.NodeID]
		switch {
		case settledCode(o.Code) || o.Code == pkg_model.HandoverReasonPreflightRoundsSpent:
			out[n.NodeID] = o
		case o.Code == pkg_model.HandoverReasonUnverifiableEdits:
			out[n.NodeID] = proposal.NodeOutcome{Status: proposal.StatusEscalated, Reason: message, Code: pkg_model.HandoverReasonUnverifiableEdits}
		default:
			out[n.NodeID] = proposal.NodeOutcome{Status: proposal.StatusEscalated, Reason: message, Code: pkg_model.HandoverReasonTriesSpent}
		}
	}
	return out, nil
}

// lastTry reads, in u's transaction, how the remediation's last try ended for
// each node and why it failed, from the try's attempt row, which records the
// outcome each of the try's clusters handed its members over with. Why is the
// row's rationale when a check refused the try's drafts, which marks each
// drafted member unverifiable_edits, and its verify error otherwise. A
// remediation that made no try has neither.
func lastTry(ctx context.Context, u uow.UnitOfWork, run tryRun) (outcomes map[string]proposal.NodeOutcome, why string, err error) {
	if run.snap.Try == nil {
		return nil, "", nil
	}
	row, err := attemptRow(ctx, u.ProposalRepo(), run)
	if err != nil {
		return nil, "", err
	}
	why = row.VerifyError
	for _, o := range row.NodeOutcomes {
		if o.Code == pkg_model.HandoverReasonUnverifiableEdits {
			why = row.Rationale
			break
		}
	}
	return row.NodeOutcomes, why, nil
}

// triedMessage is the message of a node escalated once the tries are spent:
// how many times the round was tried and, when it is known, why the last try
// failed.
func triedMessage(tried int, why string) string {
	times := "times"
	if tried == 1 {
		times = "time"
	}
	msg := fmt.Sprintf("tried %d %s", tried, times)
	if why == "" {
		return msg
	}
	return msg + "; the last try failed: " + why
}

// endWithAttempt records the attempt's terminal proposal row, hands over the
// nodes whose outcome in it carries a code and ends the remediation, in u's
// transaction.
func (r *Runner) endWithAttempt(ctx context.Context, u uow.UnitOfWork, run tryRun, attempt int, p proposal.Proposal) error {
	if err := r.recordAttempt(ctx, u, run, attempt, p); err != nil {
		return err
	}
	if err := r.handOver(ctx, u, run, p.NodeOutcomes); err != nil {
		return err
	}
	return r.endRemediation(ctx, u, run.snap.Remediation)
}

// handOver runs the flow's give-up output, in u's transaction, for every node
// of an ending remediation whose final outcome carries a code
// (remediation.HandOvers). A remediation that made a try records the output on
// that try's journal. With no node to hand over, nothing runs and nothing is
// written.
func (r *Runner) handOver(ctx context.Context, u uow.UnitOfWork, run tryRun, outcomes map[string]proposal.NodeOutcome) error {
	handed := remediation.HandOvers(outcomes)
	if len(handed) == 0 {
		return nil
	}
	name := workflow.StepName(run.flow.OnGiveUp.Action)
	out, err := lookup[step.GiveUpOutput](r.d.Registry, name, step.KindOutput)
	if err != nil {
		return err
	}
	in := step.OutputInput{Remediation: run.snap.Remediation, Trigger: run.trigger, Snapshot: run.snap, HandedOver: handed}
	if run.snap.Try != nil {
		in.Try = *run.snap.Try
	}
	if err := out.GiveUp(ctx, u, in); err != nil {
		return fmt.Errorf("step %s: %w", name, err)
	}
	if run.snap.Try != nil {
		row := r.newRow(run, flowCluster, string(name), kindOutput, flowRound, run.flow.Revision)
		r.finish(&row)
		if err := u.RemediationRepo().SaveStep(ctx, row); err != nil {
			return fmt.Errorf("save step %s: %w", name, err)
		}
	}
	r.logger.Info("nodes handed over", "remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID,
		"nodes", len(handed), "output", name)
	return nil
}

// recordAttempt stamps the trigger's identity and the remediation onto the
// attempt's proposal row, derives its representative views, and upserts it in
// u's transaction. A row that narrows no nodes addresses the trigger's whole
// failing set.
func (r *Runner) recordAttempt(ctx context.Context, u uow.UnitOfWork, run tryRun, attempt int, p proposal.Proposal) error {
	t := run.trigger
	p.Source = t.Source
	p.ReleaseID = t.ReleaseID
	p.RemediationID = run.snap.Remediation.ID.String()
	p.RemediationRound = t.Round
	p.Attempt = attempt
	p.CreatedAt = r.clock.Now()
	if len(p.ResolvedNodeIDs) == 0 {
		p.ResolvedNodeIDs = t.NodeIDs()
	}
	p.Services = proposal.UnionServices(t.Services(), verificationServices(p.Verifications))
	p.NormalizeRepresentativeViews()
	p.ErrorSignature = signatureFor(t, p.NodeID)
	if err := u.ProposalRepo().Upsert(ctx, p); err != nil {
		return fmt.Errorf("upsert proposal: %w", err)
	}
	r.logger.Info("proposal recorded", "remediation", run.snap.Remediation.ID, "release", t.ReleaseID,
		"node", p.NodeID, "nodes", len(p.ResolvedNodeIDs), "status", p.Status, "attempt", attempt)
	return nil
}

// finishTry takes the next committed step of a try whose every cluster has
// ended. A drafting try with no checked draft that has an edit, every cluster
// having been handed over or fallen back, is settled: its attempt is recorded
// with what its clusters reported, its nodes are handed over, the try fails
// and the remediation is done. Any other drafting try moves to checking. A
// checking try runs the flow's cross-cluster checks one at a time, in file
// order, each committed on its own; a finding fails the attempt and the try,
// like a gate that did not pass the fix, and the remediation's next try opens
// in the drive's next step, or the failing set is handed over once the tries
// are spent. Once every check passed, the drafts are submitted to the gate. The
// checked drafts are rebuilt from the journal each time, and a failure to read
// them back is counted on the runner's drafts row.
func (r *Runner) finishTry(ctx context.Context, run tryRun) error {
	checked, err := r.checkedClusters(run)
	if err != nil {
		return r.recordFailure(ctx, r.newRow(run, flowCluster, stepDrafts, kindRunner, flowRound, run.flow.Revision), err)
	}
	drafts := draftsOf(checked)
	if run.snap.Try.Status == remediation.TryDrafting {
		if len(drafts) == 0 {
			return r.settleTry(ctx, run)
		}
		return r.commit(ctx, run.snap.Remediation.ID, func(u uow.UnitOfWork) error {
			if err := r.clearFailures(ctx, u, run); err != nil {
				return err
			}
			return r.saveTry(ctx, u, *run.snap.Try, remediation.TryChecking, r.clock.Now().Add(tryCheckingTime))
		})
	}
	for _, ref := range run.flow.AcrossClusters {
		row, done := run.journal.done(flowCluster, string(ref.Name), flowRound)
		if !done {
			return r.runCrossCheck(ctx, run, checked, ref.Name)
		}
		if len(row.Findings) > 0 {
			return r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
				return r.failTryOnFindings(ctx, u, run, checked, row.Step, row.Findings)
			})
		}
	}
	return r.submitGate(ctx, run, checked)
}

// settleTry records the attempt of a try that has nothing to verify, hands
// over its nodes, fails the try and ends the remediation, in one transaction.
// The attempt is skipped when every node was skipped, and failed as soon as one
// node was attempted.
func (r *Runner) settleTry(ctx context.Context, run tryRun) error {
	outcomes := nodeOutcomes(run.snap.Clusters)
	p := proposal.Proposal{Status: settledStatus(outcomes), NodeOutcomes: outcomes}
	if err := r.commit(ctx, run.snap.Remediation.ID, func(u uow.UnitOfWork) error {
		if err := r.recordAttempt(ctx, u, run, run.snap.Try.Attempt, p); err != nil {
			return err
		}
		if err := r.handOver(ctx, u, run, outcomes); err != nil {
			return err
		}
		return r.endTry(ctx, u, run, remediation.TryFailed)
	}); err != nil {
		return err
	}
	r.logger.Info("no cluster of the try produced an edit; the attempt is settled",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "attempt", run.snap.Try.Attempt, "status", p.Status)
	return nil
}

// runCrossCheck runs one cross-cluster check over the drafts of the try's
// checked clusters and commits its row. A finding fails the attempt with the
// finding and fails the try in the same transaction.
func (r *Runner) runCrossCheck(ctx context.Context, run tryRun, checked []checkedCluster, name workflow.StepName) error {
	drafts := draftsOf(checked)
	row := r.newRow(run, flowCluster, string(name), string(step.KindCheck), flowRound, run.flow.Revision)
	chk, err := lookup[step.CrossCheck](r.d.Registry, name, step.KindCheck)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	if row.InputDigest, err = draftsDigest(drafts); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	res, err := chk.CheckAll(ctx, step.CrossInput{Trigger: run.trigger, Drafts: drafts})
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", name, err))
	}
	if row.Output, err = serialization.EncodeRecord(serialization.CheckRecord{Result: res}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", name, err))
	}
	row.Findings = res.Findings
	r.finish(&row)
	return r.commitFlowStep(ctx, run, row, func(u uow.UnitOfWork) error {
		if len(res.Findings) == 0 {
			return nil
		}
		return r.failTryOnFindings(ctx, u, run, checked, string(name), res.Findings)
	})
}

// commitFlowStep commits a flow-level step's row and what fn writes, in one
// transaction after holding the lease, and clears the failures run's
// remediation counted in a row before it (clearFailures). A commit that fails
// is recorded as a failure of the step.
func (r *Runner) commitFlowStep(ctx context.Context, run tryRun, row remediation.StepRun, fn func(u uow.UnitOfWork) error) error {
	err := r.commit(ctx, row.RemediationID, func(u uow.UnitOfWork) error {
		if err := r.clearFailures(ctx, u, run); err != nil {
			return err
		}
		if err := u.RemediationRepo().SaveStep(ctx, row); err != nil {
			return fmt.Errorf("save step %s: %w", row.Step, err)
		}
		return fn(u)
	})
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	return nil
}

// failTryOnFindings fails the try because the check name, a cross-cluster
// check or the gate refusing the drafts at submit, found findings in the drafts
// of the checked clusters, in u's transaction: the attempt is recorded failed,
// with the findings' messages as its reason (recordRefusal), and the try fails
// while the remediation keeps running, so the next try opens and is shown the
// findings among the earlier tries.
func (r *Runner) failTryOnFindings(ctx context.Context, u uow.UnitOfWork, run tryRun, checked []checkedCluster, name string, findings []remediation.Finding) error {
	reason := findingsReason(findings)
	r.logger.Warn("a check refused the try's drafts; the try fails",
		"remediation", run.snap.Remediation.ID, "release", run.trigger.ReleaseID, "try", run.snap.Try.Try,
		"attempt", run.snap.Try.Attempt, "check", name, "reason", reason)
	if err := r.recordRefusal(ctx, u, run, checked, reason); err != nil {
		return err
	}
	return r.saveTry(ctx, u, *run.snap.Try, remediation.TryFailed, time.Time{})
}

// recordRefusal records the attempt of the current try failed for reason, in
// u's transaction. The members of each checked cluster whose draft has an edit,
// the drafts the refusing check judged, fail for reason with
// unverifiable_edits; every other node keeps how its cluster ended
// (nodeOutcomes), such as a cluster handed over before any draft.
func (r *Runner) recordRefusal(ctx context.Context, u uow.UnitOfWork, run tryRun, checked []checkedCluster, reason string) error {
	outcomes := nodeOutcomes(run.snap.Clusters)
	refused := proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: reason, Code: pkg_model.HandoverReasonUnverifiableEdits}
	for _, d := range draftsOf(checked) {
		for _, m := range d.Cluster.Members {
			outcomes[m] = refused
		}
	}
	return r.recordAttempt(ctx, u, run, run.snap.Try.Attempt, proposal.Proposal{
		Status:       proposal.StatusFailed,
		NodeOutcomes: outcomes,
		Rationale:    reason,
	})
}

// findingsReason is the messages of findings joined by "; ", the way a failed
// gate's findings are shown to the next try.
func findingsReason(findings []remediation.Finding) string {
	messages := make([]string, 0, len(findings))
	for _, f := range findings {
		messages = append(messages, f.Message)
	}
	return strings.Join(messages, "; ")
}

// saveTry moves t to the status to in u's transaction, refusing a move the
// try's states do not allow. A deadline that is not zero becomes the try's
// deadline.
func (r *Runner) saveTry(ctx context.Context, u uow.UnitOfWork, t remediation.Try, to remediation.TryStatus, deadline time.Time) error {
	if !t.Status.CanMoveTo(to) {
		return fmt.Errorf("try %d cannot move from %s to %s", t.Try, t.Status, to)
	}
	t.Status = to
	if !deadline.IsZero() {
		t.DeadlineAt = deadline
	}
	t.UpdatedAt = r.clock.Now()
	if err := u.RemediationRepo().SaveTry(ctx, t); err != nil {
		return fmt.Errorf("save try: %w", err)
	}
	return nil
}

// endTry ends the current try with the status to and the remediation with it,
// in u's transaction.
func (r *Runner) endTry(ctx context.Context, u uow.UnitOfWork, run tryRun, to remediation.TryStatus) error {
	if err := r.saveTry(ctx, u, *run.snap.Try, to, time.Time{}); err != nil {
		return err
	}
	return r.endRemediation(ctx, u, run.snap.Remediation)
}

// generatingProposal is the in-flight row of an attempt, naming the trigger's
// whole failing set: the members of each of the try's clusters handed over as
// it opened with the cluster's outcome, and every other node as being worked
// on.
func generatingProposal(id uuid.UUID, t remediation.Trigger, attempt int, now time.Time, clusters []remediation.Cluster) proposal.Proposal {
	nodeIDs := t.NodeIDs()
	outcomes := outcomesFor(nodeIDs, proposal.NodeOutcome{Status: proposal.StatusGenerating})
	for _, c := range clusters {
		if c.Outcome == nil {
			continue
		}
		for _, m := range c.Members {
			outcomes[m] = *c.Outcome
		}
	}
	p := proposal.Proposal{
		Source:           t.Source,
		ReleaseID:        t.ReleaseID,
		RemediationID:    id.String(),
		RemediationRound: t.Round,
		ResolvedNodeIDs:  nodeIDs,
		NodeOutcomes:     outcomes,
		Attempt:          attempt,
		Status:           proposal.StatusGenerating,
		Services:         t.Services(),
		CreatedAt:        now,
	}
	p.NormalizeRepresentativeViews()
	p.ErrorSignature = signatureFor(t, p.NodeID)
	return p
}

// countAttempts returns the release's terminal attempts: inRound in the
// trigger's round, which the flow's tries are counted against, and total over
// every round up to the trigger's, which the next attempt is numbered after.
// The proposal table's attempt numbers are unique per release, so a round's
// first attempt continues the release's sequence. In-flight attempts are not
// counted, so an attempt keeps its number while it runs.
func countAttempts(ctx context.Context, repo repository.ProposalRepository, t remediation.Trigger) (inRound, total int, err error) {
	for round := 1; round <= t.Round; round++ {
		n, err := repo.CountAttempts(ctx, t.ReleaseID, round)
		if err != nil {
			return 0, 0, fmt.Errorf("count attempts (round %d): %w", round, err)
		}
		total += n
		inRound = n
	}
	return inRound, total, nil
}

// newCluster is cluster n of a try, matched to the first type of the flow that
// handles it. A single cluster whose target is a dbt test is handed over at
// once with its member skipped, before any type is matched, since a test is
// never a fix target whatever its failure. Any other cluster no type handles is
// handed over at once with its members skipped for that reason. Both hand-overs
// carry the code no_type.
func (r *Runner) newCluster(rem remediation.Remediation, try remediation.Try, flow workflow.Flow, t remediation.Trigger,
	n int, kind workflow.ClusterKind, target string, members []string) remediation.Cluster {
	c := remediation.Cluster{
		RemediationID: rem.ID,
		Try:           try.Try,
		Cluster:       n,
		Kind:          kind,
		TargetNodeID:  target,
		Members:       append([]string(nil), members...),
		Status:        remediation.ClusterGathering,
		Round:         1,
		UpdatedAt:     r.clock.Now(),
	}
	var nodeType string
	if kind == workflow.ClusterSingle {
		if node, ok := nodeByID(t, target); ok {
			nodeType = node.NodeType
		}
	}
	reason := dbtTestSkipReason
	if nodeType != string(pkg_model.NodeTypeDbtTest) {
		typ, ok := r.d.Files.TypeFor(flow, kind, t.Source, nodeType)
		if ok {
			c.TypeName = typ.Name
			return c
		}
		reason = noTypeSkipReason
	}
	c.Status = remediation.ClusterHandedOver
	c.Outcome = &proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: reason, Code: pkg_model.HandoverReasonNoType}
	r.logger.Info("cluster handed over before any step; its members are skipped",
		"release", t.ReleaseID, "cluster", n, "kind", kind, "target", target, "reason", reason)
	return c
}

// fileRevisions is the revision of the flow's file and of the file of every
// type it lists, by file name.
func fileRevisions(set workflow.Set, flow workflow.Flow) map[string]string {
	out := map[string]string{fileName(flow.File, flow.Name): flow.Revision}
	for _, name := range flow.Types {
		if t, ok := set.Types[name]; ok {
			out[fileName(t.File, t.Name)] = t.Revision
		}
	}
	return out
}

// fileName is a definition's file, or its name when it was not read from one.
func fileName(file, name string) string {
	if file == "" {
		return name
	}
	return file
}

// groupClusters partitions failing nodes of the trigger into fix targets. A
// node broken by a change in another service is fixed at that changed
// ancestor; nodes that fail the same way below one ancestor their own service
// changed are fixed at that ancestor once; every other node is its own
// cluster.
func groupClusters(failing []remediation.TriggerNode) []typology.Cluster {
	nodes := make([]typology.FailingNode, 0, len(failing))
	dag := typology.DagView{ChangedAncestorsByNode: make(map[string][]typology.ChangedAncestor, len(failing))}
	for _, n := range failing {
		nodes = append(nodes, typology.FailingNode{
			NodeID:         n.NodeID,
			ErrorSignature: n.ErrorSignature,
			Category:       n.Category,
			Reason:         n.Reason,
			Service:        n.Service,
			NodeType:       n.NodeType,
		})
		ancestors := make([]typology.ChangedAncestor, 0, len(n.ChangedAncestors))
		for _, a := range n.ChangedAncestors {
			ancestors = append(ancestors, typology.ChangedAncestor{NodeID: a.NodeID, Service: a.Service, Depth: a.Depth})
		}
		dag.ChangedAncestorsByNode[n.NodeID] = ancestors
	}
	return coalesceUpstream(typology.Group(nodes, dag, typology.CrossServiceCause{}, typology.SharedUpstreamCause{}))
}

// coalesceUpstream merges upstream clusters (shared or cross-service) that name
// the same fix target. Grouping partitions the failing set per error signature,
// so one changed ancestor that broke its descendants in two different ways
// yields one cluster per signature, both targeting that ancestor. Fixing them
// separately would call the model twice for one file and have both fixes write
// the artifact key the target's edit is keyed on. The members are unioned
// instead and the ancestor is repaired once, with every failure it caused shown
// to the model in the same call.
//
// Only upstream clusters can collide this way: a single cluster's target is
// the failing node itself, and a node appears in the failing set once. Order is
// preserved (each merged cluster keeps the position of its first occurrence)
// and members are sorted, so the result stays deterministic.
func coalesceUpstream(clusters []typology.Cluster) []typology.Cluster {
	at := map[string]int{}
	out := make([]typology.Cluster, 0, len(clusters))
	for _, c := range clusters {
		if !isUpstreamKind(c.Kind) {
			out = append(out, c)
			continue
		}
		i, seen := at[c.TargetNodeID]
		if !seen {
			at[c.TargetNodeID] = len(out)
			out = append(out, c)
			continue
		}
		merged := append(append([]string(nil), out[i].Members...), c.Members...)
		sort.Strings(merged)
		out[i].Members = merged
		// A target reached across a service boundary by any member keeps the
		// cross-service kind: its decline must never fall back to fixes in the
		// members' own services.
		if c.Kind == typology.KindCrossServiceUpstream {
			out[i].Kind = typology.KindCrossServiceUpstream
		}
	}
	return out
}

// isUpstreamKind reports whether a cluster repairs a changed ancestor rather
// than the failing node itself.
func isUpstreamKind(k typology.Kind) bool {
	return k == typology.KindSharedUpstream || k == typology.KindCrossServiceUpstream
}

// clusterKind is the flow's name for a grouping's cluster shape.
func clusterKind(k typology.Kind) workflow.ClusterKind {
	switch k {
	case typology.KindSharedUpstream:
		return workflow.ClusterSharedUpstream
	case typology.KindCrossServiceUpstream:
		return workflow.ClusterCrossService
	default:
		return workflow.ClusterSingle
	}
}

// nodeByID returns the trigger's failing node with this id.
func nodeByID(t remediation.Trigger, nodeID string) (remediation.TriggerNode, bool) {
	for _, n := range t.Nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}
	return remediation.TriggerNode{}, false
}

// outcomesFor stamps the same outcome on every named node.
func outcomesFor(nodeIDs []string, o proposal.NodeOutcome) map[string]proposal.NodeOutcome {
	out := make(map[string]proposal.NodeOutcome, len(nodeIDs))
	for _, id := range nodeIDs {
		out[id] = o
	}
	return out
}

// signatureFor returns the error signature of one of the trigger's nodes, empty
// when the trigger does not carry it.
func signatureFor(t remediation.Trigger, nodeID string) string {
	if n, ok := nodeByID(t, nodeID); ok {
		return n.ErrorSignature
	}
	return ""
}

// verificationServices is the services an attempt's verification runs cover.
func verificationServices(vs []proposal.Verification) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Service)
	}
	return out
}
