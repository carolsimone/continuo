package runner

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// afterCommit holds the metric samples the work of one step earns, to be
// counted once the transaction that stores the step has committed (commitRun).
// A step whose commit fails earns nothing, and counts once when it runs again
// and commits. A nil afterCommit holds nothing.
type afterCommit struct {
	samples []func(ports.RemediationMetrics)
}

func (a *afterCommit) add(sample func(ports.RemediationMetrics)) {
	if a != nil {
		a.samples = append(a.samples, sample)
	}
}

// flush counts every sample on m and forgets them.
func (a *afterCommit) flush(m ports.RemediationMetrics) {
	if a == nil {
		return
	}
	for _, s := range a.samples {
		s(m)
	}
	a.samples = nil
}

// discard forgets every sample.
func (a *afterCommit) discard() {
	if a != nil {
		a.samples = nil
	}
}

// commitRun is commit for the step of run: when the transaction committed, it
// counts the samples run collected while the step was built, and when it did
// not, it forgets them.
func (r *Runner) commitRun(ctx context.Context, run tryRun, fn func(u uow.UnitOfWork) error) error {
	if err := r.commit(ctx, run.snap.Remediation.ID, fn); err != nil {
		run.after.discard()
		return err
	}
	run.after.flush(r.metrics)
	return nil
}

// countStage collects the time a committed step took, from its row, for a
// stage of a cluster of the type typ, or of the flow itself when typ is empty.
func (run tryRun) countStage(typ, stage string, row remediation.StepRun) {
	var d time.Duration
	if row.FinishedAt != nil {
		d = max(0, row.FinishedAt.Sub(row.StartedAt))
	}
	flow := run.snap.Remediation.Flow
	run.after.add(func(m ports.RemediationMetrics) { m.Stage(flow, typ, stage, d) })
}

// countRound collects the end of one pre-flight pass of a cluster of the type
// typ.
func (run tryRun) countRound(typ, result string) {
	flow := run.snap.Remediation.Flow
	run.after.add(func(m ports.RemediationMetrics) { m.Round(flow, typ, result) })
}

// countTries collects how each cluster the try worked ended, as the try ends.
// checked is the result of the clusters that went on to be verified, empty for
// a try that had none. A cluster no type matched and a cluster handed over when
// the try opened, which ran no step, count nothing. A shared-upstream cluster
// that fell back counts nothing itself: its members count their own result.
func (run tryRun) countTries(checked string) {
	flow := run.snap.Remediation.Flow
	for _, c := range run.snap.Clusters {
		if !run.journal.worked(c.Cluster) {
			continue
		}
		result, ok := clusterResult(c, checked)
		if !ok {
			continue
		}
		typ := c.TypeName
		run.after.add(func(m ports.RemediationMetrics) { m.Try(flow, typ, result) })
	}
}

// clusterResult is how a cluster of the type it was matched to ended in its
// try. A cluster with no type was never worked, whatever it was handed over
// with. A cluster that fell back ends with the code that made it fall back, when
// it carries one: a cross-project cluster, whose members get no cluster of their
// own. A shared-upstream cluster that fell back carries none.
func clusterResult(c remediation.Cluster, checked string) (string, bool) {
	if c.TypeName == "" {
		return "", false
	}
	switch c.Status {
	case remediation.ClusterChecked:
		return checked, checked != ""
	case remediation.ClusterHandedOver, remediation.ClusterFellBack:
		if c.Outcome == nil {
			return "", false
		}
		switch c.Outcome.Code {
		case pkg_model.HandoverReasonDeclined:
			return ports.TryDeclined, true
		case pkg_model.HandoverReasonPreflightRoundsSpent:
			return ports.TryPreflightSpent, true
		}
	}
	return "", false
}

// countOutcomes collects how each node ends with the remediation, from the
// outcomes of its final attempt: a node a verified fix covers is fixed, and a
// node whose outcome carries a code is handed over with that code. A node
// with neither, such as one skipped because maintenance mode was on, is not
// counted: it is neither fixed nor left to a human, and the release can be
// retried. A node is counted under the type of its cluster, or none.
func (run tryRun) countOutcomes(outcomes map[string]proposal.NodeOutcome) {
	flow := run.snap.Remediation.Flow
	types := nodeTypes(run.snap.Clusters)
	for _, id := range sortedKeys(outcomes) {
		o := outcomes[id]
		var outcome, reason string
		switch {
		case o.Code != "":
			outcome, reason = ports.OutcomeHandedOver, string(o.Code)
		case o.Status == proposal.StatusProposed:
			outcome = ports.OutcomeFixed
		default:
			continue
		}
		typ := types[id]
		if typ == "" {
			typ = ports.TypeNone
		}
		run.after.add(func(m ports.RemediationMetrics) { m.Outcome(flow, typ, outcome, reason) })
	}
}

// nodeTypes is the type of the cluster each node was worked in. A node in two
// clusters, a member of one that fell back and the target of the cluster of its
// own that followed, takes the type of the later one.
func nodeTypes(clusters []remediation.Cluster) map[string]string {
	out := map[string]string{}
	for _, c := range clusters {
		for _, m := range c.Members {
			out[m] = c.TypeName
		}
	}
	return out
}

// tokenCounter returns the function a step's model handle calls with the
// tokens each answered model call used, which counts them for a cluster of the
// type typ.
func (r *Runner) tokenCounter(flow, typ string) func(in, out int) {
	return func(in, out int) {
		if in > 0 {
			r.metrics.Tokens(flow, typ, ports.TokensIn, in)
		}
		if out > 0 {
			r.metrics.Tokens(flow, typ, ports.TokensOut, out)
		}
	}
}
