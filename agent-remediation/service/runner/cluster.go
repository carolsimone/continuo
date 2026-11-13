package runner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// codeTargetNotShown is the finding of a draft whose target file the model was
// not shown. A cluster refused with it is skipped rather than failed: the
// model never edited a file it could see.
const codeTargetNotShown = "target_not_shown"

// artifactContentType is the content type of a draft's stored content and
// diff.
const artifactContentType = "text/plain"

// plannedStep is one step a cluster runs, with the revision of the file that
// lists it.
type plannedStep struct {
	name     workflow.StepName
	revision string
}

// clusterPlan is the steps a cluster runs, in the order the runner runs them:
// the flow's context steps, the type's context steps, the type's prompt, then
// the type's pre-flight checks in one pass.
type clusterPlan struct {
	typ       workflow.Type
	context   []plannedStep
	prompt    plannedStep
	preflight []plannedStep
}

func (r *Runner) planFor(run tryRun, c remediation.Cluster) (clusterPlan, error) {
	typ, ok := r.d.Files.Types[c.TypeName]
	if !ok {
		return clusterPlan{}, fmt.Errorf("cluster %d has type %q, which the loaded files do not define", c.Cluster, c.TypeName)
	}
	p := clusterPlan{typ: typ, prompt: plannedStep{name: typ.Agent.Prompt.Name, revision: typ.Revision}}
	for _, ref := range run.flow.Context {
		p.context = append(p.context, plannedStep{name: ref.Name, revision: run.flow.Revision})
	}
	for _, ref := range typ.Context {
		p.context = append(p.context, plannedStep{name: ref.Name, revision: typ.Revision})
	}
	for _, ref := range typ.Preflight {
		p.preflight = append(p.preflight, plannedStep{name: ref.Name, revision: typ.Revision})
	}
	return p, nil
}

// runClusterStep runs the next step of an open cluster and commits it: the
// first context step with no done row, else the prompt, else the pre-flight
// pass. The cluster's evidence is rebuilt from the rows of its done steps. A
// cluster whose type the loaded files do not define fails on the runner's plan
// row, and a done row that cannot be read back fails on the row of the step
// about to run, so either failure is counted like a step's own.
func (r *Runner) runClusterStep(ctx context.Context, run tryRun, c remediation.Cluster) error {
	plan, err := r.planFor(run, c)
	if err != nil {
		return r.recordFailure(ctx, r.newRow(run, c.Cluster, stepPlan, kindRunner, c.Round, run.flow.Revision), err)
	}
	f := failureFor(run.trigger, c, run.snap.Try.Attempt)
	ev := remediation.NewEvidence().With(remediation.KeyFailure, f)
	for i, ps := range plan.context {
		row, done := run.journal.done(c.Cluster, string(ps.name), c.Round)
		if !done {
			return r.runContext(ctx, run, c, f, ev, ps, i == len(plan.context)-1)
		}
		if ev, err = r.contextEvidence(ev, row); err != nil {
			return r.recordFailure(ctx, r.pendingRow(run, c, plan), err)
		}
	}
	row, done := run.journal.done(c.Cluster, string(plan.prompt.name), c.Round)
	if !done {
		return r.runPrompt(ctx, run, c, f, ev, plan.prompt)
	}
	draft, err := promptDraft(row)
	if err != nil {
		return r.recordFailure(ctx, r.pendingRow(run, c, plan), err)
	}
	return r.runPreflight(ctx, run, c, plan, f, ev, draft)
}

// pendingRow starts the row of the step the cluster runs next: its first
// context step with no done row, else its prompt, else its pre-flight pass.
func (r *Runner) pendingRow(run tryRun, c remediation.Cluster, plan clusterPlan) remediation.StepRun {
	for _, ps := range plan.context {
		if _, done := run.journal.done(c.Cluster, string(ps.name), c.Round); !done {
			return r.newRow(run, c.Cluster, string(ps.name), string(step.KindContext), c.Round, ps.revision)
		}
	}
	if _, done := run.journal.done(c.Cluster, string(plan.prompt.name), c.Round); !done {
		return r.newRow(run, c.Cluster, string(plan.prompt.name), string(step.KindPrompt), c.Round, plan.prompt.revision)
	}
	return r.newRow(run, c.Cluster, stepPreflight, kindRunner, c.Round, plan.typ.Revision)
}

// runContext runs one context step. A decline ends the cluster; the last
// context step moves it on to the prompt.
func (r *Runner) runContext(ctx context.Context, run tryRun, c remediation.Cluster, f remediation.Failure, ev remediation.Evidence, ps plannedStep, last bool) error {
	row := r.newRow(run, c.Cluster, string(ps.name), string(step.KindContext), c.Round, ps.revision)
	src, err := lookup[step.ContextSource](r.d.Registry, ps.name, step.KindContext)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	if row.InputDigest, err = inputDigest(ev, c.Round); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	out, err := src.Gather(ctx, step.ContextInput{Failure: f, Evidence: ev})
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
	}
	evidence, err := r.encodeEvidence(out.Evidence)
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
	}
	if row.Output, err = json.Marshal(contextRecord{Evidence: evidence, Declined: out.Declined, Decline: out.Decline}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", ps.name, err))
	}
	r.finish(&row)

	next := c
	var added []remediation.Cluster
	switch {
	case out.Declined:
		added, err = r.settle(run, &next, proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: out.Decline})
	case last:
		err = moveCluster(&next, remediation.ClusterProposing)
	}
	if err != nil {
		return err
	}
	return r.commitStep(ctx, run, row, []remediation.StepRun{row}, append([]remediation.Cluster{next}, added...))
}

// runPrompt runs the cluster's prompt step through a model handle keyed by the
// remediation, try, cluster and round. A decline ends the cluster; a draft
// moves it on to pre-flight.
func (r *Runner) runPrompt(ctx context.Context, run tryRun, c remediation.Cluster, f remediation.Failure, ev remediation.Evidence, ps plannedStep) error {
	row := r.newRow(run, c.Cluster, string(ps.name), string(step.KindPrompt), c.Round, ps.revision)
	p, err := lookup[step.Prompt](r.d.Registry, ps.name, step.KindPrompt)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	if row.InputDigest, err = inputDigest(ev, c.Round); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	m := newModel(r.d.LLM, run.snap.Remediation.ID, run.snap.Try.Try, c.Cluster, c.Round, promptlog.Failure{
		Source:    run.trigger.Source,
		ReleaseID: run.trigger.ReleaseID,
		NodeID:    c.TargetNodeID,
		Attempt:   run.snap.Try.Attempt,
	})
	out, err := p.Propose(ctx, step.PromptInput{Failure: f, Evidence: ev, Round: c.Round - 1}, m)
	row.Model, row.PromptVersion = m.lastModel, m.promptVersion
	row.TokensIn, row.TokensOut, row.CacheHit = m.tokensIn, m.tokensOut, m.cacheHit()
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
	}
	if row.Output, err = json.Marshal(promptRecord{Draft: out.Draft, Declined: out.Declined, Decline: out.Decline}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", ps.name, err))
	}
	r.finish(&row)

	next := c
	var added []remediation.Cluster
	if out.Declined {
		added, err = r.settle(run, &next, proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: out.Decline})
	} else {
		if next.Status == remediation.ClusterGathering {
			err = moveCluster(&next, remediation.ClusterProposing)
		}
		if err == nil {
			err = moveCluster(&next, remediation.ClusterPreflight)
		}
	}
	if err != nil {
		return err
	}
	return r.commitStep(ctx, run, row, []remediation.StepRun{row}, append([]remediation.Cluster{next}, added...))
}

// runPreflight runs every pre-flight check of the cluster's type on the draft,
// in file order, in one pass over a fresh scratch checkout, and collects every
// finding. Each check's row, the pass's own row and the cluster's end commit
// together, so a pass interrupted before its commit runs again whole.
//
// When every check passed, each edit with no original gets the one the pass's
// checkout read, the edits' content and diff are stored, and the cluster is
// checked. Otherwise the cluster ends on the first finding: its members are
// skipped when the model named a file it was not shown, and failed otherwise.
func (r *Runner) runPreflight(ctx context.Context, run tryRun, c remediation.Cluster, plan clusterPlan, f remediation.Failure, ev remediation.Evidence, draft remediation.Draft) error {
	pass := r.newRow(run, c.Cluster, stepPreflight, kindRunner, c.Round, plan.typ.Revision)
	ev = ev.With(remediation.KeyDraft, draft)
	var err error
	if pass.InputDigest, err = inputDigest(ev, c.Round); err != nil {
		return r.recordFailure(ctx, pass, err)
	}
	scratch := step.NewScratch(r.d.Collab.Archive, run.trigger.Repo, run.trigger.CommitSHA)
	defer scratch.Close()

	rows := make([]remediation.StepRun, 0, len(plan.preflight)+1)
	results := make([]remediation.CheckResult, 0, len(plan.preflight))
	var findings []remediation.Finding
	for _, ps := range plan.preflight {
		row, result, gathered, err := r.runCheck(ctx, run, c, f, ev, draft, scratch, ps)
		if err != nil {
			return r.recordFailure(ctx, row, err)
		}
		for k, v := range gathered {
			ev = ev.With(k, v)
		}
		rows = append(rows, row)
		results = append(results, result)
		findings = append(findings, result.Findings...)
	}
	scratch.Close()
	draft = withOriginals(draft, scratch.Originals())

	next := c
	var added []remediation.Cluster
	if first, refused := firstFinding(results); refused {
		status := proposal.StatusFailed
		if first.Code == codeTargetNotShown {
			status = proposal.StatusSkipped
		}
		if added, err = r.settle(run, &next, proposal.NodeOutcome{Status: status, Reason: first.Message}); err != nil {
			return err
		}
	} else {
		if draft, err = r.writeArtifacts(ctx, draft); err != nil {
			return r.recordFailure(ctx, pass, err)
		}
		checked, err := remediation.NewChecked(draft, results)
		if err != nil {
			return r.recordFailure(ctx, pass, err)
		}
		draft = checked.Draft()
		if err := moveCluster(&next, remediation.ClusterChecked); err != nil {
			return err
		}
	}
	if pass.Output, err = json.Marshal(preflightRecord{Draft: draft}); err != nil {
		return r.recordFailure(ctx, pass, fmt.Errorf("encode the pre-flight draft: %w", err))
	}
	pass.Findings = findings
	r.finish(&pass)
	rows = append(rows, pass)
	return r.commitStep(ctx, run, pass, rows, append([]remediation.Cluster{next}, added...))
}

// runCheck runs one pre-flight check and returns its finished row, its result
// and the evidence it gathered. On an error the row is returned unfinished,
// for the failure to be recorded on.
func (r *Runner) runCheck(ctx context.Context, run tryRun, c remediation.Cluster, f remediation.Failure, ev remediation.Evidence,
	draft remediation.Draft, scratch *step.Scratch, ps plannedStep) (remediation.StepRun, remediation.CheckResult, map[remediation.EvidenceKey]any, error) {
	row := r.newRow(run, c.Cluster, string(ps.name), string(step.KindCheck), c.Round, ps.revision)
	chk, err := lookup[step.Check](r.d.Registry, ps.name, step.KindCheck)
	if err != nil {
		return row, remediation.CheckResult{}, nil, err
	}
	if row.InputDigest, err = inputDigest(ev, c.Round); err != nil {
		return row, remediation.CheckResult{}, nil, err
	}
	out, err := chk.Check(ctx, step.CheckInput{Failure: f, Evidence: ev, Draft: draft, Scratch: scratch})
	if err != nil {
		return row, remediation.CheckResult{}, nil, fmt.Errorf("step %s: %w", ps.name, err)
	}
	evidence, err := r.encodeEvidence(out.Evidence)
	if err != nil {
		return row, remediation.CheckResult{}, nil, fmt.Errorf("step %s: %w", ps.name, err)
	}
	if row.Output, err = json.Marshal(checkRecord{Result: out.Result, Evidence: evidence}); err != nil {
		return row, remediation.CheckResult{}, nil, fmt.Errorf("encode the output of step %s: %w", ps.name, err)
	}
	row.Findings = out.Result.Findings
	r.finish(&row)
	return row, out.Result, out.Evidence, nil
}

// firstFinding returns the first finding of the first check that found one, in
// check order.
func firstFinding(results []remediation.CheckResult) (remediation.Finding, bool) {
	for _, res := range results {
		if len(res.Findings) > 0 {
			return res.Findings[0], true
		}
	}
	return remediation.Finding{}, false
}

// withOriginals returns d with each edit that has no original given the one
// the pre-flight checkout read for its path.
func withOriginals(d remediation.Draft, originals map[string]string) remediation.Draft {
	edits := make([]remediation.Edit, len(d.Edits))
	copy(edits, d.Edits)
	for i, e := range edits {
		if e.Original != "" {
			continue
		}
		if o, ok := originals[e.Path]; ok {
			edits[i].Original = o
		}
	}
	d.Edits = edits
	return d
}

// writeArtifacts stores each edit's content and its unified diff against the
// original under the keys the prompt step chose, and returns d with their URIs.
func (r *Runner) writeArtifacts(ctx context.Context, d remediation.Draft) (remediation.Draft, error) {
	edits := make([]remediation.Edit, len(d.Edits))
	copy(edits, d.Edits)
	for i, e := range edits {
		if e.ContentKey == "" || e.DiffKey == "" {
			return d, fmt.Errorf("the edit of %s names no key to store its content and diff under", e.Path)
		}
		contentURI, err := r.d.Collab.Artifacts.Write(ctx, e.ContentKey, e.Content, artifactContentType)
		if err != nil {
			return d, fmt.Errorf("write the content of %s: %w", e.Path, err)
		}
		diffURI, err := r.d.Collab.Artifacts.Write(ctx, e.DiffKey, proposal.ComputeUnifiedDiff(e.Original, e.Content, e.Path), artifactContentType)
		if err != nil {
			return d, fmt.Errorf("write the diff of %s: %w", e.Path, err)
		}
		edits[i].ContentURI, edits[i].DiffURI = contentURI, diffURI
	}
	d.Edits = edits
	return d, nil
}

// settle ends a cluster that declined or whose draft pre-flight refused, with o
// as its members' outcome. A single cluster is handed over. An upstream cluster
// falls back: each member of a shared-upstream cluster is added as a single
// cluster of its own, returned for the same commit, and the members of a
// cross-service cluster are skipped, because a fix in their own service cannot
// ship before the change in the other service that broke them.
func (r *Runner) settle(run tryRun, c *remediation.Cluster, o proposal.NodeOutcome) ([]remediation.Cluster, error) {
	switch c.Kind {
	case workflow.ClusterSharedUpstream:
		if err := moveCluster(c, remediation.ClusterFellBack); err != nil {
			return nil, err
		}
		r.logger.Info("shared-upstream fix unavailable; each member falls back to its own source",
			"release", run.trigger.ReleaseID, "target", c.TargetNodeID, "members", c.Members, "reason", o.Reason)
		return r.memberClusters(run, *c), nil
	case workflow.ClusterCrossService:
		if err := moveCluster(c, remediation.ClusterFellBack); err != nil {
			return nil, err
		}
		_, service := ancestorLocation(run.trigger, c.TargetNodeID)
		c.Outcome = &proposal.NodeOutcome{
			Status: proposal.StatusSkipped,
			Reason: fmt.Sprintf("the change that broke this node lives in %s; a fix in this node's own service cannot ship before it: %s",
				describeService(service), o.Reason),
		}
		r.logger.Info("cross-service upstream fix unavailable; members are skipped, not fixed in their own service",
			"release", run.trigger.ReleaseID, "target", c.TargetNodeID, "members", c.Members, "reason", o.Reason)
		return nil, nil
	default:
		if err := moveCluster(c, remediation.ClusterHandedOver); err != nil {
			return nil, err
		}
		c.Outcome = &o
		return nil, nil
	}
}

// memberClusters is one single cluster per member of a fallen-back upstream
// cluster, numbered after the try's last cluster, each matched to its type.
func (r *Runner) memberClusters(run tryRun, c remediation.Cluster) []remediation.Cluster {
	last := 0
	for _, existing := range run.snap.Clusters {
		last = max(last, existing.Cluster)
	}
	out := make([]remediation.Cluster, 0, len(c.Members))
	for i, m := range c.Members {
		out = append(out, r.newCluster(run.snap.Remediation, *run.snap.Try, run.flow, run.trigger, last+i+1,
			workflow.ClusterSingle, m, []string{m}))
	}
	return out
}

// moveCluster moves c to the status to, refusing a move the cluster's states do
// not allow.
func moveCluster(c *remediation.Cluster, to remediation.ClusterStatus) error {
	if !c.Status.CanMoveTo(to) {
		return fmt.Errorf("cluster %d cannot move from %s to %s", c.Cluster, c.Status, to)
	}
	c.Status = to
	return nil
}

// lookup returns the step registered under name as the interface of kind.
func lookup[T any](reg *Registry, name workflow.StepName, kind step.Kind) (T, error) {
	var zero T
	s, _, ok := reg.Lookup(name)
	if !ok {
		return zero, fmt.Errorf("step %q is not registered", name)
	}
	t, ok := s.(T)
	if !ok {
		return zero, fmt.Errorf("step %q cannot run as a %s step", name, kind)
	}
	return t, nil
}

// failureFor is the cluster as its steps read it: the trigger, the cluster's
// shape, the node the fix edits and its member nodes. An upstream cluster's
// target is a changed ancestor the trigger may not carry as a failing node;
// it is then named by its id alone.
func failureFor(t remediation.Trigger, c remediation.Cluster, attempt int) remediation.Failure {
	f := remediation.Failure{Trigger: t, Kind: c.Kind, TargetNodeID: c.TargetNodeID, Attempt: attempt}
	if n, ok := nodeByID(t, c.TargetNodeID); ok {
		f.Target = n
	} else {
		f.Target = remediation.TriggerNode{NodeID: c.TargetNodeID}
	}
	for _, id := range c.Members {
		if n, ok := nodeByID(t, id); ok {
			f.Members = append(f.Members, n)
		}
	}
	return f
}

// ancestorLocation is where the rejected release's candidate topology declares
// a changed ancestor, as the trigger carries it on the failing nodes that
// descend from it. Both are empty when no node carries a complete location for
// it.
func ancestorLocation(t remediation.Trigger, ancestorID string) (filePath, service string) {
	for _, n := range t.Nodes {
		for _, a := range n.ChangedAncestors {
			if a.NodeID == ancestorID && a.FilePath != "" && a.Service != "" {
				return a.FilePath, a.Service
			}
		}
	}
	return "", ""
}

// describeService names a service for an operator-facing reason, or "another
// service" when the trigger carried no service for the ancestor.
func describeService(service string) string {
	if service == "" {
		return "another service"
	}
	return "service " + service
}
