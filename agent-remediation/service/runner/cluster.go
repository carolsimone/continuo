package runner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// noEditReason is the outcome of the members of a cluster whose draft passed
// pre-flight with no edit: nothing a release check could run, and nothing
// a pull request could carry. Such members are recorded declined.
const noEditReason = "the fix could not be applied to version-controlled source, so there is nothing to verify"

// contextRound is the round a cluster's context steps run in. They run once
// per try, in its first round and only while the cluster is gathering; every
// later step rebuilds the cluster's evidence from their rows.
const contextRound = 1

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
// the flow's context steps, the type's context steps, then in each round the
// type's prompt and the type's pre-flight checks in one pass.
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

// runClusterStep runs the next step of an open cluster and commits it: while
// the cluster is gathering, the first context step the loaded files list for it
// that has no done row; else the prompt of the cluster's round when the round
// has no done prompt row; else that round's pre-flight pass on the draft of
// the round's prompt row. The cluster's evidence is rebuilt from the done
// context rows it has (gatheredEvidence), so a cluster past gathering never
// runs a context step, and keeps working when the loaded files list other
// context steps than the ones it ran. A cluster whose type the loaded files do
// not define fails on the runner's plan row, and a done row that cannot be
// read back fails on the row of the step about to run, so either failure is
// counted like a step's own.
func (r *Runner) runClusterStep(ctx context.Context, run tryRun, c remediation.Cluster) error {
	plan, err := r.planFor(run, c)
	if err != nil {
		return r.recordFailure(ctx, r.newRow(run, c.Cluster, stepPlan, kindRunner, c.Round, run.flow.Revision), err)
	}
	f := failureFor(run.trigger, c, *run.snap.Try)
	ev, err := r.gatheredEvidence(run, c, f)
	if err != nil {
		return r.recordFailure(ctx, r.pendingRow(run, c, plan), err)
	}
	if ps, last, ok := nextContextStep(run, c, plan); ok {
		return r.runContext(ctx, run, c, f, ev, ps, last)
	}
	row, done := run.journal.promptRow(c.Cluster, c.Round)
	if !done {
		return r.runPrompt(ctx, run, c, plan, f, ev)
	}
	draft, err := promptDraft(row)
	if err != nil {
		return r.recordFailure(ctx, r.pendingRow(run, c, plan), err)
	}
	return r.runPreflight(ctx, run, c, plan, f, ev, draft)
}

// nextContextStep returns the context step a gathering cluster runs next: the
// first one its plan lists with no done row in the first round. last reports
// that every other context step of the plan has one, so the cluster moves on
// to proposing once this one is done. A cluster past gathering has no next
// context step.
func nextContextStep(run tryRun, c remediation.Cluster, plan clusterPlan) (next plannedStep, last, ok bool) {
	if c.Status != remediation.ClusterGathering {
		return plannedStep{}, false, false
	}
	missing := 0
	for _, ps := range plan.context {
		if _, done := run.journal.done(c.Cluster, string(ps.name), contextRound); done {
			continue
		}
		if missing == 0 {
			next = ps
		}
		missing++
	}
	return next, missing == 1, missing > 0
}

// pendingRow starts the row of the step the cluster runs next, as
// runClusterStep chooses it: its next context step while gathering, else the
// prompt of its round, else that round's pre-flight pass.
func (r *Runner) pendingRow(run tryRun, c remediation.Cluster, plan clusterPlan) remediation.StepRun {
	if ps, _, ok := nextContextStep(run, c, plan); ok {
		return r.newRow(run, c.Cluster, string(ps.name), string(step.KindContext), contextRound, ps.revision)
	}
	if _, done := run.journal.promptRow(c.Cluster, c.Round); !done {
		return r.newRow(run, c.Cluster, string(plan.prompt.name), string(step.KindPrompt), c.Round, plan.prompt.revision)
	}
	return r.newRow(run, c.Cluster, stepPreflight, kindRunner, c.Round, plan.typ.Revision)
}

// runContext runs one context step. A decline ends the cluster with its members
// declined; the last context step moves it on to the prompt.
func (r *Runner) runContext(ctx context.Context, run tryRun, c remediation.Cluster, f remediation.Failure, ev remediation.Evidence, ps plannedStep, last bool) error {
	row := r.newRow(run, c.Cluster, string(ps.name), string(step.KindContext), contextRound, ps.revision)
	src, err := lookup[step.ContextSource](r.d.Registry, ps.name, step.KindContext)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	if row.InputDigest, err = serialization.InputDigest(ev, contextRound); err != nil {
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
	if row.Output, err = serialization.EncodeRecord(serialization.ContextRecord{Evidence: evidence, Declined: out.Declined, Decline: out.Decline}); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", ps.name, err))
	}
	r.finish(&row)

	next := c
	var added []remediation.Cluster
	switch {
	case out.Declined:
		added, err = r.settle(run, &next, declined(ps.name, out.Decline))
	case last:
		err = moveCluster(&next, remediation.ClusterProposing)
	}
	if err != nil {
		return err
	}
	run.countStage(c.TypeName, ports.StageContext, row)
	return r.commitStep(ctx, run, row, []remediation.StepRun{row}, append([]remediation.Cluster{next}, added...))
}

// runPrompt runs the cluster's prompt step through a model handle keyed by the
// remediation, try, cluster and round. The step is given the failure, the
// evidence and, in a round after the first, the findings of the round before,
// each cleaned by the Redactor of the cluster's round (redactionFor), and the
// round before itself as the journal holds it (previousRound). The prompt row
// stores the request as sent and the draft as the model's answer gave it, both
// holding placeholders. A decline ends the cluster with its members declined; a
// draft moves it on to pre-flight.
func (r *Runner) runPrompt(ctx context.Context, run tryRun, c remediation.Cluster, plan clusterPlan, f remediation.Failure, ev remediation.Evidence) error {
	ps := plan.prompt
	row := r.newRow(run, c.Cluster, string(ps.name), string(step.KindPrompt), c.Round, ps.revision)
	p, err := lookup[step.Prompt](r.d.Registry, ps.name, step.KindPrompt)
	if err != nil {
		return r.recordFailure(ctx, row, err)
	}
	if row.InputDigest, err = serialization.InputDigest(ev, c.Round); err != nil {
		return r.recordFailure(ctx, row, err)
	}
	red, err := r.redactionFor(run, c, ev)
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
	}
	in := step.PromptInput{Failure: f, Evidence: ev, Round: c.Round - 1}
	if c.Round > 1 {
		if in, err = r.previousRound(run, c, in); err != nil {
			return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
		}
	}
	in = red.prompt(in)
	m := newModel(r.d.LLM, run.snap.Remediation.ID, run.snap.Try.Try, c.Cluster, c.Round, promptlog.Failure{
		Source:    run.trigger.Source,
		ReleaseID: run.trigger.ReleaseID,
		NodeID:    c.TargetNodeID,
		Try:       run.snap.Try.Try,
	}, r.tokenCounter(run.snap.Remediation.Flow, c.TypeName))
	out, err := p.Propose(ctx, in, m)
	row.Model, row.PromptVersion = m.lastModel, m.promptVersion
	row.TokensIn, row.TokensOut, row.CacheHit = m.tokensIn, m.tokensOut, m.cacheHit()
	if err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("step %s: %w", ps.name, err))
	}
	if row.Output, err = serialization.EncodeRecord(promptRecord(out, m)); err != nil {
		return r.recordFailure(ctx, row, fmt.Errorf("encode the output of step %s: %w", ps.name, err))
	}
	r.finish(&row)

	next := c
	var added []remediation.Cluster
	if out.Declined {
		added, err = r.settle(run, &next, declined(ps.name, out.Decline))
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
	run.countStage(c.TypeName, ports.StagePropose, row)
	return r.commitStep(ctx, run, row, []remediation.StepRun{row}, append([]remediation.Cluster{next}, added...))
}

// previousRound returns in with the round before the cluster's, as the journal
// holds that round's prompt row, whatever the loaded files list now: its draft,
// its last request and its last answer, all holding the placeholders of the
// cluster's Redactor. The findings of that round come from redactionFor. A
// prompt row that stores no model call cannot be revised, and is an error.
func (r *Runner) previousRound(run tryRun, c remediation.Cluster, in step.PromptInput) (step.PromptInput, error) {
	prev := c.Round - 1
	row, done := run.journal.promptRow(c.Cluster, prev)
	if !done {
		return in, fmt.Errorf("round %d's prompt step has no done row", prev)
	}
	rec, err := serialization.DecodeRecord[serialization.PromptRecord](row.Output)
	if err != nil {
		return in, fmt.Errorf("decode the output of round %d's prompt step: %w", prev, err)
	}
	if rec.LastRequest == nil || rec.LastAnswer == nil {
		return in, fmt.Errorf("round %d's prompt row stores no model call to revise", prev)
	}
	req, answer := rec.LastRequest.Request(), rec.LastAnswer.Answer()
	in.Previous, in.PreviousRequest, in.PreviousAnswer = &rec.Draft, &req, &answer
	return in, nil
}

// promptRecord is the journal record of a prompt step's output: its draft or
// its decline and, for a step that made a model call and did not decline, the
// last call it made, which a later round revises. A declined cluster takes no
// later round, so a decline stores no call.
func promptRecord(out step.PromptOutput, m *model) serialization.PromptRecord {
	rec := serialization.PromptRecord{Draft: out.Draft, Declined: out.Declined, Decline: out.Decline}
	if !out.Declined && m.calls > 0 {
		req, answer := serialization.RequestOf(m.lastRequest), serialization.AnswerOf(m.lastAnswer)
		rec.LastRequest, rec.LastAnswer = &req, &answer
	}
	return rec
}

// runPreflight restores the draft the prompt step read from the model's answer
// with the Redactor of the cluster's round (redactionFor), then runs every
// pre-flight check of the cluster's type on the restored draft, in file order,
// in one pass over a fresh scratch checkout, and collects every finding. The
// runner's own rule comes first in the pass, so no file can drop it: an edit
// and the rationale get a value back only when the code the cluster was shown
// or an edit's original holds it (restoreDraft). A placeholder in an edit
// whose value came from a log or a check, a redaction marker an edit adds to
// its file once the edit's original is known (markerFindings), and a
// placeholder never shown in a path or the rationale are each the finding
// unknown_placeholder. The pass's row stores the restored draft, with every
// placeholder it did not restore left in it. Each check's row, the pass's own
// row and the cluster's end commit together, so a pass interrupted before its
// commit runs again whole.
//
// Each edit's content is written the way its original is, line endings and
// final newline (remediation.KeepFileConventions), once the draft is restored
// and again once every edit has its original, so the checks, the stored fix
// and its diff see only the lines the fix changes.
//
// When every check passed, each edit with no original gets the one the pass's
// checkout read, the edits' content and diff are stored, and the cluster is
// checked. A draft that passed every check with no edit is a decline: the
// cluster settles with its members failed for noEditReason, with the code
// declined, and an upstream cluster falls back as on any decline. Otherwise, while the cluster has rounds left of the ones its type
// allows, it goes back to proposing in the next round, where the prompt step
// revises the draft with every finding of this pass. In its last round the
// cluster settles with its members failed on the pass's first finding, with
// the code preflight_rounds_spent.
func (r *Runner) runPreflight(ctx context.Context, run tryRun, c remediation.Cluster, plan clusterPlan, f remediation.Failure, ev remediation.Evidence, draft remediation.Draft) error {
	pass := r.newRow(run, c.Cluster, stepPreflight, kindRunner, c.Round, plan.typ.Revision)
	red, err := r.redactionFor(run, c, ev)
	if err != nil {
		return r.recordFailure(ctx, pass, err)
	}
	draft, placeholders := restoreDraft(draft, red.redactor, codeTexts(ev, r.d.Registry.EvidenceClass))
	draft = draft.KeepingFileConventions()
	ev = ev.With(remediation.KeyDraft, draft)
	if pass.InputDigest, err = serialization.InputDigest(ev, c.Round); err != nil {
		return r.recordFailure(ctx, pass, err)
	}
	scratch := step.NewScratch(r.d.Collab.Archive, r.d.Collab.Patcher, run.trigger.Repo, run.trigger.CommitSHA)
	defer scratch.Close()

	rows := make([]remediation.StepRun, 0, len(plan.preflight)+1)
	results := make([]remediation.CheckResult, 0, len(plan.preflight)+1)
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
	draft = withOriginals(draft, scratch.Originals()).KeepingFileConventions()
	placeholders = append(placeholders, markerFindings(draft, red.redactor)...)
	results = append([]remediation.CheckResult{{Check: findingUnknownPlaceholder, Findings: placeholders}}, results...)
	findings = append(slices.Clone(placeholders), findings...)

	next := c
	var added []remediation.Cluster
	round := ports.RoundPassed
	if first, refused := firstFinding(results); refused {
		if c.Round < plan.typ.Agent.Rounds {
			round = ports.RoundRevised
			next.Round++
			err = moveCluster(&next, remediation.ClusterProposing)
		} else {
			round = ports.RoundSpent
			added, err = r.settle(run, &next, fix.NodeOutcome{
				Status: fix.StatusFailed, Reason: first.Message, Code: pkg_model.HandoverReasonPreflightRoundsSpent,
			})
		}
		if err != nil {
			return err
		}
	} else if len(draft.Edits) == 0 {
		if added, err = r.settle(run, &next, fix.NodeOutcome{
			Status: fix.StatusFailed, Reason: noEditReason, Code: pkg_model.HandoverReasonDeclined,
		}); err != nil {
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
	if pass.Output, err = serialization.EncodeRecord(serialization.PreflightRecord{Draft: draft}); err != nil {
		return r.recordFailure(ctx, pass, fmt.Errorf("encode the pre-flight draft: %w", err))
	}
	pass.Findings = findings
	r.finish(&pass)
	rows = append(rows, pass)
	run.countStage(c.TypeName, ports.StagePreflight, pass)
	run.countRound(c.TypeName, round)
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
	if row.InputDigest, err = serialization.InputDigest(ev, c.Round); err != nil {
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
	if row.Output, err = serialization.EncodeRecord(serialization.CheckRecord{Result: out.Result, Evidence: evidence}); err != nil {
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
		diffURI, err := r.d.Collab.Artifacts.Write(ctx, e.DiffKey, fix.EditDiff(e.Original, e.Content, e.Path, e.RenamedFrom), artifactContentType)
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
// cross-project cluster are skipped with the code of o, because a fix in their
// own project cannot ship before the change in the other project that broke
// them. The code says why the upstream fix failed: declined when a step or the
// draft declined it, which settles the members for good, and
// preflight_rounds_spent when pre-flight refused every round, which leaves
// them to be grouped again in the next try.
func (r *Runner) settle(run tryRun, c *remediation.Cluster, o fix.NodeOutcome) ([]remediation.Cluster, error) {
	switch c.Kind {
	case workflow.ClusterSharedUpstream:
		if err := moveCluster(c, remediation.ClusterFellBack); err != nil {
			return nil, err
		}
		r.logger.Info("shared-upstream fix unavailable; each member falls back to its own source",
			"release", run.trigger.ReleaseID, "target", c.TargetNodeID, "members", c.Members, "reason", o.Reason)
		return r.memberClusters(run, *c), nil
	case workflow.ClusterCrossProject:
		if err := moveCluster(c, remediation.ClusterFellBack); err != nil {
			return nil, err
		}
		_, project := ancestorLocation(run.trigger, c.TargetNodeID)
		c.Outcome = &fix.NodeOutcome{
			Status: fix.StatusSkipped,
			Reason: fmt.Sprintf("the change that broke this node lives in %s; a fix in this node's own project cannot ship before it: %s",
				describeProject(project), o.Reason),
			Code: o.Code,
		}
		r.logger.Info("cross-project upstream fix unavailable; members are skipped, not fixed in their own project",
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

// declined is the outcome of the members of a cluster the step named declined,
// for reason: skipped, with the code declined. A step that gives no reason is
// recorded as having declined the fix, so a hand-over never carries an empty
// message.
func declined(name workflow.StepName, reason string) fix.NodeOutcome {
	if strings.TrimSpace(reason) == "" {
		reason = fmt.Sprintf("step %s declined the fix", name)
	}
	return fix.NodeOutcome{Status: fix.StatusSkipped, Reason: reason, Code: pkg_model.HandoverReasonDeclined}
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
// shape, the node the fix edits, its member nodes, and the try. An upstream cluster's target is a changed ancestor the trigger may
// not carry as a failing node; it is then named by its id alone.
func failureFor(t remediation.Trigger, c remediation.Cluster, try remediation.Try) remediation.Failure {
	f := remediation.Failure{Trigger: t, Kind: c.Kind, TargetNodeID: c.TargetNodeID, Try: try.Try}
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
func ancestorLocation(t remediation.Trigger, ancestorID string) (filePath, project string) {
	for _, n := range t.Nodes {
		for _, a := range n.ChangedAncestors {
			if a.NodeID == ancestorID && a.FilePath != "" && a.Project != "" {
				return a.FilePath, a.Project
			}
		}
	}
	return "", ""
}

// describeProject names a project for an operator-facing reason, or "another
// project" when the trigger carried no project for the ancestor.
func describeProject(project string) string {
	if project == "" {
		return "another project"
	}
	return "project " + project
}
