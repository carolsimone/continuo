package runner_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// lowConfidenceFinding is what confidence_not_low finds in a low-confidence
// draft.
var lowConfidenceFinding = remediation.Finding{Code: "low_confidence", Message: "the model could not determine a safe fix with confidence"}

// answersInTurn answers the n-th model call with the n-th of answers, and every
// later call with the last of them.
func answersInTurn(h *harness, answers ...func(prompt.ProposeRequest) ports.ProposeResult) {
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		n := min(h.llm.calls(), len(answers))
		return answers[n-1](req)
	}
}

// lowConfidence is the compile fix with low confidence, which
// confidence_not_low refuses.
func lowConfidence(req prompt.ProposeRequest) ports.ProposeResult {
	r := fixAnswer(req)
	r.Confidence = "low"
	return r
}

// unchangedAndLowConfidence is an answer that leaves the shown file as it is,
// with low confidence, which content_changed and confidence_not_low both
// refuse.
func unchangedAndLowConfidence(req prompt.ProposeRequest) ports.ProposeResult {
	r := lowConfidence(req)
	r.ProposedContent = "select 1"
	return r
}

// all is every request the model was sent, in call order.
func (l *scriptedLLM) all() []prompt.ProposeRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]prompt.ProposeRequest(nil), l.requests...)
}

// roundRows is the journal rows of one step of one cluster of try 1, by round.
func (h *harness) roundRows(id uuid.UUID, cluster int, name string) map[int]remediation.StepRun {
	out := map[int]remediation.StepRun{}
	for _, s := range h.store.Steps(id) {
		if s.Try == 1 && s.Cluster == cluster && s.Step == name {
			out[s.Round] = s
		}
	}
	return out
}

func TestRunner_AFailedCheckGoesBackToTheModelInTheSameTry(t *testing.T) {
	h := newHarness(t, withRounds(3))
	answersInTurn(h, lowConfidence, fixAnswer)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 2, "the refused draft goes back to the model once")
	want := prompt.Revise(reqs[0], step.AnswerJSON(lowConfidence(reqs[0])), step.FindingLines([]remediation.Finding{lowConfidenceFinding}))
	assert.Equal(t, want, reqs[1], "round 2 revises round 1's request with its answer and finding")

	require.Len(t, h.store.Tries(id), 1, "the revision stays in the same try")
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterChecked, c.Status)
	assert.Equal(t, 2, c.Round)
	for _, name := range []string{"failing_node_source", "dbt_log", "failure_precedents"} {
		assert.Len(t, h.roundRows(id, 1, name), 1, "context step %s runs once, in round 1", name)
	}
	prompts := h.roundRows(id, 1, "dbt-compile")
	require.Len(t, prompts, 2)
	assert.Equal(t, remediation.StepDone, prompts[2].Status)
	passes := h.roundRows(id, 1, "preflight")
	require.Len(t, passes, 2)
	assert.Equal(t, []remediation.Finding{lowConfidenceFinding}, passes[1].Findings)
	assert.Empty(t, passes[2].Findings)
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status, "the revised draft goes on to the gate")
}

func TestRunner_EveryFindingGoesBackInOneRound(t *testing.T) {
	h := newHarness(t, withRounds(3))
	answersInTurn(h, unchangedAndLowConfidence, fixAnswer)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 2)
	findings := h.roundRows(id, 1, "preflight")[1].Findings
	require.Len(t, findings, 2, "two checks refused round 1's draft")
	assert.Equal(t, "content_unchanged", findings[0].Code)
	assert.Equal(t, "low_confidence", findings[1].Code)
	for _, line := range step.FindingLines(findings) {
		assert.Contains(t, reqs[1].User, "- "+line+"\n", "round 2's request carries every finding")
	}
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

func TestRunner_RoundsSpentHandsTheClusterOver(t *testing.T) {
	h := newHarness(t, withRounds(3))
	h.llm.answer = lowConfidence
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 3, h.llm.calls(), "one model call per round")
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	assert.Equal(t, 3, c.Round)
	want := proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: lowConfidenceFinding.Message, Code: model.HandoverReasonPreflightRoundsSpent}
	assert.Equal(t, &want, c.Outcome)
	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusFailed, p.Status)
	assert.Equal(t, want, p.NodeOutcomes["svc-a"])
	assert.Empty(t, h.pipeline.runs())
}

func TestRunner_AnUpstreamClusterWhoseRoundsAreSpentFallsBack(t *testing.T) {
	h := newHarness(t, withRounds(3))
	h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: "dbt"}}
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.ProposedSQL = "select 1"
		return r
	}
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 1}
	a := compileNode("svc-a")
	a.Service = "svc-a"
	a.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	trig := trigger("compile", a)
	trig.CodeBundleURI = "s3://bundles/rel-1.tar.gz"
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 3, h.llm.calls(), "the upstream cluster runs every round")
	clusters := h.store.Clusters(id)
	require.Len(t, clusters, 1, "a cross-service cluster adds no member cluster")
	assert.Equal(t, workflow.ClusterCrossService, clusters[0].Kind)
	assert.Equal(t, remediation.ClusterFellBack, clusters[0].Status)
	require.NotNil(t, clusters[0].Outcome)
	assert.Equal(t, proposal.StatusSkipped, clusters[0].Outcome.Status)
	assert.Equal(t, model.HandoverReasonDeclined, clusters[0].Outcome.Code, "the members of a cross-service cluster that fell back are declined")
	assert.Contains(t, clusters[0].Outcome.Reason, "the change that broke this node lives in service core; "+
		"a fix in this node's own service cannot ship before it: ")
}

func TestRunner_EachRoundHasItsOwnCacheKey(t *testing.T) {
	h := newHarness(t, withRounds(3))
	h.llm.answer = lowConfidence
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 3)
	for i, req := range reqs {
		round := i + 1
		key, err := llmcache.Key("test-model", fmt.Sprintf("remediation:%s:try:1:cluster:1:round:%d", id, round), req)
		require.NoError(t, err)
		assert.True(t, h.cache.has(key), "round %d's answer is cached under its own round", round)
	}
}

func TestRunner_ARestartBetweenRoundsResumesTheNextRound(t *testing.T) {
	h := newHarness(t, withRounds(3))
	answersInTurn(h, lowConfidence, fixAnswer)
	armed := false
	h.afterCommit = func() {
		if armed {
			return
		}
		if row, ok := h.roundRows(h.store.Remediations()[0].ID, 1, "dbt-compile")[1]; ok && row.Status == remediation.StepDone {
			armed = true
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.Error(t, h.runner.Drive(ctx, id), "round 1's pre-flight commit fails")
	pass := h.roundRows(id, 1, "preflight")[1]
	assert.Equal(t, remediation.StepFailed, pass.Status)
	assert.Equal(t, 1, h.cluster(id, 1, 1).Round, "the cluster is still in round 1")
	assert.Equal(t, 1, h.llm.calls())

	h.afterCommit = nil
	h.reclaim(id)
	require.NoError(t, h.runnerOver(h.files).Drive(ctx, id), "a restarted runner resumes")

	reqs := h.llm.all()
	require.Len(t, reqs, 2, "round 1's model call is not made again")
	want := prompt.Revise(reqs[0], step.AnswerJSON(lowConfidence(reqs[0])), step.FindingLines([]remediation.Finding{lowConfidenceFinding}))
	assert.Equal(t, want, reqs[1], "round 2's request is rebuilt from the journal")
	passes := h.roundRows(id, 1, "preflight")
	assert.Equal(t, remediation.StepDone, passes[1].Status)
	assert.Equal(t, 1, passes[1].Failures)
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterChecked, c.Status)
	assert.Equal(t, 2, c.Round)
}

func TestRunner_APreviousRoundWithNoModelCallFailsThePrompt(t *testing.T) {
	h := newHarness(t, withRounds(3))
	h.llm.answer = lowConfidence
	stripped := false
	h.afterCommit = func() {
		if stripped {
			return
		}
		row, ok := h.roundRows(h.store.Remediations()[0].ID, 1, "dbt-compile")[1]
		if ok && row.Status == remediation.StepDone {
			stripped = true
			row.Output = []byte(`{"draft":{}}`)
			h.saveStep(row)
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	err := h.runner.Drive(ctx, id)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "round 1's prompt row stores no model call to revise")
	assert.Equal(t, 1, h.llm.calls(), "round 1 is not run again in its place")
	prompt := h.roundRows(id, 1, "dbt-compile")[2]
	assert.Equal(t, remediation.StepFailed, prompt.Status)
	assert.Equal(t, 1, prompt.Failures)
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterProposing, c.Status)
	assert.Equal(t, 2, c.Round)
}
