package upstream_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/upstream"
)

const (
	fixedSource = "select new_name as old_name, new_name from raw"
	diffText    = "ancestor diff"
)

var precedentList = []prompt.Precedent{{ReleaseID: "old", NodeID: "n0", ErrorExcerpt: "same error"}}

func upstreamEvidence() remediation.Evidence {
	return remediation.NewEvidence().
		With(remediation.KeyUpstreamTarget, remediation.UpstreamTarget{
			NodeID:        targetID,
			Source:        targetSource,
			FilePath:      targetFile,
			Service:       targetSvc,
			OwnChangeDiff: diffText,
		}).
		With(remediation.KeyFailurePrecedents, precedentList)
}

func answer(source, confidence string) ports.ProposeResult {
	return ports.ProposeResult{ProposedSQL: source, Rationale: "kept the old column", Confidence: confidence, Model: "model-1"}
}

func newPrompt() *upstream.Prompt { return upstream.NewPrompt(repoPaths) }

func proposeUpstream(t *testing.T, f remediation.Failure, ev remediation.Evidence, m *fakeModel) step.PromptOutput {
	t.Helper()
	out, err := newPrompt().Propose(context.Background(), step.PromptInput{Failure: f, Evidence: ev}, m)
	require.NoError(t, err)
	require.False(t, out.Declined)
	return out
}

// expectedRequest is the request the upstream lane builds from the inputs the
// step is given: the ancestor's source, the members' excerpts, the diff and the
// precedents as gathered.
func expectedRequest(crossService bool) prompt.ProposeRequest {
	return prompt.AssembleUpstreamFix(prompt.UpstreamEvidence{
		TargetNodeID:  targetID,
		TargetSource:  targetSource,
		OwnChangeDiff: diffText,
		Members: []prompt.MemberFailure{
			{NodeID: "model.up-svc.m1", Service: targetSvc, ErrorExcerpt: "column old_name missing"},
			{NodeID: "model.up-svc.m2", ErrorExcerpt: "column old_name missing too"},
		},
		Precedents:   precedentList,
		CrossService: crossService,
	})
}

func TestPrompt_MetaNamesTheStepAndWhatItReads(t *testing.T) {
	m := upstream.NewPrompt(nil).Meta()
	assert.Equal(t, "upstream", string(m.Name))
	assert.Equal(t, step.KindPrompt, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeyUpstreamTarget, remediation.KeyFailurePrecedents}, m.Needs)
	assert.Empty(t, m.Provides)
}

func TestPrompt_MakesTheLanesRequest(t *testing.T) {
	model := &fakeModel{res: answer(fixedSource, "high")}

	proposeUpstream(t, failure(nil), upstreamEvidence(), model)

	require.Len(t, model.reqs, 1, "one model call")
	assert.Equal(t, expectedRequest(false), model.reqs[0])
}

func TestPrompt_ACrossServiceClusterGetsTheCrossServiceClause(t *testing.T) {
	model := &fakeModel{res: answer(fixedSource, "high")}

	proposeUpstream(t, failure(func(f *remediation.Failure) { f.Kind = workflow.ClusterCrossService }), upstreamEvidence(), model)

	assert.Equal(t, expectedRequest(true), model.reqs[0])
	assert.NotEqual(t, expectedRequest(false).System, model.reqs[0].System)
}

func TestPrompt_NoPrecedentsLeavesThePrecedentSectionOut(t *testing.T) {
	model := &fakeModel{res: answer(fixedSource, "high")}
	ev := upstreamEvidence()
	ev = remediation.NewEvidence().With(remediation.KeyUpstreamTarget, mustTarget(t, ev))

	proposeUpstream(t, failure(nil), ev, model)

	want := prompt.AssembleUpstreamFix(prompt.UpstreamEvidence{
		TargetNodeID:  targetID,
		TargetSource:  targetSource,
		OwnChangeDiff: diffText,
		Members: []prompt.MemberFailure{
			{NodeID: "model.up-svc.m1", Service: targetSvc, ErrorExcerpt: "column old_name missing"},
			{NodeID: "model.up-svc.m2", ErrorExcerpt: "column old_name missing too"},
		},
	})
	assert.Equal(t, want, model.reqs[0])
}

func mustTarget(t *testing.T, ev remediation.Evidence) remediation.UpstreamTarget {
	t.Helper()
	tg, ok := remediation.Get[remediation.UpstreamTarget](ev, remediation.KeyUpstreamTarget)
	require.True(t, ok)
	return tg
}

func TestPrompt_TheDraftIsOneEditOfTheAncestor(t *testing.T) {
	model := &fakeModel{res: answer(fixedSource, "medium")}

	out := proposeUpstream(t, failure(nil), upstreamEvidence(), model)

	assert.Equal(t, remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         "services/up-svc/models/up.sql",
			Original:     targetSource,
			Content:      fixedSource,
			TargetNodeID: targetID,
			ContentKey:   "proposed-fix/rel-1/model.up-svc.up/attempt-2.source.sql",
			DiffKey:      "proposed-fix/rel-1/model.up-svc.up/attempt-2.source.diff",
		}},
		Rationale:      "kept the old column",
		Confidence:     "medium",
		Model:          "model-1",
		SourceResolved: true,
	}, out.Draft)
}

// An answer the model leaves empty or unchanged, or rates low, is a draft and
// not a decline: the pre-flight checks refuse it, so a round after the first
// revises it, and the cluster falls back only once its rounds are spent.
func TestUpstream_AnUnchangedAnswerIsADraftTheChecksRefuse(t *testing.T) {
	cases := []struct {
		name   string
		answer ports.ProposeResult
		check  step.Check
		code   string
	}{
		{"an empty answer", answer("", "high"), shared.NewContentChanged(), "content_unchanged"},
		{"an unchanged answer", answer(targetSource, "high"), shared.NewContentChanged(), "content_unchanged"},
		{"a low-confidence answer", answer(fixedSource, "low"), shared.NewConfidenceNotLow(), "low_confidence"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := proposeUpstream(t, failure(nil), upstreamEvidence(), &fakeModel{res: c.answer})

			require.Len(t, out.Draft.Edits, 1)
			assert.Equal(t, c.answer.ProposedSQL, out.Draft.Edits[0].Content)
			assert.Equal(t, targetSource, out.Draft.Edits[0].Original)
			assert.Equal(t, c.answer.Confidence, out.Draft.Confidence)
			res, err := c.check.Check(context.Background(), step.CheckInput{Failure: failure(nil), Draft: out.Draft})
			require.NoError(t, err)
			require.Len(t, res.Result.Findings, 1)
			assert.Equal(t, c.code, res.Result.Findings[0].Code)
		})
	}
}

func TestPrompt_AModelErrorIsReturnedAsTheLaneReturnsIt(t *testing.T) {
	model := &fakeModel{err: errBoom}

	_, err := newPrompt().Propose(context.Background(), step.PromptInput{Failure: failure(nil), Evidence: upstreamEvidence()}, model)

	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, "llm propose: boom", err.Error())
}

func TestPrompt_RefusesEvidenceWithNoUpstreamTarget(t *testing.T) {
	_, err := newPrompt().Propose(context.Background(),
		step.PromptInput{Failure: failure(nil), Evidence: remediation.NewEvidence()}, &fakeModel{})

	require.Error(t, err)
}

func TestPrompt_RefusesAServiceWithNoRepositoryPath(t *testing.T) {
	ev := remediation.NewEvidence().With(remediation.KeyUpstreamTarget, remediation.UpstreamTarget{
		NodeID: targetID, Source: targetSource, FilePath: targetFile, Service: "ghost",
	})
	model := &fakeModel{res: answer(fixedSource, "high")}

	_, err := newPrompt().Propose(context.Background(), step.PromptInput{Failure: failure(nil), Evidence: ev}, model)

	require.Error(t, err)
	assert.Empty(t, model.reqs, "no model call is made for a target that cannot be edited")
}

func TestUpstream_RevisesTheLastCallWithTheFindings(t *testing.T) {
	first, second := answer(targetSource, "high"), answer(fixedSource, "medium")
	model := &fakeModel{res: first}
	round1 := proposeUpstream(t, failure(nil), upstreamEvidence(), model)
	findings := []remediation.Finding{
		{Code: "content_unchanged", Message: "the model returned the file unchanged, which is not a fix", File: "services/up-svc/models/up.sql"},
		{Code: "confidence_low", Message: "the model rated its own fix low"},
	}

	model.res = second
	out, err := newPrompt().Propose(context.Background(), step.PromptInput{
		Failure: failure(nil), Evidence: upstreamEvidence(), Round: 1,
		Previous: &round1.Draft, PreviousRequest: &model.reqs[0], PreviousAnswer: &first, Findings: findings,
	}, model)

	require.NoError(t, err)
	require.Len(t, model.reqs, 2, "a later round makes one model call")
	assert.Equal(t, prompt.Revise(model.reqs[0], step.AnswerJSON(first), step.FindingLines(findings)), model.reqs[1])
	assert.NotEqual(t, model.reqs[0], model.reqs[1])
	again := proposeUpstream(t, failure(nil), upstreamEvidence(), &fakeModel{res: second})
	assert.Equal(t, again.Draft, out.Draft, "the answer is read as the first round's answer is")
}

func TestPrompt_ALaterRoundWithoutThePreviousCallIsAnErrorAndCallsNoModel(t *testing.T) {
	req, res := prompt.ProposeRequest{User: "u"}, answer(fixedSource, "high")
	cases := map[string]step.PromptInput{
		"no previous call":    {Round: 1},
		"no previous request": {Round: 1, PreviousAnswer: &res},
		"no previous answer":  {Round: 1, PreviousRequest: &req},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			in.Failure, in.Evidence = failure(nil), upstreamEvidence()
			model := &fakeModel{res: res}

			_, err := newPrompt().Propose(context.Background(), in, model)

			require.Error(t, err)
			assert.ErrorContains(t, err, "upstream")
			assert.Empty(t, model.reqs)
		})
	}
}

func TestPrompt_TheFirstRoundIsShownTheEarlierTries(t *testing.T) {
	tries := []remediation.TryRecord{{
		Round: 1, Try: 1, Attempt: 1,
		Clusters: []remediation.ClusterRecord{{Members: []string{"model.up-svc.m1"}, Outcome: "failed (preflight_rounds_spent): the answer repeats the original"}},
	}}
	ev := upstreamEvidence().With(remediation.KeyPreviousTries, tries)
	model := &fakeModel{res: answer(fixedSource, "high")}

	proposeUpstream(t, failure(nil), ev, model)

	require.Len(t, model.reqs, 1)
	assert.Equal(t, step.WithPreviousTries(expectedRequest(false), ev), model.reqs[0])
	assert.Contains(t, model.reqs[0].User, "- outcome: failed (preflight_rounds_spent): the answer repeats the original")
}
