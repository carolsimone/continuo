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
	"github.com/carolsimone/continuo/agent-remediation/service/steps/upstream"
)

const (
	fixedSource = "select new_name as old_name, new_name from raw"
	diffText    = "<ancestor diff>"
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

func newPrompt() *upstream.Prompt { return upstream.NewPrompt(markSanitizer{}, repoPaths) }

func proposeUpstream(t *testing.T, f remediation.Failure, ev remediation.Evidence, m *fakeModel) step.PromptOutput {
	t.Helper()
	out, err := newPrompt().Propose(context.Background(), step.PromptInput{Failure: f, Evidence: ev}, m)
	require.NoError(t, err)
	require.False(t, out.Declined)
	return out
}

// expectedRequest is the request the upstream lane builds from the inputs the
// step is given: the ancestor's source and the members' excerpts sanitized, the
// diff and precedents as gathered.
func expectedRequest(crossService bool) prompt.ProposeRequest {
	return prompt.AssembleUpstreamFix(prompt.UpstreamEvidence{
		TargetNodeID:  targetID,
		TargetSource:  "<" + targetSource + ">",
		OwnChangeDiff: diffText,
		Members: []prompt.MemberFailure{
			{NodeID: "model.up-svc.m1", Service: targetSvc, ErrorExcerpt: "<column old_name missing>"},
			{NodeID: "model.up-svc.m2", ErrorExcerpt: "<column old_name missing too>"},
		},
		Precedents:   precedentList,
		CrossService: crossService,
	})
}

func TestPrompt_MetaNamesTheStepAndWhatItReads(t *testing.T) {
	m := upstream.NewPrompt(nil, nil).Meta()
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
		TargetSource:  "<" + targetSource + ">",
		OwnChangeDiff: diffText,
		Members: []prompt.MemberFailure{
			{NodeID: "model.up-svc.m1", Service: targetSvc, ErrorExcerpt: "<column old_name missing>"},
			{NodeID: "model.up-svc.m2", ErrorExcerpt: "<column old_name missing too>"},
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

func TestPrompt_AnEmptyOrUnchangedAnswerIsLeftToTheChecks(t *testing.T) {
	for name, source := range map[string]string{"empty": "", "unchanged": targetSource} {
		t.Run(name, func(t *testing.T) {
			out := proposeUpstream(t, failure(nil), upstreamEvidence(), &fakeModel{res: answer(source, "low")})

			require.Len(t, out.Draft.Edits, 1)
			assert.Equal(t, source, out.Draft.Edits[0].Content)
			assert.Equal(t, "low", out.Draft.Confidence)
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
