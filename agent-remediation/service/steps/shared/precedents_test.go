package shared_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
)

func precedentFailure() remediation.Failure {
	return remediation.Failure{
		Trigger: remediation.Trigger{ReleaseID: "rel-1"},
		Kind:    workflow.ClusterSingle,
		Target: remediation.TriggerNode{
			NodeID: "model.a", ErrorSignature: "sig-a", Category: "compile", Reason: "syntax",
		},
	}
}

func gatherPrecedents(t *testing.T, r *fakePrecedents, f remediation.Failure) []prompt.Precedent {
	t.Helper()
	out, err := shared.NewPrecedents(r, markSanitizer{}, discardLogger()).
		Gather(context.Background(), step.ContextInput{Failure: f})
	require.NoError(t, err)
	got, ok := out.Evidence[remediation.KeyFailurePrecedents].([]prompt.Precedent)
	require.True(t, ok)
	return got
}

func TestPrecedents_MetaNamesWhatItNeedsAndProvides(t *testing.T) {
	m := shared.NewPrecedents(nil, nil, nil).Meta()
	assert.Equal(t, "failure_precedents", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailurePrecedents}, m.Provides)
}

func TestPrecedents_ExactSignatureMatchIsSanitizedAndSkipsTheFallback(t *testing.T) {
	r := &fakePrecedents{bySignature: map[string][]prompt.Precedent{"sig-a": {{
		ReleaseID: "rel-0", NodeID: "model.z", ResolutionDiff: "diff", ErrorExcerpt: "err",
		Edited: []prompt.EditedPrecedent{{NodeID: "model.z", Diff: "ed"}},
	}}}}
	got := gatherPrecedents(t, r, precedentFailure())
	assert.Equal(t, []ports.PrecedentQuery{{Signature: "sig-a", Limit: 10}}, r.queries)
	assert.Equal(t, []prompt.Precedent{{
		ReleaseID: "rel-0", NodeID: "model.z", ResolutionDiff: "<diff>", ErrorExcerpt: "<err>",
		Edited: []prompt.EditedPrecedent{{NodeID: "model.z", Diff: "<ed>"}},
	}}, got)
	assert.Equal(t, "ed", r.bySignature["sig-a"][0].Edited[0].Diff, "the reader's slice stays untouched")
}

func TestPrecedents_DropsThisFailuresOwnRejectionBeforeTheFallback(t *testing.T) {
	self := prompt.Precedent{ReleaseID: "rel-1", NodeID: "model.a"}
	other := prompt.Precedent{ReleaseID: "rel-0", NodeID: "model.b", ErrorExcerpt: "e"}
	r := &fakePrecedents{
		bySignature: map[string][]prompt.Precedent{"sig-a": {self}},
		byClass:     map[string][]prompt.Precedent{"compile/syntax": {self, other}},
	}
	got := gatherPrecedents(t, r, precedentFailure())
	assert.Equal(t, []ports.PrecedentQuery{
		{Signature: "sig-a", Limit: 10},
		{Category: "compile", Reason: "syntax", Limit: 10},
	}, r.queries, "an exact match that is only the failure itself falls through to the class")
	assert.Equal(t, []prompt.Precedent{{ReleaseID: "rel-0", NodeID: "model.b", ErrorExcerpt: "<e>", ResolutionDiff: "<>"}}, got)
}

func TestPrecedents_NoSignatureGoesStraightToTheClass(t *testing.T) {
	f := precedentFailure()
	f.Target.ErrorSignature = ""
	r := &fakePrecedents{byClass: map[string][]prompt.Precedent{"compile/syntax": {{NodeID: "model.b"}}}}
	got := gatherPrecedents(t, r, f)
	assert.Equal(t, []ports.PrecedentQuery{{Category: "compile", Reason: "syntax", Limit: 10}}, r.queries)
	assert.Len(t, got, 1)
}

func TestPrecedents_NoClassKeyMeansNoFallback(t *testing.T) {
	f := precedentFailure()
	f.Target.Reason = ""
	r := &fakePrecedents{}
	got := gatherPrecedents(t, r, f)
	assert.Len(t, r.queries, 1)
	assert.Empty(t, got)
}

func TestPrecedents_ALookupErrorProvidesNoPrecedents(t *testing.T) {
	r := &fakePrecedents{err: errors.New("catalog down")}
	got := gatherPrecedents(t, r, precedentFailure())
	assert.Empty(t, got)
}

func TestPrecedents_AnUpstreamClusterLooksUpItsFirstMember(t *testing.T) {
	f := remediation.Failure{
		Trigger:      remediation.Trigger{ReleaseID: "rel-1"},
		Kind:         workflow.ClusterSharedUpstream,
		TargetNodeID: "model.anc",
		Target:       remediation.TriggerNode{NodeID: "model.anc"},
		Members: []remediation.TriggerNode{
			{NodeID: "model.m1", ErrorSignature: "sig-m1"},
			{NodeID: "model.m2", ErrorSignature: "sig-m2"},
		},
	}
	r := &fakePrecedents{bySignature: map[string][]prompt.Precedent{
		"sig-m1": {{ReleaseID: "rel-1", NodeID: "model.m1"}, {ReleaseID: "rel-0", NodeID: "model.m1"}},
	}}
	got := gatherPrecedents(t, r, f)
	assert.Equal(t, []ports.PrecedentQuery{{Signature: "sig-m1", Limit: 10}}, r.queries)
	assert.Equal(t, []prompt.Precedent{{ReleaseID: "rel-0", NodeID: "model.m1", ResolutionDiff: "<>", ErrorExcerpt: "<>"}}, got)
}
