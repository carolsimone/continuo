package fixer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// upstreamSvc wires every collaborator for a shared-upstream fix happy path:
// a dbt candidate source for the changed ancestor from the code bundle, a
// current version that differs from it (so an own-change diff is shown), a
// located path and owning service, and an LLM that returns a corrected
// source with high confidence.
func upstreamSvc() (Services, *fakeLLM) {
	llm := &fakeLLM{queue: []ports.ProposeResult{{
		ProposedSQL: "select id, amount from s.base", Confidence: "high", Model: "test-model",
	}}}
	return Services{
		LLM:              llm,
		CandidateSource:  &fakeCandidateSource{src: ports.CandidateSource{RawCode: "select id from s.base", Runtime: ports.RuntimeDbt}},
		Versions:         &fakeVersions{v: ports.CurrentVersion{RawCode: "select id, amount from s.base"}, ok: true},
		Locator:          fakeLocator{filePath: "models/u.sql", serviceName: "svc"},
		Sanitizer:        fakeSanitizer{},
		Artifacts:        &fakeArtifacts{},
		Precedents:       &fakePrecedents{},
		Logger:           testLogger(),
		ServiceRepoPaths: map[string]string{"svc": "services/svc"},
	}, llm
}

// upstreamInput is the trigger a shared-upstream cluster produces: the
// changed ancestor to repair and the two descendants whose failure it shares.
func upstreamInput() UpstreamInput {
	return UpstreamInput{
		ReleaseID: "r", Repo: "o/r", CommitSHA: "sha", CodeBundleURI: "s3://b/bundle.json",
		TargetNodeID: "s.u", Attempt: 1,
		Members: []MemberFailure{
			{NodeID: "s.v", ErrorExcerpt: "column u.amount does not exist"},
			{NodeID: "s.w", ErrorExcerpt: "column u.amount does not exist"},
		},
	}
}

func TestProposeUpstreamFix_EditsTheTargetNotTheMembers(t *testing.T) {
	svc, llm := upstreamSvc()

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	require.Equal(t, proposal.StatusProposed, res.Proposal.Status)
	require.Len(t, res.Proposal.Edits, 1)
	assert.Equal(t, "services/svc/models/u.sql", res.Proposal.Edits[0].Path)
	assert.Equal(t, "s.u", res.Proposal.Edits[0].TargetNodeID)
	assert.True(t, res.Proposal.SourceResolved)
	// the prompt named both descendants
	assert.Contains(t, llm.lastRequest.User, "s.v")
	assert.Contains(t, llm.lastRequest.User, "s.w")
}

func TestProposeUpstreamFix_NonDbtTargetSkips(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.CandidateSource = &fakeCandidateSource{src: ports.CandidateSource{RawCode: "def run(): ...", Runtime: ports.RuntimePython}}

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	assert.Equal(t, proposal.StatusSkipped, res.Proposal.Status)
	assert.Contains(t, res.Proposal.Rationale, "not a dbt model")
}

func TestProposeUpstreamFix_UnlocatableTargetSkips(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.Locator = fakeLocator{err: fmt.Errorf("node not found in the promoted graph")}

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	assert.Equal(t, proposal.StatusSkipped, res.Proposal.Status)
}

func TestProposeUpstreamFix_LowConfidenceFails(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.LLM = &fakeLLM{queue: []ports.ProposeResult{{
		ProposedSQL: "select id from s.base", Confidence: "low",
	}}}

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	assert.Equal(t, proposal.StatusFailed, res.Proposal.Status)
}
