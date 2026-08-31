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

// TestProposeUpstreamFix_LowConfidenceSkipsToIndependent covers the answers a
// cluster cannot be repaired from: no source, the ancestor's source returned
// unchanged, or an answer the model itself has low confidence in. Each ends the
// cluster skipped rather than failed, because the members can still be fixed in
// their own source — and the driver only falls back to that when the upstream
// attempt skips. Failing here would abandon every member on one declined answer.
func TestProposeUpstreamFix_LowConfidenceSkipsToIndependent(t *testing.T) {
	cases := []struct {
		name   string
		answer ports.ProposeResult
		reason string
	}{
		{"low_confidence", ports.ProposeResult{ProposedSQL: "select id, amount from s.base", Confidence: "low"}, "low confidence"},
		{"no_source", ports.ProposeResult{ProposedSQL: "", Confidence: "high"}, "no source"},
		{"unchanged_source", ports.ProposeResult{ProposedSQL: "select id from s.base", Confidence: "high"}, "unchanged"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _ := upstreamSvc()
			svc.LLM = &fakeLLM{queue: []ports.ProposeResult{tc.answer}}

			res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
			require.NoError(t, err)
			assert.Equal(t, proposal.StatusSkipped, res.Proposal.Status)
			assert.Contains(t, res.Proposal.Rationale, "s.u")
			assert.Contains(t, res.Proposal.Rationale, tc.reason)
			assert.Empty(t, res.Proposal.Edits, "a declined cluster proposes no edit")
		})
	}
}

// TestProposeUpstreamFix_NoMembersSkips verifies that a cluster with no
// failing members is skipped before any collaborator is touched: an upstream
// fix exists to repair the shared cause of listed descendants, and with none
// listed there is nothing to name in the prompt (in.Members[0] would panic)
// and nothing for the resulting edit to be justified by.
func TestProposeUpstreamFix_NoMembersSkips(t *testing.T) {
	svc, llm := upstreamSvc()
	cs := &fakeCandidateSource{src: ports.CandidateSource{RawCode: "select id from s.base", Runtime: ports.RuntimeDbt}}
	svc.CandidateSource = cs
	loc := &countingLocator{filePath: "models/u.sql", serviceName: "svc"}
	svc.Locator = loc

	in := upstreamInput()
	in.Members = nil

	res, err := ProposeUpstreamFix(context.Background(), svc, in)
	require.NoError(t, err)
	assert.Equal(t, proposal.StatusSkipped, res.Proposal.Status)
	assert.Contains(t, res.Proposal.Rationale, "at least one failing member")
	assert.Equal(t, 0, llm.calls, "no model call is worth making for a cluster with nothing to repair")
	assert.Equal(t, 0, cs.calls, "the code bundle must not be read before the cluster is known to have members")
	assert.Equal(t, 0, loc.calls, "the promoted graph must not be queried before the cluster is known to have members")
}

// TestProposeUpstreamFix_UnmappedServiceSkips covers the ServiceRepoPaths miss
// branch: the promoted graph locates the changed ancestor, but its owning
// service has no repository-path mapping, so the fix has nowhere to write the
// edit's path from.
func TestProposeUpstreamFix_UnmappedServiceSkips(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.Locator = fakeLocator{filePath: "models/u.sql", serviceName: "unmapped-svc"}

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	assert.Equal(t, proposal.StatusSkipped, res.Proposal.Status)
	assert.Contains(t, res.Proposal.Rationale, "unmapped-svc")
	assert.Contains(t, res.Proposal.Rationale, "no repository path mapping")
}

// TestProposeUpstreamFix_PrefersTheCandidateLocation covers the ancestor this
// release renamed or moved. The promoted graph still places it at its OLD path,
// so a fix routed by the locator would rewrite a file that no longer holds the
// node. The rejection carries the location the CANDIDATE declares, and that is
// what the edit must use — without consulting the locator at all.
func TestProposeUpstreamFix_PrefersTheCandidateLocation(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.Locator = fakeLocator{filePath: "models/u_old.sql", serviceName: "svc"}
	svc.ServiceRepoPaths = map[string]string{"svc": "services/svc", "svc-b": "services/svc-b"}
	in := upstreamInput()
	in.TargetFilePath, in.TargetService = "models/marts/u_renamed.sql", "svc-b"

	res, err := ProposeUpstreamFix(context.Background(), svc, in)
	require.NoError(t, err)
	require.Len(t, res.Proposal.Edits, 1)
	assert.Equal(t, "services/svc-b/models/marts/u_renamed.sql", res.Proposal.Edits[0].Path,
		"the candidate's own path wins over the promoted graph's stale one")
}

// TestProposeUpstreamFix_FallsBackToTheLocatorWithoutACandidateLocation covers
// the rejection that carries no location for the ancestor (a compile-stage
// rejection has no per-node topology): the promoted graph is then the only
// answer available, and is still consulted.
func TestProposeUpstreamFix_FallsBackToTheLocatorWithoutACandidateLocation(t *testing.T) {
	svc, _ := upstreamSvc()
	svc.Locator = fakeLocator{filePath: "models/u.sql", serviceName: "svc"}

	res, err := ProposeUpstreamFix(context.Background(), svc, upstreamInput())
	require.NoError(t, err)
	require.Len(t, res.Proposal.Edits, 1)
	assert.Equal(t, "services/svc/models/u.sql", res.Proposal.Edits[0].Path)
}
