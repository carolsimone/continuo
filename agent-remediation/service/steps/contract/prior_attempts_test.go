package contract_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

func gatherPriorAttempts(t *testing.T, l *fakeAttempts, e *fakeEvidence) []prompt.PriorAttempt {
	t.Helper()
	out, err := contract.NewPriorAttempts(l, e, markSanitizer{}, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})
	require.NoError(t, err)
	require.False(t, out.Declined)
	got, ok := out.Evidence[remediation.KeyPriorAttempts].([]prompt.PriorAttempt)
	require.True(t, ok, "prior_attempts must hold a []prompt.PriorAttempt")
	return got
}

func view(attempt int, verifyErr string, edits ...proposal.FileEdit) proposal.View {
	return proposal.View{Attempt: attempt, VerifyError: verifyErr, Edits: edits}
}

func TestPriorAttempts_MetaNamesTheStepAndWhatItProvides(t *testing.T) {
	m := contract.NewPriorAttempts(nil, nil, nil, nil).Meta()
	assert.Equal(t, "prior_attempts", string(m.Name))
	assert.Equal(t, step.KindContext, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyFailure}, m.Needs)
	assert.Equal(t, []remediation.EvidenceKey{remediation.KeyPriorAttempts}, m.Provides)
}

func TestPriorAttempts_ListsTheAttemptsAtThisNodeAndLeavesOutTheOneBeingMade(t *testing.T) {
	l := &fakeAttempts{views: []proposal.View{
		view(1, "first failed"),
		view(2, "this attempt"),
		view(3, "a later one"),
		view(0, "zeroth failed"),
	}}

	got := gatherPriorAttempts(t, l, &fakeEvidence{})

	assert.Equal(t, []prompt.PriorAttempt{
		{Attempt: 0, VerifyError: "<zeroth failed>", Diffs: []prompt.AttemptDiff{}},
		{Attempt: 1, VerifyError: "<first failed>", Diffs: []prompt.AttemptDiff{}},
	}, got, "attempt 2 is the one being made and attempt 3 has not happened yet; the rest come oldest first")
	assert.Equal(t, []repository.ProposalFilter{{ReleaseID: "rel-1", Source: "validation", NodeID: nodeID}}, l.filters)
}

func TestPriorAttempts_ShowsEachAttemptsStoredDiffsSanitized(t *testing.T) {
	l := &fakeAttempts{views: []proposal.View{view(1, "failed",
		proposal.FileEdit{Path: "svc/contracts/fx.yaml", DiffURI: "s3://d/1"},
		proposal.FileEdit{Path: "svc/contracts/no-diff.yaml"},
		proposal.FileEdit{Path: "svc/contracts/gone.yaml", DiffURI: "s3://d/gone"},
		proposal.FileEdit{Path: "svc/contracts/broken.yaml", DiffURI: "s3://d/broken"},
	)}}
	e := &fakeEvidence{
		bodies: map[string]string{"s3://d/1": "-a\n+b"},
		errs:   map[string]error{"s3://d/broken": errBoom},
	}

	got := gatherPriorAttempts(t, l, e)

	assert.Equal(t, []prompt.PriorAttempt{{
		Attempt:     1,
		VerifyError: "<failed>",
		Diffs:       []prompt.AttemptDiff{{Path: "svc/contracts/fx.yaml", Diff: "<-a\n+b>"}},
	}}, got, "an edit with no diff and a diff that cannot be read are left out, the attempt still shows")
	assert.Equal(t, []string{"s3://d/1", "s3://d/gone", "s3://d/broken"}, e.calls)
}

func TestPriorAttempts_AnUnreadableHistoryGivesNoAttemptsAndNeverFails(t *testing.T) {
	got := gatherPriorAttempts(t, &fakeAttempts{err: fmt.Errorf("db: %w", errBoom)}, &fakeEvidence{})

	assert.Empty(t, got)
}

func TestPriorAttempts_NoListerGivesNoAttempts(t *testing.T) {
	out, err := contract.NewPriorAttempts(nil, &fakeEvidence{}, markSanitizer{}, discardLogger()).Gather(context.Background(),
		step.ContextInput{Failure: failure(nil), Evidence: remediation.NewEvidence()})

	require.NoError(t, err)
	got, ok := out.Evidence[remediation.KeyPriorAttempts].([]prompt.PriorAttempt)
	require.True(t, ok)
	assert.Empty(t, got)
}
