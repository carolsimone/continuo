package step_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

func previousCall() (prompt.ProposeRequest, ports.ProposeResult) {
	req := prompt.ProposeRequest{System: "fix it", User: "Failed node: a", ToolName: "propose_fix"}
	res := ports.ProposeResult{ProposedSQL: "select 1", Rationale: "because", Confidence: "high"}
	return req, res
}

func TestRevisionRequest_TheFirstDraftIsNotARevision(t *testing.T) {
	req, res := previousCall()

	got, revising, err := step.RevisionRequest(step.PromptInput{Revision: 0})
	require.NoError(t, err)
	assert.False(t, revising)
	assert.Equal(t, prompt.ProposeRequest{}, got)

	// The draft decides: what an input carries from a call is not read in the first draft.
	_, revising, err = step.RevisionRequest(step.PromptInput{Revision: 0, PreviousRequest: &req, PreviousAnswer: &res})
	require.NoError(t, err)
	assert.False(t, revising)
}

func TestRevisionRequest_ARevisionRevisesThePreviousCallWithEveryFinding(t *testing.T) {
	req, res := previousCall()
	findings := []remediation.Finding{
		{Code: "content_unchanged", Message: "the model returned the file unchanged, which is not a fix", File: "models/a.sql"},
		{Code: "low_confidence", Message: "the model could not determine a safe fix with confidence"},
	}

	got, revising, err := step.RevisionRequest(step.PromptInput{Revision: 1, PreviousRequest: &req, PreviousAnswer: &res, Findings: findings})

	require.NoError(t, err)
	assert.True(t, revising)
	assert.Equal(t, prompt.Revise(req, step.AnswerJSON(res), step.FindingLines(findings)), got)
}

func TestRevisionRequest_ARevisionWithoutThePreviousCallIsAnError(t *testing.T) {
	req, res := previousCall()
	cases := map[string]step.PromptInput{
		"no previous call":    {Revision: 1},
		"no previous request": {Revision: 2, PreviousAnswer: &res},
		"no previous answer":  {Revision: 2, PreviousRequest: &req},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			got, revising, err := step.RevisionRequest(in)

			require.Error(t, err)
			assert.False(t, revising)
			assert.Equal(t, prompt.ProposeRequest{}, got)
		})
	}
}
