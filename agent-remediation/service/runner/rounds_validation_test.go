package runner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

const (
	validationNodeID  = "model.sales.orders"
	candidateURI      = "s3://candidates/orders.sql"
	candidateSQLText  = "select bad from t"
	modelSourceText   = "select  bad  from t"
	revisedSourceText = "select  better  from t"
)

// validationNode is a dbt model that failed the release's validation, whose
// candidate SQL and source are in the fakes the harness reads from.
func validationNode(h *harness) remediation.TriggerNode {
	h.evidence.objects[candidateURI] = candidateSQLText
	h.bundle.nodes = map[string]ports.CandidateSource{validationNodeID: {RawCode: modelSourceText, Runtime: ports.RuntimeDbt}}
	return remediation.TriggerNode{
		NodeID:               validationNodeID,
		ErrorSignature:       "sig-v",
		Category:             "validation",
		NodeType:             "dbt-model",
		Service:              "svc-a",
		FilePath:             "models/orders.sql",
		CandidateArtifactURI: candidateURI,
	}
}

func diagnosisAnswer(prompt.ProposeRequest) ports.ProposeResult {
	return ports.ProposeResult{ProposedSQL: "select good from t", Rationale: "the column is named good", Confidence: "high", Model: "claude-test"}
}

func sourceFixAnswer(source, confidence string) func(prompt.ProposeRequest) ports.ProposeResult {
	return func(prompt.ProposeRequest) ports.ProposeResult {
		return ports.ProposeResult{ProposedSQL: source, Rationale: "renamed the column in the model", Confidence: confidence, Model: "claude-test"}
	}
}

// The model rates its own fix of the real source low. That answer is the draft's
// edit, pre-flight refuses it, and the next round revises the source fix alone:
// the diagnosis is not made again.
func TestRunner_ALowConfidenceSourceFixGoesBackToTheModel(t *testing.T) {
	h := newHarness(t, withRounds(2))
	answersInTurn(h, diagnosisAnswer, sourceFixAnswer("select  good  from t", "low"), sourceFixAnswer(revisedSourceText, "high"))
	id := h.accept(trigger("validation", validationNode(h)))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 3, "the diagnosis and the source fix in round 1, the source fix revised in round 2")
	assert.True(t, prompt.IsSourceFix(reqs[1]))
	want := prompt.Revise(reqs[1], step.AnswerJSON(sourceFixAnswer("select  good  from t", "low")(reqs[1])), step.FindingLines([]remediation.Finding{lowConfidenceFinding}))
	assert.Equal(t, want, reqs[2], "round 2 revises the source fix with its answer and the finding")

	passes := h.roundRows(id, 1, "preflight")
	require.Len(t, passes, 2)
	assert.Equal(t, []remediation.Finding{lowConfidenceFinding}, passes[1].Findings)
	assert.Empty(t, passes[2].Findings)
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterChecked, c.Status)
	assert.Equal(t, 2, c.Round)
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
	revised, ok := h.artifacts.get("proposed-fix/rel-1/" + validationNodeID + "/attempt-1.source.sql")
	require.True(t, ok)
	assert.Equal(t, revisedSourceText, revised, "the revised answer is the edit that goes to the gate")
}

// A source fix the model gives back unchanged is refused by content_changed, and
// with no round left the cluster is handed over on that finding.
func TestRunner_AnUnchangedSourceFixInTheLastRoundHandsTheClusterOver(t *testing.T) {
	h := newHarness(t, withRounds(1))
	answersInTurn(h, diagnosisAnswer, sourceFixAnswer(modelSourceText, "high"))
	id := h.accept(trigger("validation", validationNode(h)))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, 2, h.llm.calls())
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterHandedOver, c.Status)
	require.NotNil(t, c.Outcome)
	assert.Equal(t, proposal.StatusFailed, c.Outcome.Status)
	assert.Equal(t, "the model returned the file unchanged, which is not a fix", c.Outcome.Reason)
	assert.Equal(t, model.HandoverReasonPreflightRoundsSpent, c.Outcome.Code)
}

// A failed source-fix call is the step's error, not a draft with no edit: the
// step is tried again, and the model is not asked for the diagnosis twice.
func TestRunner_AFailedSourceFixCallIsRetriedNotDowngraded(t *testing.T) {
	h := newHarness(t, withRounds(1))
	h.llm.answer = diagnosisAnswer
	h.llm.onCall = func(n int) {
		if n == 1 {
			h.llm.err = assert.AnError // every later call fails
		}
	}
	id := h.accept(trigger("validation", validationNode(h)))

	err := h.runner.Drive(ctx, id)

	require.ErrorIs(t, err, assert.AnError)
	prompts := h.roundRows(id, 1, "dbt-validation")
	require.Len(t, prompts, 1)
	assert.Equal(t, remediation.StepFailed, prompts[1].Status)
	assert.Equal(t, 1, prompts[1].Failures)
	assert.Equal(t, remediation.ClusterProposing, h.cluster(id, 1, 1).Status, "the cluster is not settled by a failed call")
	assert.Empty(t, h.roundRows(id, 1, "preflight"))

	h.llm.err = nil
	require.NoError(t, h.drive(id))
	assert.Equal(t, 3, h.llm.calls(), "the retried step is answered the diagnosis from the cache and asks the source fix again")
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
	row := h.roundRows(id, 1, "dbt-validation")[1]
	assert.Equal(t, remediation.StepDone, row.Status)
	assert.Equal(t, 1, row.Failures, "the row keeps the failure the retry followed")
}
