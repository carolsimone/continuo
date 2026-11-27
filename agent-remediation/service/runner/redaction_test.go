package runner_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ownedSource is the failing node's source, which holds an email address.
const ownedSource = "select 'ops@acme.com' as owner"

// withOwnedSource makes the failing node's file hold ownedSource.
func withOwnedSource(h *harness) {
	h.source.mu.Lock()
	defer h.source.mu.Unlock()
	h.source.files["services/svc-a/models/orders.sql"] = ownedSource
}

// answerWith is the compile fix with content as the file and rationale as its
// note.
func answerWith(content, rationale string) func(prompt.ProposeRequest) ports.ProposeResult {
	return func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.ProposedContent = content
		r.Rationale = rationale
		return r
	}
}

// preflightDraft is the draft round's pre-flight pass of cluster 1 of try 1
// stored.
func (h *harness) preflightDraft(id uuid.UUID, round int) remediation.Draft {
	h.t.Helper()
	pass, ok := h.roundRows(id, 1, "preflight")[round]
	require.True(h.t, ok, "round %d has a pre-flight row", round)
	rec, err := serialization.DecodeRecord[serialization.PreflightRecord](pass.Output)
	require.NoError(h.t, err)
	return rec.Draft
}

func TestRunner_RestoresRedactedValuesInTheDraft(t *testing.T) {
	h := newHarness(t)
	withOwnedSource(h)
	h.llm.answer = answerWith("select '[redacted:email:1]' as owner, 2", "kept the owner [redacted:email:1]")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 1)
	assert.Contains(t, reqs[0].User, "select '[redacted:email:1]' as owner", "the model is shown the placeholder")
	assert.NotContains(t, reqs[0].User+reqs[0].System, "ops@acme.com", "the model is never shown the value")

	draft := h.preflightDraft(id, 1)
	require.Len(t, draft.Edits, 1)
	assert.Equal(t, "select 'ops@acme.com' as owner, 2", draft.Edits[0].Content, "the checked edit holds the real value")
	assert.Equal(t, ownedSource, draft.Edits[0].Original)
	assert.Equal(t, "kept the owner ops@acme.com", draft.Rationale)
	assert.Empty(t, h.roundRows(id, 1, "preflight")[1].Findings)

	stored, ok := h.artifacts.get(draft.Edits[0].ContentKey)
	require.True(t, ok)
	assert.Equal(t, "select 'ops@acme.com' as owner, 2", stored, "the content a pull request carries holds the real value")

	rec, err := serialization.DecodeRecord[serialization.PromptRecord](h.roundRows(id, 1, "dbt-compile")[1].Output)
	require.NoError(t, err)
	require.NotNil(t, rec.LastRequest)
	assert.Contains(t, rec.LastRequest.Request().User, "[redacted:email:1]", "the prompt row keeps the request as sent")
	assert.Equal(t, "select '[redacted:email:1]' as owner, 2", rec.Draft.Edits[0].Content, "the prompt row keeps the answer as received")
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status)
}

func TestRunner_AnUnknownPlaceholderIsAFinding(t *testing.T) {
	const (
		added = "the answer adds a redaction marker (%s) the file did not hold; keep the text you were shown"
		never = "the answer holds a placeholder (%s) that was never in what you were shown; keep the text you were shown"
	)
	cases := []struct {
		name, content, rationale, message string
	}{
		{"a number never issued", "select '[redacted:email:9]' as owner", "fixed it", fmt.Sprintf(added, "redacted email 9")},
		{"a fixed marker", "select '[redacted:secret]' as owner", "fixed it", fmt.Sprintf(added, "redacted secret")},
		{"a placeholder in other case", "select '[REDACTED:email:1]' as owner", "fixed it", fmt.Sprintf(added, "REDACTED email 1")},
		{"a placeholder with no closing bracket", "select '[redacted:email:1 as owner", "fixed it", fmt.Sprintf(added, "redacted email 1")},
		{"a number never issued in the rationale", "select 'ops@acme.com' as owner, 2", "kept [redacted:email:9]", fmt.Sprintf(never, "redacted email 9")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			withOwnedSource(h)
			h.llm.answer = answerWith(tc.content, tc.rationale)
			id := h.accept(trigger("compile", compileNode("svc-a")))

			require.NoError(t, h.runner.Drive(ctx, id))

			message := tc.message
			findings := h.roundRows(id, 1, "preflight")[1].Findings
			require.NotEmpty(t, findings)
			assert.Equal(t, "unknown_placeholder", findings[0].Code)
			assert.Equal(t, message, findings[0].Message)
			c := h.cluster(id, 1, 1)
			assert.Equal(t, remediation.ClusterHandedOver, c.Status, "the round fails like on any other finding")
			want := fix.NodeOutcome{Status: fix.StatusFailed, Reason: message, Code: model.HandoverReasonPreflightRoundsSpent}
			assert.Equal(t, &want, c.Outcome)
			assert.Empty(t, h.checks.requests(), "no draft that holds a placeholder is verified")
		})
	}
}

func TestRunner_AnUnknownPlaceholderGoesBackToTheModel(t *testing.T) {
	h := newHarness(t, withRounds(2))
	withOwnedSource(h)
	answersInTurn(h, answerWith("select '[redacted:email:9]' as owner", "fixed it"), answerWith("select '[redacted:email:1]' as owner, 2", "fixed it"))
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 2)
	assert.Contains(t, reqs[1].User, "- unknown_placeholder: the answer adds a redaction marker (redacted email 9) the file did not hold; "+
		"keep the text you were shown (services/svc-a/models/orders.sql)\n", "the finding reaches the model as it was found")
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
	assert.Equal(t, "select 'ops@acme.com' as owner, 2", h.preflightDraft(id, 2).Edits[0].Content)
}

// quotingCheck stands in for confidence_not_low: it refuses a low-confidence
// draft with a finding that quotes the checked file and a value the model was
// never shown.
type quotingCheck struct{ orig step.Check }

func (c quotingCheck) Meta() step.Meta { return c.orig.Meta() }

func (c quotingCheck) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	res := remediation.CheckResult{Check: "confidence_not_low"}
	if in.Draft.Confidence == "low" {
		res.Findings = []remediation.Finding{{
			Code:    "low_confidence",
			Message: "the file reads " + in.Draft.Edits[0].Content + " and the warehouse wants password=hunter2",
		}}
	}
	return step.CheckOutput{Result: res}, nil
}

func TestRunner_ARevisionShowsTheFindingsCleanedAndARestartRebuildsTheSamePlaceholders(t *testing.T) {
	h := newHarness(t, withRounds(3))
	withOwnedSource(h)
	orig, _, ok := h.registry().Lookup("confidence_not_low")
	require.True(t, ok)
	h.replaceStep(quotingCheck{orig: orig.(step.Check)})
	unsure := func(req prompt.ProposeRequest) ports.ProposeResult {
		r := answerWith("select '[redacted:email:1]' as owner, 2", "not sure")(req)
		r.Confidence = "low"
		return r
	}
	answersInTurn(h, unsure, answerWith("select '[redacted:email:1]' as owner, 3", "sure"))
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
	h.afterCommit = nil
	h.reclaim(id)
	require.NoError(t, h.runnerOver(h.files).Drive(ctx, id), "a restarted runner resumes")

	reqs := h.llm.all()
	require.Len(t, reqs, 2, "round 1's model call is not made again")
	assert.Equal(t, "the file reads select 'ops@acme.com' as owner, 2 and the warehouse wants password=hunter2",
		h.roundRows(id, 1, "preflight")[1].Findings[0].Message, "the check reads the restored draft, and the journal keeps its finding raw")
	cleanedFinding := remediation.Finding{
		Code:    "low_confidence",
		Message: "the file reads select '[redacted:email:1]' as owner, 2 and the warehouse wants password=[redacted:secret:1]",
	}
	want := prompt.Revise(reqs[0], step.AnswerJSON(unsure(reqs[0])), step.FindingLines([]remediation.Finding{cleanedFinding}))
	assert.Equal(t, want, reqs[1], "the revision is rebuilt byte for byte, with the findings cleaned by the same placeholders")
	assert.NotContains(t, reqs[1].User, "hunter2")
	assert.NotContains(t, reqs[1].User, "ops@acme.com")
	assert.Equal(t, "select 'ops@acme.com' as owner, 3", h.preflightDraft(id, 2).Edits[0].Content)
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
}

// TestRunner_EveryRoundKeepsThePlaceholdersOfTheRoundsBefore checks that a
// value a finding of round 1 quotes keeps its placeholder in round 3, so no
// number means two values in one request.
func TestRunner_EveryRoundKeepsThePlaceholdersOfTheRoundsBefore(t *testing.T) {
	h := newHarness(t, withRounds(3))
	withOwnedSource(h)
	round := 0
	orig, _, ok := h.registry().Lookup("confidence_not_low")
	require.True(t, ok)
	h.replaceStep(roundCheck{orig: orig.(step.Check), round: &round})
	h.llm.answer = answerWith("select '[redacted:email:1]' as owner, 2", "fixed it")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 3)
	assert.Contains(t, reqs[1].User, "password=[redacted:secret:1]")
	assert.Contains(t, reqs[2].User, "password=[redacted:secret:1]", "round 1's finding keeps its number in round 3's request")
	assert.Contains(t, reqs[2].User, "token=[redacted:secret:2]", "round 2's finding takes the next number")
	assert.Equal(t, 1, strings.Count(reqs[2].User, "[redacted:secret:2]"))
}

// roundCheck refuses round 1 with a finding that quotes one secret and round 2
// with a finding that quotes another, and passes round 3.
type roundCheck struct {
	orig  step.Check
	round *int
}

func (c roundCheck) Meta() step.Meta { return c.orig.Meta() }

func (c roundCheck) Check(context.Context, step.CheckInput) (step.CheckOutput, error) {
	*c.round++
	res := remediation.CheckResult{Check: "confidence_not_low"}
	switch *c.round {
	case 1:
		res.Findings = []remediation.Finding{{Code: "low_confidence", Message: "set password=hunter2"}}
	case 2:
		res.Findings = []remediation.Finding{{Code: "low_confidence", Message: "set token=s3cr3tvalue"}}
	}
	return step.CheckOutput{Result: res}, nil
}

// TestRunner_AFileThatHoldsAMarkerCanBeFixed checks that a redaction marker the
// file already holds may stay in the fix, and that one more is refused.
func TestRunner_AFileThatHoldsAMarkerCanBeFixed(t *testing.T) {
	cases := []struct {
		name, source, kept, added, marker string
	}{
		{"a literal marker", "select '[REDACTED]' as owner", "select '[REDACTED]' as owner, 2", "select '[REDACTED]' as owner, '[REDACTED]'", "REDACTED"},
		{"a fixed marker", "select '[redacted:secret]' as pw", "select '[redacted:secret]' as pw, 2", "select '[redacted:secret]' as pw, '[redacted:secret]'", "redacted secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name+" kept", func(t *testing.T) {
			h := newHarness(t)
			h.source.files["services/svc-a/models/orders.sql"] = tc.source
			h.llm.answer = answerWith(tc.kept, "fixed it")
			id := h.accept(trigger("compile", compileNode("svc-a")))

			require.NoError(t, h.runner.Drive(ctx, id))

			assert.Empty(t, h.roundRows(id, 1, "preflight")[1].Findings)
			assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
			assert.Equal(t, tc.kept, h.preflightDraft(id, 1).Edits[0].Content)
		})
		t.Run(tc.name+" added", func(t *testing.T) {
			h := newHarness(t)
			h.source.files["services/svc-a/models/orders.sql"] = tc.source
			h.llm.answer = answerWith(tc.added, "fixed it")
			id := h.accept(trigger("compile", compileNode("svc-a")))

			require.NoError(t, h.runner.Drive(ctx, id))

			findings := h.roundRows(id, 1, "preflight")[1].Findings
			require.NotEmpty(t, findings)
			assert.Equal(t, remediation.Finding{
				Code:    "unknown_placeholder",
				Message: "the answer adds a redaction marker (" + tc.marker + ") the file did not hold; keep the text you were shown",
				File:    "services/svc-a/models/orders.sql",
			}, findings[0])
			assert.Equal(t, remediation.ClusterHandedOver, h.cluster(id, 1, 1).Status)
		})
	}
}

// TestRunner_AValueSeenOnlyInALogIsNotWrittenIntoAFile checks that a value the
// model saw only in the dbt log keeps its placeholder in the edit, is refused,
// and never reaches the journal or a stored artifact, while a value of the
// shown source is restored.
func TestRunner_AValueSeenOnlyInALogIsNotWrittenIntoAFile(t *testing.T) {
	h := newHarness(t)
	withOwnedSource(h)
	h.evidence.objects["s3://logs/svc-a"] = "connect failed: password=hunter2"
	node := compileNode("svc-a")
	node.DBTLogURI = "s3://logs/svc-a"
	h.llm.answer = answerWith("select '[redacted:email:1]' as owner, '[redacted:secret:1]' as pw", "fixed it")
	id := h.accept(trigger("compile", node))

	require.NoError(t, h.runner.Drive(ctx, id))

	req := h.llm.all()[0].User
	assert.Contains(t, req, "password=[redacted:secret:1]", "the model is shown the log cleaned")
	assert.NotContains(t, req, "hunter2")

	draft := h.preflightDraft(id, 1)
	assert.Equal(t, "select 'ops@acme.com' as owner, '[redacted:secret:1]' as pw", draft.Edits[0].Content,
		"the email of the shown source is restored and the log's password is not")
	findings := h.roundRows(id, 1, "preflight")[1].Findings
	require.NotEmpty(t, findings)
	assert.Equal(t, remediation.Finding{
		Code:    "unknown_placeholder",
		Message: "the answer holds a placeholder (redacted secret 1) for a value from a log or a check, not from the code; leave that value out of the file",
		File:    "services/svc-a/models/orders.sql",
	}, findings[0])
	for _, s := range h.store.Steps(id) {
		if s.Kind == "context" {
			continue // the journal keeps the evidence raw
		}
		assert.NotContains(t, string(s.Output), "hunter2", "step %s stores no value from the log", s.Step)
		assert.NotContains(t, fmt.Sprint(s.Findings), "hunter2", "step %s finds no value from the log", s.Step)
	}
	assert.Equal(t, remediation.ClusterHandedOver, h.cluster(id, 1, 1).Status)
	assert.Empty(t, h.checks.requests())
}

// TestRunner_AValueSeenOnlyInALogStaysAMarkerInTheRationale checks that the
// rationale, which the pull request shows, gets back only values of the code:
// a log's password keeps its placeholder and reaches no stored row, artifact,
// release check or pull request.
func TestRunner_AValueSeenOnlyInALogStaysAMarkerInTheRationale(t *testing.T) {
	h := newHarness(t)
	withOwnedSource(h)
	h.evidence.objects["s3://logs/svc-a"] = "connect failed: password=hunter2"
	node := compileNode("svc-a")
	node.DBTLogURI = "s3://logs/svc-a"
	h.llm.answer = answerWith("select '[redacted:email:1]' as owner, 2", "kept [redacted:email:1]; the log said [redacted:secret:1]")
	id := h.accept(trigger("compile", node))

	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	assert.Equal(t, "kept ops@acme.com; the log said [redacted:secret:1]", h.preflightDraft(id, 1).Rationale,
		"no finding: a marker in the rationale is harmless")
	assert.Empty(t, h.roundRows(id, 1, "preflight")[1].Findings)
	p := h.fixRow(1).Fix
	require.Equal(t, fix.StatusProposed, p.Status)
	assert.Contains(t, fmt.Sprint(p), "the log said [redacted:secret:1]", "the pull request shows the marker")
	require.Len(t, h.proposedEvents(), 1)

	for _, s := range h.store.Steps(id) {
		if s.Kind == "context" {
			continue // the journal keeps the evidence raw
		}
		assert.NotContains(t, string(s.Output), "hunter2", "step %s", s.Step)
		assert.NotContains(t, fmt.Sprint(s.Findings), "hunter2", "step %s", s.Step)
	}
	h.artifacts.mu.Lock()
	assert.NotContains(t, fmt.Sprint(h.artifacts.objects), "hunter2", "no stored artifact")
	h.artifacts.mu.Unlock()
	assert.NotContains(t, fmt.Sprint(h.checks.requests()), "hunter2", "no release check")
	assert.NotContains(t, fmt.Sprint(h.fixes()), "hunter2", "no fix row")
	for _, e := range h.store.OutboxEntries() {
		assert.NotContains(t, string(e.Payload), "hunter2", "no outbox entry on %s", e.StreamName)
	}
}

// TestRunner_AValueSeenOnlyInALogNeverNamesAFile checks that a contract fix
// whose path holds the placeholder of a log's password is refused, and that the
// password reaches no stored row, release check, pull request or outbox
// entry.
func TestRunner_AValueSeenOnlyInALogNeverNamesAFile(t *testing.T) {
	h := newHarness(t, withContractType(1, &refusingPackager{}))
	h.evidence.objects["s3://logs/loader"] = "connect failed: password=hunter2"
	node := contractNode()
	node.DBTLogURI = "s3://logs/loader"
	const leakedPath = "services/svc-a/contracts/[redacted:secret:1].odcs.yaml"
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := contractAnswer(req)
		r.Files = []ports.ProposedFile{{Path: leakedPath, Content: fixedContract}}
		return r
	}
	id := h.accept(trigger("validation", node))

	require.NoError(t, h.runner.Drive(ctx, id))

	require.Contains(t, h.llm.all()[0].User, "password=[redacted:secret:1]", "the model is shown the log cleaned")
	findings := h.roundRows(id, 1, "preflight")[1].Findings
	require.NotEmpty(t, findings)
	assert.Equal(t, remediation.Finding{
		Code:    "unknown_placeholder",
		Message: "the answer names a file with a placeholder (redacted secret 1) for a value from a log or a check, not from the code; leave that value out of the path",
	}, findings[0])
	assert.Equal(t, leakedPath, h.preflightDraft(id, 1).Edits[0].Path, "the stored draft keeps the placeholder")
	assert.Equal(t, remediation.ClusterHandedOver, h.cluster(id, 1, 1).Status)

	for _, s := range h.store.Steps(id) {
		if s.Kind == "context" {
			continue // the journal keeps the evidence raw
		}
		assert.NotContains(t, string(s.Output), "hunter2", "step %s", s.Step)
		assert.NotContains(t, fmt.Sprint(s.Findings), "hunter2", "step %s", s.Step)
	}
	assert.NotContains(t, fmt.Sprint(h.checks.requests()), "hunter2", "no release check")
	assert.NotContains(t, fmt.Sprint(h.fixes()), "hunter2", "no fix row")
	for _, e := range h.store.OutboxEntries() {
		assert.NotContains(t, string(e.Payload), "hunter2", "no outbox entry on %s", e.StreamName)
	}
}
