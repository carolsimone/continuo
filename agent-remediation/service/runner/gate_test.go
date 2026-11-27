package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// The checks that verify the edits of try 1 and try 2 to svc-a and svc-b.
const (
	svcACheck1 = "verify-rel-1-svc-a-r1-t1"
	svcACheck2 = "verify-rel-1-svc-a-r1-t2"
	svcBCheck1 = "verify-rel-1-svc-b-r1-t1"
)

func TestRunner_NoEditSettlesTheTry(t *testing.T) {
	t.Run("every node skipped is a skipped try", func(t *testing.T) {
		h := newHarness(t)
		test := remediation.TriggerNode{NodeID: "test.sales.not_null", ErrorSignature: "sig-t", NodeType: "dbt-test", Project: "svc-a"}
		python := remediation.TriggerNode{NodeID: "python.sales.loader", ErrorSignature: "sig-p", NodeType: "python-node", Project: "svc-a"}
		id := h.accept(trigger("validation", test, python))

		require.NoError(t, h.runner.Drive(ctx, id))

		p := h.fixRow(1).Fix
		assert.Equal(t, fix.StatusSkipped, p.Status)
		assert.Equal(t, map[string]fix.NodeOutcome{
			test.NodeID:   {Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType},
			python.NodeID: {Status: fix.StatusSkipped, Reason: noTypeReason, Code: model.HandoverReasonNoType},
		}, p.NodeOutcomes)
		assert.Equal(t, id.String(), p.RemediationID)
		assert.Empty(t, p.Rationale)
		assert.Len(t, h.fixes(), 1, "the in-flight row is the one settled")
		assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
		assert.Equal(t, []string{"hand_over"}, stepNames(h.store.Steps(id)), "no cross-cluster check or gate runs, and the hand-over is recorded")
		assert.Empty(t, h.checks.requests())
	})

	t.Run("a node that was attempted fails the try", func(t *testing.T) {
		h := newHarness(t)
		h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
			r := fixAnswer(req)
			r.Confidence = "low"
			return r
		}
		test := compileNode("svc-b")
		test.NodeType = "dbt-test"
		id := h.accept(trigger("compile", compileNode("svc-a"), test))

		require.NoError(t, h.runner.Drive(ctx, id))

		p := h.fixRow(1).Fix
		assert.Equal(t, fix.StatusFailed, p.Status)
		assert.Equal(t, map[string]fix.NodeOutcome{
			"svc-a": {Status: fix.StatusFailed, Reason: lowConfidenceReason, Code: model.HandoverReasonPreflightRoundsSpent},
			"svc-b": {Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType},
		}, p.NodeOutcomes)
		assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
		assert.Empty(t, h.checks.requests())
	})
}

// crossClusterTrigger is a trigger of two failing nodes: the consumer svc-a,
// fixed at its changed ancestor in core, and the producer core, whose own
// compile failure is fixed in the same file as the ancestor.
func crossClusterTrigger() remediation.Trigger {
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Project: "core", Depth: 1}
	consumer := compileNode("svc-a")
	consumer.Project = "svc-a"
	consumer.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	producer := compileNode("core")
	producer.Project = "core"
	producer.FilePath = "models/base.sql"
	return trigger("compile", consumer, producer)
}

// twoEditsOfOneFile makes both of crossClusterTrigger's clusters edit
// services/core/models/base.sql.
func (h *harness) twoEditsOfOneFile() {
	h.source.files["services/core/models/base.sql"] = "select 1"
	h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: ports.RuntimeDbt}}
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.ProposedSQL = "select 2"
		return r
	}
}

// With a single try, a cross-cluster finding fails the try's fix row and the try, and
// the tries being spent, the remediation hands over.
func TestRunner_CrossClusterFindingFailsTheTry(t *testing.T) {
	h := newHarness(t, withTries(1))
	h.twoEditsOfOneFile()
	id := h.accept(crossClusterTrigger())

	require.NoError(t, h.runner.Drive(ctx, id))

	for _, c := range h.store.Clusters(id) {
		require.Equal(t, remediation.ClusterChecked, c.Status, "cluster %d", c.Cluster)
	}
	const reason = "two edits change the same file: services/core/models/base.sql"
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
		"core":  {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
	}, p.NodeOutcomes)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Len(t, h.store.Tries(id), 1, "the only try is spent")
	assert.Equal(t, fix.StatusEscalated, h.fixRow(fix.HandOverTry).Fix.Status, "the tries being spent, the failing set is handed over")
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.checks.requests())

	rows := h.flowRows(id, 1)
	require.Len(t, rows["edits_map_to_a_project"], 1)
	assert.Empty(t, rows["edits_map_to_a_project"][0].Findings)
	require.Len(t, rows["no_path_edited_twice"], 1)
	dup := rows["no_path_edited_twice"][0]
	assert.Equal(t, "check", dup.Kind)
	assert.Equal(t, remediation.StepDone, dup.Status)
	assert.Equal(t, flowRevision, dup.FileRevision)
	assert.NotEmpty(t, dup.InputDigest)
	require.Len(t, dup.Findings, 1)
	assert.Equal(t, "path_edited_twice", dup.Findings[0].Code)
	assert.Empty(t, rows["one_manifest_kind"], "the checks stop at the first finding")
	assert.Empty(t, rows["release_check"])
}

// A cross-cluster finding fails the try like a failed gate does: the try
// is failed with the finding, the try fails and the remediation, still running,
// opens its next try, whose requests show the finding among the earlier tries.
// The second try's upstream fix is not confident, so the producer's edit is its
// only draft and the try reaches the gate.
func TestRunner_ACrossClusterFindingStartsTheNextTry(t *testing.T) {
	h := newHarness(t, withPreviousTries())
	h.twoEditsOfOneFile()
	upstreamAnswer := h.llm.answer
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := upstreamAnswer(req)
		if strings.HasPrefix(req.User, "Upstream node:") && strings.Contains(req.User, "## Earlier tries at this failure") {
			r.Confidence = "low"
		}
		return r
	}
	id := h.accept(crossClusterTrigger())

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "two edits change the same file: services/core/models/base.sql"
	first := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusFailed, first.Status)
	assert.Equal(t, reason, first.Rationale)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "the failed try does not end the remediation")
	assert.Equal(t, 2, h.remediation(id).CurrentTry)
	second := h.try(id, 2)
	assert.Equal(t, remediation.TryVerifying, second.Status)
	assert.Equal(t, 2, second.Try)
	assert.Equal(t, fix.StatusVerifying, h.fixRow(2).Fix.Status)
	assert.Len(t, h.checks.requests(), 1, "the second try's one edited project is verified")

	require.Equal(t, 4, h.llm.calls(), "each try asks the model once per cluster")
	for _, req := range h.llm.requests[:2] {
		assert.NotContains(t, req.User, "## Earlier tries at this failure", "the first try has no earlier try")
	}
	for _, req := range h.llm.requests[2:] {
		assert.Contains(t, req.User, "### Round 1, try 1", "the second try sees the first")
		assert.Contains(t, req.User, "What pre-flight found:\n- across clusters: path_edited_twice: "+reason+" (services/core/models/base.sql)",
			"and the cross-cluster finding that failed it")
	}
}

func TestRunner_TheVerifyingRowCarriesTheTry(t *testing.T) {
	h := newHarness(t)
	calls := 0
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		calls++
		r := fixAnswer(req)
		if calls == 2 {
			r.Confidence = "unsure"
		}
		return r
	}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))

	require.NoError(t, h.runner.Drive(ctx, id))

	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusVerifying, p.Status)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, []string{"svc-a", "svc-b"}, p.ResolvedNodeIDs)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": {Status: fix.StatusVerifying},
		"svc-b": {Status: fix.StatusVerifying},
	}, p.NodeOutcomes)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhaseQueued},
		{Project: "svc-b", Kind: "dbt", CheckID: svcBCheck1, Phase: fix.PhaseQueued},
	}, p.ReleaseChecks)
	assert.Equal(t, svcACheck1, p.ReleaseCheckID)
	assert.Equal(t, []fix.FileEdit{
		{
			Path:          "services/svc-a/models/orders.sql",
			ContentURI:    "s3://artifacts/proposed-fix/rel-1/svc-a/round-1/try-1.source.sql",
			DiffURI:       "s3://artifacts/proposed-fix/rel-1/svc-a/round-1/try-1.source.diff",
			TargetNodeID:  "svc-a",
			MemberNodeIDs: []string{"svc-a"},
		},
		{
			Path:          "services/svc-b/models/orders.sql",
			ContentURI:    "s3://artifacts/proposed-fix/rel-1/svc-b/round-1/try-1.source.sql",
			DiffURI:       "s3://artifacts/proposed-fix/rel-1/svc-b/round-1/try-1.source.diff",
			TargetNodeID:  "svc-b",
			MemberNodeIDs: []string{"svc-b"},
		},
	}, p.Edits)
	assert.Equal(t, fix.ConfidenceMedium, p.Confidence, "the weakest cluster's confidence, normalized")
	assert.Equal(t, "claude-test", p.Model)
	assert.True(t, p.SourceResolved)
	assert.Equal(t, repoName, p.Repo)
	assert.Equal(t, commitSHA, p.CommitSHA)
	assert.Equal(t, []string{"svc-a", "svc-b"}, p.Projects)
	assert.Contains(t, p.Rationale, "Failed: `svc-a`")
	assert.Contains(t, p.Rationale, "Failed: `svc-b`")
	assert.Contains(t, p.Rationale, "fixed the select")
	assert.Equal(t, 2, strings.Count(p.Rationale, "fixed the select"), "one rationale per cluster")

	tr := h.try(id, 1)
	assert.Equal(t, remediation.TryVerifying, tr.Status)
	assert.Equal(t, t0.Add(releaseCheckTimeout+time.Hour), tr.DeadlineAt)

	rows := h.flowRows(id, 1)
	for _, name := range []string{"edits_map_to_a_project", "no_path_edited_twice", "one_manifest_kind"} {
		require.Len(t, rows[name], 1, name)
		assert.Equal(t, "check", rows[name][0].Kind, name)
		assert.Equal(t, remediation.StepDone, rows[name][0].Status, name)
		assert.Empty(t, rows[name][0].Findings, name)
	}
	require.Len(t, rows["release_check"], 1)
	gate := rows["release_check"][0]
	assert.Equal(t, "check", gate.Kind)
	assert.Equal(t, remediation.StepWaiting, gate.Status)
	assert.Equal(t, []string{svcACheck1, svcBCheck1}, gate.WaitingOn)
	assert.Equal(t, t0, gate.StartedAt)
	assert.Nil(t, gate.FinishedAt)
	assert.Equal(t, flowRevision, gate.FileRevision)

	checks := h.checks.requests()
	require.Len(t, checks, 2)
	assert.Equal(t, svcACheck1, checks[0].CheckID)
	assert.Equal(t, "tag-svc-a", checks[0].ImageTag)
	assert.Equal(t, 1, checks[0].Try)
	assert.NotEmpty(t, checks[0].SourceOverlayURI)
	assert.Equal(t, svcBCheck1, checks[1].CheckID)

	require.NoError(t, h.runner.Drive(ctx, id), "a verifying try waits for the poller")
	assert.Len(t, h.checks.requests(), 2)
}

func TestRunner_GatePassEmitsOneProposedEvent(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)

	require.NoError(t, h.poll(id))
	require.NoError(t, h.poll(id), "a poll of a remediation that ended does nothing")

	events := h.proposedEvents()
	require.Len(t, events, 1)
	var payload struct {
		ReleaseID string `json:"release_id"`
		Try       int    `json:"try"`
	}
	tenantID, body := proposedPayload(t, events[0])
	assert.Equal(t, "default", tenantID)
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, releaseID, payload.ReleaseID)
	assert.Equal(t, 1, payload.Try)

	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusProposed, p.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": {Status: fix.StatusProposed}}, p.NodeOutcomes)
	act := t0
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhasePassed, ActivatedAt: &act},
	}, p.ReleaseChecks)
	assert.Equal(t, remediation.TryPassed, h.try(id, 1).Status)
	assert.Len(t, h.store.Tries(id), 1)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)

	rows := h.flowRows(id, 1)
	gate := rows["release_check"][0]
	assert.Equal(t, remediation.StepDone, gate.Status)
	assert.NotNil(t, gate.FinishedAt)
	assert.Empty(t, gate.Findings)
	require.Len(t, rows["pull_request"], 1)
	assert.Equal(t, "output", rows["pull_request"][0].Kind)
	assert.Equal(t, remediation.StepDone, rows["pull_request"][0].Status)
}

func TestRunner_GateFailStartsTheNextTry(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "column x does not exist"})

	require.NoError(t, h.poll(id))

	first := h.fixRow(1)
	assert.Equal(t, fix.StatusFailed, first.Fix.Status)
	assert.Equal(t, "svc-a: column x does not exist", first.VerifyError)
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": {Status: fix.StatusFailed}}, first.Fix.NodeOutcomes)
	act := t0
	assert.Equal(t, []fix.ReleaseCheck{{
		Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhaseFailed, ActivatedAt: &act,
		Error: "svc-a: column x does not exist",
	}}, first.Fix.ReleaseChecks)
	gate := h.flowRows(id, 1)["release_check"][0]
	assert.Equal(t, remediation.StepDone, gate.Status)
	assert.Equal(t, []remediation.Finding{{Code: "release_check_failed", Message: "svc-a: column x does not exist"}}, gate.Findings)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Empty(t, h.proposedEvents())

	second := h.try(id, 2)
	assert.Equal(t, remediation.TryDrafting, second.Status)
	assert.Equal(t, 2, second.Try)
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, 2, h.remediation(id).CurrentTry)
	generating := h.fixRow(2).Fix
	assert.Equal(t, fix.StatusGenerating, generating.Status)
	assert.Equal(t, id.String(), generating.RemediationID)

	require.NoError(t, h.drive(id), "the worker drives the next try to the gate")
	assert.Equal(t, remediation.TryVerifying, h.try(id, 2).Status)
	checks := h.checks.requests()
	require.Len(t, checks, 2)
	assert.Equal(t, svcACheck2, checks[1].CheckID)
	assert.Equal(t, 2, h.llm.calls(), "each try asks the model again")
}

func TestRunner_EachTryIsShownTheEarlierTriesAtItsFailure(t *testing.T) {
	h := newHarness(t, withPreviousTries())
	h.tryWithNoRemediation(1, fix.StatusFailed)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.drive(id))
	first := h.llm.lastRequest().User
	assert.Contains(t, first, "\n\n## Earlier tries at this failure\n", "the first try is shown the try with no remediation")
	assert.Contains(t, first, "### Round 1, try 1\nNothing was recorded for this try.")
	assert.NotContains(t, first, "### Round 1, try 2", "the try being made is not one of its earlier tries")

	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "column x does not exist"})
	require.NoError(t, h.poll(id))
	require.NoError(t, h.drive(id))

	require.Equal(t, 2, h.llm.calls())
	second := h.llm.lastRequest().User
	assert.Contains(t, second, "### Round 1, try 1\nNothing was recorded for this try.")
	assert.Contains(t, second, "### Round 1, try 2\nChanged ")
	assert.Contains(t, second, "+select 2", "the second try sees the diff the first applied")
	assert.Contains(t, second, "Why the gate failed: svc-a: column x does not exist", "and why its release check failed")
	assert.Less(t, strings.Index(second, "### Round 1, try 1"), strings.Index(second, "### Round 1, try 2"), "oldest first")
}

func TestRunner_TriesSpentHandsOver(t *testing.T) {
	h := newHarness(t)
	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "still broken"})
	id := h.accept(trigger("compile", compileNode("svc-a")))

	for try := 1; try <= 3; try++ {
		require.NoError(t, h.drive(id))
		require.Equal(t, remediation.TryVerifying, h.try(id, try).Status, "try %d", try)
		require.NoError(t, h.poll(id))
	}

	for try := 1; try <= 3; try++ {
		p := h.fixRow(try)
		assert.Equal(t, fix.StatusFailed, p.Fix.Status, "try %d", try)
		assert.Equal(t, "svc-a: still broken", p.VerifyError, "try %d", try)
	}
	escalated := h.fixRow(fix.HandOverTry).Fix
	assert.Equal(t, fix.StatusEscalated, escalated.Status)
	assert.Equal(t, id.String(), escalated.RemediationID)
	spent := fix.NodeOutcome{
		Status: fix.StatusEscalated,
		Reason: "tried 3 times; the last try failed: svc-a: still broken",
		Code:   model.HandoverReasonTriesSpent,
	}
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": spent}, escalated.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.store.Tries(id), 3)
	require.Len(t, h.flowRows(id, 3)["hand_over"], 1, "the hand-over is recorded on the last try")
	assert.Equal(t, handedOverOf(id, 1, 3, handedOverNode("svc-a", spent)), h.handedOverEvent())
	assert.Equal(t, 3, h.llm.calls())

	require.NoError(t, h.drive(id))
	assert.Len(t, h.fixes(), 4, "a remediation that ended writes nothing more")
}

func TestRunner_FixRowMovedUnderTheRunnerEndsQuietly(t *testing.T) {
	for _, phase := range []fix.Phase{fix.PhasePassed, fix.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			h := newHarness(t)
			id := h.accept(trigger("compile", compileNode("svc-a")))
			require.NoError(t, h.runner.Drive(ctx, id))
			moved, err := h.store.FixRepository().MarkVerifyFailed(ctx, h.fixRow(1).ID, "failed by hand", nil)
			require.NoError(t, err)
			require.True(t, moved)
			h.checks.answer(phase, map[string]string{"svc-a": "broken"})

			require.NoError(t, h.poll(id))

			assert.Empty(t, h.proposedEvents())
			row := h.fixRow(1)
			assert.Equal(t, fix.StatusFailed, row.Fix.Status)
			assert.Equal(t, "failed by hand", row.VerifyError, "the row keeps what it already said")
			assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			assert.Len(t, h.store.Tries(id), 1, "no next try starts")
			assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
			assert.Len(t, h.fixes(), 1)
		})
	}
}

func TestRunner_TheGateIsPolledFromWhenItsStepStarted(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.status = func(string) (ports.ReleaseCheckStatus, error) {
		return ports.ReleaseCheckStatus{}, errors.New("release-controller unreachable")
	}
	// The try opened at t0 and its gate step started ten minutes later.
	submitted := t0.Add(10 * time.Minute)
	gate := h.flowRows(id, 1)["release_check"][0]
	gate.StartedAt = submitted
	h.saveStep(gate)

	h.clock.set(submitted.Add(releaseCheckTimeout - time.Second))
	require.NoError(t, h.poll(id))
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status, "the budget counts from the gate step, not the try")

	h.clock.set(submitted.Add(releaseCheckTimeout))
	require.NoError(t, h.poll(id))
	first := h.fixRow(1)
	assert.Equal(t, fix.StatusFailed, first.Fix.Status)
	assert.Equal(t, "release check timed out", first.VerifyError)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhaseQueued},
	}, first.Fix.ReleaseChecks, "a check that was never read keeps what was recorded for it")
	assert.Equal(t, remediation.TryDrafting, h.try(id, 2).Status)
}

func TestRunner_APollWithNoNewPhaseWritesNothing(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))

	require.NoError(t, h.poll(id))
	assert.Zero(t, h.phaseWrites.Load(), "the check is still queued, as recorded")

	h.checks.answer(fix.PhaseRunning, nil)
	require.NoError(t, h.poll(id))
	assert.EqualValues(t, 1, h.phaseWrites.Load())
	act := t0
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhaseRunning, ActivatedAt: &act},
	}, h.fixRow(1).Fix.ReleaseChecks)

	require.NoError(t, h.poll(id))
	assert.EqualValues(t, 1, h.phaseWrites.Load(), "a phase that did not change is not written again")
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
}

func TestRunner_ACommitFailureAfterThePassEmitsOneProposedEvent(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)
	var armed atomic.Bool
	h.checks.onStatus = func(context.Context) {
		if armed.CompareAndSwap(false, true) {
			h.store.FailNextCommit()
		}
	}

	require.Error(t, h.poll(id), "the pass's commit fails")
	assert.Empty(t, h.proposedEvents())
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status)
	gate := h.flowRows(id, 1)["release_check"][0]
	assert.Equal(t, remediation.StepWaiting, gate.Status, "the gate still waits on its checks")
	assert.Equal(t, 1, gate.Failures)

	require.NoError(t, h.poll(id))
	require.NoError(t, h.poll(id))

	assert.Len(t, h.proposedEvents(), 1)
	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

func TestRunner_ACommitFailureAfterTheHandOverWritesOneEscalatedRow(t *testing.T) {
	h := newHarness(t, withTries(1))
	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "still broken"})
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	armed := false
	h.afterCommit = func() {
		if !armed && h.try(id, 1).Status == remediation.TryFailed {
			armed = true
			h.store.FailNextCommit()
		}
	}

	require.Error(t, h.poll(id), "the hand-over's commit fails")
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	h.afterCommit = nil

	require.NoError(t, h.drive(id))
	require.NoError(t, h.drive(id))

	var escalated []fix.Fix
	for _, p := range h.fixes() {
		if p.Fix.Status == fix.StatusEscalated {
			escalated = append(escalated, p.Fix)
		}
	}
	require.Len(t, escalated, 1)
	assert.Equal(t, fix.HandOverTry, escalated[0].Try)
	assert.Len(t, h.handedOverEvents(), 1, "the hand-over emits once")
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

func TestRunner_ASubmitErrorLeavesTheTryCheckingAndRunsAgain(t *testing.T) {
	h := newHarness(t)
	h.checks.submitErr = errors.New("release-controller unavailable")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	err := h.runner.Drive(ctx, id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "release-controller unavailable")
	tr := h.try(id, 1)
	assert.Equal(t, remediation.TryChecking, tr.Status)
	assert.Equal(t, t0.Add(10*time.Minute), tr.DeadlineAt)
	gate := h.flowRows(id, 1)["release_check"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepFailed, gate[0].Status)
	assert.Equal(t, 1, gate[0].Failures)
	assert.Equal(t, fix.StatusGenerating, h.fixRow(1).Fix.Status)

	later := t0.Add(time.Minute)
	h.clock.set(later)
	require.NoError(t, h.runner.Drive(ctx, id))

	tr = h.try(id, 1)
	assert.Equal(t, remediation.TryVerifying, tr.Status)
	assert.Equal(t, later.Add(releaseCheckTimeout+time.Hour), tr.DeadlineAt)
	rows := h.flowRows(id, 1)
	for _, name := range []string{"edits_map_to_a_project", "no_path_edited_twice", "one_manifest_kind"} {
		assert.Equal(t, t0, rows[name][0].StartedAt, "%s ran once", name)
	}
	assert.Equal(t, remediation.StepWaiting, rows["release_check"][0].Status)
	assert.Equal(t, later, rows["release_check"][0].StartedAt)
	assert.Equal(t, 1, rows["release_check"][0].Failures)
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status)
}

// The commit that leaves the try verifying can fail after the gate submitted
// its checks. The next drive submits them again under the same ids, so each
// edited project still has one release check, and the gate waits on the
// checks it submitted.
func TestRunner_AFailedVerifyingCommitResubmitsTheSameRuns(t *testing.T) {
	h := newHarness(t)
	var armed atomic.Bool
	h.checks.onSubmit = func() {
		if armed.CompareAndSwap(false, true) {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))

	require.Error(t, h.runner.Drive(ctx, id), "the commit after the submit fails")
	submitted := h.checks.requests()
	require.Len(t, submitted, 2, "the gate submitted a check per project")
	assert.Equal(t, remediation.TryChecking, h.try(id, 1).Status)
	assert.Equal(t, fix.StatusGenerating, h.fixRow(1).Fix.Status, "the fix row records no check")
	gate := h.flowRows(id, 1)["release_check"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepFailed, gate[0].Status)
	assert.Equal(t, 1, gate[0].Failures)

	require.NoError(t, h.drive(id))

	checks := h.checks.requests()
	require.Len(t, checks, 4)
	assert.Equal(t, submitted, checks[2:], "the second submit names the same checks over the same artifacts")
	assert.Equal(t, []string{svcACheck1, svcBCheck1}, []string{checks[2].CheckID, checks[3].CheckID})
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	gate = h.flowRows(id, 1)["release_check"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepWaiting, gate[0].Status)
	assert.Equal(t, []string{svcACheck1, svcBCheck1}, gate[0].WaitingOn)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "svc-a", Kind: "dbt", CheckID: svcACheck1, Phase: fix.PhaseQueued},
		{Project: "svc-b", Kind: "dbt", CheckID: svcBCheck1, Phase: fix.PhaseQueued},
	}, h.fixRow(1).Fix.ReleaseChecks, "one check per project")

	h.checks.answer(fix.PhasePassed, nil)
	require.NoError(t, h.poll(id))
	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Len(t, h.checks.requests(), 4, "the poll submits nothing")
}

// lowUpstreamConfidenceOnALaterTry makes the upstream fix of
// crossClusterTrigger low-confidence once its request shows an earlier try, so
// a later try's only draft is the producer's edit.
func (h *harness) lowUpstreamConfidenceOnALaterTry() {
	answer := h.llm.answer
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := answer(req)
		if strings.HasPrefix(req.User, "Upstream node:") && strings.Contains(req.User, "## Earlier tries at this failure") {
			r.Confidence = "low"
		}
		return r
	}
}

// A gate that refuses the try's drafts without starting a check fails the try
// like a gate whose check failed: the try is failed with the refusal, the try
// fails, and the remediation, still running, opens its next try, whose requests
// show the refusal as the reason the gate failed. With no cross-cluster check
// in the flow, the gate is the one to find two edits of one file.
func TestRunner_AGateThatRefusesTheDraftsStartsTheNextTry(t *testing.T) {
	h := newHarness(t, withoutCrossClusterChecks(), withPreviousTries())
	h.twoEditsOfOneFile()
	h.lowUpstreamConfidenceOnALaterTry()
	id := h.accept(crossClusterTrigger())

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "two edits change the same file: services/core/models/base.sql"
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
		"core":  {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
	}, p.NodeOutcomes)
	assert.Empty(t, p.ReleaseChecks)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "the refused try does not end the remediation")
	assert.Equal(t, 2, h.remediation(id).CurrentTry)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 2).Status)
	assert.Equal(t, fix.StatusVerifying, h.fixRow(2).Fix.Status)
	assert.Len(t, h.checks.requests(), 1, "only the second try started a check")
	assert.Empty(t, h.proposedEvents())

	rows := h.flowRows(id, 1)
	require.Len(t, rows, 1, "the gate is the only flow-level step of the first try")
	require.Len(t, rows["release_check"], 1)
	gate := rows["release_check"][0]
	assert.Equal(t, remediation.StepDone, gate.Status)
	assert.NotNil(t, gate.FinishedAt)
	assert.Empty(t, gate.WaitingOn)
	assert.Equal(t, []remediation.Finding{{Code: "path_edited_twice", Message: reason}}, gate.Findings)

	require.Equal(t, 4, h.llm.calls(), "each try asks the model once per cluster")
	for _, req := range h.llm.requests[2:] {
		assert.Contains(t, req.User, "### Round 1, try 1", "the second try sees the first")
		assert.Contains(t, req.User, "Why the gate failed: "+reason, "and the refusal that failed it")
	}
}

// A gate refusal with no try left hands the failing set over.
func TestRunner_AGateRefusalWithNoTryLeftHandsOver(t *testing.T) {
	h := newHarness(t, withoutCrossClusterChecks(), withTries(1))
	h.twoEditsOfOneFile()
	id := h.accept(crossClusterTrigger())

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, fix.StatusFailed, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Len(t, h.store.Tries(id), 1, "the only try is spent")
	assert.Equal(t, fix.StatusEscalated, h.fixRow(fix.HandOverTry).Fix.Status, "the tries being spent, the failing set is handed over")
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.checks.requests())
}

// A refusal of the try's drafts, by a cross-cluster check or by the gate,
// fails the members of the clusters whose drafts it judged. A cluster already
// handed over in the try keeps the outcome it was handed over with.
func TestRunner_ARefusalKeepsTheOutcomesOfClustersAlreadyHandedOver(t *testing.T) {
	const reason = "two edits change the same file: services/core/models/base.sql"
	for name, opts := range map[string][]option{
		"a cross-cluster check": {withTries(1)},
		"the gate":              {withTries(1), withoutCrossClusterChecks()},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, opts...)
			h.twoEditsOfOneFile()
			trig := crossClusterTrigger()
			test := compileNode("svc-b")
			test.NodeType = "dbt-test"
			trig.Nodes = append(trig.Nodes, test)
			id := h.accept(trig)

			require.NoError(t, h.runner.Drive(ctx, id))

			p := h.fixRow(1).Fix
			assert.Equal(t, fix.StatusFailed, p.Status)
			assert.Equal(t, reason, p.Rationale)
			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
				"core":  {Status: fix.StatusFailed, Reason: reason, Code: model.HandoverReasonUnverifiableEdits},
				"svc-b": {Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType},
			}, p.NodeOutcomes)
		})
	}
}
