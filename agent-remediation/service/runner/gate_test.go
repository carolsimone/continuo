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

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// The runs that verify the edits of attempt 1 and attempt 2 to svc-a and svc-b.
const (
	svcARun1 = "verify-rel-1-svc-a-a1"
	svcARun2 = "verify-rel-1-svc-a-a2"
	svcBRun1 = "verify-rel-1-svc-b-a1"
)

func TestRunner_NoEditSettlesTheAttempt(t *testing.T) {
	t.Run("every node skipped is a skipped attempt", func(t *testing.T) {
		h := newHarness(t)
		test := remediation.TriggerNode{NodeID: "test.sales.not_null", ErrorSignature: "sig-t", NodeType: "dbt-test", Service: "svc-a"}
		python := remediation.TriggerNode{NodeID: "python.sales.loader", ErrorSignature: "sig-p", NodeType: "python-node", Service: "svc-a"}
		id := h.accept(trigger("validation", test, python))

		require.NoError(t, h.runner.Drive(ctx, id))

		p := h.attempt(1).Proposal
		assert.Equal(t, proposal.StatusSkipped, p.Status)
		assert.Equal(t, map[string]proposal.NodeOutcome{
			test.NodeID:   {Status: proposal.StatusSkipped, Reason: "dbt tests are not fix targets; fix the model or edit the test by hand"},
			python.NodeID: {Status: proposal.StatusSkipped, Reason: "no remediation type matches this failure"},
		}, p.NodeOutcomes)
		assert.Equal(t, id.String(), p.RemediationID)
		assert.Empty(t, p.Rationale)
		assert.Len(t, h.proposals(), 1, "the in-flight row is the one settled")
		assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
		assert.Empty(t, h.flowRows(id, 1), "no cross-cluster check or gate runs")
		assert.Empty(t, h.pipeline.runs())
	})

	t.Run("a node that was attempted fails the attempt", func(t *testing.T) {
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

		p := h.attempt(1).Proposal
		assert.Equal(t, proposal.StatusFailed, p.Status)
		assert.Equal(t, map[string]proposal.NodeOutcome{
			"svc-a": {Status: proposal.StatusFailed, Reason: "the model could not determine a safe fix with confidence"},
			"svc-b": {Status: proposal.StatusSkipped, Reason: "dbt tests are not fix targets; fix the model or edit the test by hand"},
		}, p.NodeOutcomes)
		assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
		assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
		assert.Empty(t, h.pipeline.runs())
	})
}

func TestRunner_CrossClusterFindingFailsTheAttempt(t *testing.T) {
	h := newHarness(t)
	h.source.files["services/core/models/base.sql"] = "select 1"
	h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: ports.RuntimeDbt}}
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.ProposedSQL = "select 2"
		return r
	}
	// The consumer is fixed at the changed ancestor in core, and the producer's
	// own compile failure is fixed in the same file.
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 1}
	consumer := compileNode("svc-a")
	consumer.Service = "svc-a"
	consumer.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	producer := compileNode("core")
	producer.Service = "core"
	producer.FilePath = "models/base.sql"
	id := h.accept(trigger("compile", consumer, producer))

	require.NoError(t, h.runner.Drive(ctx, id))

	for _, c := range h.store.Clusters(id) {
		require.Equal(t, remediation.ClusterChecked, c.Status, "cluster %d", c.Cluster)
	}
	const reason = "two edits change the same file: services/core/models/base.sql"
	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]proposal.NodeOutcome{
		"svc-a": {Status: proposal.StatusFailed, Reason: reason},
		"core":  {Status: proposal.StatusFailed, Reason: reason},
	}, p.NodeOutcomes)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.pipeline.runs())

	rows := h.flowRows(id, 1)
	require.Len(t, rows["edits_map_to_a_service"], 1)
	assert.Empty(t, rows["edits_map_to_a_service"][0].Findings)
	require.Len(t, rows["no_path_edited_twice"], 1)
	dup := rows["no_path_edited_twice"][0]
	assert.Equal(t, "check", dup.Kind)
	assert.Equal(t, remediation.StepDone, dup.Status)
	assert.Equal(t, flowRevision, dup.FileRevision)
	assert.NotEmpty(t, dup.InputDigest)
	require.Len(t, dup.Findings, 1)
	assert.Equal(t, "path_edited_twice", dup.Findings[0].Code)
	assert.Empty(t, rows["one_manifest_kind"], "the checks stop at the first finding")
	assert.Empty(t, rows["verification_run"])
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

	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusVerifying, p.Status)
	assert.Equal(t, id.String(), p.RemediationID)
	assert.Equal(t, []string{"svc-a", "svc-b"}, p.ResolvedNodeIDs)
	assert.Equal(t, map[string]proposal.NodeOutcome{
		"svc-a": {Status: proposal.StatusVerifying},
		"svc-b": {Status: proposal.StatusVerifying},
	}, p.NodeOutcomes)
	assert.Equal(t, []proposal.Verification{
		{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseQueued},
		{Service: "svc-b", Kind: "dbt", RunID: svcBRun1, Phase: proposal.PhaseQueued},
	}, p.Verifications)
	assert.Equal(t, svcARun1, p.VerificationRunID)
	assert.Equal(t, []proposal.FileEdit{
		{
			Path:          "services/svc-a/models/orders.sql",
			ContentURI:    "s3://artifacts/proposed-fix/rel-1/svc-a/attempt-1.source.sql",
			DiffURI:       "s3://artifacts/proposed-fix/rel-1/svc-a/attempt-1.source.diff",
			TargetNodeID:  "svc-a",
			MemberNodeIDs: []string{"svc-a"},
		},
		{
			Path:          "services/svc-b/models/orders.sql",
			ContentURI:    "s3://artifacts/proposed-fix/rel-1/svc-b/attempt-1.source.sql",
			DiffURI:       "s3://artifacts/proposed-fix/rel-1/svc-b/attempt-1.source.diff",
			TargetNodeID:  "svc-b",
			MemberNodeIDs: []string{"svc-b"},
		},
	}, p.Edits)
	assert.Equal(t, proposal.ConfidenceMedium, p.Confidence, "the weakest cluster's confidence, normalized")
	assert.Equal(t, "claude-test", p.Model)
	assert.True(t, p.SourceResolved)
	assert.Equal(t, repoName, p.Repo)
	assert.Equal(t, commitSHA, p.CommitSHA)
	assert.Equal(t, []string{"svc-a", "svc-b"}, p.Services)
	assert.Contains(t, p.Rationale, "Failed: `svc-a`")
	assert.Contains(t, p.Rationale, "Failed: `svc-b`")
	assert.Contains(t, p.Rationale, "fixed the select")
	assert.Equal(t, 2, strings.Count(p.Rationale, "fixed the select"), "one rationale per cluster")

	tr := h.try(id, 1)
	assert.Equal(t, remediation.TryVerifying, tr.Status)
	assert.Equal(t, t0.Add(verificationTimeout+time.Hour), tr.DeadlineAt)

	rows := h.flowRows(id, 1)
	for _, name := range []string{"edits_map_to_a_service", "no_path_edited_twice", "one_manifest_kind"} {
		require.Len(t, rows[name], 1, name)
		assert.Equal(t, "check", rows[name][0].Kind, name)
		assert.Equal(t, remediation.StepDone, rows[name][0].Status, name)
		assert.Empty(t, rows[name][0].Findings, name)
	}
	require.Len(t, rows["verification_run"], 1)
	gate := rows["verification_run"][0]
	assert.Equal(t, "check", gate.Kind)
	assert.Equal(t, remediation.StepWaiting, gate.Status)
	assert.Equal(t, []string{svcARun1, svcBRun1}, gate.WaitingOn)
	assert.Equal(t, t0, gate.StartedAt)
	assert.Nil(t, gate.FinishedAt)
	assert.Equal(t, flowRevision, gate.FileRevision)

	runs := h.pipeline.runs()
	require.Len(t, runs, 2)
	assert.Equal(t, svcARun1, runs[0].RunID)
	assert.Equal(t, "tag-svc-a", runs[0].ImageTag)
	assert.Equal(t, 1, runs[0].Attempt)
	assert.NotEmpty(t, runs[0].SourceOverlayURI)
	assert.Equal(t, svcBRun1, runs[1].RunID)

	require.NoError(t, h.runner.Drive(ctx, id), "a verifying try waits for the poller")
	assert.Len(t, h.pipeline.runs(), 2)
}

func TestRunner_GatePassEmitsOneProposedEvent(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)

	require.NoError(t, h.poll(id))
	require.NoError(t, h.poll(id), "a poll of a remediation that ended does nothing")

	events := h.proposedEvents()
	require.Len(t, events, 1)
	var payload struct {
		ReleaseID string `json:"release_id"`
		Attempt   int    `json:"attempt"`
	}
	require.NoError(t, json.Unmarshal(events[0].Payload, &payload))
	assert.Equal(t, releaseID, payload.ReleaseID)
	assert.Equal(t, 1, payload.Attempt)

	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusProposed, p.Status)
	assert.Equal(t, map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusProposed}}, p.NodeOutcomes)
	act := t0
	assert.Equal(t, []proposal.Verification{
		{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhasePassed, ActivatedAt: &act},
	}, p.Verifications)
	assert.Equal(t, remediation.TryPassed, h.try(id, 1).Status)
	assert.Len(t, h.store.Tries(id), 1)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)

	rows := h.flowRows(id, 1)
	gate := rows["verification_run"][0]
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
	h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": "column x does not exist"})

	require.NoError(t, h.poll(id))

	first := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, first.Proposal.Status)
	assert.Equal(t, "svc-a: column x does not exist", first.VerifyError)
	assert.Equal(t, map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusFailed}}, first.Proposal.NodeOutcomes)
	act := t0
	assert.Equal(t, []proposal.Verification{{
		Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseFailed, ActivatedAt: &act,
		Error: "svc-a: column x does not exist",
	}}, first.Proposal.Verifications)
	gate := h.flowRows(id, 1)["verification_run"][0]
	assert.Equal(t, remediation.StepDone, gate.Status)
	assert.Equal(t, []remediation.Finding{{Code: "verification_failed", Message: "svc-a: column x does not exist"}}, gate.Findings)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Empty(t, h.proposedEvents())

	second := h.try(id, 2)
	assert.Equal(t, remediation.TryDrafting, second.Status)
	assert.Equal(t, 2, second.Attempt)
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, 2, h.remediation(id).CurrentTry)
	generating := h.attempt(2).Proposal
	assert.Equal(t, proposal.StatusGenerating, generating.Status)
	assert.Equal(t, id.String(), generating.RemediationID)

	require.NoError(t, h.drive(id), "the worker drives the next try to the gate")
	assert.Equal(t, remediation.TryVerifying, h.try(id, 2).Status)
	runs := h.pipeline.runs()
	require.Len(t, runs, 2)
	assert.Equal(t, svcARun2, runs[1].RunID)
	assert.Equal(t, 2, h.llm.calls(), "each try asks the model again")
}

func TestRunner_TriesSpentHandsOver(t *testing.T) {
	h := newHarness(t)
	h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": "still broken"})
	id := h.accept(trigger("compile", compileNode("svc-a")))

	for try := 1; try <= 3; try++ {
		require.NoError(t, h.drive(id))
		require.Equal(t, remediation.TryVerifying, h.try(id, try).Status, "try %d", try)
		require.NoError(t, h.poll(id))
	}

	for attempt := 1; attempt <= 3; attempt++ {
		p := h.attempt(attempt)
		assert.Equal(t, proposal.StatusFailed, p.Proposal.Status, "attempt %d", attempt)
		assert.Equal(t, "svc-a: still broken", p.VerifyError, "attempt %d", attempt)
	}
	escalated := h.attempt(4).Proposal
	assert.Equal(t, proposal.StatusEscalated, escalated.Status)
	assert.Equal(t, id.String(), escalated.RemediationID)
	assert.Equal(t, map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusEscalated}}, escalated.NodeOutcomes)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.store.Tries(id), 3)
	require.Len(t, h.flowRows(id, 3)["hand_over"], 1, "the hand-over is recorded on the last try")
	assert.Equal(t, 3, h.llm.calls())

	require.NoError(t, h.drive(id))
	assert.Len(t, h.proposals(), 4, "a remediation that ended writes nothing more")
}

func TestRunner_ProposalRowMovedUnderTheRunnerEndsQuietly(t *testing.T) {
	for _, phase := range []proposal.Phase{proposal.PhasePassed, proposal.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			h := newHarness(t)
			id := h.accept(trigger("compile", compileNode("svc-a")))
			require.NoError(t, h.runner.Drive(ctx, id))
			moved, err := h.store.ProposalRepository().MarkVerifyFailed(ctx, h.attempt(1).ID, "failed by hand", nil)
			require.NoError(t, err)
			require.True(t, moved)
			h.pipeline.answer(phase, map[string]string{"svc-a": "broken"})

			require.NoError(t, h.poll(id))

			assert.Empty(t, h.proposedEvents())
			row := h.attempt(1)
			assert.Equal(t, proposal.StatusFailed, row.Proposal.Status)
			assert.Equal(t, "failed by hand", row.VerifyError, "the row keeps what it already said")
			assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			assert.Len(t, h.store.Tries(id), 1, "no next try starts")
			assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
			assert.Len(t, h.proposals(), 1)
		})
	}
}

func TestRunner_TheGateIsPolledFromWhenItsStepStarted(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.status = func(string) (ports.VerificationStatus, error) {
		return ports.VerificationStatus{}, errors.New("release-controller unreachable")
	}
	// The try opened at t0 and its gate step started ten minutes later.
	submitted := t0.Add(10 * time.Minute)
	gate := h.flowRows(id, 1)["verification_run"][0]
	gate.StartedAt = submitted
	h.saveStep(gate)

	h.clock.set(submitted.Add(verificationTimeout - time.Second))
	require.NoError(t, h.poll(id))
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status, "the budget counts from the gate step, not the try")

	h.clock.set(submitted.Add(verificationTimeout))
	require.NoError(t, h.poll(id))
	first := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, first.Proposal.Status)
	assert.Equal(t, "verification timed out", first.VerifyError)
	assert.Equal(t, []proposal.Verification{
		{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseQueued},
	}, first.Proposal.Verifications, "a run that was never read keeps what was recorded for it")
	assert.Equal(t, remediation.TryDrafting, h.try(id, 2).Status)
}

func TestRunner_APollWithNoNewPhaseWritesNothing(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))

	require.NoError(t, h.poll(id))
	assert.Zero(t, h.phaseWrites.Load(), "the run is still queued, as recorded")

	h.pipeline.answer(proposal.PhaseRunning, nil)
	require.NoError(t, h.poll(id))
	assert.EqualValues(t, 1, h.phaseWrites.Load())
	act := t0
	assert.Equal(t, []proposal.Verification{
		{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseRunning, ActivatedAt: &act},
	}, h.attempt(1).Proposal.Verifications)

	require.NoError(t, h.poll(id))
	assert.EqualValues(t, 1, h.phaseWrites.Load(), "a phase that did not change is not written again")
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
}

func TestRunner_ACommitFailureAfterThePassEmitsOneProposedEvent(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	var armed atomic.Bool
	h.pipeline.onStatus = func(context.Context) {
		if armed.CompareAndSwap(false, true) {
			h.store.FailNextCommit()
		}
	}

	require.Error(t, h.poll(id), "the pass's commit fails")
	assert.Empty(t, h.proposedEvents())
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
	gate := h.flowRows(id, 1)["verification_run"][0]
	assert.Equal(t, remediation.StepWaiting, gate.Status, "the gate still waits on its runs")
	assert.Equal(t, 1, gate.Failures)

	require.NoError(t, h.poll(id))
	require.NoError(t, h.poll(id))

	assert.Len(t, h.proposedEvents(), 1)
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

func TestRunner_ACommitFailureAfterTheHandOverWritesOneEscalatedRow(t *testing.T) {
	h := newHarness(t, withTries(1))
	h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": "still broken"})
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

	var escalated []proposal.Proposal
	for _, p := range h.proposals() {
		if p.Proposal.Status == proposal.StatusEscalated {
			escalated = append(escalated, p.Proposal)
		}
	}
	require.Len(t, escalated, 1)
	assert.Equal(t, 2, escalated[0].Attempt)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

func TestRunner_ASubmitErrorLeavesTheTryCheckingAndRunsAgain(t *testing.T) {
	h := newHarness(t)
	h.pipeline.submitErr = errors.New("release-controller unavailable")
	id := h.accept(trigger("compile", compileNode("svc-a")))

	err := h.runner.Drive(ctx, id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "release-controller unavailable")
	tr := h.try(id, 1)
	assert.Equal(t, remediation.TryChecking, tr.Status)
	assert.Equal(t, t0.Add(10*time.Minute), tr.DeadlineAt)
	gate := h.flowRows(id, 1)["verification_run"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepFailed, gate[0].Status)
	assert.Equal(t, 1, gate[0].Failures)
	assert.Equal(t, proposal.StatusGenerating, h.attempt(1).Proposal.Status)

	later := t0.Add(time.Minute)
	h.clock.set(later)
	require.NoError(t, h.runner.Drive(ctx, id))

	tr = h.try(id, 1)
	assert.Equal(t, remediation.TryVerifying, tr.Status)
	assert.Equal(t, later.Add(verificationTimeout+time.Hour), tr.DeadlineAt)
	rows := h.flowRows(id, 1)
	for _, name := range []string{"edits_map_to_a_service", "no_path_edited_twice", "one_manifest_kind"} {
		assert.Equal(t, t0, rows[name][0].StartedAt, "%s ran once", name)
	}
	assert.Equal(t, remediation.StepWaiting, rows["verification_run"][0].Status)
	assert.Equal(t, later, rows["verification_run"][0].StartedAt)
	assert.Equal(t, 1, rows["verification_run"][0].Failures)
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
}

// The commit that leaves the try verifying can fail after the gate submitted
// its runs. The next drive submits them again under the same ids, so each
// edited service still has one verification run, and the gate waits on the
// runs it submitted.
func TestRunner_AFailedVerifyingCommitResubmitsTheSameRuns(t *testing.T) {
	h := newHarness(t)
	var armed atomic.Bool
	h.pipeline.onSubmit = func() {
		if armed.CompareAndSwap(false, true) {
			h.store.FailNextCommit()
		}
	}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))

	require.Error(t, h.runner.Drive(ctx, id), "the commit after the submit fails")
	submitted := h.pipeline.runs()
	require.Len(t, submitted, 2, "the gate submitted a run per service")
	assert.Equal(t, remediation.TryChecking, h.try(id, 1).Status)
	assert.Equal(t, proposal.StatusGenerating, h.attempt(1).Proposal.Status, "the attempt row records no run")
	gate := h.flowRows(id, 1)["verification_run"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepFailed, gate[0].Status)
	assert.Equal(t, 1, gate[0].Failures)

	require.NoError(t, h.drive(id))

	runs := h.pipeline.runs()
	require.Len(t, runs, 4)
	assert.Equal(t, submitted, runs[2:], "the second submit names the same runs over the same artifacts")
	assert.Equal(t, []string{svcARun1, svcBRun1}, []string{runs[2].RunID, runs[3].RunID})
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	gate = h.flowRows(id, 1)["verification_run"]
	require.Len(t, gate, 1)
	assert.Equal(t, remediation.StepWaiting, gate[0].Status)
	assert.Equal(t, []string{svcARun1, svcBRun1}, gate[0].WaitingOn)
	assert.Equal(t, []proposal.Verification{
		{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseQueued},
		{Service: "svc-b", Kind: "dbt", RunID: svcBRun1, Phase: proposal.PhaseQueued},
	}, h.attempt(1).Proposal.Verifications, "one run per service")

	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Len(t, h.pipeline.runs(), 4, "the poll submits nothing")
}

// A gate that refuses the try's drafts without starting a run fails the
// attempt with its first finding's message and ends the remediation. With no
// cross-cluster check in the flow, the gate is the one to find two edits of
// one file.
func TestRunner_AGateThatRefusesTheDraftsFailsTheAttempt(t *testing.T) {
	h := newHarness(t, withoutCrossClusterChecks())
	h.source.files["services/core/models/base.sql"] = "select 1"
	h.bundle.nodes = map[string]ports.CandidateSource{"model.core.base": {RawCode: "select 1", Runtime: ports.RuntimeDbt}}
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.ProposedSQL = "select 2"
		return r
	}
	ancestor := remediation.ChangedAncestor{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 1}
	consumer := compileNode("svc-a")
	consumer.Service = "svc-a"
	consumer.ChangedAncestors = []remediation.ChangedAncestor{ancestor}
	producer := compileNode("core")
	producer.Service = "core"
	producer.FilePath = "models/base.sql"
	id := h.accept(trigger("compile", consumer, producer))

	require.NoError(t, h.runner.Drive(ctx, id))

	const reason = "two edits change the same file: services/core/models/base.sql"
	p := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusFailed, p.Status)
	assert.Equal(t, reason, p.Rationale)
	assert.Equal(t, map[string]proposal.NodeOutcome{
		"svc-a": {Status: proposal.StatusFailed, Reason: reason},
		"core":  {Status: proposal.StatusFailed, Reason: reason},
	}, p.NodeOutcomes)
	assert.Empty(t, p.Verifications)
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.pipeline.runs(), "no run was started")
	assert.Empty(t, h.proposedEvents())

	rows := h.flowRows(id, 1)
	require.Len(t, rows, 1, "the gate is the only flow-level step")
	require.Len(t, rows["verification_run"], 1)
	gate := rows["verification_run"][0]
	assert.Equal(t, remediation.StepDone, gate.Status)
	assert.NotNil(t, gate.FinishedAt)
	assert.Empty(t, gate.WaitingOn)
	assert.Equal(t, []remediation.Finding{{Code: "path_edited_twice", Message: reason}}, gate.Findings)
}
