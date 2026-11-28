package runner_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// A runner runs the files it loaded, and the files can change while a try is
// open (a deploy, an install's own edit). A cluster runs context steps only
// while it is gathering: once past gathering, its evidence comes from the
// context rows it has, and its draft's prompt and pre-flight checks are read
// from the rows they wrote, whatever the loaded files now list.

// filesWith is a copy of files with change applied to the copy.
func filesWith(files flow.Set, change func(flow.Set)) flow.Set {
	out := flow.Set{Flows: map[string]flow.Flow{}, Types: map[string]flow.Type{}}
	for k, v := range files.Flows {
		out.Flows[k] = v
	}
	for k, v := range files.Types {
		out.Types[k] = v
	}
	change(out)
	return out
}

// flowContext sets the flow's context steps to names.
func flowContext(names ...string) func(flow.Set) {
	return func(s flow.Set) {
		flow := s.Flows["rejected-release"]
		flow.Context = refs(names...)
		s.Flows[flow.Name] = flow
	}
}

// compilePreflight sets the dbt-compile type's pre-flight checks to names.
func compilePreflight(names ...string) func(flow.Set) {
	return func(s flow.Set) {
		typ := s.Types["dbt-compile"]
		typ.Preflight = refs(names...)
		s.Types[typ.Name] = typ
	}
}

// stopAfter cancels the context it returns after the first commit of the
// runner that leaves stop true. The drive then stops at the top of its next
// step with nothing recorded, as a worker shut down by a deploy does.
func (h *harness) stopAfter(stop func(id uuid.UUID) bool) context.Context {
	dctx, cancel := context.WithCancel(ctx)
	h.t.Cleanup(cancel)
	h.afterCommit = func() {
		rems := h.store.Remediations()
		if len(rems) > 0 && stop(rems[0].ID) {
			h.afterCommit = nil
			cancel()
		}
	}
	return dctx
}

// assertNoFailure checks that no step and no drive of the remediation counted
// a failure, and that it did not end as a system failure.
func (h *harness) assertNoFailure(id uuid.UUID) {
	h.t.Helper()
	for _, s := range h.store.Steps(id) {
		assert.Zero(h.t, s.Failures, "step %s of cluster %d in draft %d counted a failure: %s", s.Step, s.Cluster, s.Draft, s.LastError)
	}
	rem := h.remediation(id)
	assert.Zero(h.t, rem.Failures, "the remediation counted a failure")
	assert.NotEqual(h.t, remediation.StatusSystemFailure, rem.Status)
}

// pollWith claims the remediation and polls its gate with r.
func (h *harness) pollWith(r *runner.Runner, id uuid.UUID) error {
	h.t.Helper()
	h.reclaim(id)
	snap, err := h.store.RemediationRepository().Get(ctx, id)
	require.NoError(h.t, err)
	return r.PollGate(ctx, snap)
}

func TestRunner_ACheckingTryFinishesUnderFilesThatAddAContextStep(t *testing.T) {
	h := newHarness(t)
	dctx := h.stopAfter(func(id uuid.UUID) bool {
		tries := h.store.Tries(id)
		return len(tries) == 1 && tries[0].Status == remediation.TryChecking
	})
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)
	require.Equal(t, remediation.TryChecking, h.try(id, 1).Status)
	require.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)

	h.reclaim(id)
	require.NoError(t, h.runnerOver(filesWith(h.files, flowContext("previous_tries"))).Drive(ctx, id))

	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status, "the try's drafts reach the gate")
	assert.Len(t, h.checks.requests(), 1)
	assert.Equal(t, fix.StatusVerifying, h.fixRow(1).Fix.Status)
	assert.Empty(t, h.stepRows(id, 1, 1)["previous_tries"], "a checked cluster runs no context step")
	h.assertNoFailure(id)
}

func TestRunner_AVerifyingTryReadsItsVerdictUnderFilesThatAddAContextStep(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.checks.answer(fix.PhasePassed, nil)

	require.NoError(t, h.pollWith(h.runnerOver(filesWith(h.files, flowContext("previous_tries"))), id))

	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.TryPassed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.proposedEvents(), 1)
	h.assertNoFailure(id)
}

func TestRunner_ACheckedClusterIsReadFromTheChecksThatRanUnderFilesThatAddACheck(t *testing.T) {
	h := newHarness(t)
	h.files = filesWith(h.files, compilePreflight("content_changed", "confidence_not_low"))
	h.runner = h.runnerOver(h.files)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.checks.answer(fix.PhasePassed, nil)

	added := filesWith(h.files, compilePreflight("target_file_was_shown", "content_changed", "confidence_not_low"))
	require.NoError(t, h.pollWith(h.runnerOver(added), id))

	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	h.assertNoFailure(id)
}

func TestRunner_ACheckedClusterOfATypeTheFilesNoLongerDefineIsStillVerified(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)

	removed := filesWith(h.files, func(s flow.Set) { delete(s.Types, "dbt-compile") })
	require.NoError(t, h.pollWith(h.runnerOver(removed), id))

	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	h.assertNoFailure(id)
}

// stopInDraft2 stops the drive once cluster 1 is proposing in draft 2.
func stopInDraft2(h *harness) context.Context {
	return h.stopAfter(func(id uuid.UUID) bool {
		for _, c := range h.store.Clusters(id) {
			if c.Cluster == 1 && c.Draft == 2 && c.Status == remediation.ClusterProposing {
				return true
			}
		}
		return false
	})
}

func TestRunner_TheSecondDraftRevisesUnderFilesThatAddAContextStep(t *testing.T) {
	h := newHarness(t, withDrafts(3))
	answersInTurn(h, lowConfidence, fixAnswer)
	dctx := stopInDraft2(h)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)
	require.Equal(t, 1, h.llm.calls())

	h.reclaim(id)
	require.NoError(t, h.runnerOver(filesWith(h.files, flowContext("previous_tries"))).Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 2, "draft 2 makes one model call")
	want := prompt.Revise(reqs[0], step.AnswerJSON(lowConfidence(reqs[0])), step.FindingLines([]remediation.Finding{lowConfidenceFinding}))
	assert.Equal(t, want, reqs[1], "draft 2 revises draft 1")
	assert.Empty(t, h.stepRows(id, 1, 1)["previous_tries"], "a cluster past gathering runs no context step")
	c := h.cluster(id, 1, 1)
	assert.Equal(t, remediation.ClusterChecked, c.Status)
	assert.Equal(t, 2, c.Draft)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.assertNoFailure(id)
}

func TestRunner_TheSecondDraftRevisesWithTheFindingsOfTheChecksThatRanUnderFilesThatAddACheck(t *testing.T) {
	h := newHarness(t, withDrafts(3))
	h.files = filesWith(h.files, compilePreflight("content_changed", "confidence_not_low"))
	h.runner = h.runnerOver(h.files)
	answersInTurn(h, lowConfidence, fixAnswer)
	dctx := stopInDraft2(h)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)

	h.reclaim(id)
	added := filesWith(h.files, compilePreflight("target_file_was_shown", "content_changed", "confidence_not_low"))
	require.NoError(t, h.runnerOver(added).Drive(ctx, id))

	reqs := h.llm.all()
	require.Len(t, reqs, 2)
	want := prompt.Revise(reqs[0], step.AnswerJSON(lowConfidence(reqs[0])), step.FindingLines([]remediation.Finding{lowConfidenceFinding}))
	assert.Equal(t, want, reqs[1], "draft 2 carries draft 1's findings")
	assert.Len(t, h.draftRows(id, 1, "target_file_was_shown"), 1, "draft 2 runs the checks the runner loaded")
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
	h.assertNoFailure(id)
}

func TestRunner_AClusterInPreflightRunsItsPassOnTheDraftsPromptRowUnderFilesThatRenameThePrompt(t *testing.T) {
	h := newHarness(t)
	dctx := h.stopAfter(func(id uuid.UUID) bool {
		for _, c := range h.store.Clusters(id) {
			if c.Cluster == 1 && c.Status == remediation.ClusterPreflight {
				return true
			}
		}
		return false
	})
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)
	require.Equal(t, 1, h.llm.calls())

	renamed := filesWith(h.files, func(s flow.Set) {
		typ := s.Types["dbt-compile"]
		typ.Agent.Prompt = flow.StepRef{Name: "dbt-parse"}
		s.Types[typ.Name] = typ
	})
	h.reclaim(id)
	require.NoError(t, h.runnerOver(renamed).Drive(ctx, id))

	assert.Equal(t, 1, h.llm.calls(), "the draft's prompt is not asked again")
	assert.Empty(t, h.stepRows(id, 1, 1)["dbt-parse"])
	assert.Equal(t, remediation.ClusterChecked, h.cluster(id, 1, 1).Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.assertNoFailure(id)
}

func TestRunner_AGatheringClusterRunsAContextStepTheFilesAdd(t *testing.T) {
	h := newHarness(t)
	dctx := h.stopAfter(func(id uuid.UUID) bool {
		_, ok := h.draftRows(id, 1, "failing_node_source")[1]
		return ok
	})
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)
	require.Equal(t, remediation.ClusterGathering, h.cluster(id, 1, 1).Status)

	h.reclaim(id)
	require.NoError(t, h.runnerOver(filesWith(h.files, flowContext("previous_tries"))).Drive(ctx, id))

	rows := h.draftRows(id, 1, "previous_tries")
	require.Len(t, rows, 1, "a gathering cluster runs the context step the loaded files list")
	assert.Equal(t, remediation.StepDone, rows[1].Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.assertNoFailure(id)
}

func TestRunner_AContextRowWhoseEvidenceTheBuildDoesNotKnowIsLeftOut(t *testing.T) {
	h := newHarness(t)
	dctx := h.stopAfter(func(id uuid.UUID) bool {
		tries := h.store.Tries(id)
		return len(tries) == 1 && tries[0].Status == remediation.TryChecking
	})
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.ErrorIs(t, h.runner.Drive(dctx, id), context.Canceled)
	done := h.draftRows(id, 1, "dbt_log")[1]
	retired := done
	retired.ID = 0
	retired.Step = "retired_step"
	retired.Output = []byte(`{"evidence":{"retired_key":"\"text\""}}`)
	h.saveStep(retired)

	h.reclaim(id)
	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.assertNoFailure(id)
}
