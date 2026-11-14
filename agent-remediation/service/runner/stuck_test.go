package runner_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// The deadlines the runner gives a try: an hour to draft, and ten minutes to
// check once it is checking.
const (
	draftingTime = time.Hour
	checkingTime = 10 * time.Minute
)

// heartbeatPace is how far passTime moves the clock between two holds of the
// lease: less than a lease, as the worker's heartbeat is.
const heartbeatPace = 4 * time.Minute

// passTime moves the clock on by d while a drive runs, holding the runner's
// lease again at every heartbeatPace on the way, as the worker's heartbeat
// does, so the drive keeps its lease through the time that passes.
func (h *harness) passTime(id uuid.UUID, d time.Duration) {
	h.t.Helper()
	end := h.clock.Now().Add(d)
	for now := h.clock.Now(); now.Before(end); {
		now = now.Add(heartbeatPace)
		if now.After(end) {
			now = end
		}
		h.clock.set(now)
		assert.NoError(h.t, h.store.RemediationRepository().Hold(ctx, id, workerOwner, now, lease))
	}
}

// countRemediationFailures counts n failures on the remediation's own row, as
// n drives that could not load it or open its next try would.
func (h *harness) countRemediationFailures(id uuid.UUID, n int) {
	h.t.Helper()
	for range n {
		require.NoError(h.t, h.store.RemediationRepository().CountFailure(ctx, id, "database down"))
	}
}

// requireEndedAsStuck checks that the remediation ended as a system failure
// whose reason holds want, with its current try failed and the attempt's row
// failed for the same reason.
func (h *harness) requireEndedAsStuck(id uuid.UUID, want string) {
	h.t.Helper()
	rem := h.remediation(id)
	require.Equal(h.t, remediation.StatusSystemFailure, rem.Status, rem.Failure)
	assert.Contains(h.t, rem.Failure, want)
	assert.Equal(h.t, remediation.TryFailed, h.try(id, rem.CurrentTry).Status)
	p := h.attempt(h.try(id, rem.CurrentTry).Attempt)
	assert.Equal(h.t, proposal.StatusFailed, p.Proposal.Status, "the attempt row is failed with the reason")
	for _, o := range p.Proposal.NodeOutcomes {
		assert.Equal(h.t, proposal.StatusFailed, o.Status)
	}
}

// A drafting try whose deadline passes while a step runs is ended before the
// next step starts: the drive applies the stuck rules before every step, so a
// worker that keeps renewing its lease never drafts past the deadline.
func TestRunner_ADraftingTryPastItsDeadlineEndsBeforeItsNextStep(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	// The first context step reads the failing node's source, and the try's
	// deadline passes while it does.
	var once sync.Once
	h.source.onRead = func() { once.Do(func() { h.passTime(id, draftingTime+time.Second) }) }

	require.NoError(t, h.runner.Drive(ctx, id))

	h.requireEndedAsStuck(id, "try 1 was still drafting at its deadline")
	rows := h.stepRows(id, 1, 1)
	require.Len(t, rows["failing_node_source"], 1)
	assert.Equal(t, remediation.StepDone, rows["failing_node_source"][0].Status, "the step that was running commits")
	assert.NotContains(t, rows, "dbt_log", "no step starts after the deadline")
	assert.Zero(t, h.llm.calls(), "no model call after the deadline")
	assert.Empty(t, h.pipeline.runs())
}

// A checking try past its deadline is ended before its next cross-cluster
// check, and its drafts never reach the gate.
func TestRunner_ACheckingTryPastItsDeadlineDoesNotSubmitTheGate(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	// The checking deadline passes once the try is checking.
	var once sync.Once
	h.afterCommit = func() {
		for _, tr := range h.store.Tries(id) {
			if tr.Try == 1 && tr.Status == remediation.TryChecking {
				once.Do(func() { h.passTime(id, checkingTime+time.Second) })
			}
		}
	}

	require.NoError(t, h.runner.Drive(ctx, id))

	h.requireEndedAsStuck(id, "try 1 was still checking at its deadline")
	assert.Empty(t, h.flowRows(id, 1), "no cross-cluster check and no gate ran")
	assert.Empty(t, h.pipeline.runs(), "the gate was not submitted")
	assert.Equal(t, 1, h.llm.calls())
}

// A try at its deadline, and not past it, is driven on to the gate.
func TestRunner_ATryAtItsDeadlineIsDrivenOn(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	var once sync.Once
	h.source.onRead = func() { once.Do(func() { h.passTime(id, draftingTime) }) }

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	assert.Len(t, h.pipeline.runs(), 1)
}

// The other stuck rules hold before every step too: a step failed five times
// is not run a sixth time, and a remediation that failed five times in a row
// opens no try. Each ends the remediation at once.
func TestRunner_ADriveEndsAStuckRemediationBeforeItsNextStep(t *testing.T) {
	t.Run("a step failed five times", func(t *testing.T) {
		h := newHarness(t)
		id := h.drafting()
		h.stepFailing(id, "failing_node_source", remediation.StepFailed, 5)
		reads := 0
		h.source.onRead = func() { reads++ }

		require.NoError(t, h.drive(id))

		h.requireEndedAsStuck(id, `step "failing_node_source" failed 5 times, the last time with: s3 is down`)
		assert.Zero(t, reads, "the step does not run again")
		assert.Zero(t, h.llm.calls())
	})
	t.Run("the remediation failed five times", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", compileNode("svc-a")))
		h.countRemediationFailures(id, 5)

		require.NoError(t, h.drive(id))

		rem := h.remediation(id)
		assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
		assert.Contains(t, rem.Failure, "the remediation failed 5 times in a row before a step could run, the last time with: database down")
		assert.Empty(t, h.store.Tries(id), "no try opens")
		assert.Empty(t, h.proposals())
	})
}

// NextTry, which the gate poller calls after a failed gate, applies the stuck
// rules before it opens a try.
func TestRunner_NextTryEndsAStuckRemediationInsteadOfOpeningATry(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.countRemediationFailures(id, 5)

	require.NoError(t, h.runner.NextTry(ctx, id))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "the remediation failed 5 times in a row before a step could run")
	assert.Empty(t, h.store.Tries(id), "no try opens")
}

// A verifying try has no deadline that ends it, but the gate's poll applies
// the other stuck rules before it reads the gate: a gate row failed five times
// in a row, or a remediation that did, ends the remediation, with the attempt
// row failed and no status read.
func TestRunner_PollGateEndsAStuckRemediationBeforeItReadsTheGate(t *testing.T) {
	for name, tc := range map[string]struct {
		stick func(h *harness, id uuid.UUID)
		want  string
	}{
		"the gate row failed five times": {
			stick: func(h *harness, id uuid.UUID) {
				gate := h.flowRows(id, 1)["verification_run"][0]
				gate.Failures = 5
				gate.LastError = "rebuilt run ids differ"
				h.saveStep(gate)
			},
			want: `step "verification_run" failed 5 times, the last time with: rebuilt run ids differ`,
		},
		"the remediation failed five times": {
			stick: func(h *harness, id uuid.UUID) { h.countRemediationFailures(id, 5) },
			want:  "the remediation failed 5 times in a row before a step could run",
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			id := h.accept(trigger("compile", compileNode("svc-a")))
			require.NoError(t, h.runner.Drive(ctx, id))
			require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
			h.pipeline.answer(proposal.PhasePassed, nil)
			tc.stick(h, id)

			require.NoError(t, h.poll(id))

			h.requireEndedAsStuck(id, tc.want)
			assert.Zero(t, h.pipeline.statusReads(), "the gate is not read")
			assert.Empty(t, h.proposedEvents())
			assert.Equal(t, []proposal.Verification{{Service: "svc-a", Kind: "dbt", RunID: svcARun1, Phase: proposal.PhaseQueued}},
				h.attempt(1).Proposal.Verifications, "the attempt row keeps the runs it recorded")
		})
	}
}

// The stuck rules are read on the runner's own clock: a drafting try is ended
// by a drive whose clock is past the deadline even when nothing else changed.
func TestRunner_ADriveOfATryPastItsDeadlineRunsNoStep(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.clock.set(t0.Add(draftingTime + time.Second))

	require.NoError(t, h.drive(id))

	h.requireEndedAsStuck(id, "try 1 was still drafting at its deadline")
	assert.Empty(t, h.store.Steps(id))
	assert.Zero(t, h.llm.calls())
}
