package runner_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

// runWorker starts w and returns a function that stops it and waits for it.
func runWorker(t *testing.T, w *runner.Worker) (stop func()) {
	t.Helper()
	wctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Run(wctx)
	}()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		wg.Wait()
	}
	t.Cleanup(stop)
	return stop
}

func (h *harness) workerAs(owner string) *runner.Worker {
	return runner.NewWorker(h.runnerOwnedBy(h.files, owner), h.store.RemediationRepository, owner, h.clock, discard)
}

// acceptUnclaimed accepts trig and leaves the remediation for a worker to claim.
func (h *harness) acceptUnclaimed(trig remediation.Trigger) {
	h.t.Helper()
	require.NoError(h.t, h.runner.Accept(ctx, h.accepted(trig, "1-0")))
}

func (h *harness) waitForTry(id interface{ String() string }, status remediation.TryStatus) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		for _, r := range h.store.Remediations() {
			if r.ID.String() != id.String() || r.CurrentTry == 0 {
				continue
			}
			for _, tr := range h.store.Tries(r.ID) {
				if tr.Try == r.CurrentTry && tr.Status == status {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)
}

func TestWorker_DrivesAnAcceptedRemediationToTheGate(t *testing.T) {
	h := newHarness(t)
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID

	w := h.workerAs("worker-a")
	runWorker(t, w)
	w.Wake()

	h.waitForTry(id, remediation.TryVerifying)
	assert.Len(t, h.pipeline.runs(), 1)
	// The worker lets go of the lease once the drive ends, so the poller can
	// claim the remediation.
	require.Eventually(t, func() bool { return h.remediation(id).LeaseOwner == "" }, 5*time.Second, 10*time.Millisecond)
}

func TestWorker_TwoWorkersNeverDriveOneRemediation(t *testing.T) {
	h := newHarness(t)
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID

	first, second := h.workerAs("worker-a"), h.workerAs("worker-b")
	runWorker(t, first)
	runWorker(t, second)
	first.Wake()
	second.Wake()

	h.waitForTry(id, remediation.TryVerifying)
	// Give the worker that lost the claim time to take it twice, if it could.
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 1, h.llm.calls(), "one model call: one worker wrote each step")
	assert.Len(t, h.pipeline.runs(), 1, "one verification run")
	assert.Len(t, h.proposals(), 1, "one attempt row")
	assert.Len(t, h.store.Tries(id), 1)
	rows := h.stepRows(id, 1, 1)
	require.NotEmpty(t, rows)
	for step, perStep := range rows {
		assert.Len(t, perStep, 1, step)
	}
}

func TestWorker_WorksSeveralRemediationsOneAfterAnother(t *testing.T) {
	h := newHarness(t)
	for i, rel := range []string{"rel-a", "rel-b"} {
		trig := trigger("compile", compileNode("svc-a"))
		trig.ReleaseID = rel
		require.NoError(t, h.runner.Accept(ctx, h.accepted(trig, []string{"1-0", "2-0"}[i])))
	}
	w := h.workerAs("worker-a")
	runWorker(t, w)
	w.Wake()

	require.Eventually(t, func() bool {
		n := 0
		for _, r := range h.store.Remediations() {
			for _, tr := range h.store.Tries(r.ID) {
				if tr.Status == remediation.TryVerifying {
					n++
				}
			}
		}
		return n == 2
	}, 10*time.Second, 10*time.Millisecond)
}

// promptFailures is how many times the prompt step of the remediation's first
// cluster has failed.
func (h *harness) promptFailures(id uuid.UUID) int {
	for _, rows := range h.stepRows(id, 1, 1) {
		for _, row := range rows {
			if row.Kind == "prompt" {
				return row.Failures
			}
		}
	}
	return 0
}

// A step that keeps failing ends its remediation at the fifth failure, with no
// sweeper: the worker checks the remediation after each failed drive, still
// holding its lease.
func TestWorker_EndsARemediationWhoseStepKeepsFailing(t *testing.T) {
	h := newHarness(t)
	h.llm.err = errors.New("model is down")
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID

	w := h.workerAs("worker-a")
	runWorker(t, w)
	const failureLimit = 5
	for failures := 1; failures <= failureLimit; failures++ {
		w.Wake()
		require.Eventually(t, func() bool { return h.promptFailures(id) == failures }, 10*time.Second, 5*time.Millisecond, "failure %d", failures)
		if failures < failureLimit {
			// The worker owns the lease from its claim on, so the park is what
			// is waited for: the lease ends two minutes from now. The clock moves
			// only after that, or the park would be measured from the new time.
			parkedUntil := h.clock.Now().Add(2 * time.Minute)
			require.Eventually(t, func() bool {
				rem := h.remediation(id)
				return rem.LeaseUntil != nil && rem.LeaseUntil.Equal(parkedUntil)
			}, 5*time.Second, 5*time.Millisecond, "failure %d is parked", failures)
			assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "failure %d is not the last", failures)
			// The parked lease ends two minutes on.
			h.clock.set(h.clock.Now().Add(3 * time.Minute))
		}
	}

	require.Eventually(t, func() bool { return h.remediation(id).Status == remediation.StatusSystemFailure }, 10*time.Second, 5*time.Millisecond)
	rem := h.remediation(id)
	assert.Contains(t, rem.Failure, "failed 5 times")
	assert.Contains(t, rem.Failure, "model is down")
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	require.Len(t, h.proposals(), 1)
	assert.Equal(t, proposal.StatusFailed, h.proposals()[0].Proposal.Status, "the in-flight attempt row is failed with the reason")
	assert.Equal(t, 5, h.llm.calls(), "the model was called once for each failure")
}

// A remediation whose drive failed is parked: no worker can claim it for two
// minutes, and any worker can after.
func TestWorker_AParkedRemediationIsClaimableAfterTwoMinutes(t *testing.T) {
	h := newHarness(t)
	h.llm.err = errors.New("model is down")
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID

	w := h.workerAs("worker-a")
	runWorker(t, w)
	w.Wake()
	require.Eventually(t, func() bool { return h.promptFailures(id) == 1 }, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		rem := h.remediation(id)
		return rem.LeaseUntil != nil && rem.LeaseUntil.Equal(t0.Add(2*time.Minute))
	}, 5*time.Second, 5*time.Millisecond, "the worker parks the remediation for two minutes")

	repo := h.store.RemediationRepository()
	_, found, err := repo.ClaimNext(ctx, "worker-b", t0.Add(2*time.Minute-time.Second), lease)
	require.NoError(t, err)
	assert.False(t, found, "not claimable before the park ends")

	id2, found, err := repo.ClaimNext(ctx, "worker-b", t0.Add(2*time.Minute+time.Second), lease)
	require.NoError(t, err)
	require.True(t, found, "claimable once it ends")
	assert.Equal(t, id, id2)
}

// A step that blocks is stopped after its limit, and the stop is a failure of
// the step: the row records it, so five in a row end the remediation. The limit
// bounds one step, not the drive.
func TestRunner_AStepThatBlocksIsStoppedAndCounted(t *testing.T) {
	h := newHarness(t, withStepTimeout(50*time.Millisecond))
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.source.block.Store(true)

	start := time.Now()
	err := h.runner.Drive(ctx, id)

	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Contains(t, err.Error(), "ran longer than 50ms")
	var failed []remediation.StepRun
	for _, rows := range h.stepRows(id, 1, 1) {
		for _, row := range rows {
			if row.Status == remediation.StepFailed {
				failed = append(failed, row)
			}
		}
	}
	require.Len(t, failed, 1)
	assert.Equal(t, 1, failed[0].Failures)
	assert.Contains(t, failed[0].LastError, "ran longer than 50ms")
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, "worker-1", h.remediation(id).LeaseOwner, "the drive still holds the lease it was given")

	// The next drive, with the source answering, runs the step again and
	// carries the remediation on: the limit was on the step, not the drive.
	h.source.block.Store(false)
	require.NoError(t, h.runner.Drive(ctx, id))
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
}

// A drive that is ended from outside is not a step that timed out.
func TestRunner_ACancelledDriveIsNotAStepFailure(t *testing.T) {
	h := newHarness(t, withStepTimeout(time.Hour))
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.source.block.Store(true)
	dctx, cancel := context.WithCancel(ctx)
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()

	require.Error(t, h.runner.Drive(dctx, id))

	for _, rows := range h.stepRows(id, 1, 1) {
		for _, row := range rows {
			assert.Zero(t, row.Failures, row.Step)
		}
	}
}

// A legacy handler that stopped after it recorded the attempt as generating
// leaves that row behind. The remediation the runner starts for the same
// trigger finds the attempt number taken, and its own row takes over: no row of
// the release is left generating.
func TestRunner_ALegacyRowLeftGeneratingAtTheSwitchIsTakenOver(t *testing.T) {
	h := newHarness(t)
	u := memory.NewUnitOfWork(h.store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.ProposalRepo().InsertGenerating(ctx, proposal.Proposal{
		Source: "compile", ReleaseID: releaseID, RemediationRound: 1, Attempt: 1,
		Status: proposal.StatusGenerating, ResolvedNodeIDs: []string{"svc-a"},
		NodeOutcomes: map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusGenerating}},
		CreatedAt:    t0,
	}))
	require.NoError(t, u.Commit())
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.NoError(t, h.poll(id))

	require.Len(t, h.proposals(), 1, "one row for the attempt")
	p := h.proposals()[0].Proposal
	assert.Equal(t, proposal.StatusProposed, p.Status)
	assert.Equal(t, id.String(), p.RemediationID, "the runner's row took over the legacy row")
	for _, row := range h.proposals() {
		assert.NotEqual(t, proposal.StatusGenerating, row.Proposal.Status)
	}
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

// The same, when the runner's attempt ends without a gate: the legacy row is
// settled by the runner's row too.
func TestRunner_ALegacyRowLeftGeneratingIsSettledWhenTheAttemptFails(t *testing.T) {
	h := newHarness(t)
	u := memory.NewUnitOfWork(h.store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.ProposalRepo().InsertGenerating(ctx, proposal.Proposal{
		Source: "compile", ReleaseID: releaseID, RemediationRound: 1, Attempt: 1,
		Status: proposal.StatusGenerating, ResolvedNodeIDs: []string{"svc-a"},
		NodeOutcomes: map[string]proposal.NodeOutcome{"svc-a": {Status: proposal.StatusGenerating}},
		CreatedAt:    t0,
	}))
	require.NoError(t, u.Commit())
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		r := fixAnswer(req)
		r.Confidence = "low"
		return r
	}
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	require.Len(t, h.proposals(), 1)
	assert.Equal(t, proposal.StatusFailed, h.proposals()[0].Proposal.Status)
	assert.Equal(t, id.String(), h.proposals()[0].Proposal.RemediationID)
}

// The step that opens a try, and the hand-over that ends the remediation when
// its tries are spent, run under the step limit too.
func TestRunner_OpeningATryRunsUnderTheStepLimit(t *testing.T) {
	h := newHarness(t, withStepTimeout(50*time.Millisecond))
	id := h.accept(trigger("compile", compileNode("svc-a")))
	h.blockBeginUnderADeadline.Store(true)

	start := time.Now()
	err := h.runner.Drive(ctx, id)

	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Contains(t, err.Error(), "ran longer than 50ms")
	assert.Empty(t, h.store.Tries(id), "no try was written")
	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusRunning, rem.Status)
	assert.Equal(t, 1, rem.Failures, "the stop is counted on the remediation's own row")
	assert.Contains(t, rem.LastError, "ran longer than 50ms")
}
