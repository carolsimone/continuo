package runner_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

func (h *harness) poller(wake func()) *runner.GatePoller {
	return runner.NewGatePoller(h.runner, h.store.RemediationRepository, workerOwner, time.Second, wake, h.clock, discard)
}

func TestGatePoller_PollsAVerifyingRemediationAndWakesTheWorker(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	h.pipeline.answer(proposal.PhasePassed, nil)

	var woken atomic.Int32
	require.NoError(t, h.poller(func() { woken.Add(1) }).Poll(ctx))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Empty(t, h.remediation(id).LeaseOwner, "the poller lets go of the lease")
	assert.Positive(t, woken.Load(), "a polled remediation may have opened a try for the worker")
}

func TestGatePoller_AFailedGateOpensTheNextTryForTheWorker(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": "boom"})

	require.NoError(t, h.poller(func() {}).Poll(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, remediation.TryDrafting, h.try(id, 2).Status)
	assert.Empty(t, h.remediation(id).LeaseOwner)
}

// A poll that takes longer than the lease keeps it: the poller holds the
// lease again every third of its duration while it polls, so the verdict it
// reads is committed and no other process can claim the remediation meanwhile.
func TestGatePoller_KeepsTheLeaseThroughAPollThatOutlastsIt(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	h.pipeline.answer(proposal.PhasePassed, nil)
	// The status read takes eight minutes of the clock, past the five-minute
	// lease the poller claimed at t0. The clock moves four minutes at a time,
	// and each time the poller holds the lease again from then.
	var once sync.Once
	h.pipeline.onStatus = func(context.Context) {
		once.Do(func() {
			for _, at := range []time.Duration{4 * time.Minute, 8 * time.Minute} {
				h.clock.set(t0.Add(at))
				until := t0.Add(at + lease)
				require.Eventually(t, func() bool {
					rem := h.remediation(id)
					return rem.LeaseUntil != nil && rem.LeaseUntil.Equal(until)
				}, 5*time.Second, time.Millisecond, "the lease is held again at %s", at)
			}
		})
	}
	p := h.poller(func() {})
	p.SetLeaseHeartbeat(time.Millisecond)

	require.NoError(t, p.Poll(ctx))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status, "the verdict read after the first lease ended is committed")
	assert.Equal(t, remediation.TryPassed, h.try(id, 1).Status)
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Empty(t, h.remediation(id).LeaseOwner, "the poller lets go of the lease")
}

// A lease another process took while the poller polls stops the poll at once:
// its context ends, so the status read returns, and nothing is written.
func TestGatePoller_StopsAPollWhoseLeaseAnotherProcessTook(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	repo := h.store.RemediationRepository()
	require.NoError(t, repo.ReleaseLease(ctx, id, workerOwner))
	h.pipeline.answer(proposal.PhasePassed, nil)
	gate := h.flowRows(id, 1)["verification_run"][0]
	stopped := make(chan bool, 1)
	var once sync.Once
	h.pipeline.onStatus = func(readCtx context.Context) {
		once.Do(func() {
			// The poller's lease ends and another process claims the
			// remediation while the status read runs.
			h.clock.set(t0.Add(lease + time.Second))
			claimed, err := repo.Claim(ctx, id, "somebody-else", h.clock.Now(), lease)
			require.NoError(t, err)
			require.True(t, claimed)
			select {
			case <-readCtx.Done():
				stopped <- true
			case <-time.After(5 * time.Second):
				stopped <- false
			}
		})
	}
	p := h.poller(func() {})
	p.SetLeaseHeartbeat(time.Millisecond)

	require.NoError(t, p.Poll(ctx))

	assert.True(t, <-stopped, "the status read is stopped once the lease is lost")
	assert.Equal(t, "somebody-else", h.remediation(id).LeaseOwner, "the other process keeps its lease")
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
	assert.Empty(t, h.proposedEvents())
	assert.Equal(t, gate, h.flowRows(id, 1)["verification_run"][0], "the gate's row is as it was")
}

func TestGatePoller_LeavesARemediationAnotherWorkerHolds(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	repo := h.store.RemediationRepository()
	require.NoError(t, repo.ReleaseLease(ctx, id, workerOwner))
	claimed, err := repo.Claim(ctx, id, "somebody-else", h.clock.Now(), lease)
	require.NoError(t, err)
	require.True(t, claimed)
	h.pipeline.answer(proposal.PhasePassed, nil)

	var woken atomic.Int32
	require.NoError(t, h.poller(func() { woken.Add(1) }).Poll(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "a live lease means another process is on it")
	assert.Equal(t, "somebody-else", h.remediation(id).LeaseOwner)
	assert.Zero(t, woken.Load())
}

func TestGatePoller_IgnoresARemediationThatIsNotVerifying(t *testing.T) {
	h := newHarness(t)
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	var woken atomic.Int32
	require.NoError(t, h.poller(func() { woken.Add(1) }).Poll(ctx))
	assert.Empty(t, h.store.Tries(h.store.Remediations()[0].ID))
	assert.Zero(t, woken.Load())
}

// A verifying try whose gate step cannot be found, or whose flow has no usable
// gate, is counted on the gate's row each poll, so the failures sweep ends it.
func TestRunner_APollThatCannotReachTheGateCountsOnTheGateRow(t *testing.T) {
	t.Run("a flow whose gate is not one long check", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", compileNode("svc-a")))
		require.NoError(t, h.runner.Drive(ctx, id))

		broken := h.files
		flow := broken.Flows["rejected-release"]
		flow.Gate = refs("verification_run", "verification_run")
		broken.Flows = map[string]workflow.Flow{flow.Name: flow}
		h.runner = h.runnerOver(broken)

		require.Error(t, h.poll(id))
		require.Error(t, h.poll(id))

		row := h.flowRows(id, 1)["verification_run"][0]
		assert.Equal(t, 2, row.Failures)
		assert.Contains(t, row.LastError, "gate")
		assert.Equal(t, remediation.StepWaiting, row.Status, "the row keeps waiting on its runs")
	})

	t.Run("a verifying try with no gate row", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", compileNode("svc-a")))
		// A try that is verifying and has no gate row.
		repo := h.store.RemediationRepository()
		require.NoError(t, repo.SaveTry(ctx, remediation.Try{
			RemediationID: id, Try: 1, Attempt: 1, Status: remediation.TryVerifying,
			DeadlineAt: t0.Add(time.Hour), CreatedAt: t0, UpdatedAt: t0,
		}))
		rem := h.remediation(id)
		rem.CurrentTry = 1
		require.NoError(t, repo.SaveRemediation(ctx, rem))

		require.Error(t, h.poll(id))
		row := h.flowRows(id, 1)["verification_run"][0]
		assert.Equal(t, 1, row.Failures)
		assert.Contains(t, row.LastError, "no verification_run row")
		assert.Equal(t, remediation.StepFailed, row.Status)

		// The row the first poll made counts every later error of the poll.
		require.Error(t, h.poll(id))
		assert.Equal(t, 2, h.flowRows(id, 1)["verification_run"][0].Failures)
	})
}

// A poll that got through resets the gate row's failures, so only consecutive
// failed polls add up: errors spread over a verification that waits for hours
// never end a healthy try.
func TestRunner_AGoodPollResetsTheGateRowsFailures(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	gate := h.flowRows(id, 1)["verification_run"][0]
	gate.Failures = 4
	gate.LastError = "database blip"
	h.saveStep(gate)

	require.NoError(t, h.poll(id), "the runs are still waiting")

	row := h.flowRows(id, 1)["verification_run"][0]
	assert.Zero(t, row.Failures)
	assert.Empty(t, row.LastError)
	assert.Equal(t, remediation.StepWaiting, row.Status)
	assert.Equal(t, gate.WaitingOn, row.WaitingOn, "the rest of the row is as it was")

	// Four more failures do not end it either.
	row.Failures = 4
	h.saveStep(row)
	require.NoError(t, h.sweeperAs("host:1/sweeper").Sweep(ctx))
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
}

// A poll that finds nothing new writes nothing when the row has no failures.
func TestRunner_APollOfACleanGateRowWritesNothing(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	before := h.flowRows(id, 1)["verification_run"][0]

	require.NoError(t, h.poll(id))

	assert.Equal(t, before, h.flowRows(id, 1)["verification_run"][0])
}
