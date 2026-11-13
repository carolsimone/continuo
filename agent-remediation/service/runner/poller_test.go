package runner_test

import (
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
