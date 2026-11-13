package runner_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

// remediationFailureLimit is how many failures end a remediation.
const remediationFailureLimit = 5

// withoutTheFlow is the harness's files with no flow, so no remediation the
// harness accepted can be loaded: each follows a flow the files do not define.
func (h *harness) withoutTheFlow() workflow.Set {
	return workflow.Set{Flows: map[string]workflow.Flow{}, Types: h.files.Types}
}

// A remediation whose flow the loaded files do not define cannot be loaded, so
// no step's row exists to count the failure. Each failed drive is counted on
// the remediation's own row, and the worker ends it at the fifth, as it ends
// one whose step keeps failing, instead of parking it every two minutes
// forever.
func TestWorker_EndsARemediationThatCannotBeLoaded(t *testing.T) {
	h := newHarness(t)
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID

	w := runner.NewWorker(h.runnerOwnedBy(h.withoutTheFlow(), "worker-a"), h.store.RemediationRepository, "worker-a", h.clock, discard)
	runWorker(t, w)
	for failures := 1; failures <= remediationFailureLimit; failures++ {
		w.Wake()
		require.Eventually(t, func() bool { return h.remediation(id).Failures == failures }, 10*time.Second, 5*time.Millisecond, "failure %d", failures)
		if failures < remediationFailureLimit {
			parkedUntil := h.clock.Now().Add(runner.ParkDuration)
			require.Eventually(t, func() bool {
				rem := h.remediation(id)
				return rem.LeaseUntil != nil && rem.LeaseUntil.Equal(parkedUntil)
			}, 5*time.Second, 5*time.Millisecond, "failure %d is parked", failures)
			assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "failure %d is not the last", failures)
			h.clock.set(h.clock.Now().Add(3 * time.Minute))
		}
	}

	require.Eventually(t, func() bool { return h.remediation(id).Status == remediation.StatusSystemFailure }, 10*time.Second, 5*time.Millisecond)
	rem := h.remediation(id)
	assert.Contains(t, rem.Failure, "the remediation failed 5 times before a step could run")
	assert.Contains(t, rem.Failure, `follows flow "rejected-release", which the loaded files do not define`)
	assert.Contains(t, rem.LastError, "which the loaded files do not define")
	assert.Empty(t, h.store.Tries(id), "no try ever opened")
	assert.Zero(t, h.llm.calls())
}

// A try that cannot open has no step's row either: the failure is counted on
// the remediation's own row, and the sweeper ends the remediation at the
// fifth, failing the release's in-flight row with the reason.
func TestSweeper_EndsARemediationWhoseTryCannotOpen(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	repo := h.store.RemediationRepository()

	for failures := 1; failures <= remediationFailureLimit; failures++ {
		h.store.FailNextCommit()
		err := h.drive(id)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit failed")
		rem := h.remediation(id)
		require.Equal(t, failures, rem.Failures)
		assert.Contains(t, rem.LastError, "commit failed")
		assert.Empty(t, h.store.Tries(id), "the try's commit failed")

		require.NoError(t, repo.ReleaseLease(ctx, id, workerOwner))
		require.NoError(t, h.sweeper().Sweep(ctx))
		if failures < remediationFailureLimit {
			assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "failure %d is not the last", failures)
		}
	}

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "the remediation failed 5 times before a step could run, the last time with: ")
	assert.Contains(t, rem.Failure, "commit failed")
	assert.Empty(t, rem.LeaseOwner, "the sweeper lets go of the lease")
	assert.Empty(t, h.proposals(), "no attempt row was ever written")
}

// The count is of failures before a try could open: a try that opens clears
// it, so earlier failures do not add up with later ones.
func TestRunner_ATryThatOpensClearsTheRemediationsFailures(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	for range remediationFailureLimit - 1 {
		h.store.FailNextCommit()
		require.Error(t, h.drive(id))
	}
	require.Equal(t, remediationFailureLimit-1, h.remediation(id).Failures)

	require.NoError(t, h.drive(id))

	rem := h.remediation(id)
	assert.Zero(t, rem.Failures)
	assert.Empty(t, rem.LastError)
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
}

// The gate's poller loads the remediation too: a verifying remediation whose
// flow the files no longer define has each poll counted on its own row, and
// the sweeper ends it at the fifth, failing the attempt's verifying row with
// the reason.
func TestSweeper_EndsAVerifyingRemediationThatCannotBeLoaded(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	poller := h.runnerOver(h.withoutTheFlow())

	for failures := 1; failures <= remediationFailureLimit; failures++ {
		h.reclaim(id)
		snap, err := h.store.RemediationRepository().Load(ctx, id)
		require.NoError(t, err)
		require.Error(t, poller.PollGate(ctx, snap))
		require.Equal(t, failures, h.remediation(id).Failures)
	}
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "the remediation failed 5 times before a step could run")
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	row := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, row.Proposal.Status)
	assert.Equal(t, rem.Failure, row.VerifyError, "the verifying attempt row is failed with the reason")
	assert.Equal(t, []string{svcARun1}, []string{row.Proposal.Verifications[0].RunID}, "it keeps the run it recorded")
}
