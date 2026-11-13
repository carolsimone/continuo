package runner_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

func (h *harness) sweeper() *runner.Sweeper {
	return runner.NewSweeper(
		func() uow.UnitOfWork { return memory.NewUnitOfWork(h.store) },
		h.store.RemediationRepository, workerOwner, h.clock, discard)
}

// drafting accepts the trigger and opens its first try, leaving the
// remediation unclaimed with its attempt's in-flight row.
func (h *harness) drafting() uuid.UUID {
	h.t.Helper()
	h.acceptUnclaimed(trigger("compile", compileNode("svc-a")))
	id := h.store.Remediations()[0].ID
	h.reclaim(id)
	require.NoError(h.t, h.runner.NextTry(ctx, id))
	require.NoError(h.t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	require.Equal(h.t, remediation.TryDrafting, h.try(id, 1).Status)
	return id
}

// stepFailing records a step of the current try that has failed n times.
func (h *harness) stepFailing(id uuid.UUID, step string, status remediation.StepStatus, n int) {
	h.saveStep(remediation.StepRun{
		RemediationID: id, Try: 1, Cluster: 1, Step: step, Kind: "context", Round: 1,
		Status: status, Failures: n, LastError: "s3 is down", StartedAt: t0,
	})
}

func (h *harness) inFlightRow() proposal.Proposal {
	h.t.Helper()
	require.Len(h.t, h.proposals(), 1)
	return h.proposals()[0].Proposal
}

func TestSweeper_FiveFailuresIsASystemFailure(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)

	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Equal(t, `step "dbt_log" failed 5 times, the last time with: s3 is down`, rem.Failure)
	assert.Empty(t, rem.LeaseOwner, "the sweeper lets go of the lease")
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	p := h.inFlightRow()
	assert.Equal(t, proposal.StatusFailed, p.Status, "today's in-flight row is failed with the reason")
	assert.Equal(t, rem.Failure, p.Rationale)
	assert.Equal(t, proposal.StatusFailed, p.NodeOutcomes["svc-a"].Status)
}

func TestSweeper_CountsAStepThatWaitsOnItsRuns(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "verification_run", remediation.StepWaiting, 5)

	require.NoError(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
}

func TestSweeper_LeavesFewerFailuresAndFinishedStepsAlone(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 4)
	h.stepFailing(id, "failing_node_source", remediation.StepDone, 9)

	require.NoError(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "a done row keeps the failures it counted and does not count")
	assert.Equal(t, proposal.StatusGenerating, h.inFlightRow().Status)
}

func TestSweeper_ActsOnlyOnARemediationItCanClaim(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)
	claimed, err := h.store.RemediationRepository().Claim(ctx, id, "worker-b", h.clock.Now(), lease)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "a live lease means a worker is on it")
	assert.Equal(t, "worker-b", h.remediation(id).LeaseOwner)
	assert.Equal(t, proposal.StatusGenerating, h.inFlightRow().Status)

	// Once the lease has ended, the next sweep takes it.
	h.clock.set(t0.Add(lease + time.Second))
	require.NoError(t, h.sweeper().Sweep(ctx))
	assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
}

func TestSweeper_ADraftingTryPastItsDeadline(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	deadline := h.try(id, 1).DeadlineAt
	require.Equal(t, t0.Add(time.Hour), deadline, "a try may draft for an hour")

	h.clock.set(deadline)
	require.NoError(t, h.sweeper().Sweep(ctx))
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "at its deadline, not past it")

	h.clock.set(deadline.Add(time.Second))
	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "still drafting")
	assert.Contains(t, rem.Failure, "deadline")
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
	assert.Equal(t, proposal.StatusFailed, h.inFlightRow().Status)
	assert.Equal(t, rem.Failure, h.inFlightRow().Rationale)
}

func TestSweeper_ACheckingTryPastItsDeadline(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	repo := h.store.RemediationRepository()
	tr := h.try(id, 1)
	tr.Status = remediation.TryChecking
	tr.DeadlineAt = t0.Add(10 * time.Minute)
	require.NoError(t, repo.SaveTry(ctx, tr))

	h.clock.set(t0.Add(11 * time.Minute))
	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "still checking")
	assert.Equal(t, remediation.TryFailed, h.try(id, 1).Status)
}

func TestSweeper_NeverEndsAVerifyingTryByItsDeadline(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)

	h.clock.set(h.try(id, 1).DeadlineAt.Add(100 * time.Hour))
	require.NoError(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status, "the gate's timeout, not the sweeper, ends a verifying try")
	assert.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
}

func TestSweeper_EndsAVerifyingTryWhoseGateKeepsFailing(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	gate := h.flowRows(id, 1)["verification_run"][0]
	gate.Failures = 5
	gate.LastError = "rebuilt run ids differ"
	h.saveStep(gate)

	require.NoError(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	assert.Contains(t, h.remediation(id).Failure, "verification_run")
}

// The remediation ends and the attempt row fails together: a commit that does
// not happen leaves both as they were, for the next sweep.
func TestSweeper_TheFailureAndTheAttemptRowCommitTogether(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "dbt_log", remediation.StepFailed, 5)

	h.store.FailNextCommit()
	require.Error(t, h.sweeper().Sweep(ctx))

	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Equal(t, proposal.StatusGenerating, h.inFlightRow().Status)
	assert.Empty(t, h.remediation(id).LeaseOwner, "the lease is let go after a failed sweep too")

	require.NoError(t, h.sweeper().Sweep(ctx))
	assert.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	assert.Equal(t, proposal.StatusFailed, h.inFlightRow().Status)
}

// A done step keeps the failures it counted before it succeeded; when a try
// is swept for its deadline, that step is not the reason.
func TestSweeper_ADoneStepIsNotTheReason(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()
	h.stepFailing(id, "failing_node_source", remediation.StepDone, 9)
	h.clock.set(h.try(id, 1).DeadlineAt.Add(time.Second))

	require.NoError(t, h.sweeper().Sweep(ctx))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusSystemFailure, rem.Status)
	assert.Contains(t, rem.Failure, "still drafting")
	assert.NotContains(t, rem.Failure, "failing_node_source")
}
