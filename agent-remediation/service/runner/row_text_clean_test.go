package runner_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// A failed verification run records what its nodes printed, on the attempt's row as
// verify_error and on the run's own entry, and both are shown: neither holds a
// credential or an email address a node's error line echoed.
func TestRunner_GateFailRecordsTheVerificationErrorCleaned(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhaseFailed, map[string]string{"svc-a": dirtyDetail})

	require.NoError(t, h.poll(id))

	first := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, first.Proposal.Status)
	assert.Equal(t, "svc-a: "+cleanDetail, first.VerifyError)
	require.Len(t, first.Proposal.Verifications, 1)
	assert.Equal(t, "svc-a: "+cleanDetail, first.Proposal.Verifications[0].Error)
}

// failStepWithTheDirtyError records five failures of a step whose last error holds a
// credential and an email address, on the remediation's first try, and sweeps: the
// sweeper ends the remediation as a system failure, whose reason carries that error.
func (h *harness) failStepWithTheDirtyError(id uuid.UUID) {
	h.t.Helper()
	h.saveStep(remediation.StepRun{
		RemediationID: id, Try: 1, Cluster: 1, Step: "dbt_log", Kind: "context", Round: 1,
		Status: remediation.StepFailed, Failures: 5, LastError: dirtyDetail, StartedAt: t0,
	})
	require.NoError(h.t, h.sweeper().Sweep(ctx))
	require.Equal(h.t, remediation.StatusSystemFailure, h.remediation(id).Status)
}

// The reason a remediation stopped holds the last error of its step, and the attempt's
// row is failed with it, as an attempt still drafting and as one being verified: the
// row keeps what the error said, with no credential and no email address.
func TestSweeper_ASystemFailureFailsADraftingRowWithAReasonThatIsCleaned(t *testing.T) {
	h := newHarness(t)
	id := h.drafting()

	h.failStepWithTheDirtyError(id)

	row := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, row.Proposal.Status)
	assert.Contains(t, row.Proposal.Rationale, cleanDetail)
	requireClean(t, row.Proposal.Rationale, "the reason of the drafting attempt's row")
}

func TestRunner_ASystemFailureFailsAVerifyingRowWithAReasonThatIsCleaned(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.Equal(t, remediation.TryVerifying, h.try(id, 1).Status)
	h.pipeline.answer(proposal.PhasePassed, nil)
	gate := h.flowRows(id, 1)["verification_run"][0]
	gate.Failures, gate.LastError = 5, dirtyDetail
	h.saveStep(gate)

	require.NoError(t, h.poll(id))

	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	row := h.attempt(1)
	assert.Equal(t, proposal.StatusFailed, row.Proposal.Status)
	assert.Contains(t, row.VerifyError, cleanDetail)
	requireClean(t, row.VerifyError, "the reason of the verifying attempt's row")
}
