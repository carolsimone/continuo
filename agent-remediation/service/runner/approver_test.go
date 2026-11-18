package runner_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/outputs"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// outputCalls records the approvers each output of a remediation was handed.
type outputCalls struct {
	mu     sync.Mutex
	pass   [][]proposal.Approver
	giveUp [][]proposal.Approver
}

// recordingPass is the pull_request output, recording what it is handed.
type recordingPass struct {
	step.PassOutput
	calls *outputCalls
}

func (p recordingPass) Pass(ctx context.Context, u uow.UnitOfWork, in step.OutputInput, v remediation.Verified) error {
	p.calls.mu.Lock()
	p.calls.pass = append(p.calls.pass, in.Approvers)
	p.calls.mu.Unlock()
	return p.PassOutput.Pass(ctx, u, in, v)
}

// recordingGiveUp is the hand_over output, recording what it is handed.
type recordingGiveUp struct {
	step.GiveUpOutput
	calls *outputCalls
}

func (g recordingGiveUp) GiveUp(ctx context.Context, u uow.UnitOfWork, in step.OutputInput) error {
	g.calls.mu.Lock()
	g.calls.giveUp = append(g.calls.giveUp, in.Approvers)
	g.calls.mu.Unlock()
	return g.GiveUpOutput.GiveUp(ctx, u, in)
}

// recordOutputs puts recording outputs in place of pull_request and hand_over.
func (h *harness) recordOutputs() *outputCalls {
	calls := &outputCalls{}
	h.replaceStep(recordingPass{PassOutput: outputs.NewPullRequest(h.clock), calls: calls})
	h.replaceStep(recordingGiveUp{GiveUpOutput: outputs.NewHandOver(), calls: calls})
	return calls
}

// The pass output is handed the approvers of the nodes the verified fix
// covers, and the give-up output those of the nodes the remediation leaves to
// a human; a node with no owner is named with none.
func TestRunner_HandsTheOutputsTheApproversOfTheirNodes(t *testing.T) {
	h := newHarness(t)
	calls := h.recordOutputs()
	h.bundle.owners = map[string]string{"svc-a": "finance"}
	noFile := compileNode("svc-b")
	noFile.FilePath = ""
	id := h.accept(trigger("compile", compileNode("svc-a"), noFile))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)

	require.NoError(t, h.poll(id))

	require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
	}}, calls.pass, "the pass output is handed the fixed node's approver")
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-b", Owner: "", Source: proposal.ApproverSourceNone},
	}}, calls.giveUp, "the give-up output is handed the handed-over node's approver")
	assert.Equal(t, 2, h.bundle.ownerReadCount(), "one bundle read for the fixed nodes and one for the handed-over ones")
}

// The approvers of every node a verified fix covers come from one read of the
// bundle.
func TestRunner_ThePassReadsTheBundleOnceForItsNodes(t *testing.T) {
	h := newHarness(t)
	calls := h.recordOutputs()
	h.bundle.owners = map[string]string{"svc-a": "finance", "svc-b": "e2e"}
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)

	require.NoError(t, h.poll(id))

	require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "svc-b", Owner: "e2e", Source: proposal.ApproverSourceBundle},
	}}, calls.pass)
	assert.Empty(t, calls.giveUp)
	assert.Equal(t, 1, h.bundle.ownerReadCount(), "one bundle read for both nodes")
}

// A remediation that ends with no try, here one whose trigger names no
// repository, hands the give-up output the approver of every node it hands
// over, sorted by node id.
func TestRunner_AHandOverWithNoTryHandsOverTheApproversOfItsNodes(t *testing.T) {
	h := newHarness(t)
	calls := h.recordOutputs()
	h.bundle.owners = map[string]string{"svc-a": "finance", "svc-b": "e2e"}
	trig := trigger("compile", compileNode("svc-b"), compileNode("svc-a"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, calls.pass)
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "svc-b", Owner: "e2e", Source: proposal.ApproverSourceBundle},
	}}, calls.giveUp)
}

// A bundle read that fails for another reason than a permanent miss fails the
// commit that ends the remediation: nothing is proposed and the gate keeps
// waiting, and the next poll, with the bundle back, ends it with the approvers.
func TestRunner_ABundleReadErrorFailsTheEndAndTheNextPollRetries(t *testing.T) {
	h := newHarness(t)
	calls := h.recordOutputs()
	h.bundle.owners = map[string]string{"svc-a": "finance"}
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	h.bundle.err = errors.New("s3 answered HTTP 503")

	err := h.poll(id)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 answered HTTP 503")
	assert.Empty(t, h.proposedEvents())
	assert.Equal(t, proposal.StatusVerifying, h.attempt(1).Proposal.Status)
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	gate := h.flowRows(id, 1)["verification_run"][0]
	assert.Equal(t, remediation.StepWaiting, gate.Status, "the gate still waits on its runs")
	assert.Equal(t, 1, gate.Failures)

	h.bundle.err = nil
	require.NoError(t, h.poll(id))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
	}}, calls.pass, "the failed commit stopped before the output ran")
}

// A bundle read error at an end with no try fails the commit too, and the
// next drive hands the nodes over.
func TestRunner_ABundleReadErrorFailsAHandOverAndTheNextDriveRetries(t *testing.T) {
	h := newHarness(t)
	h.bundle.err = errors.New("s3 answered HTTP 503")
	trig := trigger("compile", compileNode("svc-a"))
	trig.Repo = ""
	id := h.accept(trig)

	require.Error(t, h.drive(id))
	assert.Equal(t, remediation.StatusRunning, h.remediation(id).Status)
	assert.Empty(t, h.handedOverEvents())

	h.bundle.err = nil
	require.NoError(t, h.drive(id))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Len(t, h.handedOverEvents(), 1)
}

// A python node the code bundle names no owner for takes the team.name of the
// contract file its cluster gathered, whether the fix covers it or it is handed
// over.
func TestRunner_APythonNodeWithNoBundleOwnerTakesItsContractOwner(t *testing.T) {
	for name, tc := range map[string]struct {
		refuse     int
		wantPass   [][]proposal.Approver
		wantGiveUp [][]proposal.Approver
	}{
		"fixed": {
			refuse:   0,
			wantPass: [][]proposal.Approver{{{NodeID: contractNodeID, Owner: "sales-team", Source: proposal.ApproverSourceContractFile}}},
		},
		"handed over": {
			refuse:     100,
			wantGiveUp: [][]proposal.Approver{{{NodeID: contractNodeID, Owner: "sales-team", Source: proposal.ApproverSourceContractFile}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, withContractType(1, &refusingPackager{refuse: tc.refuse, refusal: toolRefusal}))
			h.archive.(*checkoutOf).files[contractYAML] = brokenContract + "team:\n  name: sales-team\n"
			calls := h.recordOutputs()
			h.llm.answer = contractAnswer
			h.pipeline.answer(proposal.PhasePassed, nil)
			id := h.accept(trigger("validation", contractNode()))

			require.NoError(t, h.runner.Drive(ctx, id))
			if tc.refuse == 0 {
				require.NoError(t, h.poll(id))
			}

			require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			assert.Equal(t, tc.wantPass, calls.pass)
			assert.Equal(t, tc.wantGiveUp, calls.giveUp)
		})
	}
}
