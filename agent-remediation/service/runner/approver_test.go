package runner_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/approver"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/outputs"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
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

// warningLog puts a node_owner step whose warnings are kept in place of the
// registered one, and returns the buffer that keeps them.
func (h *harness) warningLog() *bytes.Buffer {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	h.replaceStep(approver.NewNodeOwner(h.bundle, contractFiles, logger))
	return &logs
}

// A bundle read that fails for another reason than a permanent miss never
// fails the commit that ends the remediation: the read is logged as a
// warning, the fix is proposed in that commit with each node's approver taken
// from its contract file, else none, and no failure is counted.
func TestRunner_ABundleReadErrorFallsBackAndThePassCommits(t *testing.T) {
	h := newHarness(t)
	calls := h.recordOutputs()
	logs := h.warningLog()
	h.bundle.owners = map[string]string{"svc-a": "finance"}
	id := h.accept(trigger("compile", compileNode("svc-a")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	h.bundle.err = errors.New("s3 answered HTTP 503")

	require.NoError(t, h.poll(id))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusDone, rem.Status)
	assert.Zero(t, rem.Failures)
	assert.Zero(t, h.flowRows(id, 1)["verification_run"][0].Failures, "the gate counted no failure")
	assert.Len(t, h.proposedEvents(), 1)
	assert.Equal(t, proposal.StatusProposed, h.attempt(1).Proposal.Status)
	assert.Equal(t, [][]proposal.Approver{{
		{NodeID: "svc-a", Owner: "", Source: proposal.ApproverSourceNone},
	}}, calls.pass, "a node with no contract file falls back to none")
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "s3 answered HTTP 503")
}

// A bundle read error at an end with no try never fails the hand-over either:
// the drive hands the node over with no owner and logs a warning.
func TestRunner_ABundleReadErrorFallsBackAndTheHandOverCommits(t *testing.T) {
	h := newHarness(t)
	logs := h.warningLog()
	h.bundle.err = errors.New("s3 answered HTTP 503")
	trig := trigger("compile", compileNode("svc-a"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.drive(id))

	rem := h.remediation(id)
	assert.Equal(t, remediation.StatusDone, rem.Status)
	assert.Zero(t, rem.Failures)
	require.Len(t, h.handedOverEvents(), 1)
	require.Len(t, h.handedOverEvent().Nodes, 1)
	assert.Empty(t, h.handedOverEvent().Nodes[0].Owner)
	assert.Contains(t, logs.String(), "s3 answered HTTP 503")
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

// The row of the attempt a remediation passed keeps the approvers of the nodes
// it settled: the fixed node, in the transaction that marks the row verified,
// and the node handed over beside it, which the hand-over event names with its
// owner.
func TestRunner_ThePassedRowStoresTheApproversOfItsFixedAndHandedOverNodes(t *testing.T) {
	h := newHarness(t)
	h.bundle.owners = map[string]string{"svc-a": "finance", "svc-b": "growth"}
	noFile := compileNode("svc-b")
	noFile.FilePath = ""
	id := h.accept(trigger("compile", compileNode("svc-a"), noFile))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.pipeline.answer(proposal.PhasePassed, nil)
	require.Empty(t, h.attempt(1).Proposal.Approvers, "a row in flight has none yet")

	require.NoError(t, h.poll(id))

	row := h.attempt(1).Proposal
	assert.Equal(t, proposal.StatusProposed, row.Status)
	assert.Equal(t, []proposal.Approver{
		{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
		{NodeID: "svc-b", Owner: "growth", Source: proposal.ApproverSourceBundle},
	}, row.Approvers)
	assert.Equal(t, []events.HandedOverNode{
		{NodeID: "svc-b", Reason: string(model.HandoverReasonDeclined), Message: noFileReason, Owner: "growth"},
	}, h.handedOverEvent().Nodes)
}

// The attempt row a remediation records at its end stores the approvers of the
// nodes it hands over, whichever way it ends, and the hand-over event names
// each node's owner.
func TestRunner_AHandedOverAttemptRowStoresItsNodesApprovers(t *testing.T) {
	noFile := func(node string) remediation.TriggerNode {
		n := compileNode(node)
		n.FilePath = ""
		return n
	}
	for name, tc := range map[string]struct {
		setup   func(h *harness)
		trigger func() remediation.Trigger
		attempt int
		status  proposal.Status
		code    model.HandoverReason
	}{
		"no repository": {
			trigger: func() remediation.Trigger {
				trig := trigger("compile", compileNode("svc-b"), compileNode("svc-a"))
				trig.Repo = ""
				return trig
			},
			attempt: 1, status: proposal.StatusFailed, code: model.HandoverReasonNoRepository,
		},
		"the tries are spent": {
			setup: func(h *harness) {
				for attempt := 1; attempt <= 3; attempt++ {
					h.legacyAttempt(attempt, proposal.StatusFailed)
				}
			},
			trigger: func() remediation.Trigger { return trigger("compile", compileNode("svc-b"), compileNode("svc-a")) },
			attempt: 4, status: proposal.StatusEscalated, code: model.HandoverReasonTriesSpent,
		},
		"a try with nothing to verify": {
			trigger: func() remediation.Trigger { return trigger("compile", noFile("svc-b"), noFile("svc-a")) },
			attempt: 1, status: proposal.StatusSkipped, code: model.HandoverReasonDeclined,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			h.bundle.owners = map[string]string{"svc-a": "finance", "svc-b": "growth"}
			id := h.accept(tc.trigger())

			require.NoError(t, h.drive(id))

			require.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			row := h.attempt(tc.attempt).Proposal
			assert.Equal(t, tc.status, row.Status)
			assert.Equal(t, []proposal.Approver{
				{NodeID: "svc-a", Owner: "finance", Source: proposal.ApproverSourceBundle},
				{NodeID: "svc-b", Owner: "growth", Source: proposal.ApproverSourceBundle},
			}, row.Approvers)
			nodes := h.handedOverEvent().Nodes
			require.Len(t, nodes, 2)
			assert.Equal(t, "finance", nodes[0].Owner)
			assert.Equal(t, "growth", nodes[1].Owner)
			for _, n := range nodes {
				assert.Equal(t, string(tc.code), n.Reason)
			}
		})
	}
}

// A node with no owner is recorded with none, so a reader can tell that nobody
// owns it from a node that was never looked at.
func TestRunner_ANodeWithNoOwnerIsRecordedWithNone(t *testing.T) {
	h := newHarness(t)
	trig := trigger("compile", compileNode("svc-a"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.drive(id))

	assert.Equal(t, []proposal.Approver{{NodeID: "svc-a", Source: proposal.ApproverSourceNone}}, h.attempt(1).Proposal.Approvers)
	assert.Equal(t, []events.HandedOverNode{{
		NodeID: "svc-a", Reason: string(model.HandoverReasonNoRepository), Message: "the trigger carried no repository for rel-1",
	}}, h.handedOverEvent().Nodes)
}
