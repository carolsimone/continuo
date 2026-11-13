package runner_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
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
	for _, rows := range h.stepRows(id, 1, 1) {
		assert.Len(t, rows, 1)
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
