package runner_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

const (
	// rawCredential and rawEmail are the values a hand-over message must never
	// hold, wherever the text that holds them comes from.
	rawCredential = "hunter2"
	rawEmail      = "ops@acme.com"

	// dirtyDetail is the key error line of a node the classifier marked
	// unfixable, as a producer that did not clean it sends it, and cleanDetail
	// is that line cleaned.
	dirtyDetail = "dial postgres://etl:" + rawCredential + "@db/x refused, contact " + rawEmail
	cleanDetail = "dial postgres://etl:[redacted:secret]@db/x refused, contact [redacted:email]"
)

// requireHandOverTextClean expects that no credential and no email address is
// in any message the remediation handed a node over with: not on a coded
// outcome of any fix row of the release, in flight or ended, and not in a
// remediation.handed_over:v1 event.
func (h *harness) requireHandOverTextClean() {
	h.t.Helper()
	for _, p := range h.fixes() {
		for node, o := range p.Fix.NodeOutcomes {
			if o.Code == "" {
				continue
			}
			requireClean(h.t, o.Reason, fmt.Sprintf("the %s outcome of %s on the row of try %d (%s)", o.Code, node, p.Fix.Try, p.Fix.Status))
		}
	}
	for _, e := range h.handedOverEvents() {
		for _, n := range e.event.Nodes {
			requireClean(h.t, n.Message, fmt.Sprintf("the %s message of %s on the handed-over event", n.Reason, n.NodeID))
		}
	}
}

func requireClean(t *testing.T, text, what string) {
	t.Helper()
	assert.NotContains(t, text, rawCredential, what)
	assert.NotContains(t, text, rawEmail, what)
}

// A node handed over once the tries are spent is handed over with the last try's
// verify error, and a node the classifier marked unfixable with its key error
// line: neither the try's row nor the event holds a credential or an email
// address, each replaced by its fixed marker.
func TestRunner_HandOverMessagesAreCleaned(t *testing.T) {
	h := newHarness(t)
	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "connect failed, password=" + rawCredential})
	id := h.accept(trigger("compile",
		compileNode("svc-a"),
		unfixable(compileNode("svc-c"), model.HandoverReasonClassifierInfra, dirtyDetail)))
	for try := 1; try <= 3; try++ {
		require.NoError(t, h.drive(id))
		require.Equal(t, remediation.TryVerifying, h.try(id, try).Status, "try %d", try)
		require.NoError(t, h.poll(id))
	}

	spent := fix.NodeOutcome{
		Status: fix.StatusHandedOver,
		Reason: "tried 3 times; the last try failed: svc-a: connect failed, password=[redacted:secret]",
		Code:   model.HandoverReasonTriesSpent,
	}
	classifier := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: cleanDetail, Code: model.HandoverReasonClassifierInfra}
	handedOver := h.fixRow(fix.HandOverTry).Fix
	assert.Equal(t, fix.StatusHandedOver, handedOver.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": spent, "svc-c": classifier}, handedOver.NodeOutcomes)
	assert.Equal(t, handedOverOf(id, 1, 3,
		handedOverNode("svc-a", spent),
		handedOverNode("svc-c", classifier),
	), h.handedOverEvent())
	h.requireHandOverTextClean()
}

// The same holds at every end of a remediation, and in the rows written before
// it ends: a node's message is cleaned whichever way its outcome is recorded.
func TestRunner_EveryEndCleansTheMessagesItHandsOver(t *testing.T) {
	dirty := func() remediation.TriggerNode {
		return unfixable(compileNode("svc-c"), model.HandoverReasonClassifierInfra, dirtyDetail)
	}
	t.Run("every node is unfixable", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", dirty()))

		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
		assert.Equal(t, cleanDetail, h.fixRow(1).Fix.NodeOutcomes["svc-c"].Reason)
		assert.Equal(t, cleanDetail, h.handedOverEvent().Nodes[0].Message)
		h.requireHandOverTextClean()
	})
	t.Run("maintenance mode", func(t *testing.T) {
		h := newHarness(t, withMaintenance())
		id := h.accept(trigger("compile", dirty(), compileNode("svc-b")))

		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, cleanDetail, h.fixRow(1).Fix.NodeOutcomes["svc-c"].Reason)
		assert.Equal(t, cleanDetail, h.handedOverEvent().Nodes[0].Message)
		h.requireHandOverTextClean()
	})
	t.Run("no repository", func(t *testing.T) {
		h := newHarness(t)
		trig := trigger("compile", dirty(), compileNode("svc-b"))
		trig.Repo = ""
		id := h.accept(trig)

		require.NoError(t, h.runner.Drive(ctx, id))

		assert.Equal(t, cleanDetail, h.fixRow(1).Fix.NodeOutcomes["svc-c"].Reason)
		h.requireHandOverTextClean()
	})
	t.Run("a verified fix", func(t *testing.T) {
		h := newHarness(t)
		id := h.accept(trigger("compile", compileNode("svc-a"), dirty()))
		require.NoError(t, h.runner.Drive(ctx, id))
		h.requireHandOverTextClean() // the row that is in flight, then the verifying one
		h.checks.answer(fix.PhasePassed, nil)

		require.NoError(t, h.poll(id))

		assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
		assert.Equal(t, cleanDetail, h.fixRow(1).Fix.NodeOutcomes["svc-c"].Reason)
		assert.Equal(t, cleanDetail, h.handedOverEvent().Nodes[0].Message)
		h.requireHandOverTextClean()
	})
	t.Run("the in-flight row of a try that opens", func(t *testing.T) {
		h := newHarness(t)
		h.acceptUnclaimed(trigger("compile", compileNode("svc-a"), dirty()))
		id := h.store.Remediations()[0].ID
		h.reclaim(id)

		require.NoError(t, h.runner.NextTry(ctx, id))

		row := h.inFlightRow()
		assert.Equal(t, fix.StatusGenerating, row.Status)
		assert.Equal(t, cleanDetail, row.NodeOutcomes["svc-c"].Reason)
		h.requireHandOverTextClean()
	})
}

// A system failure of a verifying try reopens its fix row from the
// outcomes its clusters hold: the message of a node settled before the try is
// cleaned in the row, as it was when the row was first written.
func TestRunner_AReopenedFixRowHoldsCleanMessages(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile",
		compileNode("svc-a"),
		unfixable(compileNode("svc-c"), model.HandoverReasonClassifierInfra, dirtyDetail)))
	require.NoError(t, h.runner.Drive(ctx, id))
	require.NoError(t, h.store.RemediationRepository().ReleaseLease(ctx, id, workerOwner))
	gate := h.flowRows(id, 1)["release_check"][0]
	gate.Failures = 5
	gate.LastError = "rebuilt run ids differ"
	h.saveStep(gate)
	require.NoError(t, h.sweeper().Sweep(ctx))
	require.Equal(t, remediation.StatusSystemFailure, h.remediation(id).Status)
	dls := h.deadLetters()
	require.Len(t, dls, 1)

	h.redrive(dls[0], "2-0")

	reopened := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusVerifying, reopened.Status)
	assert.Equal(t, cleanDetail, reopened.NodeOutcomes["svc-c"].Reason)
	h.requireHandOverTextClean()
}
