package runner_test

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// storedHandOver is one remediation.handed_over:v1 outbox entry: its id, the
// tenant it is stored under and the event it wraps.
type storedHandOver struct {
	id       uuid.UUID
	tenantID string
	event    events.RemediationHandedOver
}

// handedOverEvents is every remediation.handed_over:v1 entry of the outbox.
func (h *harness) handedOverEvents() []storedHandOver {
	h.t.Helper()
	var out []storedHandOver
	for _, e := range h.store.OutboxEntries() {
		if e.StreamName != streams.RemediationHandedOverV1 {
			continue
		}
		tenantID, body, err := outbox.UnwrapTenant(e.Payload)
		require.NoError(h.t, err)
		var ev events.RemediationHandedOver
		require.NoError(h.t, json.Unmarshal(body, &ev))
		out = append(out, storedHandOver{id: e.ID, tenantID: tenantID, event: ev})
	}
	return out
}

// handedOverEvent is the one remediation.handed_over:v1 event of the outbox.
func (h *harness) handedOverEvent() events.RemediationHandedOver {
	h.t.Helper()
	all := h.handedOverEvents()
	require.Len(h.t, all, 1, "one remediation.handed_over event")
	return all[0].event
}

// handedOverOf is the event the remediation id emits when it ends after tries
// tries, naming nodes.
func handedOverOf(id uuid.UUID, round, tries int, nodes ...events.HandedOverNode) events.RemediationHandedOver {
	return events.RemediationHandedOver{
		RemediationID: id.String(), ReleaseID: releaseID, Round: round,
		Flow: "rejected-release", Tries: tries, Nodes: nodes,
	}
}

// handedOverNode is node handed over with outcome o.
func handedOverNode(node string, o fix.NodeOutcome) events.HandedOverNode {
	return events.HandedOverNode{NodeID: node, Reason: string(o.Code), Message: o.Reason}
}

// stepNames is the step name of each journal row, sorted.
func stepNames(rows []remediation.StepRun) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Step)
	}
	sort.Strings(out)
	return out
}

// requireHandOverRow expects one done output row of hand_over on the try.
func (h *harness) requireHandOverRow(id uuid.UUID, try int) {
	h.t.Helper()
	rows := h.flowRows(id, try)["hand_over"]
	require.Len(h.t, rows, 1, "the hand-over is recorded on try %d", try)
	assert.Equal(h.t, "output", rows[0].Kind)
	assert.Equal(h.t, remediation.StepDone, rows[0].Status)
	assert.NotNil(h.t, rows[0].FinishedAt)
}

// A remediation whose every node is fixed hands nothing over: no event and no
// output row of hand_over.
func TestHandOver_AFullyFixedRemediationEmitsNothing(t *testing.T) {
	h := newHarness(t)
	id := h.accept(trigger("compile", compileNode("svc-a"), compileNode("svc-b")))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)

	require.NoError(t, h.poll(id))

	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Equal(t, fix.StatusProposed, h.fixRow(1).Fix.Status)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Empty(t, h.handedOverEvents())
	assert.Empty(t, h.flowRows(id, 1)["hand_over"], "no output row is saved for an output that did not run")
}

// A passed remediation hands over the nodes it did not fix: only the declined
// node is named, beside the proposed fix of the other.
func TestRunner_APassedRemediationHandsOverOnlyItsDeclinedNodes(t *testing.T) {
	h := newHarness(t)
	noFile := compileNode("svc-b")
	noFile.FilePath = ""
	id := h.accept(trigger("compile", compileNode("svc-a"), noFile))
	require.NoError(t, h.runner.Drive(ctx, id))
	h.checks.answer(fix.PhasePassed, nil)

	require.NoError(t, h.poll(id))

	declined := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: noFileReason, Code: model.HandoverReasonDeclined}
	p := h.fixRow(1).Fix
	assert.Equal(t, fix.StatusProposed, p.Status)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": {Status: fix.StatusProposed},
		"svc-b": declined,
	}, p.NodeOutcomes)
	assert.Len(t, h.proposedEvents(), 1)
	assert.Equal(t, handedOverOf(id, 1, 1, handedOverNode("svc-b", declined)), h.handedOverEvent())
	h.requireHandOverRow(id, 1)
	assert.Equal(t, remediation.TryPassed, h.try(id, 1).Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)

	require.NoError(t, h.poll(id))
	assert.Len(t, h.handedOverEvents(), 1, "a remediation that ended emits nothing more")
}

// A trigger whose every node the classifier marked unfixable emits one event,
// under the trigger's tenant, naming every node with its classifier code.
func TestRunner_AnAllUnfixableRemediationHandsEveryNodeOver(t *testing.T) {
	const (
		infraDetail = "the warehouse connection dropped; release again once it is back"
		parseDetail = "the parser refuses a construct no edit of the source avoids"
	)
	h := newHarness(t)
	orders := remediation.TriggerNode{NodeID: "model.sales.orders", ErrorSignature: "sig-o", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/orders.sql"}
	items := remediation.TriggerNode{NodeID: "model.sales.items", ErrorSignature: "sig-i", NodeType: "dbt-model", Project: "svc-a", FilePath: "models/items.sql"}
	trig := trigger("parse",
		unfixable(orders, model.HandoverReasonClassifierInfra, infraDetail),
		unfixable(items, model.HandoverReasonClassifierUnhealableParse, parseDetail))
	trig.TenantID = "acme"
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	all := h.handedOverEvents()
	require.Len(t, all, 1)
	assert.Equal(t, "acme", all[0].tenantID, "the event is stored under the remediation's tenant")
	assert.Equal(t, uuid.NewSHA1(uuid.NameSpaceOID, []byte("handed_over|"+id.String())), all[0].id)
	assert.Equal(t, handedOverOf(id, 1, 1,
		events.HandedOverNode{NodeID: items.NodeID, Reason: string(model.HandoverReasonClassifierUnhealableParse), Message: parseDetail},
		events.HandedOverNode{NodeID: orders.NodeID, Reason: string(model.HandoverReasonClassifierInfra), Message: infraDetail},
	), all[0].event)
	h.requireHandOverRow(id, 1)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Zero(t, h.llm.calls())
}

// Maintenance mode is not a hand-over: a remediation that came due in it with
// no unfixable node emits nothing.
func TestRunner_AMaintenanceRemediationWithNoUnfixableNodeEmitsNothing(t *testing.T) {
	h := newHarness(t, withMaintenance())
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, fix.StatusSkipped, h.fixRow(1).Fix.Status)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.store.OutboxEntries())
}

// A remediation that came due in maintenance mode hands over the nodes the
// classifier marked unfixable, and only those.
func TestRunner_AMaintenanceRemediationHandsOverItsUnfixableNodes(t *testing.T) {
	const detail = "the warehouse connection dropped; release again once it is back"
	h := newHarness(t, withMaintenance())
	id := h.accept(trigger("compile", unfixable(compileNode("svc-a"), model.HandoverReasonClassifierInfra, detail), compileNode("svc-b")))

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, fix.StatusSkipped, h.fixRow(1).Fix.Status)
	assert.Equal(t, handedOverOf(id, 1, 0,
		events.HandedOverNode{NodeID: "svc-a", Reason: string(model.HandoverReasonClassifierInfra), Message: detail},
	), h.handedOverEvent())
	assert.Empty(t, h.store.Steps(id), "a remediation with no try records no output row")
}

// A trigger with no repository hands every node over: the unfixable one with
// its classifier code, every other with no_repository.
func TestRunner_NoRepositoryHandsEveryNodeOver(t *testing.T) {
	const detail = "the warehouse connection dropped; release again once it is back"
	h := newHarness(t)
	trig := trigger("compile", unfixable(compileNode("svc-a"), model.HandoverReasonClassifierInfra, detail), compileNode("svc-b"))
	trig.Repo = ""
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	assert.Equal(t, handedOverOf(id, 1, 0,
		events.HandedOverNode{NodeID: "svc-a", Reason: string(model.HandoverReasonClassifierInfra), Message: detail},
		events.HandedOverNode{NodeID: "svc-b", Reason: string(model.HandoverReasonNoRepository), Message: "the trigger carried no repository for rel-1"},
	), h.handedOverEvent())
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
}

// Spent tries whose last try failed at the gate's runs: the hand-over row
// hands over every node the tries could not fix with tries_spent and the gate's
// verify error, while a node handed over with a settled code or because its
// drafts were spent keeps its outcome, and the event names them all.
func TestRunner_TriesSpent(t *testing.T) {
	const detail = "the warehouse connection dropped; release again once it is back"
	h := newHarness(t)
	h.llm.answer = func(req prompt.ProposeRequest) ports.ProposeResult {
		if strings.Contains(req.User, "services/svc-b/") {
			return lowConfidence(req)
		}
		return fixAnswer(req)
	}
	h.checks.answer(fix.PhaseFailed, map[string]string{"svc-a": "still broken"})
	noFile := compileNode("core")
	noFile.FilePath = ""
	id := h.accept(trigger("compile",
		compileNode("svc-a"), compileNode("svc-b"), noFile,
		unfixable(compileNode("svc-c"), model.HandoverReasonClassifierInfra, detail)))

	for try := 1; try <= 3; try++ {
		require.NoError(t, h.drive(id))
		require.Equal(t, remediation.TryVerifying, h.try(id, try).Status, "try %d", try)
		require.NoError(t, h.poll(id))
	}

	spent := fix.NodeOutcome{
		Status: fix.StatusHandedOver,
		Reason: "tried 3 times; the last try failed: svc-a: still broken",
		Code:   model.HandoverReasonTriesSpent,
	}
	draftsSpent := fix.NodeOutcome{Status: fix.StatusFailed, Reason: lowConfidenceReason, Code: model.HandoverReasonDraftsSpent}
	declined := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: noFileReason, Code: model.HandoverReasonDeclined}
	classifier := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: detail, Code: model.HandoverReasonClassifierInfra}
	handedOver := h.fixRow(fix.HandOverTry).Fix
	assert.Equal(t, fix.StatusHandedOver, handedOver.Status)
	assert.Equal(t, id.String(), handedOver.RemediationID)
	assert.Equal(t, map[string]fix.NodeOutcome{
		"svc-a": spent,
		"svc-b": draftsSpent,
		"core":  declined,
		"svc-c": classifier,
	}, handedOver.NodeOutcomes)
	assert.Equal(t, handedOverOf(id, 1, 3,
		handedOverNode("core", declined),
		handedOverNode("svc-a", spent),
		handedOverNode("svc-b", draftsSpent),
		handedOverNode("svc-c", classifier),
	), h.handedOverEvent())
	h.requireHandOverRow(id, 3)
	assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
	assert.Empty(t, h.proposedEvents())
}

// Spent tries whose last try a check refused, a cross-cluster check or the gate
// at submit: each drafted member is handed over with unverifiable_edits and the
// refusal as why the last try failed; a node handed over before any draft keeps
// its outcome.
func TestRunner_UnverifiableEdits(t *testing.T) {
	for name, opts := range map[string][]option{
		"a cross-cluster check": {withTries(2)},
		"the gate at submit":    {withTries(2), withoutCrossClusterChecks()},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, opts...)
			h.twoEditsOfOneFile()
			trig := crossClusterTrigger()
			test := compileNode("svc-b")
			test.NodeType = "dbt-test"
			trig.Nodes = append(trig.Nodes, test)
			id := h.accept(trig)

			for range 3 {
				require.NoError(t, h.drive(id))
			}

			unverifiable := fix.NodeOutcome{
				Status: fix.StatusHandedOver,
				Reason: "tried 2 times; the last try failed: " + twoEditsReason,
				Code:   model.HandoverReasonUnverifiableEdits,
			}
			noType := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: dbtTestReason, Code: model.HandoverReasonNoType}
			require.Len(t, h.store.Tries(id), 2, "both tries ran")
			handedOver := h.fixRow(fix.HandOverTry).Fix
			assert.Equal(t, fix.StatusHandedOver, handedOver.Status)
			assert.Equal(t, map[string]fix.NodeOutcome{
				"svc-a": unverifiable,
				"core":  unverifiable,
				"svc-b": noType,
			}, handedOver.NodeOutcomes)
			assert.Equal(t, handedOverOf(id, 1, 2,
				handedOverNode("core", unverifiable),
				handedOverNode("svc-a", unverifiable),
				handedOverNode("svc-b", noType),
			), h.handedOverEvent())
			h.requireHandOverRow(id, 2)
			assert.Equal(t, remediation.StatusDone, h.remediation(id).Status)
			assert.Empty(t, h.checks.requests())
		})
	}
}

// The round's hand-over row is try 0 of the trigger's round, whatever the
// earlier rounds hold. With no try of its own, the remediation's nodes are
// handed over with how many times the round was tried alone.
func TestRunner_TheHandOverRowIsTryZeroOfItsRound(t *testing.T) {
	h := newHarness(t)
	h.tryWithNoRemediationInRound(1, 1, fix.StatusFailed)
	h.tryWithNoRemediationInRound(1, fix.HandOverTry, fix.StatusHandedOver)
	for try := 1; try <= 3; try++ {
		h.tryWithNoRemediationInRound(2, try, fix.StatusFailed)
	}
	trig := trigger("compile", compileNode("svc-a"))
	trig.Round = 2
	id := h.accept(trig)

	require.NoError(t, h.runner.Drive(ctx, id))

	spent := fix.NodeOutcome{Status: fix.StatusHandedOver, Reason: "tried 3 times", Code: model.HandoverReasonTriesSpent}
	handedOver := h.fixRowIn(2, fix.HandOverTry).Fix
	assert.Equal(t, fix.StatusHandedOver, handedOver.Status)
	assert.Equal(t, 2, handedOver.Round)
	assert.Equal(t, map[string]fix.NodeOutcome{"svc-a": spent}, handedOver.NodeOutcomes)
	assert.Equal(t, handedOverOf(id, 2, 0, handedOverNode("svc-a", spent)), h.handedOverEvent())
	assert.Equal(t, fix.StatusHandedOver, h.fixRow(fix.HandOverTry).Fix.Status, "round 1's hand-over row is left as it was")
	assert.Empty(t, h.store.Tries(id))
	assert.Empty(t, h.store.Steps(id), "a remediation with no try records no output row")
}

// A try in flight is not counted, so the hand-over row, which is no try,
// leaves the in-flight row as it is.
func TestRunner_TheHandOverRowLeavesATryInFlightAlone(t *testing.T) {
	h := newHarness(t)
	for try := 1; try <= 3; try++ {
		h.tryWithNoRemediation(try, fix.StatusFailed)
	}
	h.tryWithNoRemediation(4, fix.StatusGenerating)
	id := h.accept(trigger("compile", compileNode("svc-a")))

	require.NoError(t, h.runner.Drive(ctx, id))

	require.Len(t, h.fixes(), 5, "the hand-over row is a row of its own")
	handedOver := h.fixRow(fix.HandOverTry).Fix
	assert.Equal(t, fix.StatusHandedOver, handedOver.Status)
	assert.Equal(t, id.String(), handedOver.RemediationID)
	assert.Equal(t, fix.StatusGenerating, h.fixRow(4).Fix.Status)
}
