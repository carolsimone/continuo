package conformance

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// rowsWorld is a world whose store lists the given journal rows, in the order
// given.
type rowsWorld struct {
	World
	rows []remediation.StepRun
}

func (w rowsWorld) Steps(uuid.UUID) []remediation.StepRun { return w.rows }

// The journal of a cluster that played several rounds holds one row of its
// prompt step per round. A read picks the row of the round it asks for, so
// its answer is the same in whatever order the store lists the rows.
func TestExpectation_ReadsTheRowOfTheRoundItAsksForInEitherOrder(t *testing.T) {
	id := uuid.New()
	const prompt = "dbt-compile"
	round1 := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 1, Step: prompt, Status: remediation.StepDone, Failures: 1}
	round2 := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 2, Step: prompt, Status: remediation.StepFailed, Failures: 3}
	typ := workflow.Type{Agent: workflow.Agent{Prompt: workflow.StepRef{Name: prompt}}}

	for _, rows := range [][]remediation.StepRun{{round1, round2}, {round2, round1}} {
		e := (&scenarioRun{w: rowsWorld{rows: rows}, id: id}).expect(remediation.Trigger{})

		assert.True(t, e.reachedModel(remediation.Cluster{Try: 1, Cluster: 1}, typ), "round 1's prompt step ran")
		first, ok := e.stepRow(1, 1, 1, prompt)
		require.True(t, ok)
		assert.Equal(t, 1, first.Failures)
		second, ok := e.stepRow(1, 1, 2, prompt)
		require.True(t, ok)
		assert.Equal(t, 3, second.Failures)
		_, ok = e.stepRow(1, 1, 3, prompt)
		assert.False(t, ok)
		assert.NoError(t, e.err())
	}
}

// Two rows of one step in one round of one cluster cannot both be the row a
// read asks for, so the read records the problem instead of answering with
// whichever row the store listed last.
func TestExpectation_TwoRowsOfOneStepInOneRoundAreAProblem(t *testing.T) {
	id := uuid.New()
	row := remediation.StepRun{RemediationID: id, Try: 1, Cluster: 1, Round: 1, Step: "dbt-compile", Status: remediation.StepDone}
	e := (&scenarioRun{w: rowsWorld{rows: []remediation.StepRun{row, row}}, id: id}).expect(remediation.Trigger{})

	_, _ = e.stepRow(1, 1, 1, "dbt-compile")

	require.Error(t, e.err())
	assert.Contains(t, e.err().Error(), "try 1 cluster 1 round 1 has two rows of step dbt-compile")
}

// journalRow is a journal row of the named step, in the cluster, of the kind.
func journalRow(id uuid.UUID, cluster int, name, kind string) remediation.StepRun {
	return remediation.StepRun{RemediationID: id, Try: 1, Cluster: cluster, Round: 1, Step: name, Kind: kind, Status: remediation.StepDone}
}

// A remediation that hands every node over before any draft runs one step: the
// flow's give-up output, journalled once on its first try. A cluster's step, a
// cross-cluster check or the gate running beside it, or the output missing,
// is a problem.
func TestExpectation_OnlyTheHandOverMayRunWhenEveryNodeIsHandedOver(t *testing.T) {
	id := uuid.New()
	flow := workflow.Flow{OnGiveUp: workflow.Output{Action: "hand_over"}}
	handOver := journalRow(id, remediation.FlowCluster, "hand_over", string(step.KindOutput))
	for name, tc := range map[string]struct {
		rows []remediation.StepRun
		want string
	}{
		"the hand-over alone": {rows: []remediation.StepRun{handOver}},
		"a cluster's step": {
			rows: []remediation.StepRun{handOver, journalRow(id, 1, "dbt-compile", string(step.KindPrompt))},
			want: "ran 1 steps besides the output hand_over",
		},
		"a cross-cluster check": {
			rows: []remediation.StepRun{handOver, journalRow(id, remediation.FlowCluster, "one_manifest_kind", remediation.KindCheck)},
			want: "ran 1 steps besides the output hand_over",
		},
		"the gate": {
			rows: []remediation.StepRun{handOver, journalRow(id, remediation.FlowCluster, "release_check", remediation.KindCheck)},
			want: "ran 1 steps besides the output hand_over",
		},
		"an output of another name": {
			rows: []remediation.StepRun{handOver, journalRow(id, remediation.FlowCluster, "pull_request", string(step.KindOutput))},
			want: "ran 1 steps besides the output hand_over",
		},
		"no hand-over": {want: "the journal holds 0 rows of the output hand_over, want 1"},
		"the hand-over twice": {
			rows: []remediation.StepRun{handOver, handOver},
			want: "the journal holds 2 rows of the output hand_over, want 1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := (&scenarioRun{w: rowsWorld{rows: tc.rows}, id: id}).expect(remediation.Trigger{})

			e.onlyHandOverRan(flow)

			if tc.want == "" {
				assert.NoError(t, e.err())
				return
			}
			require.Error(t, e.err())
			assert.Contains(t, e.err().Error(), tc.want)
		})
	}
}

// handOverWorld is a world whose remediation, tries, clusters and outbox are
// the given ones.
type handOverWorld struct {
	World
	rem      remediation.Remediation
	tries    []remediation.Try
	clusters []remediation.Cluster
	outbox   []outbox.Entry
}

func (w handOverWorld) Tries(uuid.UUID) []remediation.Try        { return w.tries }
func (w handOverWorld) Clusters(uuid.UUID) []remediation.Cluster { return w.clusters }

func (w handOverWorld) Remediations() []remediation.Remediation {
	return []remediation.Remediation{w.rem}
}
func (w handOverWorld) Outbox() []outbox.Entry { return w.outbox }

// handedOverRow is the outbox row of a remediation.handed_over event, stored
// under tenant as the hand_over step stores it.
func handedOverRow(t *testing.T, tenant string, p events.RemediationHandedOver) outbox.Entry {
	t.Helper()
	body, err := json.Marshal(p)
	require.NoError(t, err)
	stored, err := outbox.WrapTenant(tenant, body)
	require.NoError(t, err)
	return outbox.Entry{ID: uuid.New(), StreamName: streams.RemediationHandedOverV1, Payload: stored}
}

// A hand-over names exactly the nodes the attempt handed over, each with its
// code, under the remediation's tenant, with the remediation's identity and
// the tries it made, and appears once. Any difference is a problem.
func TestExpectation_HandedOverReadsTheEventFromTheOutbox(t *testing.T) {
	id := uuid.New()
	rem := remediation.Remediation{ID: id, TenantID: "tenant-1", Flow: "rejected-release"}
	trigger := remediation.Trigger{ReleaseID: "rel-1", Round: 1}
	attempt := Attempt{Fix: fix.Fix{Attempt: 1, NodeOutcomes: map[string]fix.NodeOutcome{
		"a": {Status: fix.StatusSkipped, Reason: "why a", Code: model.HandoverReasonDeclined},
		"b": {Status: fix.StatusFailed, Reason: "why b", Code: model.HandoverReasonPreflightRoundsSpent},
	}}}
	event := events.RemediationHandedOver{
		RemediationID: id.String(), ReleaseID: "rel-1", RemediationRound: 1, Flow: "rejected-release", Tries: 1,
		Nodes: []events.HandedOverNode{
			{NodeID: "a", Reason: string(model.HandoverReasonDeclined), Message: "why a"},
			{NodeID: "b", Reason: string(model.HandoverReasonPreflightRoundsSpent), Message: "why b"},
		},
	}
	want := map[string]model.HandoverReason{"a": model.HandoverReasonDeclined, "b": model.HandoverReasonPreflightRoundsSpent}
	edit := func(f func(p *events.RemediationHandedOver)) events.RemediationHandedOver {
		p := event
		p.Nodes = append([]events.HandedOverNode(nil), event.Nodes...)
		f(&p)
		return p
	}

	for name, tc := range map[string]struct {
		rows []outbox.Entry
		want string
	}{
		"the event of the attempt": {rows: []outbox.Entry{handedOverRow(t, "tenant-1", event)}},
		"no event":                 {want: "0 remediation.handed_over events were enqueued, want 1"},
		"two events": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", event), handedOverRow(t, "tenant-1", event)},
			want: "2 remediation.handed_over events were enqueued, want 1",
		},
		"another tenant": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-2", event)},
			want: `stored under the tenant "tenant-2", want "tenant-1"`,
		},
		"another remediation": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) { p.RemediationID = "other" }))},
			want: `names the remediation "other"`,
		},
		"another try count": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) { p.Tries = 3 }))},
			want: "counts 3 tries, want 1",
		},
		"a node left out": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) { p.Nodes = p.Nodes[:1] }))},
			want: "names the nodes [a], want [a b]",
		},
		"a node the scenario does not hand over": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) {
				p.Nodes = append(p.Nodes, events.HandedOverNode{NodeID: "c", Reason: string(model.HandoverReasonDeclined)})
			}))},
			want: "names node c (declined), which the scenario does not hand over",
		},
		"a node with another reason": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) {
				p.Nodes[0].Reason = string(model.HandoverReasonNoType)
			}))},
			want: `gives node a the reason "no_type", want "declined"`,
		},
		"a node with another message": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) { p.Nodes[1].Message = "other" }))},
			want: `message for node b is "other", but its outcome says "why b"`,
		},
		"nodes out of order": {
			rows: []outbox.Entry{handedOverRow(t, "tenant-1", edit(func(p *events.RemediationHandedOver) {
				p.Nodes[0], p.Nodes[1] = p.Nodes[1], p.Nodes[0]
			}))},
			want: "names the nodes [b a], want [a b]",
		},
		"a row that is not a hand-over": {
			rows: []outbox.Entry{{StreamName: streams.RemediationHandedOverV1, Payload: []byte(`{"tenant_id":"tenant-1","payload":[1]}`)}},
			want: "does not hold a hand-over payload",
		},
		"a row of no tenant": {
			rows: []outbox.Entry{{StreamName: streams.RemediationHandedOverV1, Payload: []byte(`{}`)}},
			want: "does not carry its tenant",
		},
	} {
		t.Run(name, func(t *testing.T) {
			e := (&scenarioRun{w: handOverWorld{rem: rem, outbox: tc.rows}, id: id}).expect(trigger)

			e.handedOver(attempt, 1, want)

			if tc.want == "" {
				assert.NoError(t, e.err())
				return
			}
			require.Error(t, e.err())
			assert.Contains(t, e.err().Error(), tc.want)
		})
	}

	t.Run("a node whose outcome carries another code", func(t *testing.T) {
		e := (&scenarioRun{w: handOverWorld{rem: rem, outbox: []outbox.Entry{handedOverRow(t, "tenant-1", event)}}, id: id}).expect(trigger)

		e.handedOver(attempt, 1, map[string]model.HandoverReason{"a": model.HandoverReasonNoType, "b": model.HandoverReasonPreflightRoundsSpent})

		require.Error(t, e.err())
		assert.Contains(t, e.err().Error(), `node a ended attempt 1 with the code "declined" (why a), want "no_type"`)
	})
}

// A scenario whose every node ends in a proposed fix expects no hand-over event, and
// a row on another stream is not one.
func TestExpectation_NoHandedOverFailsOnAnEventOnlyOnItsStream(t *testing.T) {
	id := uuid.New()
	other := outbox.Entry{StreamName: streams.FixProposedV1, Payload: []byte(`{}`)}
	handedOver := handedOverRow(t, "tenant-1", events.RemediationHandedOver{RemediationID: id.String()})

	quiet := (&scenarioRun{w: handOverWorld{outbox: []outbox.Entry{other}}, id: id}).expect(remediation.Trigger{})
	quiet.noHandedOver()
	assert.NoError(t, quiet.err())

	loud := (&scenarioRun{w: handOverWorld{outbox: []outbox.Entry{other, handedOver}}, id: id}).expect(remediation.Trigger{})
	loud.noHandedOver()
	require.Error(t, loud.err())
	assert.Contains(t, loud.err().Error(), "1 remediation.handed_over events were enqueued, want none")
}

// A node handed over before any draft is not tried again: a second try, or a
// cluster of one, is a problem.
func TestExpectation_NoLaterTryFailsWhenASecondTryRan(t *testing.T) {
	id := uuid.New()
	first := remediation.Try{RemediationID: id, Try: 1}
	second := remediation.Try{RemediationID: id, Try: 2}
	cluster := func(try int) remediation.Cluster { return remediation.Cluster{RemediationID: id, Try: try, Cluster: 1} }

	one := (&scenarioRun{w: handOverWorld{tries: []remediation.Try{first}, clusters: []remediation.Cluster{cluster(1)}}, id: id}).expect(remediation.Trigger{})
	one.noLaterTry()
	assert.NoError(t, one.err())

	two := (&scenarioRun{
		w:  handOverWorld{tries: []remediation.Try{first, second}, clusters: []remediation.Cluster{cluster(1), cluster(2)}},
		id: id,
	}).expect(remediation.Trigger{})
	two.noLaterTry()
	require.Error(t, two.err())
	assert.Contains(t, two.err().Error(), "the remediation made 2 tries, want 1")
	assert.Contains(t, two.err().Error(), "cluster 1 of try 2 exists, but no try follows the first")
}
