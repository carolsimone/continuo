package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/typology"
)

func TestRationaleFactsFor_ProjectsTriggerClusterAndOutcome(t *testing.T) {
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.report", Service: "finance", ErrorExcerpt: `column "amount" does not exist`,
			ChangedAncestors: []ChangedAncestor{{NodeID: "s.far", Service: "core", Depth: 2}, {NodeID: "s.orders", Service: "core", Depth: 1}}},
		{NodeID: "s.spend", Service: "marketing", ErrorExcerpt: `column "amount" does not exist`,
			ChangedAncestors: []ChangedAncestor{{NodeID: "s.orders", Service: "core", Depth: 1}}},
	}
	c := typology.Cluster{TargetNodeID: "s.orders", Members: []string{"s.report", "s.spend"}, Kind: typology.KindCrossServiceUpstream}
	out := clusterOutcome{edits: []proposal.FileEdit{{Path: "services/core/models/orders.sql"}}, rationale: "kept both"}

	got := rationaleFactsFor(tr, c, out)

	assert.Equal(t, proposal.RationaleFacts{
		Members: []proposal.RationaleMember{
			{NodeID: "s.report", Service: "finance", ErrorLine: `column "amount" does not exist`},
			{NodeID: "s.spend", Service: "marketing", ErrorLine: `column "amount" does not exist`},
		},
		Upstream:      []proposal.RationaleUpstream{{NodeID: "s.orders", Service: "core", Depth: 1}, {NodeID: "s.far", Service: "core", Depth: 2}},
		UpstreamKnown: true,
		Edits:         []string{"services/core/models/orders.sql"},
		TargetNodeID:  "s.orders",
		TargetService: "core",
		CrossService:  true,
		ModelNote:     "kept both",
	}, got)
}

func TestRationaleFactsFor_CompileTriggerKnowsNoUpstream(t *testing.T) {
	tr := baseTrigger()
	tr.Source = "compile"
	c := typology.Cluster{TargetNodeID: "s.n", Members: []string{"s.n"}, Kind: typology.KindIndependent}

	got := rationaleFactsFor(tr, c, clusterOutcome{})

	assert.False(t, got.UpstreamKnown)
	assert.Empty(t, got.Upstream)
}

// TestRationaleFactsFor_DuplicateTableTriggerKnowsNoUpstream verifies P2a: the
// changed-ancestor analysis is supplied only by a validation rejection, so a
// duplicate_table trigger reports UpstreamKnown false even when one of its
// nodes carries a real ChangedAncestor — the composer must not render a "No
// upstream changed" claim from it: absence of the facts is not an empty
// analyzed set.
func TestRationaleFactsFor_DuplicateTableTriggerKnowsNoUpstream(t *testing.T) {
	tr := baseTrigger()
	tr.Source = "duplicate_table"
	tr.Nodes = []TriggerNode{
		{NodeID: "s.n", Service: "svc", ChangedAncestors: []ChangedAncestor{{NodeID: "s.up", Service: "svc", Depth: 1}}},
	}
	c := typology.Cluster{TargetNodeID: "s.n", Members: []string{"s.n"}, Kind: typology.KindIndependent}

	got := rationaleFactsFor(tr, c, clusterOutcome{})

	assert.False(t, got.UpstreamKnown)
	composed := proposal.ComposeRationale(got)
	assert.NotContains(t, composed, "No upstream")
	assert.NotContains(t, composed, "changed upstream")
}

// TestRationaleFactsFor_RedactsErrorLine verifies P1: a member's ErrorLine is
// projected through proposal.RedactDataValues, so a warehouse data value
// embedded in the trigger's ErrorExcerpt never reaches the persisted or
// published rationale.
func TestRationaleFactsFor_RedactsErrorLine(t *testing.T) {
	tr := baseTrigger()
	tr.Nodes = []TriggerNode{
		{NodeID: "s.n", Service: "svc", ErrorExcerpt: `invalid input syntax for type integer: "a@b.test"`},
	}
	c := typology.Cluster{TargetNodeID: "s.n", Members: []string{"s.n"}, Kind: typology.KindIndependent}

	got := rationaleFactsFor(tr, c, clusterOutcome{})

	assert.Len(t, got.Members, 1)
	assert.Equal(t, `invalid input syntax for type integer: "?"`, got.Members[0].ErrorLine)
	assert.NotContains(t, got.Members[0].ErrorLine, "a@b.test")
}
