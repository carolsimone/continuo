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
