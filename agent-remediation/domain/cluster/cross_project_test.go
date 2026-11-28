package cluster

import (
	"reflect"
	"testing"
)

var _ Strategy = CrossProjectCause{}

// One consumer in finance broken by a change in core: claimed at the changed
// ancestor with no minimum group size and whatever its signature is.
func TestCrossProject_SingleConsumer_ClaimedAtTheOtherProjectsAncestor(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.report", ErrorSignature: "sig", Project: "finance"}}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report": {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
	}}

	claimed, rest := CrossProjectCause{}.Claim(nodes, view)

	want := []Cluster{{TargetNodeID: "analytics.orders", Members: []string{"analytics.report"}, Kind: KindCrossProjectUpstream}}
	if !reflect.DeepEqual(claimed, want) || len(rest) != 0 {
		t.Fatalf("want %+v and nothing left, got %+v rest=%+v", want, claimed, rest)
	}
}

// Two consumers failing DIFFERENTLY on one change form one cluster: the
// signature is not a grouping key here.
func TestCrossProject_DifferentSignatures_OneClusterPerTarget(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.b", ErrorSignature: "sig_b", Project: "finance"},
		{NodeID: "analytics.a", ErrorSignature: "sig_a", Project: "marketing"},
	}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.a": {{NodeID: "analytics.u", Project: "core", Depth: 1}},
		"analytics.b": {{NodeID: "analytics.u", Project: "core", Depth: 2}},
	}}

	claimed, _ := CrossProjectCause{}.Claim(nodes, view)

	want := []Cluster{{TargetNodeID: "analytics.u", Members: []string{"analytics.a", "analytics.b"}, Kind: KindCrossProjectUpstream}}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("want %+v, got %+v", want, claimed)
	}
}

// Several cross-project ancestors: the nearest wins, ties on the smallest id.
func TestCrossProject_SeveralAncestors_NearestThenSmallestID(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.x", Project: "finance"}}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.x": {
			{NodeID: "analytics.far", Project: "core", Depth: 2},
			{NodeID: "analytics.near_b", Project: "core", Depth: 1},
			{NodeID: "analytics.near_a", Project: "core", Depth: 1},
		},
	}}

	claimed, _ := CrossProjectCause{}.Claim(nodes, view)

	if len(claimed) != 1 || claimed[0].TargetNodeID != "analytics.near_a" {
		t.Fatalf("want the nearest ancestor with the smallest id, got %+v", claimed)
	}
}

// A same-project ancestor is not this strategy's business: the node is left
// for SharedUpstreamCause or the independent default.
func TestCrossProject_SameProjectAncestor_NotClaimed(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.x", Project: "core"}}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.x": {{NodeID: "analytics.u", Project: "core", Depth: 1}},
	}}

	claimed, rest := CrossProjectCause{}.Claim(nodes, view)

	if len(claimed) != 0 || len(rest) != 1 {
		t.Fatalf("same-project ancestor must not be claimed; got %+v rest=%+v", claimed, rest)
	}
}

// No changed ancestor, or an ancestor/node with no project to compare, is left
// alone.
func TestCrossProject_NoAncestorOrUnknownProject_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.none", Project: "finance"},
		{NodeID: "analytics.noproject", Project: ""},
		{NodeID: "analytics.ancnoproject", Project: "finance"},
	}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.noproject":    {{NodeID: "analytics.u", Project: "core", Depth: 1}},
		"analytics.ancnoproject": {{NodeID: "analytics.u", Project: "", Depth: 1}},
	}}

	claimed, rest := CrossProjectCause{}.Claim(nodes, view)

	if len(claimed) != 0 || len(rest) != 3 {
		t.Fatalf("nothing to compare must claim nothing; got %+v rest=%+v", claimed, rest)
	}
}

// A cross-project target reached only by dbt-test nodes is not a fix target: a
// consumer-side test binds again when the producer it reads is fixed. The tests
// are handed back unclaimed rather than forming a producer fix.
func TestCrossProject_AllTestMembers_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.report.not_null_total", ErrorSignature: "sig_a", Project: "finance", NodeType: "dbt-test"},
		{NodeID: "analytics.assert_report_positive", ErrorSignature: "sig_b", Project: "finance", NodeType: "dbt-test"},
	}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report.not_null_total":  {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
		"analytics.assert_report_positive": {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
	}}

	claimed, rest := CrossProjectCause{}.Claim(nodes, view)

	if len(claimed) != 0 {
		t.Fatalf("a tests-only cross-project cluster must not be claimed, got %+v", claimed)
	}
	if len(rest) != 2 {
		t.Fatalf("both tests must be handed back unclaimed, got %+v", rest)
	}
}

// A cross-project cluster that mixes a consumer model with its tests still forms
// at the producer: the consumer model is fixed and its tests ride along.
func TestCrossProject_ModelPlusTestMembers_RidesAlong(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.report", ErrorSignature: "sig_a", Project: "finance", NodeType: "dbt-model"},
		{NodeID: "analytics.report.not_null_total", ErrorSignature: "sig_a", Project: "finance", NodeType: "dbt-test"},
	}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report":                {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
		"analytics.report.not_null_total": {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
	}}

	claimed, rest := CrossProjectCause{}.Claim(nodes, view)

	if len(rest) != 0 {
		t.Fatalf("a mixed cross-project cluster must claim every member, got rest=%+v", rest)
	}
	want := []Cluster{{
		TargetNodeID: "analytics.orders",
		Members:      []string{"analytics.report", "analytics.report.not_null_total"},
		Kind:         KindCrossProjectUpstream,
	}}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("model + test must form one cross-project cluster at the producer, want %+v got %+v", want, claimed)
	}
}

// Group runs the strategies in order: cross-project first, shared-upstream on
// the rest, independents last — mixed input yields all three deterministically.
func TestGroup_CrossProjectThenSharedUpstreamThenIndependent(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.lone", ErrorSignature: "s0", Project: "core"},
		{NodeID: "analytics.v", ErrorSignature: "s1", Project: "core"},
		{NodeID: "analytics.w", ErrorSignature: "s1", Project: "core"},
		{NodeID: "analytics.report", ErrorSignature: "s2", Project: "finance"},
	}
	view := TopologyView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.v":      {{NodeID: "analytics.u", Project: "core", Depth: 1}},
		"analytics.w":      {{NodeID: "analytics.u", Project: "core", Depth: 1}},
		"analytics.report": {{NodeID: "analytics.orders", Project: "core", Depth: 1}},
	}}

	got := Group(nodes, view, CrossProjectCause{}, SharedUpstreamCause{})

	want := []Cluster{
		{TargetNodeID: "analytics.orders", Members: []string{"analytics.report"}, Kind: KindCrossProjectUpstream},
		{TargetNodeID: "analytics.u", Members: []string{"analytics.v", "analytics.w"}, Kind: KindSharedUpstream},
		{TargetNodeID: "analytics.lone", Members: []string{"analytics.lone"}, Kind: KindIndependent},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}
