package typology

import (
	"reflect"
	"testing"
)

var _ Typology = CrossServiceCause{}

// One consumer in finance broken by a change in core: claimed at the changed
// ancestor with no minimum group size and whatever its signature is.
func TestCrossService_SingleConsumer_ClaimedAtTheOtherServicesAncestor(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.report", ErrorSignature: "sig", Service: "finance"}}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report": {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
	}}

	claimed, rest := CrossServiceCause{}.Claim(nodes, dag)

	want := []Cluster{{TargetNodeID: "analytics.orders", Members: []string{"analytics.report"}, Kind: KindCrossServiceUpstream}}
	if !reflect.DeepEqual(claimed, want) || len(rest) != 0 {
		t.Fatalf("want %+v and nothing left, got %+v rest=%+v", want, claimed, rest)
	}
}

// Two consumers failing DIFFERENTLY on one change form one cluster: the
// signature is not a grouping key here.
func TestCrossService_DifferentSignatures_OneClusterPerTarget(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.b", ErrorSignature: "sig_b", Service: "finance"},
		{NodeID: "analytics.a", ErrorSignature: "sig_a", Service: "marketing"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.a": {{NodeID: "analytics.u", Service: "core", Depth: 1}},
		"analytics.b": {{NodeID: "analytics.u", Service: "core", Depth: 2}},
	}}

	claimed, _ := CrossServiceCause{}.Claim(nodes, dag)

	want := []Cluster{{TargetNodeID: "analytics.u", Members: []string{"analytics.a", "analytics.b"}, Kind: KindCrossServiceUpstream}}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("want %+v, got %+v", want, claimed)
	}
}

// Several cross-service ancestors: the nearest wins, ties on the smallest id.
func TestCrossService_SeveralAncestors_NearestThenSmallestID(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.x", Service: "finance"}}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.x": {
			{NodeID: "analytics.far", Service: "core", Depth: 2},
			{NodeID: "analytics.near_b", Service: "core", Depth: 1},
			{NodeID: "analytics.near_a", Service: "core", Depth: 1},
		},
	}}

	claimed, _ := CrossServiceCause{}.Claim(nodes, dag)

	if len(claimed) != 1 || claimed[0].TargetNodeID != "analytics.near_a" {
		t.Fatalf("want the nearest ancestor with the smallest id, got %+v", claimed)
	}
}

// A same-service ancestor is not this strategy's business: the node is left
// for SharedUpstreamCause or the independent default.
func TestCrossService_SameServiceAncestor_NotClaimed(t *testing.T) {
	nodes := []FailingNode{{NodeID: "analytics.x", Service: "core"}}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.x": {{NodeID: "analytics.u", Service: "core", Depth: 1}},
	}}

	claimed, rest := CrossServiceCause{}.Claim(nodes, dag)

	if len(claimed) != 0 || len(rest) != 1 {
		t.Fatalf("same-service ancestor must not be claimed; got %+v rest=%+v", claimed, rest)
	}
}

// No changed ancestor, or an ancestor/node with no service to compare, is left
// alone.
func TestCrossService_NoAncestorOrUnknownService_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.none", Service: "finance"},
		{NodeID: "analytics.noservice", Service: ""},
		{NodeID: "analytics.ancnoservice", Service: "finance"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.noservice":    {{NodeID: "analytics.u", Service: "core", Depth: 1}},
		"analytics.ancnoservice": {{NodeID: "analytics.u", Service: "", Depth: 1}},
	}}

	claimed, rest := CrossServiceCause{}.Claim(nodes, dag)

	if len(claimed) != 0 || len(rest) != 3 {
		t.Fatalf("nothing to compare must claim nothing; got %+v rest=%+v", claimed, rest)
	}
}

// A cross-service target reached only by dbt-test nodes is not a fix target: a
// consumer-side test binds again when the producer it reads is fixed. The tests
// are handed back unclaimed rather than forming a producer fix.
func TestCrossService_AllTestMembers_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.report.not_null_total", ErrorSignature: "sig_a", Service: "finance", NodeType: "dbt-test"},
		{NodeID: "analytics.assert_report_positive", ErrorSignature: "sig_b", Service: "finance", NodeType: "dbt-test"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report.not_null_total":  {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
		"analytics.assert_report_positive": {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
	}}

	claimed, rest := CrossServiceCause{}.Claim(nodes, dag)

	if len(claimed) != 0 {
		t.Fatalf("a tests-only cross-service cluster must not be claimed, got %+v", claimed)
	}
	if len(rest) != 2 {
		t.Fatalf("both tests must be handed back unclaimed, got %+v", rest)
	}
}

// A cross-service cluster that mixes a consumer model with its tests still forms
// at the producer: the consumer model is fixed and its tests ride along.
func TestCrossService_ModelPlusTestMembers_RidesAlong(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.report", ErrorSignature: "sig_a", Service: "finance", NodeType: "dbt-model"},
		{NodeID: "analytics.report.not_null_total", ErrorSignature: "sig_a", Service: "finance", NodeType: "dbt-test"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.report":                {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
		"analytics.report.not_null_total": {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
	}}

	claimed, rest := CrossServiceCause{}.Claim(nodes, dag)

	if len(rest) != 0 {
		t.Fatalf("a mixed cross-service cluster must claim every member, got rest=%+v", rest)
	}
	want := []Cluster{{
		TargetNodeID: "analytics.orders",
		Members:      []string{"analytics.report", "analytics.report.not_null_total"},
		Kind:         KindCrossServiceUpstream,
	}}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("model + test must form one cross-service cluster at the producer, want %+v got %+v", want, claimed)
	}
}

// Group runs the strategies in order: cross-service first, shared-upstream on
// the rest, independents last — mixed input yields all three deterministically.
func TestGroup_CrossServiceThenSharedUpstreamThenIndependent(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.lone", ErrorSignature: "s0", Service: "core"},
		{NodeID: "analytics.v", ErrorSignature: "s1", Service: "core"},
		{NodeID: "analytics.w", ErrorSignature: "s1", Service: "core"},
		{NodeID: "analytics.report", ErrorSignature: "s2", Service: "finance"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]ChangedAncestor{
		"analytics.v":      {{NodeID: "analytics.u", Service: "core", Depth: 1}},
		"analytics.w":      {{NodeID: "analytics.u", Service: "core", Depth: 1}},
		"analytics.report": {{NodeID: "analytics.orders", Service: "core", Depth: 1}},
	}}

	got := Group(nodes, dag, CrossServiceCause{}, SharedUpstreamCause{})

	want := []Cluster{
		{TargetNodeID: "analytics.orders", Members: []string{"analytics.report"}, Kind: KindCrossServiceUpstream},
		{TargetNodeID: "analytics.u", Members: []string{"analytics.v", "analytics.w"}, Kind: KindSharedUpstream},
		{TargetNodeID: "analytics.lone", Members: []string{"analytics.lone"}, Kind: KindIndependent},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("want %+v, got %+v", want, got)
	}
}
