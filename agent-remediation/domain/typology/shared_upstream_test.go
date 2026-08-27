package typology

import (
	"reflect"
	"testing"
)

var _ Typology = SharedUpstreamCause{}

func TestSharedUpstream_SameSignatureSharedAncestor_OneCluster(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.b", ErrorSignature: "missing_col_x"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
	}}

	claimed, rest := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(rest) != 0 {
		t.Fatalf("want nothing left, got %+v", rest)
	}
	want := []Cluster{{TargetNodeID: "analytics.u", Members: []string{"analytics.a", "analytics.b"}, Kind: KindSharedUpstream}}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("want %+v, got %+v", want, claimed)
	}
}

func TestSharedUpstream_DifferentSignatures_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "sig_a"},
		{NodeID: "analytics.b", ErrorSignature: "sig_b"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
	}}

	claimed, rest := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(claimed) != 0 {
		t.Fatalf("different signatures must not group, got %+v", claimed)
	}
	if len(rest) != 2 {
		t.Fatalf("want both nodes left, got %+v", rest)
	}
}

func TestSharedUpstream_SameSignatureNoCommonAncestor_NotClaimed(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.b", ErrorSignature: "missing_col_x"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.v"},
	}}

	claimed, rest := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(claimed) != 0 {
		t.Fatalf("no common ancestor must not group, got %+v", claimed)
	}
	if len(rest) != 2 {
		t.Fatalf("want both nodes left, got %+v", rest)
	}
}

func TestSharedUpstream_MixedViaGroup_TwoClusters(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.b", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.c", ErrorSignature: "other"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
		"analytics.c": {"analytics.u"},
	}}

	got := Group(nodes, dag, SharedUpstreamCause{})

	if len(got) != 2 {
		t.Fatalf("want 2 clusters (shared {a,b}→u, independent c), got %+v", got)
	}
	if got[0].Kind != KindSharedUpstream || got[0].TargetNodeID != "analytics.u" ||
		!reflect.DeepEqual(got[0].Members, []string{"analytics.a", "analytics.b"}) {
		t.Errorf("cluster 0 wrong: %+v", got[0])
	}
	if got[1].Kind != KindIndependent || got[1].TargetNodeID != "analytics.c" {
		t.Errorf("cluster 1 wrong: %+v", got[1])
	}
}
