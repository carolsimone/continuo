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

func TestSharedUpstream_MultipleCommonAncestors_PicksSmallestId(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.b", ErrorSignature: "missing_col_x"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.z", "analytics.m"},
		"analytics.b": {"analytics.m", "analytics.z"},
	}}

	claimed, _ := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(claimed) != 1 || claimed[0].TargetNodeID != "analytics.m" {
		t.Fatalf("want target analytics.m (smallest common id), got %+v", claimed)
	}
}

func TestSharedUpstream_SameSignatureSplitAncestors_TwoClusters(t *testing.T) {
	// One signature reached through two unrelated changed ancestors must split
	// into one cluster per ancestor-sharing subset, not fall through whole.
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.b", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.c", ErrorSignature: "missing_col_x"},
		{NodeID: "analytics.d", ErrorSignature: "missing_col_x"},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
		"analytics.c": {"analytics.v"},
		"analytics.d": {"analytics.v"},
	}}

	claimed, rest := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(rest) != 0 {
		t.Fatalf("want nothing left, got %+v", rest)
	}
	want := []Cluster{
		{TargetNodeID: "analytics.u", Members: []string{"analytics.a", "analytics.b"}, Kind: KindSharedUpstream},
		{TargetNodeID: "analytics.v", Members: []string{"analytics.c", "analytics.d"}, Kind: KindSharedUpstream},
	}
	if !reflect.DeepEqual(claimed, want) {
		t.Fatalf("want two ancestor-partitioned clusters, got %+v", claimed)
	}
}

func TestSharedUpstream_EmptySignatureNeverGroups(t *testing.T) {
	// An empty error signature carries no "failed the same way" evidence, so it
	// must never form a shared-upstream cluster even under a common ancestor.
	nodes := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: ""},
		{NodeID: "analytics.b", ErrorSignature: ""},
	}
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
	}}

	claimed, rest := SharedUpstreamCause{}.Claim(nodes, dag)

	if len(claimed) != 0 {
		t.Fatalf("empty signature must not group, got %+v", claimed)
	}
	if len(rest) != 2 {
		t.Fatalf("want both nodes left, got %+v", rest)
	}
}

func TestGroup_MultipleClustersStableAcrossInputOrder(t *testing.T) {
	// Two signatures each forming a shared-upstream cluster: the emitted cluster
	// order must not depend on which signature appears first in the input.
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
		"analytics.c": {"analytics.v"},
		"analytics.d": {"analytics.v"},
	}}
	base := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "s1"},
		{NodeID: "analytics.b", ErrorSignature: "s1"},
		{NodeID: "analytics.c", ErrorSignature: "s2"},
		{NodeID: "analytics.d", ErrorSignature: "s2"},
	}
	reordered := []FailingNode{base[2], base[3], base[0], base[1]}

	g1 := Group(base, dag, SharedUpstreamCause{})
	g2 := Group(reordered, dag, SharedUpstreamCause{})

	if !reflect.DeepEqual(g1, g2) {
		t.Fatalf("multi-cluster grouping must be input-order independent:\n%+v\n%+v", g1, g2)
	}
	if len(g1) != 2 || g1[0].TargetNodeID != "analytics.u" || g1[1].TargetNodeID != "analytics.v" {
		t.Fatalf("want [u-cluster, v-cluster] sorted by member id, got %+v", g1)
	}
}

func TestGroup_StableAcrossInputOrder(t *testing.T) {
	dag := DagView{ChangedAncestorsByNode: map[string][]string{
		"analytics.a": {"analytics.u"},
		"analytics.b": {"analytics.u"},
		"analytics.c": nil,
	}}
	base := []FailingNode{
		{NodeID: "analytics.a", ErrorSignature: "s"},
		{NodeID: "analytics.b", ErrorSignature: "s"},
		{NodeID: "analytics.c", ErrorSignature: "t"},
	}
	reordered := []FailingNode{base[2], base[0], base[1]}

	g1 := Group(base, dag, SharedUpstreamCause{})
	g2 := Group(reordered, dag, SharedUpstreamCause{})

	if !reflect.DeepEqual(g1, g2) {
		t.Fatalf("grouping must be input-order independent:\n%+v\n%+v", g1, g2)
	}
}
