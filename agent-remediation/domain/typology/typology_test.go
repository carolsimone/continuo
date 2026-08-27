package typology

import "testing"

func TestGroup_NoStrategies_EachNodeIsItsOwnIndependentCluster(t *testing.T) {
	nodes := []FailingNode{
		{NodeID: "analytics.b", ErrorSignature: "sig2"},
		{NodeID: "analytics.a", ErrorSignature: "sig1"},
	}

	got := Group(nodes, DagView{})

	if len(got) != 2 {
		t.Fatalf("want 2 clusters, got %d: %+v", len(got), got)
	}
	// Independents are emitted sorted by node id, so order is deterministic.
	if got[0].TargetNodeID != "analytics.a" || got[1].TargetNodeID != "analytics.b" {
		t.Fatalf("want independents sorted by node id, got %+v", got)
	}
	for _, c := range got {
		if c.Kind != KindIndependent {
			t.Errorf("cluster %v: want %q, got %q", c.Members, KindIndependent, c.Kind)
		}
		if len(c.Members) != 1 || c.Members[0] != c.TargetNodeID {
			t.Errorf("independent cluster must target its single member; got target=%q members=%v", c.TargetNodeID, c.Members)
		}
	}
}
