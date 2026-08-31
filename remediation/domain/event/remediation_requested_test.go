package event

import (
	"encoding/json"
	"testing"
)

func TestRemediationEventID_KeyedOnReleaseAndRound(t *testing.T) {
	if RemediationEventID("r1", 1) != RemediationEventID("r1", 0) {
		t.Fatal("round 0 and round 1 must mint the same id")
	}
	if RemediationEventID("r1", 1) == RemediationEventID("r1", 2) {
		t.Fatal("a later round must mint a distinct id")
	}
	if RemediationEventID("r1", 1) == RemediationEventID("r2", 1) {
		t.Fatal("different releases must mint distinct ids")
	}
}

func TestRemediationRequested_WireShapeCarriesNodes(t *testing.T) {
	body, err := json.Marshal(RemediationRequested{
		EventID: "e", Source: "validation", ReleaseID: "r1", RemediationRound: 1,
		Nodes: []FailingNode{{NodeID: "s.a", ErrorSignature: "sig",
			ChangedAncestors: []ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svc"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(body, &wire)
	if _, ok := wire["nodes"]; !ok {
		t.Fatalf("payload must carry nodes[]: %s", body)
	}
	// The ancestors travel as objects: an id alone cannot say where THIS
	// release's candidate holds a node it renamed or moved.
	var decoded RemediationRequested
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Nodes[0].ChangedAncestors) != 1 ||
		decoded.Nodes[0].ChangedAncestors[0].FilePath != "models/u.sql" ||
		decoded.Nodes[0].ChangedAncestors[0].Service != "svc" {
		t.Fatalf("changed_ancestors must round-trip with their location: %s", body)
	}
	if _, ok := wire["node_id"]; ok {
		t.Fatalf("a batched payload carries no top-level node_id: %s", body)
	}
}
