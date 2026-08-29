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
		Nodes: []FailingNode{{NodeID: "s.a", ErrorSignature: "sig", ChangedAncestorIDs: []string{"s.u"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(body, &wire)
	if _, ok := wire["nodes"]; !ok {
		t.Fatalf("payload must carry nodes[]: %s", body)
	}
	if _, ok := wire["node_id"]; ok {
		t.Fatalf("a batched payload carries no top-level node_id: %s", body)
	}
}
