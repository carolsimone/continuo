package event

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRemediationEventIDVariesByAttempt(t *testing.T) {
	a1 := RemediationEventID("r1", 1)
	a1b := RemediationEventID("r1", 1)
	a2 := RemediationEventID("r1", 2)
	if a1 != a1b {
		t.Fatal("same (release,attempt) must be stable")
	}
	if a1 == a2 {
		t.Fatal("different attempt must differ")
	}
}

// TestRemediationEventID_KeyedOnReleaseAndAttempt verifies the id is a
// function of (releaseID, attempt) alone, with no node segment.
func TestRemediationEventID_KeyedOnReleaseAndAttempt(t *testing.T) {
	if RemediationEventID("r", 1) == RemediationEventID("r", 2) {
		t.Fatal("attempts must mint distinct ids")
	}
	if RemediationEventID("r", 1) != RemediationEventID("r", 1) {
		t.Fatal("must be stable")
	}
}

func TestRemediationProposedJSON(t *testing.T) {
	b, _ := json.Marshal(RemediationProposed{
		EventID: "id", Source: "validation", ReleaseID: "r1", NodeID: "s.n",
		ErrorSignature: "sig", ProposedSQLURI: "s3://b/p.sql", DiffURI: "s3://b/p.diff",
		Rationale: "fix typo", Confidence: "high", Model: "claude-opus-4-8", Attempt: 1,
		ProposedAt: "2026-06-23T00:00:00Z", SourceResolved: true,
	})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"event_id", "source", "release_id", "node_id", "error_signature",
		"proposed_sql_uri", "diff_uri", "rationale", "confidence", "model", "attempt", "proposed_at",
		"source_resolved", "resolved_node_ids", "edits"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q", k)
		}
	}
}

// TestRemediationProposed_NamesNoSuspectedRootCause pins that the event carries
// no free-text guess at which node caused the failure. One attempt now repairs a
// whole failing set, so there is no single node to name; each edit already says
// which node's source it changes, in target_node_id, and that is structured
// where a guess was not.
func TestRemediationProposed_NamesNoSuspectedRootCause(t *testing.T) {
	b, err := json.Marshal(RemediationProposed{
		EventID: "id", ReleaseID: "r1", NodeID: "s.n", Attempt: 1,
		Edits: []ProposedEdit{{Path: "services/svc/models/a.sql", TargetNodeID: "s.a"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// The field is read off the type, not off one marshalled value: an omitempty
	// field is absent from an empty payload whether or not it still exists.
	typ := reflect.TypeOf(RemediationProposed{})
	for i := range typ.NumField() {
		if strings.Contains(typ.Field(i).Tag.Get("json"), "suspected_root_cause_node") {
			t.Errorf("RemediationProposed must not declare %s", typ.Field(i).Name)
		}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	edits, ok := m["edits"].([]any)
	if !ok || len(edits) != 1 {
		t.Fatalf("edits must survive: %s", b)
	}
	if got := edits[0].(map[string]any)["target_node_id"]; got != "s.a" {
		t.Errorf("target_node_id = %v, want s.a — it is what replaced the guess", got)
	}
}
