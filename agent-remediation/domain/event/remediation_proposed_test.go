package event

import (
	"encoding/json"
	"testing"
)

func TestRemediationEventIDVariesByAttempt(t *testing.T) {
	a1 := RemediationEventID("r1", "s.n", 1)
	a1b := RemediationEventID("r1", "s.n", 1)
	a2 := RemediationEventID("r1", "s.n", 2)
	if a1 != a1b {
		t.Fatal("same (release,node,attempt) must be stable")
	}
	if a1 == a2 {
		t.Fatal("different attempt must differ")
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
		"source_resolved"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q", k)
		}
	}
}
