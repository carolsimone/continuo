package event

import (
	"encoding/json"
	"testing"
)

func TestRemediationEventIDDeterministic(t *testing.T) {
	a := RemediationEventID("rel-1", "schema.node")
	b := RemediationEventID("rel-1", "schema.node")
	c := RemediationEventID("rel-2", "schema.node")
	if a != b {
		t.Fatal("same (release,node) must yield same event id")
	}
	if a == c {
		t.Fatal("different release must yield different event id")
	}
}

func TestRemediationRequestedJSON(t *testing.T) {
	p := RemediationRequested{
		EventID:        "id",
		Source:         "validation",
		ReleaseID:      "rel-1",
		NodeID:         "schema.node",
		Category:       "logic",
		ErrorSignature: "sig",
		DBTLogURI:      "s3://b/log",
		Repo:           "owner/repo",
		CommitSHA:      "abc",
		ClassifiedAt:   "2026-06-23T00:00:00Z",
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"event_id", "source", "release_id", "node_id", "category", "error_signature", "dbt_log_uri", "repo", "commit_sha", "classified_at"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing json key %q", k)
		}
	}
}
