package serialization

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/remediation/domain/event"
)

// goldenRemediationRequested has every tagged field set so the DTO reproduces
// each tag, name, casing and order.
const goldenRemediationRequested = `{"event_id":"e","source":"release_rejected","release_id":"r","remediation_round":2,"repo":"acme/demo","commit_sha":"abc","code_bundle_uri":"s3://b","classified_at":"2026-01-02T03:04:05Z","nodes":[{"node_id":"n","relation_id":"sch.rel","category":"compile","error_signature":"sig","reason":"why","error_excerpt":"exc","dbt_log_uri":"s3://l","candidate_artifact_uri":"s3://c","file_path":"models/m.sql","service":"svc","node_type":"dbt-model","other_service":"svc2","other_file_path":"models/o.sql","changed_ancestors":[{"node_id":"a","file_path":"models/a.sql","service":"svc","depth":1}]}]}`

func TestRemediationRequestedRoundTrip(t *testing.T) {
	var dto RemediationRequestedDTO
	if err := json.Unmarshal([]byte(goldenRemediationRequested), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	out, err := json.Marshal(RemediationRequestedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenRemediationRequested {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenRemediationRequested)
	}
	if got.EventID != "e" || got.RemediationRound != 2 || len(got.Nodes) != 1 {
		t.Fatalf("toDomain fields wrong: %+v", got)
	}
	n := got.Nodes[0]
	if n.NodeID != "n" || n.RelationID != "sch.rel" || len(n.ChangedAncestors) != 1 || n.ChangedAncestors[0].Depth != 1 {
		t.Fatalf("toDomain node wrong: %+v", n)
	}
}

// TestRemediationRequestedOmitemptyShape pins that a minimal node omits every
// omitempty field and that a nil nodes slice serialises as null.
func TestRemediationRequestedOmitemptyShape(t *testing.T) {
	out, err := json.Marshal(RemediationRequestedFromDomain(event.RemediationRequested{
		EventID: "e", Source: "s", ReleaseID: "r", RemediationRound: 1, Repo: "repo", CommitSHA: "sha", ClassifiedAt: "t",
		Nodes: []event.FailingNode{{NodeID: "n", Category: "c", ErrorSignature: "sig", Reason: "why", DBTLogURI: "s3://l"}},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"event_id":"e","source":"s","release_id":"r","remediation_round":1,"repo":"repo","commit_sha":"sha","classified_at":"t","nodes":[{"node_id":"n","category":"c","error_signature":"sig","reason":"why","dbt_log_uri":"s3://l"}]}`
	if string(out) != want {
		t.Fatalf("omitempty shape changed:\n got %s\nwant %s", out, want)
	}
}
