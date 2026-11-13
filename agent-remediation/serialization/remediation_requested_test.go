package serialization

import (
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

const goldenRemediationRequestedV2 = `{"event_id":"e1","source":"validation","release_id":"rel-1","remediation_round":2,` +
	`"repo":"org/repo","commit_sha":"abc123","code_bundle_uri":"s3://bundle","classified_at":"2026-11-13T09:00:00Z","nodes":[` +
	`{"node_id":"svc-a","relation_id":"rel","category":"compile","error_signature":"sig-svc-a","reason":"syntax",` +
	`"error_excerpt":"syntax error near select","dbt_log_uri":"s3://log","candidate_artifact_uri":"s3://cand",` +
	`"file_path":"models/orders.sql","service":"svc-a","node_type":"dbt-model","other_service":"svc-b",` +
	`"other_file_path":"models/other.sql","changed_ancestors":[{"node_id":"model.core.base","file_path":"models/base.sql","service":"core","depth":2}]},` +
	`{"node_id":"svc-b","category":"compile","error_signature":"sig-svc-b","reason":"syntax","dbt_log_uri":""}]}`

func TestDecodeRemediationRequestedV2_ReadsEveryField(t *testing.T) {
	got, err := DecodeRemediationRequestedV2([]byte(goldenRemediationRequestedV2))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := remediation.Trigger{
		Source:        "validation",
		ReleaseID:     "rel-1",
		Round:         2,
		Repo:          "org/repo",
		CommitSHA:     "abc123",
		CodeBundleURI: "s3://bundle",
		Nodes: []remediation.TriggerNode{
			{
				NodeID: "svc-a", RelationID: "rel", ErrorSignature: "sig-svc-a", Category: "compile", Reason: "syntax",
				ErrorExcerpt: "syntax error near select", DBTLogURI: "s3://log", CandidateArtifactURI: "s3://cand",
				FilePath: "models/orders.sql", Service: "svc-a", NodeType: "dbt-model",
				OtherService: "svc-b", OtherFilePath: "models/other.sql",
				ChangedAncestors: []remediation.ChangedAncestor{{NodeID: "model.core.base", FilePath: "models/base.sql", Service: "core", Depth: 2}},
			},
			{NodeID: "svc-b", ErrorSignature: "sig-svc-b", Category: "compile", Reason: "syntax"},
		},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("decoded trigger differs:\n got %+v\nwant %+v", got, want)
	}
}

func TestDecodeRemediationRequestedV2_AMissingRoundIsRoundOne(t *testing.T) {
	for _, payload := range []string{
		`{"release_id":"rel-1","nodes":[]}`,
		`{"release_id":"rel-1","remediation_round":0,"nodes":[]}`,
	} {
		got, err := DecodeRemediationRequestedV2([]byte(payload))
		if err != nil {
			t.Fatalf("decode %s: %v", payload, err)
		}
		if got.Round != 1 {
			t.Fatalf("round of %s = %d, want 1", payload, got.Round)
		}
		if got.TenantID != "" {
			t.Fatalf("the payload carries no tenant, got %q", got.TenantID)
		}
	}
}

func TestDecodeRemediationRequestedV2_RefusesAPayloadThatIsNotJSON(t *testing.T) {
	if _, err := DecodeRemediationRequestedV2([]byte("{")); err == nil {
		t.Fatal("want an error for a payload that is not JSON")
	}
}
