package serialization

import (
	"errors"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

// goldenStored is a heal.triggered:v1 payload as a store holds it: every
// field, and no unfixable node.
const goldenStored = `{"source":"validation","release_id":"rel-1","remediation_round":2,` +
	`"repo":"org/repo","commit_sha":"abc123","code_bundle_uri":"s3://bundle","classified_at":"2026-11-13T09:00:00Z","nodes":[` +
	`{"node_id":"svc-a","relation_id":"rel","category":"compile","error_signature":"sig-svc-a","reason":"syntax",` +
	`"error_excerpt":"syntax error near select","dbt_log_uri":"s3://log","candidate_artifact_uri":"s3://cand",` +
	`"file_path":"models/orders.sql","project":"svc-a","node_type":"dbt-model","other_project":"svc-b",` +
	`"other_file_path":"models/other.sql","changed_ancestors":[{"node_id":"model.core.base","file_path":"models/base.sql","project":"core","depth":2}]},` +
	`{"node_id":"svc-b","category":"compile","error_signature":"sig-svc-b","reason":"syntax","dbt_log_uri":""}]}`

// goldenUnfixable is a heal.triggered:v1 payload whose second node the
// classifier marked unfixable.
const goldenUnfixable = `{"source":"validation","release_id":"rel-1","remediation_round":2,` +
	`"repo":"org/repo","commit_sha":"abc123","code_bundle_uri":"s3://bundle","classified_at":"2026-11-13T09:00:00Z","nodes":[` +
	`{"node_id":"svc-a","category":"compile","error_signature":"sig-svc-a","reason":"syntax","dbt_log_uri":"s3://log"},` +
	`{"node_id":"svc-b","category":"infra","error_signature":"sig-svc-b","reason":"timeout","dbt_log_uri":"s3://log-b",` +
	`"unfixable":{"reason":"classifier_infra","detail":"the warehouse timed out"}}]}`

func TestDecodeTrigger_ReadsEveryFieldOfAStoredPayload(t *testing.T) {
	got, err := DecodeTrigger([]byte(goldenStored))
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
				FilePath: "models/orders.sql", Project: "svc-a", NodeType: "dbt-model",
				OtherProject: "svc-b", OtherFilePath: "models/other.sql",
				ChangedAncestors: []remediation.ChangedAncestor{{NodeID: "model.core.base", FilePath: "models/base.sql", Project: "core", Depth: 2}},
			},
			{NodeID: "svc-b", ErrorSignature: "sig-svc-b", Category: "compile", Reason: "syntax"},
		},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("decoded trigger differs:\n got %+v\nwant %+v", got, want)
	}
}

func TestDecodeTrigger_KeepsTheUnfixableNodes(t *testing.T) {
	got, err := DecodeTrigger([]byte(goldenUnfixable))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("nodes = %d, want both: the runner hands the unfixable one over", len(got.Nodes))
	}
	if got.Nodes[0].Unfixable != nil {
		t.Fatalf("node %s is fixable, got %+v", got.Nodes[0].NodeID, got.Nodes[0].Unfixable)
	}
	want := &remediation.Unfixable{Reason: model.HandoverReasonClassifierInfra, Detail: "the warehouse timed out"}
	if !reflect.DeepEqual(want, got.Nodes[1].Unfixable) {
		t.Fatalf("unfixable = %+v, want %+v", got.Nodes[1].Unfixable, want)
	}
}

// A stored payload carries its round, as the stream did: one with none, or a
// 0, is refused.
func TestDecodeTrigger_RefusesAMissingRound(t *testing.T) {
	for _, payload := range []string{
		`{"source":"validation","release_id":"rel-1","nodes":[]}`,
		`{"source":"validation","release_id":"rel-1","remediation_round":0,"nodes":[]}`,
	} {
		if _, err := DecodeTrigger([]byte(payload)); err == nil {
			t.Fatalf("decode %s: want an error", payload)
		}
	}
}

func TestDecodeTrigger_RefusesAPayloadThatIsNotATrigger(t *testing.T) {
	for name, payload := range map[string]string{
		"not JSON":              `{`,
		"no release":            `{"source":"validation","nodes":[]}`,
		"a source off the list": `{"source":"nope","release_id":"rel-1","nodes":[]}`,
	} {
		_, err := DecodeTrigger([]byte(payload))
		if !errors.Is(err, events.ErrMalformedEnvelope) {
			t.Fatalf("%s: err = %v, want a malformed payload", name, err)
		}
	}
}

func TestEncodeTrigger_DecodesToTheSameTrigger(t *testing.T) {
	for name, payload := range map[string]string{"stored": goldenStored, "with an unfixable node": goldenUnfixable} {
		want, err := DecodeTrigger([]byte(payload))
		if err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		raw, err := EncodeTrigger(want)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		got, err := DecodeTrigger(raw)
		if err != nil {
			t.Fatalf("%s: decode the encoded payload: %v", name, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s: round trip differs:\n got %+v\nwant %+v", name, got, want)
		}
	}
}

func TestEncodeTrigger_LeavesTheTenantOut(t *testing.T) {
	raw, err := EncodeTrigger(remediation.Trigger{TenantID: "acme", Source: "validation", ReleaseID: "rel-1", Round: 1})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const want = `{"source":"validation","release_id":"rel-1","remediation_round":1,"repo":"","commit_sha":"","classified_at":"","nodes":[]}`
	if string(raw) != want {
		t.Fatalf("payload = %s, want %s", raw, want)
	}
}
