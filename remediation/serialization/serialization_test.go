package serialization

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/remediation/domain/event"
)

// goldenRemediationRequested has every tagged field set, an unfixable node
// among them, so the payload reproduces each tag, name, casing and order of
// remediation.requested:v3.
const goldenRemediationRequested = `{"source":"validation","release_id":"r","remediation_round":2,"repo":"acme/demo","commit_sha":"abc","code_bundle_uri":"s3://b","classified_at":"2026-01-02T03:04:05Z","nodes":[{"node_id":"n","relation_id":"sch.rel","category":"logic","error_signature":"sig","reason":"why","error_excerpt":"exc","dbt_log_uri":"s3://l","candidate_artifact_uri":"s3://c","file_path":"models/m.sql","project":"svc","node_type":"dbt-model","other_project":"svc2","other_file_path":"models/o.sql","changed_ancestors":[{"node_id":"a","file_path":"models/a.sql","project":"svc","depth":1}]},{"node_id":"m","category":"infra_transient","error_signature":"sig2","reason":"infra:connection_refused","dbt_log_uri":"s3://l2","unfixable":{"reason":"classifier_infra","detail":"could not connect to server: connection refused"}}]}`

func TestRemediationRequestedMapsEveryField(t *testing.T) {
	got := RemediationRequestedFromDomain(event.RemediationRequested{
		Source: model.RemediationSourceValidation, ReleaseID: "r", RemediationRound: 2,
		Repo: "acme/demo", CommitSHA: "abc", CodeBundleURI: "s3://b", ClassifiedAt: "2026-01-02T03:04:05Z",
		Nodes: []event.FailingNode{
			{
				NodeID: "n", RelationID: "sch.rel", Category: "logic", ErrorSignature: "sig", Reason: "why",
				ErrorExcerpt: "exc", DBTLogURI: "s3://l", CandidateArtifactURI: "s3://c", FilePath: "models/m.sql",
				Project: "svc", NodeType: "dbt-model", OtherProject: "svc2", OtherFilePath: "models/o.sql",
				ChangedAncestors: []event.ChangedAncestor{{NodeID: "a", FilePath: "models/a.sql", Project: "svc", Depth: 1}},
			},
			{
				NodeID: "m", Category: "infra_transient", ErrorSignature: "sig2", Reason: "infra:connection_refused", DBTLogURI: "s3://l2",
				Unfixable: &event.Unfixable{Reason: model.HandoverReasonClassifierInfra, Detail: "could not connect to server: connection refused"},
			},
		},
	})

	out, err := json.Marshal(got)
	require.NoError(t, err)
	assert.JSONEq(t, goldenRemediationRequested, string(out))
	assert.Equal(t, goldenRemediationRequested, string(out), "field order is the wire contract's")
}

// TestRemediationRequestedOmitemptyShape pins that a minimal node omits every
// omitempty field, the unfixable marker among them: a node a fix can reach
// carries none, so a stored v2 payload and a v3 payload read alike.
func TestRemediationRequestedOmitemptyShape(t *testing.T) {
	out, err := json.Marshal(RemediationRequestedFromDomain(event.RemediationRequested{
		Source: model.RemediationSourceCompile, ReleaseID: "r", RemediationRound: 1, Repo: "repo", CommitSHA: "sha", ClassifiedAt: "t",
		Nodes: []event.FailingNode{{NodeID: "n", Category: "c", ErrorSignature: "sig", Reason: "why", DBTLogURI: "s3://l"}},
	}))
	require.NoError(t, err)
	const want = `{"source":"compile","release_id":"r","remediation_round":1,"repo":"repo","commit_sha":"sha","classified_at":"t","nodes":[{"node_id":"n","category":"c","error_signature":"sig","reason":"why","dbt_log_uri":"s3://l"}]}`
	assert.Equal(t, want, string(out))
}
