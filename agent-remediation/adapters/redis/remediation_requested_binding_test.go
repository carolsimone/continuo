package redis

import (
	"encoding/json"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
)

func TestTriggerFromPayload_DecodesBatch(t *testing.T) {
	raw := []byte(`{"event_id":"e","source":"validation","release_id":"r","remediation_round":1,"repo":"o/r","commit_sha":"sha","code_bundle_uri":"s3://b/bundle.json",
	  "nodes":[{"node_id":"s.a","error_signature":"sig","category":"logic","reason":"logic:missing_object","dbt_log_uri":"s3://l/a","candidate_artifact_uri":"s3://c/a","file_path":"models/a.sql","service":"svc","node_type":"dbt-model","changed_ancestors":[{"node_id":"s.u","file_path":"models/u.sql","service":"svc"}]},
	           {"node_id":"s.b","error_signature":"sig","category":"logic","reason":"logic:missing_object","dbt_log_uri":"s3://l/b"}]}`)
	tr, err := TriggerFromPayload(raw)
	require.NoError(t, err)
	assert.Equal(t, "r", tr.ReleaseID)
	assert.Equal(t, "s3://b/bundle.json", tr.CodeBundleURI)
	require.Len(t, tr.Nodes, 2)
	assert.Equal(t, []handlers.ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svc"}},
		tr.Nodes[0].ChangedAncestors)
	assert.Equal(t, "svc", tr.Nodes[0].Service)
	assert.Equal(t, raw, tr.RawPayload)
}

// requestedPayloadFixture is a remediation.requested:v2 wire payload with every
// release-level and node-level field populated.
var requestedPayloadFixture = map[string]any{
	"event_id":          "evt-abc-123",
	"source":            "validation",
	"release_id":        "rel-456",
	"remediation_round": 2,
	"repo":              "acme/dbt-project",
	"commit_sha":        "deadbeef1234",
	"code_bundle_uri":   "s3://bucket/code-bundles/rel-456/bundle.json",
	"classified_at":     "2026-08-28T00:00:00Z",
	"nodes": []any{
		map[string]any{
			"node_id":                "orders.model.orders_daily",
			"relation_id":            "analytics.orders_daily",
			"category":               "sql_syntax_error",
			"error_signature":        "column \"foo\" does not exist",
			"reason":                 "logic:missing_column",
			"error_excerpt":          "column \"foo\" does not exist",
			"dbt_log_uri":            "s3://bucket/logs/orders_daily.log",
			"candidate_artifact_uri": "s3://bucket/sql/orders_daily.sql",
			"file_path":              "models/orders_daily.sql",
			"service":                "svc-orders",
			"node_type":              "dbt-model",
			"other_service":          "svc-finance",
			"other_file_path":        "models/orders_legacy.sql",
			"changed_ancestors": []any{map[string]any{
				"node_id": "orders.model.orders_base", "file_path": "models/orders_base.sql", "service": "svc-orders"}},
		},
	},
}

func TestTriggerFromRequested_AllFieldsMap(t *testing.T) {
	raw, err := json.Marshal(requestedPayloadFixture)
	require.NoError(t, err)

	msg := goredis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{},
	}

	trigger, err := triggerFromRequested(msg, raw)
	require.NoError(t, err)

	require.Equal(t, "validation", trigger.Source)
	require.Equal(t, "rel-456", trigger.ReleaseID)
	require.Equal(t, 2, trigger.RemediationRound)
	require.Equal(t, "acme/dbt-project", trigger.Repo)
	require.Equal(t, "deadbeef1234", trigger.CommitSHA)
	require.Equal(t, "s3://bucket/code-bundles/rel-456/bundle.json", trigger.CodeBundleURI)
	require.Equal(t, "1-0", trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID) // no outbox_entry_id in Values

	require.Len(t, trigger.Nodes, 1)
	n := trigger.Nodes[0]
	require.Equal(t, "orders.model.orders_daily", n.NodeID)
	require.Equal(t, "analytics.orders_daily", n.RelationID)
	require.Equal(t, "sql_syntax_error", n.Category)
	require.Equal(t, "column \"foo\" does not exist", n.ErrorSignature)
	require.Equal(t, "logic:missing_column", n.Reason)
	require.Equal(t, "column \"foo\" does not exist", n.ErrorExcerpt)
	require.Equal(t, "s3://bucket/logs/orders_daily.log", n.DBTLogURI)
	require.Equal(t, "s3://bucket/sql/orders_daily.sql", n.CandidateArtifactURI)
	require.Equal(t, "models/orders_daily.sql", n.FilePath)
	require.Equal(t, "svc-orders", n.Service)
	require.Equal(t, "dbt-model", n.NodeType)
	require.Equal(t, "svc-finance", n.OtherService,
		"other_service must decode — the only datum identifying the competing claimant when both share one service")
	require.Equal(t, "models/orders_legacy.sql", n.OtherFilePath,
		"other_file_path must decode — deleting its mapping would leave this assertion, and every other assertion here, green")
	require.Equal(t, []handlers.ChangedAncestor{
		{NodeID: "orders.model.orders_base", FilePath: "models/orders_base.sql", Service: "svc-orders"}}, n.ChangedAncestors,
		"changed_ancestors must decode with their candidate location — without the ids every failure is fixed in its own source instead of the ancestor they share, and without the location the fix edits wherever the PROMOTED graph still places a renamed ancestor")
}

func TestTriggerFromRequested_InvalidJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := triggerFromRequested(msg, []byte("not-json"))
	require.Error(t, err)
}

// TestTriggerFromRequested_SeedServiceField verifies that when a node carries
// the service field (set for seed_build failures from the candidate topology),
// it is decoded onto the node so the fixer can skip the NodeLocator lookup.
func TestTriggerFromRequested_SeedServiceField(t *testing.T) {
	payload := map[string]any{
		"event_id":   "evt-seed-1",
		"source":     "seed_build",
		"release_id": "rel-seed-1",
		"repo":       "o/r",
		"commit_sha": "sha1",
		"nodes": []any{
			map[string]any{
				"node_id":         "seed.svc.customers",
				"category":        "logic",
				"error_signature": "extra column",
				"dbt_log_uri":     "s3://b/seed.log",
				"file_path":       "seeds/customers.csv",
				"service":         "svc-data",
			},
		},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := goredis.XMessage{ID: "2-0", Values: map[string]interface{}{}}
	trigger, err := triggerFromRequested(msg, raw)
	require.NoError(t, err)

	require.Equal(t, "seed_build", trigger.Source)
	require.Len(t, trigger.Nodes, 1)
	require.Equal(t, "seeds/customers.csv", trigger.Nodes[0].FilePath)
	require.Equal(t, "svc-data", trigger.Nodes[0].Service,
		"service must be decoded so the source fix can skip the NodeLocator lookup")
}

// TestTriggerFromPayload_LeavesTheDedupIdentityToTheCaller pins the split
// between the payload and the message that delivered it: decoding the payload
// alone yields every trigger field but no dedup identity, so a caller
// replaying stored bytes — the shadow-verify reconciler starting the attempt
// that follows a failed verification — supplies an identity of its own rather
// than inheriting the identity of the message the first attempt consumed.
func TestTriggerFromPayload_LeavesTheDedupIdentityToTheCaller(t *testing.T) {
	raw, err := json.Marshal(requestedPayloadFixture)
	require.NoError(t, err)

	trigger, err := TriggerFromPayload(raw)
	require.NoError(t, err)

	require.Equal(t, "validation", trigger.Source)
	require.Equal(t, "rel-456", trigger.ReleaseID)
	require.Len(t, trigger.Nodes, 1)
	require.Equal(t, "dbt-model", trigger.Nodes[0].NodeType)
	require.Equal(t, raw, trigger.RawPayload)
	require.Empty(t, trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID)
}
