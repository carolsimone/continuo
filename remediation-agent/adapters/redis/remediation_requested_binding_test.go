package redis

import (
	"encoding/json"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// requestedPayloadFixture is a remediation.requested:v1 wire payload with all
// fields populated.
var requestedPayloadFixture = map[string]any{
	"event_id":               "evt-abc-123",
	"source":                 "validation",
	"release_id":             "rel-456",
	"node_id":                "orders.model.orders_daily",
	"relation_id":            "analytics.orders_daily",
	"category":               "sql_syntax_error",
	"error_signature":        "column \"foo\" does not exist",
	"dbt_log_uri":            "s3://bucket/logs/orders_daily.log",
	"candidate_artifact_uri": "s3://bucket/sql/orders_daily.sql",
	"file_path":              "models/orders_daily.sql",
	"service":                "svc-orders",
	"node_type":              "dbt-model",
	"other_service":          "svc-finance",
	"other_file_path":        "models/orders_legacy.sql",
	"repo":                   "acme/dbt-project",
	"commit_sha":             "deadbeef1234",
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
	require.Equal(t, "orders.model.orders_daily", trigger.NodeID)
	require.Equal(t, "analytics.orders_daily", trigger.RelationID)
	require.Equal(t, "sql_syntax_error", trigger.Category)
	require.Equal(t, "column \"foo\" does not exist", trigger.ErrorSignature)
	require.Equal(t, "s3://bucket/logs/orders_daily.log", trigger.DBTLogURI)
	require.Equal(t, "s3://bucket/sql/orders_daily.sql", trigger.CandidateArtifactURI)
	require.Equal(t, "models/orders_daily.sql", trigger.FilePath)
	require.Equal(t, "svc-orders", trigger.Service)
	require.Equal(t, "dbt-model", trigger.NodeType)
	require.Equal(t, "svc-finance", trigger.OtherService,
		"other_service must decode — the only datum identifying the competing claimant when both share one service")
	require.Equal(t, "models/orders_legacy.sql", trigger.OtherFilePath,
		"other_file_path must decode — deleting its mapping would leave this assertion, and every other assertion here, green")
	require.Equal(t, "acme/dbt-project", trigger.Repo)
	require.Equal(t, "deadbeef1234", trigger.CommitSHA)
	require.Equal(t, "1-0", trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID) // no outbox_entry_id in Values
}

func TestTriggerFromRequested_InvalidJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := triggerFromRequested(msg, []byte("not-json"))
	require.Error(t, err)
}

// TestTriggerFromRequested_SeedServiceField verifies that when a
// remediation.requested:v1 payload carries the service field (set for
// seed_build failures from the candidate topology), it is decoded into
// trigger.Service so proposeFromSource can skip the NodeLocator lookup.
func TestTriggerFromRequested_SeedServiceField(t *testing.T) {
	payload := map[string]any{
		"event_id":        "evt-seed-1",
		"source":          "seed_build",
		"release_id":      "rel-seed-1",
		"node_id":         "seed.svc.customers",
		"category":        "logic",
		"error_signature": "extra column",
		"dbt_log_uri":     "s3://b/seed.log",
		"file_path":       "seeds/customers.csv",
		"service":         "svc-data",
		"repo":            "o/r",
		"commit_sha":      "sha1",
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := goredis.XMessage{ID: "2-0", Values: map[string]interface{}{}}
	trigger, err := triggerFromRequested(msg, raw)
	require.NoError(t, err)

	require.Equal(t, "seed_build", trigger.Source)
	require.Equal(t, "seeds/customers.csv", trigger.FilePath)
	require.Equal(t, "svc-data", trigger.Service,
		"service field must be decoded so proposeFromSource can skip the NodeLocator lookup")
}

// TestTriggerFromRequested_DecodesErrorExcerpt verifies that a
// remediation.requested:v1 payload's error_excerpt field decodes onto
// trigger.ErrorExcerpt, so downstream fixers have the failure text without an
// extra evidence fetch.
func TestTriggerFromRequested_DecodesErrorExcerpt(t *testing.T) {
	raw := []byte(`{"source":"validation","release_id":"rel-1","node_id":"analytics.orders",` +
		`"error_signature":"sig-1","category":"sql_error",` +
		`"error_excerpt":"column \"x\" does not exist"}`)
	tr, err := triggerFromRequested(goredis.XMessage{ID: "1-1"}, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if tr.ErrorExcerpt != `column "x" does not exist` {
		t.Fatalf("ErrorExcerpt = %q", tr.ErrorExcerpt)
	}
}

func TestTriggerFromRequested_DecodesReasonAndBundleURI(t *testing.T) {
	raw := []byte(`{"source":"validation","release_id":"rel-1","node_id":"analytics.orders",` +
		`"error_signature":"sig-1","category":"sql_error","reason":"missing_column",` +
		`"code_bundle_uri":"s3://continuo/code-bundles/rel-1/bundle.json"}`)
	tr, err := triggerFromRequested(goredis.XMessage{ID: "1-1"}, raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if tr.Reason != "missing_column" {
		t.Fatalf("Reason = %q", tr.Reason)
	}
	if tr.CodeBundleURI != "s3://continuo/code-bundles/rel-1/bundle.json" {
		t.Fatalf("CodeBundleURI = %q", tr.CodeBundleURI)
	}
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
	require.Equal(t, "orders.model.orders_daily", trigger.NodeID)
	require.Equal(t, "dbt-model", trigger.NodeType)
	require.Equal(t, raw, trigger.RawPayload)
	require.Empty(t, trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID)
}
