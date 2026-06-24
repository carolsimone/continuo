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
	"event_id":          "evt-abc-123",
	"source":            "validation",
	"release_id":        "rel-456",
	"node_id":           "orders.model.orders_daily",
	"category":          "sql_syntax_error",
	"error_signature":   "column \"foo\" does not exist",
	"dbt_log_uri":       "s3://bucket/logs/orders_daily.log",
	"candidate_sql_uri": "s3://bucket/sql/orders_daily.sql",
	"repo":              "acme/dbt-project",
	"commit_sha":        "deadbeef1234",
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
	require.Equal(t, "sql_syntax_error", trigger.Category)
	require.Equal(t, "column \"foo\" does not exist", trigger.ErrorSignature)
	require.Equal(t, "s3://bucket/logs/orders_daily.log", trigger.DBTLogURI)
	require.Equal(t, "s3://bucket/sql/orders_daily.sql", trigger.CandidateSQLURI)
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
