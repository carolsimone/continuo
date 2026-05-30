// executor-controller/adapters/redis/validation_node_completed_parser_test.go
package redis

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodeCompletedPayload mirrors the flat JSON body k8s-controller emits in the
// "payload" field of a validation.node.completed:v1 message.
func nodeCompletedPayload() map[string]any {
	return map[string]any{
		"release_id":  "rel-123",
		"node_id":     "model.shop.orders",
		"outcome":     "ok",
		"dbt_log_uri": "s3://logs/rel-123/orders",
	}
}

func nodeCompletedMsg(t *testing.T, payload map[string]any, outboxEntryID string) goredis.XMessage {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	values := map[string]interface{}{"payload": string(body)}
	if outboxEntryID != "" {
		values["outbox_entry_id"] = outboxEntryID
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseValidationNodeCompleted_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	evt, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), outboxEntryID.String()))
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-123", evt.ReleaseID)
	assert.Equal(t, "model.shop.orders", evt.NodeID)
	assert.Equal(t, "ok", evt.Outcome)
	assert.Equal(t, "s3://logs/rel-123/orders", evt.DBTLogURI)
}

func TestParseValidationNodeCompleted_FailedOutcome(t *testing.T) {
	p := nodeCompletedPayload()
	p["outcome"] = "failed"
	evt, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.NoError(t, err)
	assert.Equal(t, "failed", evt.Outcome)
}

func TestParseValidationNodeCompleted_DBTLogURIOptional(t *testing.T) {
	p := nodeCompletedPayload()
	delete(p, "dbt_log_uri")
	evt, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.NoError(t, err)
	assert.Equal(t, "", evt.DBTLogURI, "dbt_log_uri is optional")
}

func TestParseValidationNodeCompleted_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	evt, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), ""))
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID)
}

func TestParseValidationNodeCompleted_OutboxEntryIDInvalidIsError(t *testing.T) {
	_, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), "not-a-uuid"))
	require.Error(t, err)
}

func TestParseValidationNodeCompleted_MissingPayloadIsError(t *testing.T) {
	_, err := ParseValidationNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseValidationNodeCompleted_MalformedPayloadIsError(t *testing.T) {
	_, err := ParseValidationNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)
}

func TestParseValidationNodeCompleted_MissingReleaseID(t *testing.T) {
	p := nodeCompletedPayload()
	delete(p, "release_id")
	_, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationNodeCompleted_MissingNodeID(t *testing.T) {
	p := nodeCompletedPayload()
	delete(p, "node_id")
	_, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationNodeCompleted_InvalidOutcome(t *testing.T) {
	for _, bad := range []string{"", "success", "pending", "OK"} {
		p := nodeCompletedPayload()
		p["outcome"] = bad
		_, err := ParseValidationNodeCompleted(nodeCompletedMsg(t, p, ""))
		require.Error(t, err, "outcome %q must be rejected", bad)
	}
}
