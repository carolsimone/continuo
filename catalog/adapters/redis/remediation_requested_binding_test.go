package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRemediationRequested_HappyPath(t *testing.T) {
	payload := `{
		"event_id": "evt-1",
		"source": "validation",
		"release_id": "rel-1",
		"code_bundle_uri": "s3://b/code-bundles/rel-1/bundle.json",
		"classified_at": "2026-08-12T09:00:00Z",
		"nodes": [{
			"node_id": "analytics.revenue",
			"category": "sql_syntax_error",
			"error_signature": "sig-1",
			"reason": "column \"foo\" does not exist",
			"error_excerpt": "ERROR: column \"foo\" does not exist",
			"dbt_log_uri": "s3://b/logs/rel-1/analytics.revenue.log"
		}]
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": payload}}

	evt, err := ParseRemediationRequested(msg)
	require.NoError(t, err)
	assert.Equal(t, "evt-1", evt.EventID)
	assert.Equal(t, "validation", evt.Source)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "s3://b/code-bundles/rel-1/bundle.json", evt.CodeBundleURI)
	assert.Equal(t, "2026-08-12T09:00:00Z", evt.ClassifiedAt)
	require.Len(t, evt.Nodes, 1)
	assert.Equal(t, "analytics.revenue", evt.Nodes[0].NodeID)
	assert.Equal(t, "sql_syntax_error", evt.Nodes[0].Category)
	assert.Equal(t, "sig-1", evt.Nodes[0].ErrorSignature)
	assert.Equal(t, `column "foo" does not exist`, evt.Nodes[0].Reason)
	assert.Equal(t, `ERROR: column "foo" does not exist`, evt.Nodes[0].ErrorExcerpt)
	assert.Equal(t, "s3://b/logs/rel-1/analytics.revenue.log", evt.Nodes[0].DBTLogURI)
}

func TestParseRemediationRequested_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseRemediationRequested_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": ""}}
	_, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseRemediationRequested_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json"}}
	_, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}
