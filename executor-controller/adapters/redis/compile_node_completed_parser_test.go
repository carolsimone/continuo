// executor-controller/adapters/redis/compile_node_completed_parser_test.go
package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The compile.node.completed:v1 body is identical to
// seed.build.node.completed:v1, so these tests reuse nodeCompletedPayload /
// nodeCompletedMsg from validation_node_completed_parser_test.go.

func TestParseCompileNodeCompleted_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	evt, err := ParseCompileNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), outboxEntryID.String()))
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-123", evt.ReleaseID)
	assert.Equal(t, "model.shop.orders", evt.NodeID)
	assert.Equal(t, "ok", evt.Outcome)
	assert.Equal(t, "s3://logs/rel-123/orders", evt.DBTLogURI)
}

func TestParseCompileNodeCompleted_FailedOutcome(t *testing.T) {
	p := nodeCompletedPayload()
	p["outcome"] = "failed"
	evt, err := ParseCompileNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.NoError(t, err)
	assert.Equal(t, "failed", evt.Outcome)
}

func TestParseCompileNodeCompleted_RunResultsURIOptional(t *testing.T) {
	evt, err := ParseCompileNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), ""))
	require.NoError(t, err)
	assert.Equal(t, "", evt.RunResultsURI)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID)
}

// TestParseCompileNodeCompleted_FailedContainer verifies the optional
// failed_container field (added by k8s-controller for compile-leg failure
// attribution) is read into the event when present.
func TestParseCompileNodeCompleted_FailedContainer(t *testing.T) {
	p := nodeCompletedPayload()
	p["outcome"] = "failed"
	p["failed_container"] = "parse-prod"
	evt, err := ParseCompileNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.NoError(t, err)
	assert.Equal(t, "parse-prod", evt.FailedContainer)
}

// TestParseCompileNodeCompleted_FailedContainerOptional verifies a payload
// without failed_container (e.g. any "ok" outcome) parses to "".
func TestParseCompileNodeCompleted_FailedContainerOptional(t *testing.T) {
	evt, err := ParseCompileNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), ""))
	require.NoError(t, err)
	assert.Equal(t, "", evt.FailedContainer)
}

func TestParseCompileNodeCompleted_Errors(t *testing.T) {
	// missing payload
	_, err := ParseCompileNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)

	// malformed payload
	_, err = ParseCompileNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)

	// missing release_id
	p := nodeCompletedPayload()
	delete(p, "release_id")
	_, err = ParseCompileNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)

	// missing node_id
	p = nodeCompletedPayload()
	delete(p, "node_id")
	_, err = ParseCompileNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)

	// invalid outcome
	for _, bad := range []string{"", "success", "OK"} {
		p = nodeCompletedPayload()
		p["outcome"] = bad
		_, err = ParseCompileNodeCompleted(nodeCompletedMsg(t, p, ""))
		require.Error(t, err, "outcome %q must be rejected", bad)
	}

	// invalid outbox_entry_id
	_, err = ParseCompileNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), "not-a-uuid"))
	require.Error(t, err)
}
