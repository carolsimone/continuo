// executor-controller/adapters/redis/seed_build_node_completed_parser_test.go
package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The seed.build.node.completed:v1 body is identical to
// validation.node.completed:v1, so these tests reuse nodeCompletedPayload /
// nodeCompletedMsg from validation_node_completed_parser_test.go.

func TestParseSeedBuildNodeCompleted_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	evt, err := ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), outboxEntryID.String()))
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-123", evt.ReleaseID)
	assert.Equal(t, "model.shop.orders", evt.NodeID)
	assert.Equal(t, "ok", evt.Outcome)
	assert.Equal(t, "s3://logs/rel-123/orders", evt.DBTLogURI)
}

func TestParseSeedBuildNodeCompleted_FailedOutcome(t *testing.T) {
	p := nodeCompletedPayload()
	p["outcome"] = "failed"
	evt, err := ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.NoError(t, err)
	assert.Equal(t, "failed", evt.Outcome)
}

func TestParseSeedBuildNodeCompleted_RunResultsURIOptional(t *testing.T) {
	evt, err := ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), ""))
	require.NoError(t, err)
	assert.Equal(t, "", evt.RunResultsURI)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID)
}

func TestParseSeedBuildNodeCompleted_Errors(t *testing.T) {
	// missing payload
	_, err := ParseSeedBuildNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)

	// malformed payload
	_, err = ParseSeedBuildNodeCompleted(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)

	// missing release_id
	p := nodeCompletedPayload()
	delete(p, "release_id")
	_, err = ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)

	// missing node_id
	p = nodeCompletedPayload()
	delete(p, "node_id")
	_, err = ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, p, ""))
	require.Error(t, err)

	// invalid outcome
	for _, bad := range []string{"", "success", "OK"} {
		p = nodeCompletedPayload()
		p["outcome"] = bad
		_, err = ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, p, ""))
		require.Error(t, err, "outcome %q must be rejected", bad)
	}

	// invalid outbox_entry_id
	_, err = ParseSeedBuildNodeCompleted(nodeCompletedMsg(t, nodeCompletedPayload(), "not-a-uuid"))
	require.Error(t, err)
}
