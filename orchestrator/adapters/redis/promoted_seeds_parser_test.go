package redis_test

import (
	"errors"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func promotedSeedsMsg(overrides map[string]interface{}) goredis.XMessage {
	values := map[string]interface{}{
		"schedule_id":   "6f1a2b3c-4d5e-4f60-8a9b-0c1d2e3f4a5b",
		"schedule_name": "promote-seed-6f1a2b3c",
		"release_id":    "rel-1",
		"kind":          "promote_seed",
		"nodes":         `[{"service_name":"core","schema_name":"analytics","table_name":"seed_users","node_type":"dbt-seed","image_tag":"v1"},{"service_name":"core","schema_name":"analytics","table_name":"seed_fx_transactions","node_type":"dbt-seed","image_tag":"v1"}]`,
		"initiated_by":  "system",
	}
	for k, v := range overrides {
		if v == nil {
			delete(values, k)
			continue
		}
		values[k] = v
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

// nodes crosses the wire as a JSON array inside one field: a Redis stream field
// holds only a scalar, so the publisher encodes any non-scalar payload value as
// JSON before XADD.
func TestParsePromotedSeedsRun_DecodesNodesFromTheJSONField(t *testing.T) {
	in, err := redis.ParsePromotedSeedsRun(promotedSeedsMsg(nil))
	require.NoError(t, err)

	assert.Equal(t, "6f1a2b3c-4d5e-4f60-8a9b-0c1d2e3f4a5b", in.RunID)
	assert.Equal(t, "promote-seed-6f1a2b3c", in.ScheduleName)
	assert.Equal(t, "rel-1", in.ReleaseID)
	require.Len(t, in.Nodes, 2)
	assert.Equal(t, "core", in.Nodes[0].ServiceName)
	assert.Equal(t, "analytics", in.Nodes[0].SchemaName)
	assert.Equal(t, "seed_users", in.Nodes[0].TableName)
	assert.Equal(t, "seed_fx_transactions", in.Nodes[1].TableName)
	// The release-pinned metadata must survive the wire: without it the run would
	// fall back to whatever image the topology currently holds.
	assert.Equal(t, "dbt-seed", in.Nodes[0].NodeType)
	assert.Equal(t, "v1", in.Nodes[0].ImageTag)
}

// Every rejection is permanent: the consumer must ACK and drop a message that
// can never parse rather than redeliver it forever.
func TestParsePromotedSeedsRun_RejectsMalformedMessagesPermanently(t *testing.T) {
	for _, tc := range []struct {
		name      string
		overrides map[string]interface{}
	}{
		{"missing schedule_id", map[string]interface{}{"schedule_id": nil}},
		{"schedule_id not a uuid", map[string]interface{}{"schedule_id": "not-a-uuid"}},
		{"missing release_id", map[string]interface{}{"release_id": nil}},
		{"missing nodes", map[string]interface{}{"nodes": nil}},
		{"nodes not JSON", map[string]interface{}{"nodes": "core.analytics.seed_users"}},
		{"nodes empty", map[string]interface{}{"nodes": "[]"}},
		{"node missing table_name", map[string]interface{}{
			"nodes": `[{"service_name":"core","schema_name":"analytics","node_type":"dbt-seed","image_tag":"v1"}]`,
		}},
		{"node missing pinned image_tag", map[string]interface{}{
			"nodes": `[{"service_name":"core","schema_name":"analytics","table_name":"seed_users","node_type":"dbt-seed"}]`,
		}},
		{"node missing pinned node_type", map[string]interface{}{
			"nodes": `[{"service_name":"core","schema_name":"analytics","table_name":"seed_users","image_tag":"v1"}]`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := redis.ParsePromotedSeedsRun(promotedSeedsMsg(tc.overrides))
			require.Error(t, err)
			assert.True(t, errors.Is(err, events.ErrPermanent),
				"want ErrPermanent so the consumer drops the message, got %v", err)
		})
	}
}
