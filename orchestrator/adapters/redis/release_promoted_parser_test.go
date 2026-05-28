package redis

import (
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReleasePromoted_HappyPath(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"payload": `{"release_id":"rel-1","topology":[{"unique_id":"a","schema_name":"public","table_name":"orders","service_name":"svc-a","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":[]},{"unique_id":"b","schema_name":"public","table_name":"customers","service_name":"svc-a","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":["a"]}],"image_tags":{"svc-a":"sha256:aaa"}}`,
		},
	}
	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	require.Len(t, evt.Topology, 2)
	assert.Equal(t, "a", evt.Topology[0].UniqueID)
	assert.Equal(t, []string{"a"}, evt.Topology[1].UpstreamUniqueIDs)
	assert.Equal(t, "sha256:aaa", evt.ImageTags["svc-a"])
}

func TestParseReleasePromoted_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
}

func TestParseReleasePromoted_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json"}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
}

func TestParseReleasePromoted_EmptyReleaseID(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"release_id":"","topology":[]}`}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
}

func TestParseReleasePromoted_NilTopology(t *testing.T) {
	// topology field absent from JSON → decoded as nil slice → permanent error.
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"release_id":"rel-1"}`}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
}
