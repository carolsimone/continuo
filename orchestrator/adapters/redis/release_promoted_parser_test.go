package redis

import (
	"testing"

	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
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

// TestExtractOutboxEntryID_FromReleasePromotedMessage verifies that
// ExtractOutboxEntryID correctly parses outbox_entry_id from a
// release.promoted:v1 message, and returns nil when the field is absent.
func TestExtractOutboxEntryID_FromReleasePromotedMessage(t *testing.T) {
	const wantUUID = "7f3e4b2a-1c5d-4e8f-9012-3a4b5c6d7e8f"

	// Field present and valid.
	gotPtr := messageprocessing.ExtractOutboxEntryID(map[string]interface{}{
		"outbox_entry_id": wantUUID,
	})
	require.NotNil(t, gotPtr, "should parse a valid outbox_entry_id")
	assert.Equal(t, wantUUID, gotPtr.String())

	// Field absent — nil expected.
	nilPtr := messageprocessing.ExtractOutboxEntryID(map[string]interface{}{
		"payload": `{"release_id":"rel-1","topology":[]}`,
	})
	assert.Nil(t, nilPtr, "should return nil when outbox_entry_id is absent")

	// Field present but not a valid UUID.
	nilPtr2 := messageprocessing.ExtractOutboxEntryID(map[string]interface{}{
		"outbox_entry_id": "not-a-uuid",
	})
	assert.Nil(t, nilPtr2, "should return nil when outbox_entry_id is not a valid UUID")
}
