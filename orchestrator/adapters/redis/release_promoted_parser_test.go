package redis

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReleasePromoted_HappyPath(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"payload": `{"release_id":"rel-1","topology":[{"unique_id":"a","schema_name":"public","table_name":"orders","service_name":"svc-a","node_type":"dbt-seed","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":[]},{"unique_id":"b","schema_name":"public","table_name":"customers","service_name":"svc-a","node_type":"dbt-model","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":["a"]}],"image_tags":{"svc-a":"sha256:aaa"}}`,
		},
	}
	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	require.Len(t, evt.Topology, 2)
	assert.Equal(t, "a", evt.Topology[0].UniqueID)
	// node_type must round-trip: the run reader pulls seed upstreams into a
	// model's run only when they are typed "dbt-seed", so dropping it stalls
	// any promoted schedule with seed dependencies.
	assert.Equal(t, "dbt-seed", evt.Topology[0].NodeType)
	assert.Equal(t, "dbt-model", evt.Topology[1].NodeType)
	assert.Equal(t, []string{"a"}, evt.Topology[1].UpstreamUniqueIDs)
	assert.Equal(t, "sha256:aaa", evt.ImageTags["svc-a"])
}

func TestParseReleasePromoted_ParsesRuntimeManifestPin(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"payload": `{"release_id":"rel-1","topology":[{"unique_id":"public.orders","schema_name":"public","table_name":"orders","service_name":"svc-a","node_type":"dbt-model","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":[],"dbt_unique_id":"model.finance.orders","runtime_manifest_uri":"s3://artifacts/svc-a/rel-1/partial_parse.msgpack","runtime_manifest_sha256":"1111111111111111111111111111111111111111111111111111111111111111","runtime_manifest_dbt_version":"1.12.0b1","runtime_manifest_parse_context_sha256":"2222222222222222222222222222222222222222222222222222222222222222"}],"image_tags":{"svc-a":"sha256:aaa"}}`,
		},
	}
	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	require.Len(t, evt.Topology, 1)
	n := evt.Topology[0]
	// The graph id and the dbt id are separate identities and must not collide.
	assert.Equal(t, "public.orders", n.UniqueID)
	assert.Equal(t, "model.finance.orders", n.DBTUniqueID)
	assert.Equal(t, "s3://artifacts/svc-a/rel-1/partial_parse.msgpack", n.RuntimeManifestURI)
	assert.Equal(t, "1111111111111111111111111111111111111111111111111111111111111111", n.RuntimeManifestSHA256)
	assert.Equal(t, "1.12.0b1", n.RuntimeManifestDBTVersion)
	assert.Equal(t, "2222222222222222222222222222222222222222222222222222222222222222", n.RuntimeManifestParseContextSHA256)
}

// A release promoted before runtime manifests existed carries no reference.
// That is a supported topology, not a poison message.
func TestParseReleasePromoted_NodeWithoutRuntimeManifestIsAccepted(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"payload": `{"release_id":"rel-1","topology":[{"unique_id":"public.orders","schema_name":"public","table_name":"orders","service_name":"svc-a","node_type":"dbt-model","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":[]}],"image_tags":{"svc-a":"sha256:aaa"}}`,
		},
	}
	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	require.Len(t, evt.Topology, 1)
	assert.Empty(t, evt.Topology[0].DBTUniqueID)
	assert.Empty(t, evt.Topology[0].RuntimeManifestURI)
}

// A half-filled reference cannot be executed and no retry can complete it, so
// it is rejected at the boundary rather than persisted into the topology and
// discovered at dispatch time.
func TestParseReleasePromoted_PartialRuntimeManifestIsPermanentError(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"payload": `{"release_id":"rel-1","topology":[{"unique_id":"public.orders","schema_name":"public","table_name":"orders","service_name":"svc-a","node_type":"dbt-model","image_tag":"sha256:aaa","schedule":"daily","upstream_unique_ids":[],"runtime_manifest_uri":"s3://artifacts/svc-a/rel-1/partial_parse.msgpack"}],"image_tags":{"svc-a":"sha256:aaa"}}`,
		},
	}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Contains(t, err.Error(), "public.orders")
}

func TestParseReleasePromoted_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromoted_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json"}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromoted_EmptyReleaseID(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"release_id":"","topology":[]}`}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromoted_NilTopology(t *testing.T) {
	// topology field absent from JSON → decoded as nil slice → permanent error.
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"release_id":"rel-1"}`}}
	_, err := ParseReleasePromoted(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseReleasePromoted_DecodesProvenanceAndChanged(t *testing.T) {
	raw := `{
		"release_id": "rA",
		"repo": "acme/demo",
		"commit_sha": "deadbeef",
		"promoted_at": "2026-06-18T10:00:00Z",
		"image_tags": {"svc-a": "sha-a"},
		"topology": [
			{"unique_id": "a", "service_name": "svc-a", "changed": false, "upstream_unique_ids": []},
			{"unique_id": "b", "service_name": "svc-a", "changed": true, "upstream_unique_ids": ["a"]}
		]
	}`
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": raw}}

	evt, err := ParseReleasePromoted(msg)
	require.NoError(t, err)
	assert.Equal(t, "acme/demo", evt.Repo)
	assert.Equal(t, "deadbeef", evt.CommitSHA)
	assert.Equal(t, time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC), evt.PromotedAt.UTC())
	require.Len(t, evt.Topology, 2)
	assert.False(t, evt.Topology[0].Changed)
	assert.True(t, evt.Topology[1].Changed)
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
