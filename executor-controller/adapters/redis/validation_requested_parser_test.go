// executor-controller/adapters/redis/validation_requested_parser_test.go
package redis

import (
	"encoding/json"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validationPayload mirrors the flat JSON body release-controller emits in the
// "payload" field of a validation.requested:v1 message (see
// release-controller/service/handlers/handle_parsed_manifest.go). Per-node
// identity is keyed by "unique_id".
func validationPayload() map[string]any {
	return map[string]any{
		"release_id": "rel-123",
		"mode":       "validation",
		"nodes": []map[string]any{
			{
				"unique_id":    "model.shop.orders",
				"service_name": "shop",
				"node_type":    "dbt-model",
				"schema_name":  "public",
				"table_name":   "orders",
				"image_tag":    "sha-abc",
			},
			{
				"unique_id":    "model.shop.customers",
				"service_name": "shop",
				"node_type":    "dbt-seed",
				"schema_name":  "public",
				"table_name":   "customers",
				"image_tag":    "sha-def",
			},
		},
		"node_ids_in_order": []string{"model.shop.orders", "model.shop.customers"},
		"image_tags":        map[string]string{"shop": "sha-abc"},
		"candidate_schema":  "_candidate_rel_123",
		"dbt_flags":         []string{"--empty"},
	}
}

// validationMsg builds a goredis.XMessage whose "payload" field holds the
// marshaled JSON body, matching the outbox publisher's wire format.
func validationMsg(t *testing.T, payload map[string]any, outboxEntryID string) goredis.XMessage {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	values := map[string]interface{}{"payload": string(body)}
	if outboxEntryID != "" {
		values["outbox_entry_id"] = outboxEntryID
	}
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseValidationRequested_HappyPath(t *testing.T) {
	outboxEntryID := uuid.New()
	msg := validationMsg(t, validationPayload(), outboxEntryID.String())

	evt, err := ParseValidationRequested(msg)
	require.NoError(t, err)

	assert.Equal(t, outboxEntryID, evt.OutboxEntryID)
	assert.Equal(t, "rel-123", evt.ReleaseID)
	assert.Equal(t, "validation", evt.Mode)
	assert.Equal(t, "_candidate_rel_123", evt.CandidateSchema)
	assert.Equal(t, []string{"--empty"}, evt.DBTFlags)
	assert.Equal(t, map[string]string{"shop": "sha-abc"}, evt.ImageTags)
	assert.Equal(t, []string{"model.shop.orders", "model.shop.customers"}, evt.NodeIDsInOrder)

	require.Len(t, evt.Nodes, 2)
	assert.Equal(t, "model.shop.orders", evt.Nodes[0].NodeID)
	assert.Equal(t, "shop", evt.Nodes[0].ServiceName)
	assert.Equal(t, "public", evt.Nodes[0].SchemaName)
	assert.Equal(t, "orders", evt.Nodes[0].TableName)
	assert.Equal(t, pkg_model.NodeTypeDbtModel, evt.Nodes[0].NodeType)
	assert.Equal(t, "sha-abc", evt.Nodes[0].ImageTag)
	assert.Equal(t, "model.shop.customers", evt.Nodes[1].NodeID)
	assert.Equal(t, pkg_model.NodeTypeDbtSeed, evt.Nodes[1].NodeType)
}

func TestParseValidationRequested_OutboxEntryIDAbsentIsNilUUID(t *testing.T) {
	msg := validationMsg(t, validationPayload(), "")
	evt, err := ParseValidationRequested(msg)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, evt.OutboxEntryID,
		"absent outbox_entry_id is uuid.Nil so dedup degrades to (msg.ID, stream_name)")
}

func TestParseValidationRequested_OutboxEntryIDInvalidIsError(t *testing.T) {
	msg := validationMsg(t, validationPayload(), "not-a-uuid")
	_, err := ParseValidationRequested(msg)
	require.Error(t, err)
}

func TestParseValidationRequested_MissingPayloadIsError(t *testing.T) {
	_, err := ParseValidationRequested(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	require.Error(t, err)
}

func TestParseValidationRequested_MalformedPayloadIsError(t *testing.T) {
	_, err := ParseValidationRequested(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": "{not json",
	}})
	require.Error(t, err)
}

func TestParseValidationRequested_MissingReleaseID(t *testing.T) {
	p := validationPayload()
	delete(p, "release_id")
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationRequested_WrongMode(t *testing.T) {
	p := validationPayload()
	p["mode"] = "deploy"
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationRequested_EmptyNodes(t *testing.T) {
	p := validationPayload()
	p["nodes"] = []map[string]any{}
	p["node_ids_in_order"] = []string{}
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationRequested_NodeMissingField(t *testing.T) {
	p := validationPayload()
	nodes := p["nodes"].([]map[string]any)
	delete(nodes[0], "schema_name")
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationRequested_NodeMissingUniqueID(t *testing.T) {
	p := validationPayload()
	nodes := p["nodes"].([]map[string]any)
	delete(nodes[0], "unique_id")
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

func TestParseValidationRequested_InvalidNodeType(t *testing.T) {
	p := validationPayload()
	nodes := p["nodes"].([]map[string]any)
	nodes[0]["node_type"] = "no_such_type"
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}

// TestParseValidationRequested_UpstreamNodeIDsRoundTrip verifies that per-node
// upstream_node_ids present in the payload are threaded onto the parsed
// ValidationNode and absent upstream_node_ids produce a nil/empty slice rather
// than an error.
func TestParseValidationRequested_UpstreamNodeIDsRoundTrip(t *testing.T) {
	p := validationPayload()
	// Set upstream_node_ids on the second node; leave the first without the field.
	nodes := p["nodes"].([]map[string]any)
	nodes[1]["upstream_node_ids"] = []string{"model.shop.orders"}

	evt, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.NoError(t, err)

	// First node has no upstream_node_ids in payload → empty slice.
	assert.Empty(t, evt.Nodes[0].UpstreamNodeIDs,
		"node without upstream_node_ids must have empty slice")

	// Second node has upstream_node_ids threaded through.
	assert.Equal(t, []string{"model.shop.orders"}, evt.Nodes[1].UpstreamNodeIDs,
		"upstream_node_ids must round-trip from wire payload to ValidationNode")
}

func TestParseValidationRequested_NodesMismatchOrderList(t *testing.T) {
	p := validationPayload()
	// node_ids_in_order references a node not present in nodes[].unique_id.
	p["node_ids_in_order"] = []string{"model.shop.orders", "model.shop.missing"}
	_, err := ParseValidationRequested(validationMsg(t, p, ""))
	require.Error(t, err)
}
