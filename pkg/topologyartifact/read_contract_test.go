package topologyartifact_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

// readSetV1 is the read set of topology schema_version 1, as
// docs/read-contract.md declares it: what an external consumer reads from the
// gunzipped object at Key(tenant, release). The golden fixture's canonical text
// is that gunzipped content, byte for byte, as every writer produces it.
//
// A key that is removed, renamed or retyped breaks every reader of version 1
// and needs a new SchemaVersion. A key added here must be added to the
// declaration too.
var (
	readSetV1Document = map[string]string{
		"schema_version": "number",
		"tenant_id":      "string",
		"release_id":     "string",
		"nodes":          "array",
	}
	readSetV1Node = map[string]string{
		"unique_id":            "string",
		"resolved_relation_id": "string",
		"service_name":         "string",
	}
)

func jsonKind(v any) string {
	switch v.(type) {
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	default:
		return "other"
	}
}

func TestReadSetV1_HoldsInTheWrittenDocument(t *testing.T) {
	fx := loadTopologyFixture(t)
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(fx.Canonical), &doc))

	for key, kind := range readSetV1Document {
		v, ok := doc[key]
		require.Truef(t, ok, "document key %q is missing", key)
		assert.Equalf(t, kind, jsonKind(v), "document key %q", key)
	}
	assert.EqualValues(t, topologyartifact.SchemaVersion, doc["schema_version"])

	nodes := doc["nodes"].([]any)
	require.NotEmpty(t, nodes, "the fixture must hold nodes for the node keys to be checked")
	for i, n := range nodes {
		node := n.(map[string]any)
		for key, kind := range readSetV1Node {
			v, ok := node[key]
			require.Truef(t, ok, "node %d: key %q is missing", i, key)
			assert.Equalf(t, kind, jsonKind(v), "node %d: key %q", i, key)
		}
	}
}

// Additive changes do not bump the version, so a reader of version 1 must
// ignore keys it does not know, at both levels.
func TestDecode_IgnoresKeysItDoesNotKnow(t *testing.T) {
	obj := gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"r","added":true,`+
		`"nodes":[{"unique_id":"a.b","service_name":"one","resolved_relation_id":"a.b","added_on_a_node":{"x":1}}]}`))

	d, err := topologyartifact.Decode(obj, sha(obj))
	require.NoError(t, err)
	require.Len(t, d.Nodes, 1)
	assert.Equal(t, "a.b", d.Nodes[0].ResolvedRelationID)
}
