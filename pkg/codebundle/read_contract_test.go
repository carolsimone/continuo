package codebundle_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readSetV1 is the read set of bundle contract_version 1, as docs/read-contract.md
// declares it: the paths an external consumer reads, each with the JSON type it
// has. testdata/bundle_v1.json is the document topology-controller writes
// (a copy of its tests/fixtures/code_bundle_v1.json), so these paths hold for
// what is written, not only for what this package decodes.
//
// A path that is removed, renamed or retyped breaks every reader of version 1
// and needs a new contract_version. A path added here must be added to the
// declaration too.
var readSetV1 = []struct {
	path []string
	kind string
}{
	{[]string{"contract_version"}, "number"},
	{[]string{"release_id"}, "string"},
	{[]string{"nodes"}, "object"},
	{[]string{"nodes", "analytics.revenue", "runtime"}, "string"},
	{[]string{"nodes", "analytics.revenue", "raw_code"}, "string"},
	{[]string{"nodes", "analytics.revenue", "compiled_code"}, "string"},
	{[]string{"nodes", "analytics.revenue", "config", "meta", "owner"}, "string"},
	{[]string{"nodes", "analytics.table_test", "runtime"}, "string"},
	{[]string{"nodes", "analytics.table_test", "raw_code"}, "string"},
}

// pythonRawCodeReadSetV1 is the read set inside a python node's raw_code, which
// is JSON text: the node's contract entry.
var pythonRawCodeReadSetV1 = []struct {
	key  string
	kind string
}{
	{"kind", "string"},
	{"owner", "string"},
	{"reads", "object"},
}

func kindOf(v any) string {
	switch v.(type) {
	case float64:
		return "number"
	case string:
		return "string"
	case map[string]any:
		return "object"
	default:
		return "other"
	}
}

func loadFixture(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/bundle_v1.json")
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	return doc
}

func TestReadSetV1_HoldsInTheWrittenDocument(t *testing.T) {
	doc := loadFixture(t)
	for _, want := range readSetV1 {
		var cur any = doc
		for _, seg := range want.path {
			obj, ok := cur.(map[string]any)
			require.Truef(t, ok, "path %v: %q is not under an object", want.path, seg)
			cur, ok = obj[seg]
			require.Truef(t, ok, "path %v: key %q is missing", want.path, seg)
		}
		assert.Equalf(t, want.kind, kindOf(cur), "path %v", want.path)
	}
}

func TestReadSetV1_PythonRawCodeIsJSONWithTheDeclaredKeys(t *testing.T) {
	doc := loadFixture(t)
	node := doc["nodes"].(map[string]any)["analytics.table_test"].(map[string]any)
	require.Equal(t, "python", node["runtime"])

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(node["raw_code"].(string)), &entry))
	for _, want := range pythonRawCodeReadSetV1 {
		v, ok := entry[want.key]
		require.Truef(t, ok, "raw_code key %q is missing", want.key)
		assert.Equalf(t, want.kind, kindOf(v), "raw_code key %q", want.key)
	}
	for name, sql := range entry["reads"].(map[string]any) {
		assert.Equalf(t, "string", kindOf(sql), "reads.%s is the SELECT text", name)
	}
}

func TestDecode_ReadsTheWrittenDocument(t *testing.T) {
	raw, err := os.ReadFile("testdata/bundle_v1.json")
	require.NoError(t, err)

	b, err := codebundle.Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", b.ReleaseID)
	assert.Equal(t, "dbt", b.Nodes["analytics.revenue"].Runtime)
	assert.Equal(t, "python", b.Nodes["analytics.table_test"].Runtime)
}

// Additive changes do not bump the version, so a reader of version 1 must
// ignore keys it does not know, at every level.
func TestDecode_IgnoresKeysItDoesNotKnow(t *testing.T) {
	doc := loadFixture(t)
	doc["added_at_top_level"] = "x"
	doc["nodes"].(map[string]any)["analytics.revenue"].(map[string]any)["added_on_a_node"] = []any{1}
	raw, err := json.Marshal(doc)
	require.NoError(t, err)

	b, err := codebundle.Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, "dbt", b.Nodes["analytics.revenue"].Runtime)
}

func TestDecode_RefusesTheNextVersionOfTheWrittenDocument(t *testing.T) {
	doc := loadFixture(t)
	doc["contract_version"] = codebundle.SupportedContractVersion + 1
	raw, err := json.Marshal(doc)
	require.NoError(t, err)

	_, err = codebundle.Decode(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contract_version")
}
