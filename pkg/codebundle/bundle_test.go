package codebundle_test

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validBundle = `{
  "contract_version": 1,
  "release_id": "rel-1",
  "nodes": {
    "analytics.revenue": {
      "runtime": "dbt",
      "raw_code": "select 1",
      "compiled_code": "select 1 compiled",
      "config": {"materialized": "table"},
      "source_hash": "s1",
      "shared_code_hash": "m1",
      "config_hash": "c1",
      "content_hash": "sha256:abc",
      "code_unit_ids": ["service-a:macro.svc.m1"]
    }
  },
  "shared_code": {
    "service-a:macro.svc.m1": {"source": "select 1", "checksum": "u1",
                               "depends_on": ["service-a:macro.svc.m2"]},
    "service-a:macro.svc.m2": {"source": "select 2", "checksum": "u2", "depends_on": []}
  }
}`

func TestDecode_ReadsTheContract(t *testing.T) {
	b, err := codebundle.Decode([]byte(validBundle))
	require.NoError(t, err)
	assert.Equal(t, "rel-1", b.ReleaseID)
	n := b.Nodes["analytics.revenue"]
	assert.Equal(t, "dbt", n.Runtime)
	assert.Equal(t, "sha256:abc", n.ContentHash)
	assert.Equal(t, "s1", n.SourceHash)
	assert.Equal(t, "m1", n.SharedCodeHash)
	assert.Equal(t, "c1", n.ConfigHash)
	assert.Equal(t, []string{"service-a:macro.svc.m1"}, n.CodeUnitIDs)
	assert.Equal(t, map[string]any{"materialized": "table"}, n.Config)
	assert.Equal(t, "u2", b.SharedCode["service-a:macro.svc.m2"].Checksum)
	assert.Equal(t, []string{"service-a:macro.svc.m2"},
		b.SharedCode["service-a:macro.svc.m1"].DependsOn)
}

func TestDecode_RejectsUnsupportedContractVersion(t *testing.T) {
	_, err := codebundle.Decode([]byte(`{"contract_version": 2, "release_id": "r", "nodes": {}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contract_version")
}

func TestDecode_RejectsMalformedJSON(t *testing.T) {
	_, err := codebundle.Decode([]byte(`{"contract_version": 1,`))
	require.Error(t, err)
}

func TestDecode_RejectsNodeWithoutContentHash(t *testing.T) {
	_, err := codebundle.Decode([]byte(`{"contract_version": 1, "release_id": "r",
	  "nodes": {"a.b": {"runtime": "dbt"}}}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "content_hash")
}

func TestDecode_AllowsEmptyNodeMap(t *testing.T) {
	b, err := codebundle.Decode([]byte(`{"contract_version": 1, "release_id": "r", "nodes": {}}`))
	require.NoError(t, err)
	assert.Empty(t, b.Nodes)
}

func TestDecode_ReadsHashedConfigAndCompiledHashAndIgnoresTestsAndProvenance(t *testing.T) {
	raw := `{"contract_version":1,"release_id":"r","nodes":{"a.b":{"runtime":"dbt","content_hash":"sha256:x",
	  "compiled_hash":"6e0f","hashed_config":{"materialized":"table","relation":{"schema":"a","alias":"b"}}}},
	  "shared_code":{},"tests":{"test.p.t":{"compiled_code":"select 1"}},"provenance":{"core":{"dbt_version":"1.12.0b1"}}}`
	b, err := codebundle.Decode([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, "6e0f", b.Nodes["a.b"].CompiledHash)
	assert.Equal(t, "table", b.Nodes["a.b"].HashedConfig["materialized"])
	assert.Empty(t, b.Nodes["a.b"].Projection, "a dbt node carries no projection")
}

func TestDecode_ReadsAPythonNodesProjectionBesideItsRawCode(t *testing.T) {
	raw := `{"contract_version":1,"release_id":"r","nodes":{"analytics.kpis":{"runtime":"python","content_hash":"sha256:x",
	  "raw_code":"{\"kind\": \"python-node\", \"owner\": \"e2e\"}","compiled_hash":"",
	  "projection":"{\"kind\": \"python-node\", \"table\": \"kpis\"}"}},"shared_code":{}}`
	b, err := codebundle.Decode([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, `{"kind": "python-node", "table": "kpis"}`, b.Nodes["analytics.kpis"].Projection)
	assert.Contains(t, b.Nodes["analytics.kpis"].RawCode, `"owner"`, "raw_code stays the contract entry")
}
