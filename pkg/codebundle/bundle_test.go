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
