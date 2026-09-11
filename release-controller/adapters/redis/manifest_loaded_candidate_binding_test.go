package redis

import (
	"encoding/json"
	"log/slog"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsedManifestDTO_ToInput_Failed(t *testing.T) {
	raw := `{"release_id":"r1","status":"failed","failure_kind":"invalid_sql","detail":"1 node failed to parse: a.b",
	  "failed_nodes":[{"node_id":"a.b","kind":"invalid_sql","service":"s","file_path":"models/b.sql","node_type":"dbt-model","detail":"Expecting )"}]}`
	var dto parsedManifestDTO
	require.NoError(t, json.Unmarshal([]byte(raw), &dto))
	in := dto.toInput(slog.Default())
	assert.Equal(t, pkg_model.ParseFailureKindInvalidSQL, in.FailureKind)
	assert.Equal(t, "1 node failed to parse: a.b", in.Detail)
	require.Len(t, in.FailedNodes, 1)
	assert.Equal(t, handlers.ParsedFailedNode{NodeID: "a.b", Kind: pkg_model.ParseFailureKindInvalidSQL, Service: "s",
		FilePath: "models/b.sql", NodeType: "dbt-model", Detail: "Expecting )"}, in.FailedNodes[0])
}

func TestParsedManifestDTO_ToInput_UnknownKindPassesThrough(t *testing.T) {
	var dto parsedManifestDTO
	require.NoError(t, json.Unmarshal([]byte(`{"release_id":"r1","status":"failed","failure_kind":"from_the_future","detail":"x"}`), &dto))
	in := dto.toInput(slog.Default())
	assert.False(t, in.FailureKind.IsValid())
	assert.Equal(t, "internal_error", handlers.ParseReason(in.FailureKind))
}
