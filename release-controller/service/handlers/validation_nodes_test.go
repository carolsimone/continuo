package handlers

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestValidationNodesInOrder_CarriesRelationNameOnlyWhenSet(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders", RelationName: "Orders_Tbl", NodeType: "dbt-model", ServiceName: "s", ImageTag: "t"},
		{UniqueID: "analytics.dim.v2", SchemaName: "analytics", TableName: "dim.v2", RelationName: "dim_v2", NodeType: "dbt-model", ServiceName: "s", ImageTag: "t"},
		{UniqueID: "analytics.plain", SchemaName: "analytics", TableName: "plain", NodeType: "dbt-model", ServiceName: "s", ImageTag: "t"},
	}
	ids := []string{"analytics.dim.v2", "analytics.orders", "analytics.plain"}
	in := map[string]bool{"analytics.dim.v2": true, "analytics.orders": true, "analytics.plain": true}
	got := validationNodesInOrder("b", "rel-1", topo, ids, in, in)
	assert.Equal(t, "dim_v2", got[0]["relation_name"])
	assert.Equal(t, "dim.v2", got[0]["table_name"])
	assert.Equal(t, "Orders_Tbl", got[1]["relation_name"])
	assert.NotContains(t, got[2], "relation_name")
}
