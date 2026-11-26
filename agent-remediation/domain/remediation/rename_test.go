package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextModelName(t *testing.T) {
	none := func(string) bool { return false }
	taken := func(names ...string) func(string) bool {
		set := map[string]bool{}
		for _, n := range names {
			set[n] = true
		}
		return func(n string) bool { return set[n] }
	}
	assert.Equal(t, "table_b_v2", NextModelName("table_b", none))
	assert.Equal(t, "table_b_v4", NextModelName("table_b", taken("table_b_v2", "table_b_v3")))
	assert.Equal(t, "orders_v3", NextModelName("orders_v2", none), "a versioned name goes on from its version")
	assert.Equal(t, "orders_v12", NextModelName("orders_v11", none))
}

func TestRefsModel(t *testing.T) {
	assert.True(t, RefsModel("select * from {{ ref('table_b') }}", "marketing", "table_b"))
	assert.True(t, RefsModel(`select * from {{ ref( "table_b" ) }}`, "marketing", "table_b"))
	assert.True(t, RefsModel("select * from {{ ref('table_b', v=2) }}", "marketing", "table_b"), "a versioned ref")
	assert.True(t, RefsModel("def model(dbt, session):\n    return dbt.ref('table_b')", "marketing", "table_b"), "a Python model")
	assert.True(t, RefsModel("tests:\n  - relationships:\n      to: ref('table_b')\n", "marketing", "table_b"), "a relationships test")
	assert.True(t, RefsModel("select * from {{ ref('marketing', 'table_b') }}", "marketing", "table_b"), "a ref qualified with the own project")
	assert.False(t, RefsModel("select * from {{ ref('shared', 'table_b') }}", "marketing", "table_b"), "another package's model of that name")
	assert.False(t, RefsModel("select * from {{ ref('marketing', 'table_b') }}", "", "table_b"), "with no project known, a qualified ref is not this model")
	assert.False(t, RefsModel("select * from {{ ref('table_b_v2') }}", "marketing", "table_b"))
	assert.False(t, RefsModel("select * from analytics.table_b", "marketing", "table_b"), "a physical name is not a ref")
}

func TestRelationTableAndSchema(t *testing.T) {
	assert.Equal(t, "orders", RelationTable("analytics.orders"))
	assert.Equal(t, "analytics", RelationSchema("analytics.orders"))
	assert.Equal(t, "orders", RelationTable("orders"))
	assert.Equal(t, "", RelationSchema("orders"))
}

func TestModelRename_Explanation(t *testing.T) {
	r := ModelRename{
		Project: "marketing", Relation: "analytics.table_b",
		OtherProject: "service-1", OtherFilePath: "services/service-1/models/table_b.sql",
		NewName: "table_b_v2",
	}
	assert.Equal(t,
		"analytics.table_b is already built by service-1 (services/service-1/models/table_b.sql). "+
			"This release added a second model that builds it in marketing, so continuo refused it. "+
			"The new model is renamed to table_b_v2 so the release can pass; rename it as you prefer before merging.",
		r.Explanation())

	r.ChangesAlias, r.NewName = true, "orders_v2"
	assert.Contains(t, r.Explanation(), "The new model's alias is changed to orders_v2 so the release can pass")
}
