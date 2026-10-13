package topology_test

import (
	"testing"

	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func count(n int) *int { return &n }

func spec(schema, table, nodeType string, upstream ...string) topology.NodeSpec {
	return topology.NodeSpec{
		UniqueID: schema + "." + table, ServiceName: "svc", SchemaName: schema, TableName: table,
		NodeType: nodeType, TestCount: count(0), ContentHash: "h-" + schema + "-" + table, ImageTag: "svc:1",
		ScheduleName: "daily", UpstreamUniqueIDs: upstream,
	}
}

func testSpec(name, upstream string) topology.NodeSpec {
	return topology.NodeSpec{
		UniqueID: "test.svc." + name, ServiceName: "svc", SchemaName: "stg", TableName: name,
		NodeType: "dbt-test", UpstreamUniqueIDs: []string{upstream},
	}
}

func TestKeyOf_LowercasesSchemaAndTable(t *testing.T) {
	assert.Equal(t, topology.NodeKey("raw.orders"), topology.KeyOf("RAW", "Orders"))
}

func TestNewRelease_DropsTestsAndSortsNodesByKey(t *testing.T) {
	rel, err := topology.NewRelease("default", "rel-1", 7, "s3://continuo/k", "ab12", []topology.NodeSpec{
		spec("stg", "orders", "dbt-model", "raw.orders"),
		testSpec("not_null_orders_id", "stg.orders"),
		spec("raw", "orders", "dbt-seed"),
	})
	require.NoError(t, err)

	assert.Equal(t, "default", rel.TenantID)
	assert.Equal(t, "rel-1", rel.ReleaseID)
	assert.Equal(t, int64(7), rel.PromotionSeq)
	assert.Equal(t, "s3://continuo/k", rel.TopologyURI)
	require.Len(t, rel.Nodes, 2)
	assert.Equal(t, topology.NodeKey("raw.orders"), rel.Nodes[0].Key)
	assert.Equal(t, topology.Node{
		Key: "stg.orders", UniqueID: "stg.orders", ServiceName: "svc", SchemaName: "stg", TableName: "orders",
		NodeType: "dbt-model", TestCount: count(0), ContentHash: "h-stg-orders", ImageTag: "svc:1", ScheduleName: "daily",
	}, rel.Nodes[1])
}

// An upstream that names a dbt test or a node the release does not hold yields
// no edge, and a repeated upstream yields one.
func TestNewRelease_DerivesEdgesBetweenNonTestNodes(t *testing.T) {
	rel, err := topology.NewRelease("default", "rel-1", 1, "u", "s", []topology.NodeSpec{
		spec("mart", "revenue", "dbt-model", "stg.orders", "stg.orders", "raw.customers", "test.svc.not_null_orders_id", "ext.unknown"),
		spec("stg", "orders", "dbt-model", "raw.orders"),
		spec("raw", "orders", "dbt-seed"),
		spec("raw", "customers", "dbt-seed"),
		testSpec("not_null_orders_id", "stg.orders"),
	})
	require.NoError(t, err)
	assert.Equal(t, []topology.Edge{
		{Upstream: "raw.customers", Downstream: "mart.revenue"},
		{Upstream: "raw.orders", Downstream: "stg.orders"},
		{Upstream: "stg.orders", Downstream: "mart.revenue"},
	}, rel.Edges)
}

func TestNewRelease_WithoutUpstreamsHasNoEdges(t *testing.T) {
	rel, err := topology.NewRelease("default", "rel-1", 1, "u", "s", []topology.NodeSpec{spec("raw", "orders", "dbt-seed")})
	require.NoError(t, err)
	assert.NotNil(t, rel.Edges)
	assert.Empty(t, rel.Edges)
}

func TestNewRelease_RejectsTwoNodesWithOneKey(t *testing.T) {
	_, err := topology.NewRelease("default", "rel-1", 1, "u", "s", []topology.NodeSpec{
		spec("raw", "orders", "dbt-seed"),
		{UniqueID: "RAW.Orders", ServiceName: "svc-2", SchemaName: "RAW", TableName: "Orders", NodeType: "python-csv"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, topology.ErrDuplicateNodeKey)
	assert.Contains(t, err.Error(), `"raw.orders"`)
}

// A dbt test is dropped before keys are compared, so a test that shares its
// model's schema and table is not a collision.
func TestNewRelease_TestsNeverCollide(t *testing.T) {
	rel, err := topology.NewRelease("default", "rel-1", 1, "u", "s", []topology.NodeSpec{
		spec("stg", "orders", "dbt-model"),
		testSpec("orders", "stg.orders"),
	})
	require.NoError(t, err)
	assert.Len(t, rel.Nodes, 1)
}

func TestNewRelease_LowercasesTheChecksum(t *testing.T) {
	rel, err := topology.NewRelease("default", "rel-1", 1, "u", "ABCDEF01", nil)
	require.NoError(t, err)
	assert.Equal(t, "abcdef01", rel.TopologySHA256)
	assert.Empty(t, rel.Nodes)
}

// An unknown test count stays unknown, and a known one is copied rather than
// shared with the caller's spec.
func TestNewRelease_CopiesTheTestCount(t *testing.T) {
	unknown := spec("raw", "a", "dbt-seed")
	unknown.TestCount = nil
	known := spec("raw", "b", "dbt-seed")
	known.TestCount = count(3)

	rel, err := topology.NewRelease("default", "rel-1", 1, "u", "s", []topology.NodeSpec{unknown, known})
	require.NoError(t, err)
	*known.TestCount = 9

	assert.Nil(t, rel.Nodes[0].TestCount)
	require.NotNil(t, rel.Nodes[1].TestCount)
	assert.Equal(t, 3, *rel.Nodes[1].TestCount)
}

func TestPointer_IsZero(t *testing.T) {
	assert.True(t, topology.Pointer{}.IsZero())
	assert.False(t, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}.IsZero())
}

func TestDecidePointer(t *testing.T) {
	live := topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 5}
	cases := []struct {
		name      string
		live      topology.Pointer
		releaseID string
		seq       int64
		want      topology.PointerOutcome
	}{
		{"no live release", topology.Pointer{}, "rel-1", 1, topology.PointerMoved},
		{"a higher seq of another release", live, "rel-2", 6, topology.PointerMoved},
		{"a higher seq of the live release", live, "rel-1", 6, topology.PointerMoved},
		{"the live promotion again", live, "rel-1", 5, topology.PointerRedelivered},
		{"another release under the live seq", live, "rel-2", 5, topology.PointerConflict},
		{"a lower seq of another release", live, "rel-0", 4, topology.PointerStale},
		{"a lower seq of the live release", live, "rel-1", 4, topology.PointerStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, topology.DecidePointer(tc.live, tc.releaseID, tc.seq))
		})
	}
}

func TestPointerOutcome_String(t *testing.T) {
	assert.Equal(t, "moved", topology.PointerMoved.String())
	assert.Equal(t, "redelivered", topology.PointerRedelivered.String())
	assert.Equal(t, "stale", topology.PointerStale.String())
	assert.Equal(t, "conflict", topology.PointerConflict.String())
	assert.Equal(t, "unknown", topology.PointerOutcome(0).String())
}
