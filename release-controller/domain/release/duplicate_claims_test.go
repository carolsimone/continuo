package release

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func node(uniqueID, service, filePath string) Node {
	return Node{UniqueID: uniqueID, ServiceName: service, OriginalFilePath: filePath}
}

func TestDuplicateClaims_AcrossServices(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "marketing", "models/orders.sql"),
		node("analytics.customers", "finance", "models/customers.sql"),
		node("analytics.orders", "finance", "models/orders.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, "analytics.orders", claims[0].UniqueID)
	assert.Equal(t, []Claimant{
		{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
		{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
	}, claims[0].Claimants, "claimants are sorted by service then path")
}

func TestDuplicateClaims_WithinOneService(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		node("analytics.orders", "finance", "models/orders_v2.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	require.Len(t, claims[0].Claimants, 2)
	assert.Equal(t, "models/orders.sql", claims[0].Claimants[0].OriginalFilePath)
	assert.Equal(t, "models/orders_v2.sql", claims[0].Claimants[1].OriginalFilePath)
}

func TestDuplicateClaims_ThreeWay(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "sales", "models/orders.sql"),
		node("analytics.orders", "finance", "models/orders.sql"),
		node("analytics.orders", "marketing", "models/orders.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	require.Len(t, claims[0].Claimants, 3)
	assert.Equal(t, "finance", claims[0].Claimants[0].ServiceName)
	assert.Equal(t, "marketing", claims[0].Claimants[1].ServiceName)
	assert.Equal(t, "sales", claims[0].Claimants[2].ServiceName)
}

// unique_id values are lowercased where they are minted, in manifest-controller,
// so a differently-cased pair cannot reach this function in practice. The
// assertion pins the division of labour: normalization happens at the mint site
// and this function compares with plain equality. Folding case here as well
// would put the same rule in two layers, and this test fails if that happens.
func TestDuplicateClaims_DoesNotFoldCaseItself(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		node("analytics.Orders", "marketing", "models/orders.sql"),
	}

	assert.Empty(t, DuplicateClaims(topo))
}

func TestDuplicateClaims_NoFalsePositiveOnDistinctRelations(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		node("marketing.orders", "marketing", "models/orders.sql"),
		node("analytics.customers", "finance", "models/customers.sql"),
	}

	assert.Empty(t, DuplicateClaims(topo),
		"same schema or same table alone is not a collision; only both together")
}

func TestDuplicateClaims_MultipleCollisionsSortedByUniqueID(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		node("analytics.customers", "finance", "models/customers.sql"),
		node("analytics.orders", "marketing", "models/orders.sql"),
		node("analytics.customers", "sales", "models/customers.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 2)
	assert.Equal(t, "analytics.customers", claims[0].UniqueID)
	assert.Equal(t, "analytics.orders", claims[1].UniqueID)
}

func TestDuplicateClaim_TargetPrefersChangedService(t *testing.T) {
	c := DuplicateClaim{
		UniqueID: "analytics.orders",
		Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
		},
	}

	target, other := c.Target("marketing")

	assert.Equal(t, "marketing", target.ServiceName, "the service changing now is the one that renames")
	assert.Equal(t, "finance", other.ServiceName)
}

func TestDuplicateClaim_TargetFallsBackToFirstClaimant(t *testing.T) {
	c := DuplicateClaim{
		UniqueID: "analytics.orders",
		Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
		},
	}

	target, other := c.Target("sales")

	assert.Equal(t, "finance", target.ServiceName,
		"no claimant is the changed service (bootstrap): pick deterministically")
	assert.Equal(t, "marketing", other.ServiceName)
}

func TestFormatDuplicateClaims_NamesEveryClaimant(t *testing.T) {
	claims := []DuplicateClaim{{
		UniqueID: "analytics.orders",
		Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
		},
	}}

	got := FormatDuplicateClaims(claims)

	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql); "+
			"a relation may be produced by exactly one node — rename one of them",
		got)
}

func TestFormatDuplicateClaims_ThreeClaimantsAndTwoCollisions(t *testing.T) {
	claims := []DuplicateClaim{
		{UniqueID: "analytics.customers", Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/customers.sql"},
			{ServiceName: "sales", OriginalFilePath: "models/customers.sql"},
		}},
		{UniqueID: "analytics.orders", Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "sales", OriginalFilePath: "models/orders.sql"},
		}},
	}

	got := FormatDuplicateClaims(claims)

	assert.Equal(t,
		"analytics.customers is produced by finance (models/customers.sql) and sales (models/customers.sql); "+
			"analytics.orders is produced by finance (models/orders.sql), marketing (models/orders.sql) and sales (models/orders.sql); "+
			"a relation may be produced by exactly one node — rename one of them",
		got)
}
