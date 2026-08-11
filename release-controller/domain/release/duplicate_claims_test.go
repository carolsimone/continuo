package release

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func node(uniqueID, service, filePath string) Node {
	return Node{UniqueID: uniqueID, ServiceName: service, OriginalFilePath: filePath}
}

func nodeOfType(uniqueID, service, filePath, nodeType string) Node {
	return Node{UniqueID: uniqueID, ServiceName: service, OriginalFilePath: filePath, NodeType: nodeType}
}

// nodeWithRelation sets ResolvedRelationID independently of UniqueID, so a
// test can construct a claimant whose declared identity and physical relation
// differ (an alias override) or coincide only by declared name (no alias).
func nodeWithRelation(uniqueID, relationID, service, filePath string) Node {
	return Node{UniqueID: uniqueID, ResolvedRelationID: relationID, ServiceName: service, OriginalFilePath: filePath}
}

// A python claimant's NodeType must survive into its Claimant so the
// remediation fixer can tell it apart from a dbt claimant without a second
// topology lookup.
func TestDuplicateClaims_CarriesNodeType(t *testing.T) {
	topo := Topology{
		nodeOfType("analytics.orders", "finance", "models/orders.sql", "dbt-model"),
		nodeOfType("analytics.orders", "marketing", "contract.yaml", "python-model"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, "dbt-model", claims[0].Claimants[0].NodeType)
	assert.Equal(t, "python-model", claims[0].Claimants[1].NodeType)
}

func TestDuplicateClaims_AcrossServices(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "marketing", "models/orders.sql"),
		node("analytics.customers", "finance", "models/customers.sql"),
		node("analytics.orders", "finance", "models/orders.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, "analytics.orders", claims[0].RelationID)
	assert.Equal(t, []Claimant{
		{UniqueID: "analytics.orders", ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
		{UniqueID: "analytics.orders", ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
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

// The test above uses node(), which leaves ResolvedRelationID empty, so both
// claimants fall back to comparing their (differently-cased) UniqueIDs — it
// exercises the fallback, not the relation key itself. This test pins the
// same case-folding invariant on the relation key directly: two claimants
// with distinct, differently-cased UniqueIDs and differently-cased
// ResolvedRelationIDs must not collide either.
func TestDuplicateClaims_DoesNotFoldCaseItself_RelationKey(t *testing.T) {
	topo := Topology{
		nodeWithRelation("analytics.orders_a", "analytics.orders", "finance", "models/orders_a.sql"),
		nodeWithRelation("analytics.orders_b", "analytics.Orders", "marketing", "models/orders_b.sql"),
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

func TestDuplicateClaims_MultipleCollisionsSortedByRelationID(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		node("analytics.customers", "finance", "models/customers.sql"),
		node("analytics.orders", "marketing", "models/orders.sql"),
		node("analytics.customers", "sales", "models/customers.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 2)
	assert.Equal(t, "analytics.customers", claims[0].RelationID)
	assert.Equal(t, "analytics.orders", claims[1].RelationID)
}

// Two nodes with DIFFERENT declared names (different unique_id) but the SAME
// resolved relation (both alias to "orders") write the same warehouse table.
// Grouping on unique_id alone would miss this entirely — the exact false
// negative the resolved-relation field exists to close. Each claimant keeps
// its own unique_id: they are genuinely different nodes.
func TestDuplicateClaims_SameAliasDifferentNames_CollisionFound(t *testing.T) {
	topo := Topology{
		nodeWithRelation("analytics.orders_v1", "analytics.orders", "finance", "models/orders_v1.sql"),
		nodeWithRelation("analytics.orders_v2", "analytics.orders", "marketing", "models/orders_v2.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, "analytics.orders", claims[0].RelationID)
	require.Len(t, claims[0].Claimants, 2)
	gotUniqueIDs := []string{claims[0].Claimants[0].UniqueID, claims[0].Claimants[1].UniqueID}
	assert.ElementsMatch(t, []string{"analytics.orders_v1", "analytics.orders_v2"}, gotUniqueIDs,
		"claimants of one relation collision can carry different unique_ids")
}

// Two nodes sharing a declared name (same unique_id) but resolving to
// DIFFERENT relations must still collide — as an identity collision, not a
// relation one. unique_id is the identity key for every downstream lookup
// keyed on it (the code bundle, the candidate object key, NodeRegistry,
// release-controller's own topology walks, the orchestrator's :Table MERGE);
// sharing it means one of the two nodes is silently erased wherever that key
// is used, independent of the fact that their resolved relations differ.
// This is also the exact state the duplicate-table fixer's own rename
// proposal can produce: the fixer can only edit a claimant's alias (it
// cannot rename the claimant's .sql file), so a fix that clears a RELATION
// collision leaves unique_id untouched — and if the underlying problem was
// actually two nodes sharing a declared name, the alias-only edit could
// never have cleared it.
func TestDuplicateClaims_SameNameDifferentAlias_IdentityCollision(t *testing.T) {
	topo := Topology{
		nodeWithRelation("analytics.orders", "analytics.orders_finance", "finance", "models/orders.sql"),
		nodeWithRelation("analytics.orders", "analytics.orders_marketing", "marketing", "models/orders.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, CollisionIdentity, claims[0].Kind)
	assert.Equal(t, "analytics.orders", claims[0].UniqueID)
	assert.Len(t, claims[0].Claimants, 2)
}

// A node whose ResolvedRelationID is empty (a payload from before that field
// existed, or a node the parser never set it on) falls back to its own
// UniqueID for grouping, exactly like the pre-existing behavior.
func TestDuplicateClaims_FallsBackToUniqueIDWhenResolvedRelationEmpty(t *testing.T) {
	topo := Topology{
		node("analytics.orders", "finance", "models/orders.sql"),
		nodeWithRelation("analytics.orders", "", "marketing", "models/orders.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 1)
	assert.Equal(t, "analytics.orders", claims[0].RelationID)
}

func TestDuplicateClaim_TargetPrefersChangedService(t *testing.T) {
	c := DuplicateClaim{
		RelationID: "analytics.orders",
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
		RelationID: "analytics.orders",
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
		RelationID: "analytics.orders",
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
		{RelationID: "analytics.customers", Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/customers.sql"},
			{ServiceName: "sales", OriginalFilePath: "models/customers.sql"},
		}},
		{RelationID: "analytics.orders", Claimants: []Claimant{
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

func TestFormatDuplicateClaims_IdentityCollisionNamesTheRemedy(t *testing.T) {
	claims := []DuplicateClaim{{
		Kind:     CollisionIdentity,
		UniqueID: "analytics.orders",
		Claimants: []Claimant{
			{ServiceName: "finance", OriginalFilePath: "models/orders.sql"},
			{ServiceName: "marketing", OriginalFilePath: "models/orders.sql"},
		},
	}}

	got := FormatDuplicateClaims(claims)

	assert.Equal(t,
		"unique_id analytics.orders is declared by finance (models/orders.sql) and marketing (models/orders.sql); "+
			"a unique_id must be declared by exactly one node — an alias rename cannot fix this, the declared model itself must be renamed",
		got)
}

// A release can trip a relation collision and an identity collision at the
// same time, on different pairs of nodes. Both must be named in the detail,
// and an operator (or anything parsing error_detail) must be able to tell
// which is which, because the remedies differ.
func TestDuplicateClaims_RelationAndIdentityCollisionsAreDistinguishableInOneRelease(t *testing.T) {
	topo := Topology{
		// Relation collision: different unique_ids, same resolved relation.
		nodeWithRelation("analytics.orders_v1", "analytics.orders", "finance", "models/orders_v1.sql"),
		nodeWithRelation("analytics.orders_v2", "analytics.orders", "marketing", "models/orders_v2.sql"),
		// Identity collision: same unique_id, different resolved relations.
		nodeWithRelation("analytics.customers", "analytics.customers_finance", "finance", "models/customers.sql"),
		nodeWithRelation("analytics.customers", "analytics.customers_sales", "sales", "models/customers.sql"),
	}

	claims := DuplicateClaims(topo)

	require.Len(t, claims, 2, "one relation collision and one identity collision")
	byKind := map[CollisionKind]DuplicateClaim{}
	for _, c := range claims {
		byKind[c.Kind] = c
	}
	require.Contains(t, byKind, CollisionRelation)
	require.Contains(t, byKind, CollisionIdentity)
	assert.Equal(t, "analytics.orders", byKind[CollisionRelation].RelationID)
	assert.Equal(t, "analytics.customers", byKind[CollisionIdentity].UniqueID)

	detail := FormatDuplicateClaims(claims)
	assert.Contains(t, detail, "analytics.orders is produced by", "relation collision is named as a relation")
	assert.Contains(t, detail, "unique_id analytics.customers is declared by", "identity collision is named as an identity")
	assert.Contains(t, detail, "rename one of them", "relation remedy: alias rename")
	assert.Contains(t, detail, "the declared model itself must be renamed", "identity remedy: cannot be fixed by alias rename")
}
