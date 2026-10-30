package neo4jinfra_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedGetNodeFixture creates a single live :Node of nodeQueryTenant for
// GetNode integration coverage. Omitting testCount (pass nil) mirrors a node that
// predates test_count capture, so the property is unset rather than zero.
func seedGetNodeFixture(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, service, schema, table, nodeType string, testCount *int) {
	t.Helper()
	seedGetNode(t, ctx, client, service, schema, table, nodeType, testCount, true)
}

// seedGetNode creates a single :Node of nodeQueryTenant with an explicit
// active flag, so a node a later release retired (active: false) can be
// seeded.
func seedGetNode(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, service, schema, table, nodeType string, testCount *int, active bool) {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	params := map[string]any{
		"tenant_id": nodeQueryTenant,
		"service":   service,
		"schema":    schema,
		"table":     table,
		"nodeType":  nodeType,
		"active":    active,
	}
	query := `CREATE (:Node {tenant_id: $tenant_id, service_name: $service, schema_name: $schema, table_name: $table, active: $active, node_type: $nodeType})`
	if testCount != nil {
		query = `CREATE (:Node {tenant_id: $tenant_id, service_name: $service, schema_name: $schema, table_name: $table, active: $active, node_type: $nodeType, test_count: $testCount})`
		params["testCount"] = *testCount
	}
	_, err := session.Run(ctx, query, params)
	require.NoError(t, err)
}

func TestNodeQueryRepository_GetNode_KnownTestCount(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-known", t.Name())
	schema, table := "an", "fct_known"
	tc := 3
	seedGetNodeFixture(t, ctx, client, service, schema, table, "dbt-model", &tc)

	got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.ActiveNodesOnly)
	require.NoError(t, err)
	assert.Equal(t, "dbt-model", got.NodeType)
	assert.Equal(t, 3, got.TestCount)
	assert.True(t, got.TestCountKnown)
	assert.False(t, got.Inactive)
}

func TestNodeQueryRepository_GetNode_UnknownTestCount(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-unknown", t.Name())
	schema, table := "an", "fct_unknown"
	seedGetNodeFixture(t, ctx, client, service, schema, table, "dbt-model", nil)

	got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.ActiveNodesOnly)
	require.NoError(t, err)
	assert.Equal(t, 0, got.TestCount)
	assert.False(t, got.TestCountKnown, "test_count property unset must report TestCountKnown=false, not a misleading zero")
}

func TestNodeQueryRepository_GetNode_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	_, err := repo.GetNode(ctx, nodeQueryTenant, fmt.Sprintf("svc-%s-absent", t.Name()), "an", "does_not_exist", domain.IncludeInactiveNodes)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}

func TestNodeQueryRepository_GetNode_InactiveIsNotFoundByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-inactive", t.Name())
	schema, table := "an", "fct_retired"
	seedGetNode(t, ctx, client, service, schema, table, "dbt-model", nil, false)

	_, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.ActiveNodesOnly)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound), "an inactive node must not match with ActiveNodesOnly")
}

func TestNodeQueryRepository_GetNode_IncludeInactiveReturnsInactiveNode(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-inactive", t.Name())
	schema, table := "an", "seed_retired"
	tc := 2
	seedGetNode(t, ctx, client, service, schema, table, "dbt-seed", &tc, false)

	got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.IncludeInactiveNodes)
	require.NoError(t, err)
	assert.Equal(t, "dbt-seed", got.NodeType)
	assert.True(t, got.Inactive)
	assert.Equal(t, 2, got.TestCount)
	assert.True(t, got.TestCountKnown)
}

func TestNodeQueryRepository_GetNode_IncludeInactiveReportsActiveNodeAsActive(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-active", t.Name())
	schema, table := "an", "fct_live"
	seedGetNodeFixture(t, ctx, client, service, schema, table, "dbt-model", nil)

	got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.IncludeInactiveNodes)
	require.NoError(t, err)
	assert.Equal(t, "dbt-model", got.NodeType)
	assert.False(t, got.Inactive)
}

func TestNodeQueryRepository_GetNode_IncludeInactivePrefersActiveMatch(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-both", t.Name())
	schema, table := "an", "fct_both"
	seedGetNode(t, ctx, client, service, schema, table, "dbt-seed", nil, false)
	seedGetNode(t, ctx, client, service, schema, table, "dbt-model", nil, true)

	got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.IncludeInactiveNodes)
	require.NoError(t, err)
	assert.Equal(t, "dbt-model", got.NodeType, "the live :Node must win over a retired one with the same identity")
	assert.False(t, got.Inactive)
}

// seedRetiredGetNode creates a retired :Node of nodeQueryTenant with an
// explicit unique_id and retired_at, so several retired nodes can share one
// identity.
func seedRetiredGetNode(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, service, schema, table, uniqueID, nodeType string, retiredAt time.Time) {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx, `CREATE (:Node {tenant_id: $tenant_id, service_name: $service, schema_name: $schema, table_name: $table,
		unique_id: $uid, active: false, node_type: $nodeType, retired_at: $retiredAt})`,
		map[string]any{"tenant_id": nodeQueryTenant, "service": service, "schema": schema, "table": table, "uid": uniqueID, "nodeType": nodeType, "retiredAt": retiredAt.UTC()})
	require.NoError(t, err)
}

func TestNodeQueryRepository_GetNode_MostRecentlyRetiredWinsAmongInactive(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s", t.Name())
	schema, table := "an", "fct_moved"
	now := time.Now()
	// The older row is created last and has the smaller unique_id, so neither
	// creation order nor unique_id order alone can pick the newer row.
	seedRetiredGetNode(t, ctx, client, service, schema, table, "seed.a", "dbt-seed", now.Add(-time.Hour))
	seedRetiredGetNode(t, ctx, client, service, schema, table, "model.z", "dbt-model", now)
	seedRetiredGetNode(t, ctx, client, service, schema, table, "seed.b", "dbt-seed", now.Add(-2*time.Hour))

	for i := 0; i < 5; i++ {
		got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.IncludeInactiveNodes)
		require.NoError(t, err)
		assert.Equal(t, "dbt-model", got.NodeType, "call %d: the most recently retired row must win", i)
		assert.True(t, got.Inactive)
	}
}

func TestNodeQueryRepository_GetNode_EqualRetirementFallsBackToUniqueID(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s", t.Name())
	schema, table := "an", "fct_tie"
	at := time.Now()
	seedRetiredGetNode(t, ctx, client, service, schema, table, "b.model", "dbt-model", at)
	seedRetiredGetNode(t, ctx, client, service, schema, table, "a.seed", "dbt-seed", at)

	for i := 0; i < 5; i++ {
		got, err := repo.GetNode(ctx, nodeQueryTenant, service, schema, table, domain.IncludeInactiveNodes)
		require.NoError(t, err)
		assert.Equal(t, "dbt-seed", got.NodeType, "call %d: ties break on the smallest unique_id", i)
	}
}
