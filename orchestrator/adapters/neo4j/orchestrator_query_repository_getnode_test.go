package neo4jinfra_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedGetNodeFixture creates a single :Table row for GetNode integration
// coverage. Omitting testCount (pass nil) mirrors a node that predates
// test_count capture, so the property is unset rather than zero.
func seedGetNodeFixture(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, service, schema, table, nodeType string, testCount *int) {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	params := map[string]any{
		"service":  service,
		"schema":   schema,
		"table":    table,
		"nodeType": nodeType,
	}
	query := `CREATE (:Table {service_name: $service, schema_name: $schema, table_name: $table, active: true, node_type: $nodeType})`
	if testCount != nil {
		query = `CREATE (:Table {service_name: $service, schema_name: $schema, table_name: $table, active: true, node_type: $nodeType, test_count: $testCount})`
		params["testCount"] = *testCount
	}
	_, err := session.Run(ctx, query, params)
	require.NoError(t, err)
}

func TestOrchestratorQueryRepository_GetNode_KnownTestCount(t *testing.T) {
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
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `MATCH (t:Table {service_name: $service, schema_name: $schema, table_name: $table}) DETACH DELETE t`,
			map[string]any{"service": service, "schema": schema, "table": table})
	})

	got, err := repo.GetNode(ctx, service, schema, table)
	require.NoError(t, err)
	assert.Equal(t, "dbt-model", got.NodeType)
	assert.Equal(t, 3, got.TestCount)
	assert.True(t, got.TestCountKnown)
}

func TestOrchestratorQueryRepository_GetNode_UnknownTestCount(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	service := fmt.Sprintf("svc-%s-unknown", t.Name())
	schema, table := "an", "fct_unknown"
	seedGetNodeFixture(t, ctx, client, service, schema, table, "dbt-model", nil)
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `MATCH (t:Table {service_name: $service, schema_name: $schema, table_name: $table}) DETACH DELETE t`,
			map[string]any{"service": service, "schema": schema, "table": table})
	})

	got, err := repo.GetNode(ctx, service, schema, table)
	require.NoError(t, err)
	assert.Equal(t, 0, got.TestCount)
	assert.False(t, got.TestCountKnown, "test_count property unset must report TestCountKnown=false, not a misleading zero")
}

func TestOrchestratorQueryRepository_GetNode_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	_, err := repo.GetNode(ctx, fmt.Sprintf("svc-%s-absent", t.Name()), "an", "does_not_exist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}
