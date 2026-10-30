package neo4jinfra_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodeQueryTenant is the tenant the node query tests seed and read under.
const nodeQueryTenant = "node-query-test"

// newTestQueryRepo builds a NodeQueryRepository against the live Neo4j and
// returns it together with a cleanup function that deletes every node of
// nodeQueryTenant, and no other tenant's.
func newTestQueryRepo(t *testing.T) (*neo4jinfra.NodeQueryRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t)
	repo := neo4jinfra.NewNodeQueryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	cleanup := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer session.Close(ctx)
		res, err := session.Run(ctx, "MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n", map[string]any{"tenant_id": nodeQueryTenant})
		require.NoError(t, err)
		_, err = res.Consume(ctx)
		require.NoError(t, err)
	}
	cleanup()
	return repo, client, cleanup
}

func TestGetNodeLocation_ReturnsPathAndService(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := s.Run(ctx, `
        CREATE (:Node {tenant_id: $tenant_id, unique_id:'analytics.loc_target', service_name:'service-1',
                       schema_name:'analytics', table_name:'loc_target',
                       original_file_path:'models/loc_target.sql', active:true})
    `, map[string]any{"tenant_id": nodeQueryTenant})
	require.NoError(t, err)
	s.Close(ctx)

	loc, err := repo.GetNodeLocation(ctx, nodeQueryTenant, "analytics.loc_target")
	require.NoError(t, err)
	assert.Equal(t, "models/loc_target.sql", loc.FilePath)
	assert.Equal(t, "service-1", loc.ServiceName)
}

func TestGetNodeLocation_UnknownNode_ErrNodeNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()

	_, err := repo.GetNodeLocation(context.Background(), nodeQueryTenant, "analytics.absent")
	require.True(t, errors.Is(err, domain.ErrNodeNotFound))
}
