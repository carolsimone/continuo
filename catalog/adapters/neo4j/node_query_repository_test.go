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

// newTestQueryRepo builds a NodeQueryRepository against the live Neo4j and returns
// it together with a cleanup function that wipes any nodes created by the test.
// It intentionally re-uses the env conventions already in use by run_repository_test.go
// (NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD with the docker-compose defaults).
func newTestQueryRepo(t *testing.T) (*neo4jinfra.NodeQueryRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t) // already defined in run_repository_test.go
	repo := neo4jinfra.NewNodeQueryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	cleanup := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer session.Close(ctx)
		_, _ = session.Run(ctx, "MATCH (r:Run {test_marker: $m}) DETACH DELETE r", map[string]any{"m": t.Name()})
	}
	return repo, client, cleanup
}

func TestGetNodeLocation_ReturnsPathAndService(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()
	marker := t.Name()

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := s.Run(ctx, `
        CREATE (:Table {unique_id:'analytics.loc_target', service_name:'service-1',
                        schema_name:'analytics', table_name:'loc_target',
                        original_file_path:'models/loc_target.sql', active:true,
                        test_marker:$m})
    `, map[string]any{"m": marker})
	require.NoError(t, err)
	s.Close(ctx)
	t.Cleanup(func() {
		ws := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer ws.Close(ctx)
		_, _ = ws.Run(ctx, "MATCH (t:Table {test_marker:$m}) DETACH DELETE t",
			map[string]any{"m": marker})
	})

	loc, err := repo.GetNodeLocation(ctx, "analytics.loc_target")
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

	_, err := repo.GetNodeLocation(context.Background(), "analytics.absent")
	require.True(t, errors.Is(err, domain.ErrNodeNotFound))
}
