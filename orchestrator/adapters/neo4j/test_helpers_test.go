package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
)

// neo4jURI returns the Neo4j bolt URI from the environment, defaulting to the
// docker-compose service name used in CI / local development.
func neo4jURI() string {
	if u := os.Getenv("NEO4J_URI"); u != "" {
		return u
	}
	return "bolt://neo4j:7687"
}

// neo4jUser returns the Neo4j user from the environment, defaulting to "neo4j".
func neo4jUser() string {
	if u := os.Getenv("NEO4J_USER"); u != "" {
		return u
	}
	return "neo4j"
}

// neo4jPassword returns the Neo4j password from the environment, defaulting to
// the development docker-compose value.
func neo4jPassword() string {
	if p := os.Getenv("NEO4J_PASSWORD"); p != "" {
		return p
	}
	return "atlas_password"
}

// newTestClient creates a Neo4j client for integration tests.
// Tests that call this will be skipped if Neo4j is unreachable.
func newTestClient(t *testing.T) neo4jinfra.Neo4jClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := neo4jinfra.NewNeo4jClient(neo4jURI(), neo4jUser(), neo4jPassword(), logger)
	if err != nil {
		t.Skipf("Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}
