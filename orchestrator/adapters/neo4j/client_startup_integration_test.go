package neo4jinfra_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
)

// Rejected credentials are a value waiting cannot fix: against a real Neo4j the
// startup wait must return the authentication error at once instead of retrying
// until its budget is spent.
func TestNewNeo4jClientWithRetry_FailsFastOnRejectedCredentials(t *testing.T) {
	newTestClient(t) // skips when Neo4j is unavailable
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	policy := neo4jinfra.StartupPolicy{Timeout: time.Minute, InitialDelay: time.Second, MaxDelay: time.Second}
	start := time.Now()

	client, err := neo4jinfra.NewNeo4jClientWithRetry(
		context.Background(), neo4jURI(), neo4jUser(), "definitely-not-the-password", logger, policy)

	if err == nil {
		_ = client.Close(context.Background())
		t.Fatal("expected the wrong password to be rejected")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %s: rejected credentials were retried instead of failing fast", elapsed)
	}
}
