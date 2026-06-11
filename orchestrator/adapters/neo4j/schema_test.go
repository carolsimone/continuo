package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// TestInitSchema_Idempotent verifies the schema DDL applies cleanly and that a
// second application is a no-op (no error), proving restart-safety.
func TestInitSchema_Idempotent(t *testing.T) {
	client := newTestClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "first apply")
	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "second apply must be a no-op")

	// The expected constraints and indexes must be present after init.
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	names := map[string]bool{}
	res, err := session.Run(ctx, "SHOW CONSTRAINTS YIELD name RETURN name", nil)
	require.NoError(t, err)
	for res.Next(ctx) {
		if v, ok := res.Record().Get("name"); ok {
			if s, ok := v.(string); ok {
				names[s] = true
			}
		}
	}
	require.NoError(t, res.Err())

	res, err = session.Run(ctx, "SHOW INDEXES YIELD name RETURN name", nil)
	require.NoError(t, err)
	for res.Next(ctx) {
		if v, ok := res.Record().Get("name"); ok {
			if s, ok := v.(string); ok {
				names[s] = true
			}
		}
	}
	require.NoError(t, res.Err())

	for _, want := range []string{
		"run_id_unique",
		"table_uid_unique",
		"table_fqn",
		"table_schedule",
		"run_schedule",
	} {
		require.Truef(t, names[want], "expected schema object %q to exist after InitSchema", want)
	}
}
