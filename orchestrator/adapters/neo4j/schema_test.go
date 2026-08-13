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
		"node_version_unique",
		"node_version_uid",
		"code_unit_unique",
		"code_unit_version_unique",
		"code_unit_version_unit_id",
	} {
		require.Truef(t, names[want], "expected schema object %q to exist after InitSchema", want)
	}
}

// TestInitSchema_NormalizesLegacyTerminalStatusCasing verifies the one-off data
// migration folds legacy uppercase terminal_status values down to the canonical
// lowercase form, and that already-lowercase values (and 'cancelled') are left
// untouched. Re-running InitSchema must remain a no-op.
func TestInitSchema_NormalizesLegacyTerminalStatusCasing(t *testing.T) {
	client := newTestClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	marker := t.Name()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	cleanup := func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, "MATCH (n) WHERE n.test_marker = $m DETACH DELETE n", map[string]any{"m": marker})
	}
	cleanup()
	defer cleanup()

	_, err := session.Run(ctx, `
		CREATE (:Run {run_id: $u1, terminal_status: 'SUCCEEDED', test_marker: $m})
		CREATE (:Run {run_id: $u2, terminal_status: 'FAILED',    test_marker: $m})
		CREATE (:Run {run_id: $u3, terminal_status: 'succeeded', test_marker: $m})
		CREATE (:Run {run_id: $u4, terminal_status: 'cancelled', test_marker: $m})
	`, map[string]any{
		"u1": marker + "-1", "u2": marker + "-2",
		"u3": marker + "-3", "u4": marker + "-4", "m": marker,
	})
	require.NoError(t, err)
	session.Close(ctx)

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "first apply")

	read := client.NewSession(ctx, neo4j.AccessModeRead)
	defer read.Close(ctx)
	got := map[string]string{}
	res, err := read.Run(ctx,
		"MATCH (r:Run) WHERE r.test_marker = $m RETURN r.run_id AS id, r.terminal_status AS ts",
		map[string]any{"m": marker})
	require.NoError(t, err)
	for res.Next(ctx) {
		rec := res.Record()
		id, _ := rec.Get("id")
		ts, _ := rec.Get("ts")
		got[id.(string)] = ts.(string)
	}
	require.NoError(t, res.Err())

	require.Equal(t, "succeeded", got[marker+"-1"], "uppercase SUCCEEDED must be lowercased")
	require.Equal(t, "failed", got[marker+"-2"], "uppercase FAILED must be lowercased")
	require.Equal(t, "succeeded", got[marker+"-3"], "already-lowercase value unchanged")
	require.Equal(t, "cancelled", got[marker+"-4"], "cancelled must be left untouched")

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "second apply must be a no-op")
}
