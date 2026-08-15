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
		"rejection_unique",
		"rejection_node_id",
		"rejection_category_reason",
		"error_signature_unique",
		"error_signature_category_reason",
		"proposal_unique",
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

// TestDataMigration_RemovesTableLastStamps verifies the data migration strips
// the retired last_commit_sha/last_repo/last_changed_at/last_release_id stamp
// from a :Table while leaving the node's other properties untouched, and that
// re-running InitSchema against an already-clean node stays a no-op.
func TestDataMigration_RemovesTableLastStamps(t *testing.T) {
	client := newTestClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := context.Background()

	marker := t.Name()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	cleanup := func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, "MATCH (t:Table {unique_id: $u}) DETACH DELETE t", map[string]any{"u": marker})
	}
	cleanup()
	defer cleanup()

	_, err := session.Run(ctx, `
		CREATE (:Table {
			unique_id: $u, schema_name: 'p', table_name: 't', service_name: 's',
			last_commit_sha: 'a1', last_repo: 'acme/demo',
			last_changed_at: datetime(), last_release_id: 'rel-1'
		})
	`, map[string]any{"u": marker})
	require.NoError(t, err)
	session.Close(ctx)

	assertStampsGone := func(label string) {
		read := client.NewSession(ctx, neo4j.AccessModeRead)
		defer read.Close(ctx)
		res, err := read.Run(ctx, `
			MATCH (t:Table {unique_id: $u})
			RETURN t.last_commit_sha AS c, t.last_repo AS r, t.last_changed_at AS at,
			       t.last_release_id AS rel, t.schema_name AS schema_name,
			       t.table_name AS table_name, t.service_name AS service_name
		`, map[string]any{"u": marker})
		require.NoError(t, err)
		require.True(t, res.Next(ctx))
		rec := res.Record()
		for _, k := range []string{"c", "r", "at", "rel"} {
			v, _ := rec.Get(k)
			require.Nilf(t, v, "%s: last_* property %q must be gone", label, k)
		}
		schemaName, _ := rec.Get("schema_name")
		tableName, _ := rec.Get("table_name")
		serviceName, _ := rec.Get("service_name")
		require.Equal(t, "p", schemaName, "%s: schema_name must survive the migration", label)
		require.Equal(t, "t", tableName, "%s: table_name must survive the migration", label)
		require.Equal(t, "s", serviceName, "%s: service_name must survive the migration", label)
	}

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "first apply")
	assertStampsGone("after first InitSchema")

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "second apply must be a no-op")
	assertStampsGone("after second InitSchema")
}

// TestInitSchema_DeletesLegacyPreviousChainEdges verifies the data migration
// removes the retired :PREVIOUS chain (both node and unit versions) while
// leaving the version nodes themselves untouched.
func TestInitSchema_DeletesLegacyPreviousChainEdges(t *testing.T) {
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
		CREATE (a:NodeVersion {unique_id: $m, content_hash: $m + '-2', test_marker: $m})
		CREATE (b:NodeVersion {unique_id: $m, content_hash: $m + '-1', test_marker: $m})
		CREATE (a)-[:PREVIOUS]->(b)
		CREATE (c:CodeUnitVersion {unit_id: $m, checksum: $m + '-2', test_marker: $m})
		CREATE (d:CodeUnitVersion {unit_id: $m, checksum: $m + '-1', test_marker: $m})
		CREATE (c)-[:PREVIOUS]->(d)
	`, map[string]any{"m": marker})
	require.NoError(t, err)
	session.Close(ctx)

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger))

	read := client.NewSession(ctx, neo4j.AccessModeRead)
	defer read.Close(ctx)
	res, err := read.Run(ctx,
		`MATCH (n)-[p:PREVIOUS]->() WHERE n.test_marker = $m RETURN count(p) AS edges`,
		map[string]any{"m": marker})
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	edges, _ := res.Record().Get("edges")
	require.EqualValues(t, 0, edges, ":PREVIOUS edges must be deleted")

	res, err = read.Run(ctx,
		`MATCH (n) WHERE n.test_marker = $m RETURN count(n) AS nodes`,
		map[string]any{"m": marker})
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	nodes, _ := res.Record().Get("nodes")
	require.EqualValues(t, 4, nodes, "version nodes themselves must survive")

	require.NoError(t, neo4jinfra.InitSchema(ctx, client, logger), "second apply must be a no-op")
}
