package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaTenant returns a tenant id no other test uses and deletes every row the
// V40 tables hold for it when the test ends.
func schemaTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "schema-" + uuid.NewString()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM live_release WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(ctx, `DELETE FROM topology_releases WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(ctx, `DELETE FROM node_materializations WHERE tenant_id = $1`, tenant)
	})
	return tenant
}

// insertSchemaRelease inserts a release row and one seed node per key.
func insertSchemaRelease(t *testing.T, db *sqlx.DB, tenant, releaseID string, keys ...string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO topology_releases (tenant_id, release_id, promotion_seq, topology_uri, topology_sha256)
		VALUES ($1, $2, 1, 's3://continuo/k', 'abc')`, tenant, releaseID)
	require.NoError(t, err)
	for _, key := range keys {
		_, err := db.Exec(`
			INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name, table_name, node_type)
			VALUES ($1, $2, $3, $3, 'svc', 'raw', $3, 'dbt-seed')`, tenant, releaseID, key)
		require.NoError(t, err)
	}
}

// pqCode returns the SQLSTATE of a Postgres error.
func pqCode(t *testing.T, err error) string {
	t.Helper()
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	return string(pqErr.Code)
}

// Deleting a release removes its nodes and, through them, its edges.
func TestTopologySchema_ReleaseDeleteCascadesToNodesAndEdges(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-1", "raw.a", "raw.b")
	_, err := db.Exec(`
		INSERT INTO topology_edges (tenant_id, release_id, upstream_key, downstream_key)
		VALUES ($1, 'rel-1', 'raw.a', 'raw.b')`, tenant)
	require.NoError(t, err)

	_, err = db.Exec(`DELETE FROM topology_releases WHERE tenant_id = $1 AND release_id = 'rel-1'`, tenant)
	require.NoError(t, err)

	var nodes, edges int
	require.NoError(t, db.Get(&nodes, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1`, tenant))
	require.NoError(t, db.Get(&edges, `SELECT count(*) FROM topology_edges WHERE tenant_id = $1`, tenant))
	assert.Zero(t, nodes)
	assert.Zero(t, edges)
}

// An edge joins two nodes of its own release.
func TestTopologySchema_EdgeNeedsBothNodes(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-1", "raw.a")

	_, err := db.Exec(`
		INSERT INTO topology_edges (tenant_id, release_id, upstream_key, downstream_key)
		VALUES ($1, 'rel-1', 'raw.a', 'raw.missing')`, tenant)
	require.Error(t, err)
	assert.Equal(t, "23503", pqCode(t, err), "foreign_key_violation")
}

// The release live_release names cannot be deleted.
func TestTopologySchema_LiveReleaseCannotBeDeleted(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-1", "raw.a")
	_, err := db.Exec(`
		INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at)
		VALUES ($1, 'rel-1', 1, now())`, tenant)
	require.NoError(t, err)

	_, err = db.Exec(`DELETE FROM topology_releases WHERE tenant_id = $1 AND release_id = 'rel-1'`, tenant)
	require.Error(t, err)
	assert.Equal(t, "23503", pqCode(t, err), "foreign_key_violation")
}

// source is 'build' or 'adopted', or NULL for a row that records only a build
// request; a row starts with empty hashes and no runs.
func TestTopologySchema_MaterializationSourceIsBuildOrAdopted(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	for i, source := range []any{"build", "adopted", nil} {
		_, err := db.Exec(`INSERT INTO node_materializations (tenant_id, node_key, source) VALUES ($1, $2, $3)`,
			tenant, fmt.Sprintf("raw.s%d", i), source)
		require.NoError(t, err, "source %v", source)
	}

	_, err := db.Exec(`INSERT INTO node_materializations (tenant_id, node_key, source) VALUES ($1, 'raw.bad', 'rebuilt')`, tenant)
	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err), "check_violation")

	var row struct {
		ContentHash        string        `db:"content_hash"`
		ReleaseID          string        `db:"release_id"`
		RunID              uuid.NullUUID `db:"run_id"`
		AttemptContentHash string        `db:"attempt_content_hash"`
		AttemptRunID       uuid.NullUUID `db:"attempt_run_id"`
		MaterializedAt     sql.NullTime  `db:"materialized_at"`
	}
	require.NoError(t, db.Get(&row, `
		SELECT content_hash, release_id, run_id, attempt_content_hash, attempt_run_id, materialized_at
		FROM node_materializations WHERE tenant_id = $1 AND node_key = 'raw.s2'`, tenant))
	assert.Empty(t, row.ContentHash)
	assert.Empty(t, row.ReleaseID)
	assert.False(t, row.RunID.Valid)
	assert.Empty(t, row.AttemptContentHash)
	assert.False(t, row.AttemptRunID.Valid)
	assert.False(t, row.MaterializedAt.Valid)
}

// runs.release_id is optional: a run without a release stores NULL.
func TestTopologySchema_RunReleaseIDIsOptional(t *testing.T) {
	db := newTestDB(t)
	without, with := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id IN ($1, $2)`, without, with)
	})
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at)
		VALUES ('default', $1, $2, 'succeeded', now())`, without, "rel-null-"+without.String()[:8])
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, release_id)
		VALUES ('default', $1, $2, 'succeeded', now(), 'rel-x')`, with, "rel-set-"+with.String()[:8])
	require.NoError(t, err)

	var got []sql.NullString
	require.NoError(t, db.Select(&got, `
		SELECT release_id FROM runs WHERE run_id IN ($1, $2) ORDER BY release_id NULLS FIRST`, without, with))
	assert.Equal(t, []sql.NullString{{}, {String: "rel-x", Valid: true}}, got)
}

// topology_nodes.timeout_seconds is optional: a node inserted without it reads
// NULL, the form of a node that declares no timeout.
func TestTopologySchema_TimeoutSecondsIsOptional(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenant(t, db)
	insertSchemaRelease(t, db, tenant, "rel-1", "raw.a")

	var got sql.NullInt32
	require.NoError(t, db.Get(&got, `
		SELECT timeout_seconds FROM topology_nodes
		WHERE tenant_id = $1 AND release_id = 'rel-1' AND node_key = 'raw.a'`, tenant))
	assert.False(t, got.Valid)
}
