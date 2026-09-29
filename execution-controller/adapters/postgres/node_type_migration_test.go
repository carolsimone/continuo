//go:build integration

package postgres_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func v6SQL(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "execution", "V6__python_node_type_job_params.sql"))
	require.NoError(t, err)
	return string(b)
}

// TestV6_RewritesJobParamsNodeTypeOnly proves the execution-DB migration
// rewrites the node_type field inside a queued deployment's job_params from
// python-model to python-node, so a row that sat pending/blocked across an
// upgrade resumes instead of failing the dispatcher's node-type parse. It also
// proves the rewrite is field-scoped: a non-python row is left alone, and a
// job_params whose node_type is some other kind but that merely mentions
// "python-model" in another string field (service_name) is NOT rewritten.
// Re-running the migration is a no-op.
func TestV6_RewritesJobParamsNodeTypeOnly(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	pyID := uuid.New()
	otherID := uuid.New()
	mentionID := uuid.New()

	// A queued python-node deployment written under the retired kind.
	_, err := db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, status)
		 VALUES ($1, $2, $3, '{"node_type":"python-model","service_name":"svc","table_name":"orders"}'::jsonb, 'blocked')`,
		pyID, uuid.New(), uuid.New())
	require.NoError(t, err)

	// An unrelated dbt-model deployment.
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, status)
		 VALUES ($1, $2, $3, '{"node_type":"dbt-model","service_name":"svc","table_name":"orders"}'::jsonb, 'pending')`,
		otherID, uuid.New(), uuid.New())
	require.NoError(t, err)

	// A dbt row that merely mentions python-model in a NON-node_type field:
	// its node_type must not change, and the mention must survive verbatim.
	_, err = db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, status)
		 VALUES ($1, $2, $3, '{"node_type":"dbt-model","service_name":"python-model","table_name":"orders"}'::jsonb, 'pending')`,
		mentionID, uuid.New(), uuid.New())
	require.NoError(t, err)

	sql := v6SQL(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var pyParams, otherParams, mentionParams string
	require.NoError(t, db.Get(&pyParams, `SELECT job_params::text FROM deployments WHERE id = $1`, pyID))
	require.JSONEq(t,
		`{"node_type":"python-node","service_name":"svc","table_name":"orders"}`,
		pyParams)

	require.NoError(t, db.Get(&otherParams, `SELECT job_params::text FROM deployments WHERE id = $1`, otherID))
	require.JSONEq(t,
		`{"node_type":"dbt-model","service_name":"svc","table_name":"orders"}`,
		otherParams)

	require.NoError(t, db.Get(&mentionParams, `SELECT job_params::text FROM deployments WHERE id = $1`, mentionID))
	require.JSONEq(t,
		`{"node_type":"dbt-model","service_name":"python-model","table_name":"orders"}`,
		mentionParams)
}
