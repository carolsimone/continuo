//go:build integration

package postgres_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func v22SQL(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "release", "V22__python_node_type_values.sql"))
	require.NoError(t, err)
	return string(b)
}

func TestV22_RewritesOnlyExactNodeTypeValues(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'r1', '[{"unique_id":"a.b","node_type":"python-model"},{"unique_id":"c.d","node_type":"dbt-model"}]'::jsonb, now())`)
	require.NoError(t, err)

	sql := v22SQL(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var snapshot string
	require.NoError(t, db.Get(&snapshot, `SELECT topology_snapshot::text FROM current_prod WHERE id = 1`))
	require.JSONEq(t,
		`[{"unique_id":"a.b","node_type":"python-node"},{"unique_id":"c.d","node_type":"dbt-model"}]`,
		snapshot)
}

// TestV22_RewritesPerNodeResultsNodeTypeButNotFreeTextDetail proves the
// migration's exact-quote match only rewrites the JSON string VALUE of
// node_type; a free-text detail field that merely mentions "python-model"
// mid-sentence is left byte-for-byte untouched. The row is built with every
// NOT NULL column release_pipeline_runs requires (the same column list
// RunRepository.Save uses), filled with non-null placeholders, since this
// test writes the row directly with SQL rather than through the repository.
func TestV22_RewritesPerNodeResultsNodeTypeButNotFreeTextDetail(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO release_pipeline_runs (run_id, run_kind, status, image_tags, changed_service,
		candidate_topology, validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ('run-detail-1', 'candidate', 'received', '{}'::jsonb, 'svc',
		NULL, NULL, NULL, '', NULL,
		'[{"node_id":"a.b","node_type":"python-model","detail":"a.b is a python-model node"}]'::jsonb,
		now(), '[]'::jsonb, '', 'dbt',
		false, 'acme/demo', 'deadbeef', 1, NULL,
		'', 0, '')`)
	require.NoError(t, err)

	sql := v22SQL(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var perNodeResults string
	require.NoError(t, db.Get(&perNodeResults,
		`SELECT per_node_results::text FROM release_pipeline_runs WHERE run_id = 'run-detail-1'`))
	require.JSONEq(t,
		`[{"node_id":"a.b","node_type":"python-node","detail":"a.b is a python-model node"}]`,
		perNodeResults)
}
