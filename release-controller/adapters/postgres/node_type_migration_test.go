//go:build integration

package postgres_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// v22SQLOnRemainingColumns returns the statements of release migration V22
// that rewrite columns the release schema still has: per_node_results and
// rejection_payload. The statements on current_prod.topology_snapshot and
// release_pipeline_runs.candidate_topology target columns release migration
// V26 dropped, so they are left out.
func v22SQLOnRemainingColumns(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "release", "V22__python_node_type_values.sql"))
	require.NoError(t, err)

	var code []string
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			code = append(code, line)
		}
	}
	var kept []string
	for _, stmt := range strings.SplitAfter(strings.Join(code, "\n"), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.Contains(stmt, "topology_snapshot") || strings.Contains(stmt, "candidate_topology") {
			continue
		}
		kept = append(kept, stmt)
	}
	require.Len(t, kept, 2, "V22 rewrites per_node_results and rejection_payload on the remaining columns")
	return strings.Join(kept, "\n")
}

// TestV22_RewritesPerNodeResultsButNotSiblingStrings proves the migration
// rewrites node_type inside per_node_results, while a free-text detail that
// merely mentions "python-model" is left byte-for-byte. The row is built with
// every NOT NULL column release_pipeline_runs requires, filled with non-null
// placeholders, since this test writes the row directly with SQL rather than
// through the repository. Re-running the statements is a no-op.
func TestV22_RewritesPerNodeResultsButNotSiblingStrings(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO release_pipeline_runs (run_id, run_kind, status, image_tags, changed_service,
		validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ('run-detail-1', 'candidate', 'received', '{}'::jsonb, 'svc',
		NULL, NULL, '', NULL,
		'[{"node_id":"a.b","node_type":"python-model","detail":"a.b is a python-model node"}]'::jsonb,
		now(), '[]'::jsonb, '', 'dbt',
		false, 'acme/demo', 'deadbeef', 1, NULL,
		'', 0, '')`)
	require.NoError(t, err)

	sql := v22SQLOnRemainingColumns(t)
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

// TestV22_RewritesRejectionPayloadPerNodeNodeTypeOnly proves the migration
// reaches node_type inside the rejection_payload body's per_node array, while
// leaving that same element's service field and the body's top-level
// error_detail — both of which equal or mention "python-model" — untouched.
func TestV22_RewritesRejectionPayloadPerNodeNodeTypeOnly(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO release_pipeline_runs (run_id, run_kind, status, image_tags, changed_service,
		validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ('run-reject-1', 'candidate', 'rejected', '{}'::jsonb, 'svc',
		NULL, NULL, '', NULL,
		NULL, now(), '[]'::jsonb, '', 'dbt',
		false, 'acme/demo', 'deadbeef', 1,
		'{"release_id":"run-reject-1","reason":"validation_failed",
		  "error_detail":"the python-model node failed",
		  "per_node":[{"node_id":"a.b","status":"failed","node_type":"python-model","service":"python-model"}]}'::jsonb,
		'', 0, '')`)
	require.NoError(t, err)

	sql := v22SQLOnRemainingColumns(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var rejection string
	require.NoError(t, db.Get(&rejection,
		`SELECT rejection_payload::text FROM release_pipeline_runs WHERE run_id = 'run-reject-1'`))
	require.JSONEq(t,
		`{"release_id":"run-reject-1","reason":"validation_failed",
		  "error_detail":"the python-model node failed",
		  "per_node":[{"node_id":"a.b","status":"failed","node_type":"python-node","service":"python-model"}]}`,
		rejection)
}
