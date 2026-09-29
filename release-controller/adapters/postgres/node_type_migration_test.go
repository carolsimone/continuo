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

// TestV22_RewritesOnlyExactNodeTypeValues proves the migration rewrites the
// node_type field of a topology_snapshot node from python-model to python-node,
// leaves an unrelated node's node_type alone, and — crucially — does NOT touch
// a sibling string value that merely equals "python-model" (a service named
// python-model keeps its name). Re-running is a no-op.
func TestV22_RewritesOnlyExactNodeTypeValues(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'r1', '[
			{"unique_id":"a.b","node_type":"python-model","service_name":"svc"},
			{"unique_id":"c.d","node_type":"dbt-model","service_name":"python-model"}
		]'::jsonb, now())`)
	require.NoError(t, err)

	sql := v22SQL(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var snapshot string
	require.NoError(t, db.Get(&snapshot, `SELECT topology_snapshot::text FROM current_prod WHERE id = 1`))
	require.JSONEq(t,
		`[
			{"unique_id":"a.b","node_type":"python-node","service_name":"svc"},
			{"unique_id":"c.d","node_type":"dbt-model","service_name":"python-model"}
		]`,
		snapshot)
}

// TestV22_PreservesArrayOrder proves the array rebuild keeps element order:
// the migration uses WITH ORDINALITY + jsonb_agg(... ORDER BY ord), and a
// silently reordered topology would be a regression. A three-node snapshot
// with the python-model node in the MIDDLE must come back in the same order,
// asserted both by an order-sensitive JSONEq and by the per-position unique_id.
func TestV22_PreservesArrayOrder(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'r-order', '[
			{"unique_id":"n.first","node_type":"dbt-model"},
			{"unique_id":"n.middle","node_type":"python-model"},
			{"unique_id":"n.last","node_type":"dbt-seed"}
		]'::jsonb, now())`)
	require.NoError(t, err)

	sql := v22SQL(t)
	for i := 0; i < 2; i++ { // second run must be a no-op
		_, err = db.Exec(sql)
		require.NoError(t, err)
	}

	var snapshot string
	require.NoError(t, db.Get(&snapshot, `SELECT topology_snapshot::text FROM current_prod WHERE id = 1`))
	// require.JSONEq compares parsed JSON; array element order is significant,
	// so this fails if the rebuild reordered the nodes.
	require.JSONEq(t,
		`[
			{"unique_id":"n.first","node_type":"dbt-model"},
			{"unique_id":"n.middle","node_type":"python-node"},
			{"unique_id":"n.last","node_type":"dbt-seed"}
		]`,
		snapshot)

	// Belt-and-braces: assert the unique_id at each position directly.
	var ids []string
	require.NoError(t, db.Select(&ids,
		`SELECT elem->>'unique_id'
		   FROM jsonb_array_elements((SELECT topology_snapshot FROM current_prod WHERE id = 1))
		        WITH ORDINALITY AS x(elem, ord)
		  ORDER BY ord`))
	require.Equal(t, []string{"n.first", "n.middle", "n.last"}, ids)
}

// TestV22_RewritesCandidateTopologyAndPerNodeButNotSiblingStrings covers the
// release_pipeline_runs columns: candidate_topology (an array of node objects)
// and per_node_results (an array of per-node result objects). It proves the
// node_type field is rewritten in both, while a sibling service_name equal to
// "python-model" and a free-text detail that merely mentions "python-model"
// are left byte-for-byte. The row is built with every NOT NULL column
// release_pipeline_runs requires, filled with non-null placeholders, since this
// test writes the row directly with SQL rather than through the repository.
func TestV22_RewritesCandidateTopologyAndPerNodeButNotSiblingStrings(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO release_pipeline_runs (run_id, run_kind, status, image_tags, changed_service,
		candidate_topology, validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ('run-detail-1', 'candidate', 'received', '{}'::jsonb, 'svc',
		'[{"unique_id":"a.b","node_type":"python-model","service_name":"python-model"}]'::jsonb,
		NULL, NULL, '', NULL,
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

	var candidateTopology, perNodeResults string
	require.NoError(t, db.Get(&candidateTopology,
		`SELECT candidate_topology::text FROM release_pipeline_runs WHERE run_id = 'run-detail-1'`))
	require.JSONEq(t,
		`[{"unique_id":"a.b","node_type":"python-node","service_name":"python-model"}]`,
		candidateTopology)

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
		candidate_topology, validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ('run-reject-1', 'candidate', 'rejected', '{}'::jsonb, 'svc',
		NULL, NULL, NULL, '', NULL,
		NULL, now(), '[]'::jsonb, '', 'dbt',
		false, 'acme/demo', 'deadbeef', 1,
		'{"release_id":"run-reject-1","reason":"validation_failed",
		  "error_detail":"the python-model node failed",
		  "per_node":[{"node_id":"a.b","status":"failed","node_type":"python-model","service":"python-model"}]}'::jsonb,
		'', 0, '')`)
	require.NoError(t, err)

	sql := v22SQL(t)
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
