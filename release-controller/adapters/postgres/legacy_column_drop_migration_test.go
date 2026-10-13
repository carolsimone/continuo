//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/carolsimone/continuo/pkg/testmigrations"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Release migration V26 drops the columns that held topologies inline and
// refuses to run while any row still needs them. A refusal can only be seen on
// a database at V25, and the shared test database has every migration applied,
// so this test builds a private Postgres server from the release migration
// files instead and never touches the shared database.

// legacyColumnDrop is the version of the release migration under test.
const legacyColumnDrop = 26

// allLegacyColumns are the columns V26 drops, as legacyColumns lists them.
var allLegacyColumns = []string{
	"current_prod.topology_snapshot",
	"release_pipeline_runs.candidate_topology",
	"release_pipeline_runs.parsing_at_upgrade",
}

// upgradeThrough is the instruction a refusal carries: the release to upgrade
// through first, named by its version number.
var upgradeThrough = regexp.MustCompile(`upgrade to continuo [0-9]+\.[0-9]+\.[0-9]+ and let release-controller start once before upgrading to this release`)

// TestV26_RefusesUnsettledRowsAndDropsTheColumnsOnceSettled runs V26 against
// rows the release to upgrade through leaves settled, adding one unsettled row
// at a time: each is refused with counts that name only that row, and the
// refused migration drops nothing. Once only settled rows remain, V26 drops
// the three columns and keeps every row.
func TestV26_RefusesUnsettledRowsAndDropsTheColumnsOnceSettled(t *testing.T) {
	db := releaseDBBefore(t, legacyColumnDrop)
	seedSettledRows(t, db)
	v26 := releaseMigrationSQL(t, legacyColumnDrop)

	refusals := []struct {
		name   string
		seed   func(t *testing.T)
		undo   string
		counts string
	}{
		{
			name: "a run holding an inline candidate topology and no artifact",
			seed: func(t *testing.T) {
				insertCandidateRun(t, db, "rInline", "validating")
				mustExec(t, db, `UPDATE release_pipeline_runs SET candidate_topology = '[{"unique_id":"a"}]'::jsonb WHERE run_id = 'rInline'`)
			},
			undo:   `DELETE FROM release_pipeline_runs WHERE run_id = 'rInline'`,
			counts: "1 run(s) hold an inline candidate topology, 0 run(s) wait on a parse result from before artifacts, 0 current_prod row(s) reference no artifact",
		},
		{
			name: "a run marked at the topology upgrade and still parsing",
			seed: func(t *testing.T) {
				insertCandidateRun(t, db, "rWaiting", "parsing")
				mustExec(t, db, `UPDATE release_pipeline_runs SET parsing_at_upgrade = true WHERE run_id = 'rWaiting'`)
			},
			undo:   `DELETE FROM release_pipeline_runs WHERE run_id = 'rWaiting'`,
			counts: "0 run(s) hold an inline candidate topology, 1 run(s) wait on a parse result from before artifacts, 0 current_prod row(s) reference no artifact",
		},
		{
			name:   "current_prod naming a release that holds only its inline snapshot",
			seed:   func(t *testing.T) { mustExec(t, db, `UPDATE current_prod SET release_id = 'rLive' WHERE id = 1`) },
			undo:   `UPDATE current_prod SET release_id = '' WHERE id = 1`,
			counts: "0 run(s) hold an inline candidate topology, 0 run(s) wait on a parse result from before artifacts, 1 current_prod row(s) reference no artifact",
		},
		{
			name: "current_prod naming a release with neither an artifact nor a snapshot",
			seed: func(t *testing.T) {
				mustExec(t, db, `UPDATE current_prod SET release_id = 'rLive', topology_snapshot = NULL WHERE id = 1`)
			},
			undo:   `UPDATE current_prod SET release_id = '', topology_snapshot = '[{"unique_id":"a"}]'::jsonb WHERE id = 1`,
			counts: "0 run(s) hold an inline candidate topology, 0 run(s) wait on a parse result from before artifacts, 1 current_prod row(s) reference no artifact",
		},
	}
	for _, c := range refusals {
		t.Run("refuses "+c.name, func(t *testing.T) {
			c.seed(t)
			t.Cleanup(func() { _, _ = db.Exec(c.undo) })

			_, err := db.Exec(v26)
			require.Error(t, err, "the migration must refuse")
			assert.Contains(t, err.Error(), c.counts)
			assert.Regexp(t, upgradeThrough, err.Error())
			assert.Equal(t, allLegacyColumns, legacyColumns(t, db), "a refused migration drops nothing")
		})
	}

	// Two unsettled kinds at once exercise the summed guard (a + b + c > 0 with
	// more than one non-zero term) and show that each count is reported
	// independently.
	t.Run("refuses multiple unsettled kinds at once", func(t *testing.T) {
		insertCandidateRun(t, db, "rCombo", "validating")
		mustExec(t, db, `UPDATE release_pipeline_runs SET candidate_topology = '[{"unique_id":"a"}]'::jsonb WHERE run_id = 'rCombo'`)
		mustExec(t, db, `UPDATE current_prod SET release_id = 'rLive' WHERE id = 1`)
		t.Cleanup(func() { _, _ = db.Exec(`UPDATE current_prod SET release_id = '' WHERE id = 1`) })
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM release_pipeline_runs WHERE run_id = 'rCombo'`) })

		_, err := db.Exec(v26)
		require.Error(t, err, "the migration must refuse")
		assert.Contains(t, err.Error(), "1 run(s) hold an inline candidate topology, 0 run(s) wait on a parse result from before artifacts, 1 current_prod row(s) reference no artifact")
		assert.Regexp(t, upgradeThrough, err.Error())
		assert.Equal(t, allLegacyColumns, legacyColumns(t, db), "a refused migration drops nothing")
	})

	t.Run("drops the columns once every row is settled", func(t *testing.T) {
		mustExec(t, db, `UPDATE current_prod SET release_id = 'rLive',
			topology_uri = 's3://continuo/tenants/default/topologies/rLive/topology.json.gz',
			topology_sha256 = 'f00d', node_count = 1 WHERE id = 1`)

		_, err := db.Exec(v26)
		require.NoError(t, err)
		assert.Empty(t, legacyColumns(t, db))

		var runs int
		require.NoError(t, db.Get(&runs, `SELECT count(*) FROM release_pipeline_runs`))
		assert.Equal(t, 4, runs, "the migration drops columns, never rows")
		var uri string
		require.NoError(t, db.Get(&uri, `SELECT topology_uri FROM current_prod WHERE id = 1`))
		assert.Equal(t, "s3://continuo/tenants/default/topologies/rLive/topology.json.gz", uri)
	})
}

// releaseMigrationDir is <repo>/db/migration/release.
func releaseMigrationDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "locate this test file")
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	return filepath.Join(repoRoot, "db", "migration", "release")
}

// releaseMigrationSQL returns the release migration of version v.
func releaseMigrationSQL(t *testing.T, v int) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(releaseMigrationDir(t), fmt.Sprintf("V%d__*.sql", v)))
	require.NoError(t, err)
	require.Len(t, matches, 1, "exactly one release migration has version %d", v)
	b, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	return string(b)
}

// releaseDBBefore starts a throwaway Postgres and applies every release
// migration below version v from the repository's files, in version order.
// The container is removed when the test ends.
func releaseDBBefore(t *testing.T, v int) *sqlx.DB {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16.4",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "continuo_svc",
				"POSTGRES_PASSWORD": "continuo",
				"POSTGRES_DB":       "continuo_release",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		testdeps.Unavailable(t, "start a throwaway postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	if host == "localhost" {
		host = "127.0.0.1" // colima on macOS can resolve localhost to ::1, where the port is not published
	}
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err)
	dsn := fmt.Sprintf("host=%s port=%s user=continuo_svc password=continuo dbname=continuo_release sslmode=disable", host, port.Port())
	var db *sqlx.DB
	for attempt := 0; attempt < 10; attempt++ {
		if db, err = sqlx.Connect("postgres", dsn); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "connect to the throwaway postgres")
	t.Cleanup(func() { _ = db.Close() })

	src := releaseMigrationDir(t)
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	versioned := regexp.MustCompile(`^V([0-9]+)__.*\.sql$`)
	for _, e := range entries {
		m := versioned.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		if n >= v {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), b, 0o600))
	}
	require.NoError(t, testmigrations.Apply(db.DB, dst), "apply the release migrations below V%d", v)
	return db
}

// seedSettledRows writes rows that may not stop the migration: a run saved
// with a JSON null topology, a run whose inline topology already has its
// artifact, a run marked as parsing at the topology upgrade that had left
// parsing before release-controller started, an ordinary parsing run, and a
// current_prod row that names no release.
func seedSettledRows(t *testing.T, db *sqlx.DB) {
	t.Helper()
	insertCandidateRun(t, db, "rNullTopology", "rejected")
	insertCandidateRun(t, db, "rMoved", "validating")
	insertCandidateRun(t, db, "rMarkedMovedOn", "validating")
	insertCandidateRun(t, db, "rParsing", "parsing")
	mustExec(t, db, `UPDATE release_pipeline_runs SET candidate_topology = 'null'::jsonb WHERE run_id = 'rNullTopology'`)
	mustExec(t, db, `UPDATE release_pipeline_runs
		SET candidate_topology = '[{"unique_id":"a"}]'::jsonb,
		    candidate_topology_uri = 's3://continuo/tenants/default/topologies/rMoved/topology.json.gz',
		    candidate_topology_sha256 = 'f00d', candidate_node_count = 1
		WHERE run_id = 'rMoved'`)
	mustExec(t, db, `UPDATE release_pipeline_runs SET parsing_at_upgrade = true WHERE run_id = 'rMarkedMovedOn'`)
	mustExec(t, db, `INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, '', '[{"unique_id":"a"}]'::jsonb, now())`)
}

// insertCandidateRun writes a candidate run in status with every column
// release_pipeline_runs requires at V25.
func insertCandidateRun(t *testing.T, db *sqlx.DB, runID, status string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO release_pipeline_runs (run_id, run_kind, status, image_tags, changed_service,
		validation_node_ids, fail_reason, fail_detail, failing_nodes,
		per_node_results, created_at, transitions, code_bundle_uri, manifest_kind,
		bootstrap, repo, commit_sha, remediation_round, rejection_payload,
		verifies_release_id, attempt, source_overlay_uri)
		VALUES ($1, 'candidate', $2, '{}'::jsonb, 'svc',
		NULL, NULL, '', NULL,
		NULL, now(), '[]'::jsonb, '', 'dbt',
		false, 'acme/demo', 'deadbeef', 1, NULL,
		'', 0, '')`, runID, status)
	require.NoError(t, err, "insert run %s", runID)
}

func mustExec(t *testing.T, db *sqlx.DB, stmt string) {
	t.Helper()
	_, err := db.Exec(stmt)
	require.NoError(t, err, stmt)
}

// legacyColumns lists which of the three columns V26 drops exist, as
// "<table>.<column>", sorted.
func legacyColumns(t *testing.T, db *sqlx.DB) []string {
	t.Helper()
	var cols []string
	require.NoError(t, db.Select(&cols, `
		SELECT table_name::text || '.' || column_name::text
		  FROM information_schema.columns
		 WHERE table_schema = current_schema()
		   AND (table_name::text, column_name::text) IN (('release_pipeline_runs', 'candidate_topology'),
		                                                 ('release_pipeline_runs', 'parsing_at_upgrade'),
		                                                 ('current_prod', 'topology_snapshot'))
		 ORDER BY 1`))
	return cols
}
