// Package deploycheck holds static consistency guards over deploy-time
// artifacts (migration scripts, Helm values) that cannot be exercised by the
// e2e suite because e2e always runs against a freshly-initialised database.
package deploycheck

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// repoRoot walks up from the test working directory until it finds go.work.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.work not found above %s", dir)
		}
		dir = parent
	}
}

// databasesAssignment extracts the space-separated token list from the first
// shell assignment of the form `DATABASES="a b c"` in the given content.
func databasesAssignment(t *testing.T, content, where string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^[[:space:]]*(?:DATABASES|local DATABASES)="([^"]*)"`)
	m := re.FindStringSubmatch(content)
	if m == nil {
		t.Fatalf("no DATABASES=\"...\" assignment found in %s — the migration "+
			"databases must come from a single named list so the create-loop and "+
			"the flyway-loop cannot drift", where)
	}
	return strings.Fields(m[1])
}

func toSet(items []string) map[string]bool {
	s := make(map[string]bool, len(items))
	for _, it := range items {
		s[it] = true
	}
	return s
}

// TestDockerfileMigrateCoversAllDatabases is the regression guard for the
// production outage where db/Dockerfile.migrate did not COPY the remediation
// and agent_remediation migration directories, so Flyway silently skipped
// those databases on a fresh Hetzner deploy (non-existent filesystem location
// = no migrations applied, tables never created).
//
// The test asserts a 1-to-1 relationship: every token in migrate-all.sh's
// DATABASES list must have a matching
//
//	COPY migration/<token> /flyway/sql/<token>
//
// line in db/Dockerfile.migrate, and vice-versa, so the two files cannot
// drift in either direction.
func TestDockerfileMigrateCoversAllDatabases(t *testing.T) {
	root := repoRoot(t)

	// --- read migrate-all.sh DATABASES list ---
	scriptPath := filepath.Join(root, "db", "migrate-all.sh")
	scriptRaw, err := os.ReadFile(scriptPath) //nolint:gosec // G304: scriptPath is repoRoot(t) (discovered by walking up for go.work) joined with fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read %s: %v", scriptPath, err)
	}
	dbs := databasesAssignment(t, string(scriptRaw), "db/migrate-all.sh")
	if len(dbs) == 0 {
		t.Fatal("db/migrate-all.sh DATABASES list is empty")
	}

	// --- read Dockerfile.migrate COPY lines ---
	dockerfilePath := filepath.Join(root, "db", "Dockerfile.migrate")
	dfRaw, err := os.ReadFile(dockerfilePath) //nolint:gosec // G304: dockerfilePath is repoRoot(t) joined with fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read %s: %v", dockerfilePath, err)
	}
	// Match lines of the form: COPY migration/<token> /flyway/sql/<token>
	copyRe := regexp.MustCompile(`(?m)^COPY\s+migration/(\S+)\s+/flyway/sql/(\S+)`)
	copyMatches := copyRe.FindAllStringSubmatch(string(dfRaw), -1)

	// Build the set of tokens covered by the Dockerfile.
	dockerfileTokens := make(map[string]bool)
	for _, m := range copyMatches {
		src, dst := m[1], m[2]
		if src != dst {
			t.Errorf("db/Dockerfile.migrate: COPY migration/%s /flyway/sql/%s — "+
				"source and destination token mismatch; they must be identical", src, dst)
		}
		dockerfileTokens[src] = true
	}

	scriptSet := toSet(dbs)

	// Every migrate-all.sh token must be in the Dockerfile.
	var missingInDockerfile []string
	for _, db := range dbs {
		if !dockerfileTokens[db] {
			missingInDockerfile = append(missingInDockerfile, db)
		}
	}
	if len(missingInDockerfile) > 0 {
		sort.Strings(missingInDockerfile)
		t.Errorf("db/migrate-all.sh DATABASES tokens missing a "+
			"`COPY migration/<token> /flyway/sql/<token>` line in "+
			"db/Dockerfile.migrate: %v — Flyway silently skips non-existent "+
			"locations, so these databases would receive zero migrations on a "+
			"fresh deploy", missingInDockerfile)
	}

	// Every Dockerfile COPY token must be in migrate-all.sh (no orphan dirs).
	var orphanInDockerfile []string
	for tok := range dockerfileTokens {
		if !scriptSet[tok] {
			orphanInDockerfile = append(orphanInDockerfile, tok)
		}
	}
	if len(orphanInDockerfile) > 0 {
		sort.Strings(orphanInDockerfile)
		t.Errorf("db/Dockerfile.migrate COPY migration/<token> lines with no "+
			"matching entry in db/migrate-all.sh DATABASES: %v — remove stale "+
			"COPY lines or add the token to DATABASES", orphanInDockerfile)
	}
}

// TestMigrateAllProvisionsEveryDatabaseItMigrates is the regression guard for
// the production outage where the migration job ran Flyway against
// `continuo_release` before that database existed on the long-lived Hetzner
// volume. The Postgres initdb scripts only run on a fresh data directory, so a
// database added after first boot was never created and Flyway hung until the
// Helm pre-upgrade hook timed out.
//
// The durable fix makes db/migrate-all.sh create every database it migrates.
// This test enforces that contract: for every database in the script's single
// DATABASES list, the script must both create it (idempotently) and migrate it.
func TestMigrateAllProvisionsEveryDatabaseItMigrates(t *testing.T) {
	root := repoRoot(t)
	scriptPath := filepath.Join(root, "db", "migrate-all.sh")
	raw, err := os.ReadFile(scriptPath) //nolint:gosec // G304: scriptPath is repoRoot(t) joined with fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read %s: %v", scriptPath, err)
	}
	script := string(raw)

	dbs := databasesAssignment(t, script, "db/migrate-all.sh")
	if len(dbs) == 0 {
		t.Fatal("db/migrate-all.sh DATABASES list is empty")
	}

	// The script must create each database it intends to migrate, guarded by an
	// existence check so re-runs are idempotent. The create statement and the
	// existence probe are templated over the loop variable.
	if !strings.Contains(script, "CREATE DATABASE continuo_${db}") {
		t.Errorf("db/migrate-all.sh must idempotently create each database it "+
			"migrates (expected a templated `CREATE DATABASE continuo_${db}`); "+
			"relying on Postgres initdb scripts breaks on existing volumes")
	}
	if !strings.Contains(script, "pg_database") {
		t.Errorf("db/migrate-all.sh must guard CREATE DATABASE with an existence "+
			"check against pg_database so the step is idempotent")
	}

	// The flyway migrate loop must target the same templated database name, so
	// the create-loop and migrate-loop are driven by one source of truth.
	if !strings.Contains(script, "continuo_${db}") {
		t.Errorf("db/migrate-all.sh must run flyway against continuo_${db} using " +
			"the same DATABASES list it creates")
	}

	// Every migrated database must also be provisioned by the chart's db-init
	// initContainer, so fresh clusters and the migration job stay consistent.
	// db-init-migrate-job.yaml's `for db in ...; do` loop is its own hardcoded
	// list (not derived from db/migrate-all.sh — it also provisions
	// continuo_dbt, the dbt warehouse database that Flyway never migrates), so
	// it can drift independently and needs its own cross-check.
	jobPath := filepath.Join(root, "deploy", "continuo", "templates", "db-init-migrate-job.yaml")
	jobRaw, err := os.ReadFile(jobPath) //nolint:gosec // G304: jobPath is repoRoot(t) joined with fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read %s: %v", jobPath, err)
	}
	jobDBs := dbInitLoopDatabases(t, string(jobRaw), "deploy/continuo/templates/db-init-migrate-job.yaml")
	jobSet := toSet(jobDBs)

	var missing []string
	for _, db := range dbs {
		full := "continuo_" + db
		if !jobSet[full] {
			missing = append(missing, full)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("databases migrated by db/migrate-all.sh but absent from the "+
			"deploy/continuo/templates/db-init-migrate-job.yaml db-init `for db in "+
			"...; do` list: %v — fresh clusters would never create them", missing)
	}
}

// dbInitLoopDatabases extracts the whitespace-separated (and possibly
// backslash-line-continued) token list from the db-init initContainer's
// `for db in <tokens>; do` shell loop in the given content.
func dbInitLoopDatabases(t *testing.T, content, where string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?s)for db in (.*?);\s*do`)
	m := re.FindStringSubmatch(content)
	if m == nil {
		t.Fatalf("no `for db in ...; do` loop found in %s — the db-init "+
			"database list must stay a single shell for-loop so this check can "+
			"parse it", where)
	}
	tokens := strings.Fields(strings.ReplaceAll(m[1], "\\", " "))
	if len(tokens) == 0 {
		t.Fatalf("`for db in ...; do` loop in %s has no databases", where)
	}
	return tokens
}
