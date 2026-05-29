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
	raw, err := os.ReadFile(scriptPath)
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

	// Every migrated database must also be provisioned by the infra chart's
	// initdb list, so fresh clusters and the migration job stay consistent.
	infraPath := filepath.Join(root, "deploy", "infra", "values.yaml")
	infraRaw, err := os.ReadFile(infraPath)
	if err != nil {
		t.Fatalf("read %s: %v", infraPath, err)
	}
	infraDBs := databasesAssignment(t, string(infraRaw), "deploy/infra/values.yaml")
	infraSet := toSet(infraDBs)

	var missing []string
	for _, db := range dbs {
		full := "continuo_" + db
		if !infraSet[full] {
			missing = append(missing, full)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("databases migrated by db/migrate-all.sh but absent from the "+
			"deploy/infra/values.yaml initdb DATABASES list: %v — fresh clusters "+
			"would never create them", missing)
	}
}
