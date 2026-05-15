package test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// migrationVersionRE matches Flyway versioned migrations: V<n>__<description>.sql
var migrationVersionRE = regexp.MustCompile(`^V(\d+)__`)

// ApplyMigrations runs every Flyway migration in db/migration/state/ against
// the provided *sql.DB, in ascending version order. Keeps integration and
// e2e tests in lock-step with production schema; eliminates the recurring
// drift-from-hardcoded-DDL bug.
//
// Resolution: the directory is located relative to this source file's path,
// so tests work both inside the service container (source mounted at /app)
// and on a developer machine running `go test ./state/test/...` from the
// repo root.
func ApplyMigrations(db *sql.DB) error {
	migrationDir, err := stateMigrationDir()
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migration dir %s: %w", migrationDir, err)
	}

	type migration struct {
		version int
		name    string
		path    string
	}
	var migrations []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		match := migrationVersionRE.FindStringSubmatch(e.Name())
		if match == nil {
			continue
		}
		v, err := strconv.Atoi(match[1])
		if err != nil {
			return fmt.Errorf("parse version from %s: %w", e.Name(), err)
		}
		migrations = append(migrations, migration{v, e.Name(), filepath.Join(migrationDir, e.Name())})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })

	for _, m := range migrations {
		contents, err := os.ReadFile(m.path)
		if err != nil {
			return fmt.Errorf("read %s: %w", m.name, err)
		}
		if _, err := db.Exec(string(contents)); err != nil {
			return fmt.Errorf("apply %s: %w", m.name, err)
		}
	}
	return nil
}

// stateMigrationDir returns the absolute path to db/migration/state/ as a
// sibling of state/ at the repo root.
func stateMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate state/test/migrations.go")
	}
	// thisFile = <repo>/state/test/migrations.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile))) // up out of state/test, then state
	return filepath.Join(repoRoot, "db", "migration", "state"), nil
}
