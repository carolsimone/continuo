package test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/carolsimone/continuo/pkg/testmigrations"
)

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
	dir, err := stateMigrationDir()
	if err != nil {
		return err
	}
	return testmigrations.Apply(db, dir)
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
