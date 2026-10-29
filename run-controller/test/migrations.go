package test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/carolsimone/continuo/pkg/testmigrations"
)

// ApplyMigrations runs every Flyway migration in db/migration/run_controller/ against
// the provided *sql.DB, in ascending version order. Keeps integration and
// e2e tests in lock-step with production schema; eliminates the recurring
// drift-from-hardcoded-DDL bug.
//
// Resolution: the directory is located relative to this source file's path,
// so tests work both inside the service container (source mounted at /app)
// and on a developer machine running `go test ./run-controller/test/...` from the
// repo root.
func ApplyMigrations(db *sql.DB) error {
	dir, err := runControllerMigrationDir()
	if err != nil {
		return err
	}
	return testmigrations.Apply(db, dir)
}

// runControllerMigrationDir returns the absolute path to
// db/migration/run_controller/ as a sibling of run-controller/ at the repo root.
func runControllerMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate run-controller/test/migrations.go")
	}
	// thisFile = <repo>/run-controller/test/migrations.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile))) // up out of run-controller/test, then run-controller
	return filepath.Join(repoRoot, "db", "migration", "run_controller"), nil
}
