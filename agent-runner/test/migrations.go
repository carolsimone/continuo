package test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/carolsimone/continuo/pkg/testmigrations"
)

// ApplyMigrations runs every Flyway migration in db/migration/agent/
// against the provided *sql.DB, in ascending version order. Keeps integration
// tests in lock-step with production schema.
//
// The directory is located relative to this source file's path so tests work
// both inside the service container (source mounted at /app) and on a
// developer machine running `go test` from any working directory.
func ApplyMigrations(db *sql.DB) error {
	dir, err := agentRunnerMigrationDir()
	if err != nil {
		return err
	}
	return testmigrations.Apply(db, dir)
}

// agentRunnerMigrationDir returns the absolute path to db/migration/agent/
// as a sibling of agent-runner/ at the repo root.
func agentRunnerMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate agent-runner/test/migrations.go")
	}
	// thisFile = <repo>/agent-runner/test/migrations.go
	// filepath.Dir x3: test → agent-runner → repo root
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repoRoot, "db", "migration", "agent"), nil
}
