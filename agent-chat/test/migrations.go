package test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/carolsimone/continuo/pkg/testmigrations"
)

// ApplyMigrations runs every Flyway migration in db/migration/agent_chat/
// against the provided *sql.DB, in ascending version order. Keeps integration
// tests in lock-step with production schema.
//
// The directory is located relative to this source file's path so tests work
// both inside the service container (source mounted at /app) and on a
// developer machine running `go test` from any working directory.
func ApplyMigrations(db *sql.DB) error {
	dir, err := agentChatMigrationDir()
	if err != nil {
		return err
	}
	return testmigrations.Apply(db, dir)
}

// agentChatMigrationDir returns the absolute path to db/migration/agent_chat/
// as a sibling of agent-chat/ at the repo root.
func agentChatMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate agent-chat/test/migrations.go")
	}
	// thisFile = <repo>/agent-chat/test/migrations.go
	// filepath.Dir x3: test → agent-chat → repo root
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repoRoot, "db", "migration", "agent_chat"), nil
}
