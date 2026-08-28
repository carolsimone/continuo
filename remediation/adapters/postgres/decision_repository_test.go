//go:build integration

package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testmigrations"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// remediationMigrationDir resolves <repo>/db/migration/remediation/ from this
// source file's location so the testcontainer schema stays in lock-step with
// the production migration.
func remediationMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate decision_repository_test.go")
	}
	// thisFile = <repo>/remediation/adapters/postgres/decision_repository_test.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	return filepath.Join(repoRoot, "db", "migration", "remediation"), nil
}

// getEnvOrDefault returns the env var value or fallback default.
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// newTestDB boots a PostgreSQL testcontainer, applies the remediation
// migration, and returns a ready *sqlx.DB. The container is terminated when
// the test finishes via t.Cleanup.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start postgres container")

	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	host, err := container.Host(ctx)
	require.NoError(t, err, "get container host")
	port, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err, "get container port")

	// Force IPv4 for macOS compatibility.
	if host == "localhost" {
		host = "127.0.0.1"
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=testuser password=testpass dbname=testdb sslmode=disable",
		host, port.Port(),
	)

	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		t.Logf("connection attempt %d/10 failed, retrying...", i+1)
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "connect to postgres after retries")

	dir, err := remediationMigrationDir()
	require.NoError(t, err, "resolve remediation migration dir")
	require.NoError(t, testmigrations.Apply(db.DB, dir), "apply remediation migrations")

	return db
}

func TestDecisionRepositoryUpsertIdempotent(t *testing.T) {
	db := newTestDB(t)
	tx, err := db.Beginx()
	require.NoError(t, err)
	repo := NewDecisionRepository(tx) // bound to the tx, matching the UnitOfWork
	d := repository.ClassificationDecision{
		Source: failure.SourceValidation, ReleaseID: "r1", RemediationRound: 1, NodeID: "s.n",
		Category: failure.CategoryLogic, ErrorSignature: "sig",
		Decision: failure.DecisionEmit, Reason: "logic:x", CreatedAt: time.Now().UTC(),
	}
	first, err := repo.Upsert(context.Background(), d)
	require.NoError(t, err)
	require.True(t, first, "first upsert inserts")
	second, err := repo.Upsert(context.Background(), d)
	require.NoError(t, err)
	require.False(t, second, "second upsert is a no-op (idempotent)")
	require.NoError(t, tx.Commit())
}

// TestDecisionRepositoryUpsertScopedByRound verifies that
// remediation_round is part of the natural key: reclassifying the same
// (source, release, node) at a later round — a human's "try again" on the
// rejected release — inserts a fresh row rather than colliding with the
// earlier round's decision.
func TestDecisionRepositoryUpsertScopedByRound(t *testing.T) {
	db := newTestDB(t)
	tx, err := db.Beginx()
	require.NoError(t, err)
	repo := NewDecisionRepository(tx)

	base := repository.ClassificationDecision{
		Source: failure.SourceValidation, ReleaseID: "r1", NodeID: "s.n",
		Category: failure.CategoryLogic, ErrorSignature: "sig",
		Decision: failure.DecisionEmit, Reason: "logic:x", CreatedAt: time.Now().UTC(),
	}

	roundOne := base
	roundOne.RemediationRound = 1
	inserted, err := repo.Upsert(context.Background(), roundOne)
	require.NoError(t, err)
	require.True(t, inserted, "round 1 inserts")

	roundOneAgain := roundOne
	inserted, err = repo.Upsert(context.Background(), roundOneAgain)
	require.NoError(t, err)
	require.False(t, inserted, "redelivering round 1 is a no-op")

	roundTwo := base
	roundTwo.RemediationRound = 2
	inserted, err = repo.Upsert(context.Background(), roundTwo)
	require.NoError(t, err)
	require.True(t, inserted, "round 2 is a fresh insert, not a conflict with round 1")

	require.NoError(t, tx.Commit())
}
