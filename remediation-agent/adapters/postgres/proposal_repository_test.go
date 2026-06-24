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
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// remediationAgentMigrationDir resolves <repo>/db/migration/remediation_agent/
// from this source file's location so the testcontainer schema stays in
// lock-step with the production migration.
func remediationAgentMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate proposal_repository_test.go")
	}
	// thisFile = <repo>/remediation-agent/adapters/postgres/proposal_repository_test.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	return filepath.Join(repoRoot, "db", "migration", "remediation_agent"), nil
}

// getEnvOrDefault returns the env var value or the fallback default.
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// newTestDB boots a PostgreSQL testcontainer, applies the remediation_agent
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

	dir, err := remediationAgentMigrationDir()
	require.NoError(t, err, "resolve remediation_agent migration dir")
	require.NoError(t, testmigrations.Apply(db.DB, dir), "apply remediation_agent migrations")

	return db
}

// TestProposalRepositoryCountAttempts inserts 3 proposal attempts for one
// (source, node_id, error_signature) triplet and 1 for a different node,
// verifying that CountAttempts returns the correct count per triplet.
func TestProposalRepositoryCountAttempts(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)

	const source = "validation"
	const nodeID = "schema.model_a"
	const sig = "err-sig-abc"
	now := time.Now().UTC()

	// Insert 3 attempts for the same (source, nodeID, sig).
	for i := 1; i <= 3; i++ {
		p := proposal.Proposal{
			Source:         source,
			ReleaseID:      "release-1",
			NodeID:         nodeID,
			ErrorSignature: sig,
			Attempt:        i,
			Status:         proposal.StatusProposed,
			Confidence:     proposal.ConfidenceHigh,
			Rationale:      fmt.Sprintf("attempt %d rationale", i),
			ProposedSQLURI: fmt.Sprintf("s3://bucket/sql/%d", i),
			DiffURI:        fmt.Sprintf("s3://bucket/diff/%d", i),
			Model:          "claude-3-5-sonnet",
			CreatedAt:      now,
		}
		require.NoError(t, repo.Insert(ctx, p), "insert attempt %d", i)
	}

	count, err := repo.CountAttempts(ctx, source, nodeID, sig)
	require.NoError(t, err)
	require.Equal(t, 3, count, "expected 3 attempts for the original node")

	// Insert a 4th proposal for a DIFFERENT node; the count for the first node must stay 3.
	other := proposal.Proposal{
		Source:         source,
		ReleaseID:      "release-1",
		NodeID:         "schema.model_b",
		ErrorSignature: sig,
		Attempt:        1,
		Status:         proposal.StatusSkipped,
		Confidence:     proposal.ConfidenceLow,
		Rationale:      "other node",
		ProposedSQLURI: "",
		DiffURI:        "",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      now,
	}
	require.NoError(t, repo.Insert(ctx, other), "insert other node attempt")

	count, err = repo.CountAttempts(ctx, source, nodeID, sig)
	require.NoError(t, err)
	require.Equal(t, 3, count, "count for original node must remain 3 after inserting different node")

	require.NoError(t, tx.Commit())
}
