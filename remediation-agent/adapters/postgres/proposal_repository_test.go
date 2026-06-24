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

// TestProposalRepositorySourceFixFields inserts a proposal with all three
// source-fix columns populated and verifies they round-trip through the DB.
// A second row with SourceResolved=true verifies the boolean is persisted
// independently of the URI fields.
func TestProposalRepositorySourceFixFields(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Insert a proposal with all three source-fix fields set.
	p := proposal.Proposal{
		Source:              "validation",
		ReleaseID:           "release-sf-1",
		NodeID:              "schema.model_sf",
		ErrorSignature:      "err-sig-sf",
		Attempt:             1,
		Status:              proposal.StatusProposed,
		Confidence:          proposal.ConfidenceHigh,
		Rationale:           "source fix rationale",
		ProposedSQLURI:      "s3://bucket/sql/sf1",
		DiffURI:             "s3://bucket/diff/sf1",
		CandidateFixSQLURI:  "s3://bucket/candidate/sql/sf1",
		CandidateFixDiffURI: "s3://bucket/candidate/diff/sf1",
		SourceResolved:      true,
		Model:               "claude-3-5-sonnet",
		CreatedAt:           now,
	}
	require.NoError(t, repo.Insert(ctx, p), "insert proposal with source-fix fields")

	// Read back and verify round-trip.
	type row struct {
		CandidateFixSQLURI  string `db:"candidate_fix_sql_uri"`
		CandidateFixDiffURI string `db:"candidate_fix_diff_uri"`
		SourceResolved      bool   `db:"source_resolved"`
	}
	var got row
	require.NoError(t, tx.GetContext(ctx, &got,
		`SELECT candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved
		 FROM proposal WHERE release_id=$1 AND node_id=$2 AND attempt=$3`,
		p.ReleaseID, p.NodeID, p.Attempt,
	), "read back source-fix fields")
	require.Equal(t, p.CandidateFixSQLURI, got.CandidateFixSQLURI, "candidate_fix_sql_uri round-trip")
	require.Equal(t, p.CandidateFixDiffURI, got.CandidateFixDiffURI, "candidate_fix_diff_uri round-trip")
	require.True(t, got.SourceResolved, "source_resolved must be true")

	// Insert a second row with SourceResolved=true and non-empty CandidateFixSQLURI
	// to verify the column stores distinct true values across rows.
	p2 := proposal.Proposal{
		Source:              "validation",
		ReleaseID:           "release-sf-1",
		NodeID:              "schema.model_sf",
		ErrorSignature:      "err-sig-sf",
		Attempt:             2,
		Status:              proposal.StatusProposed,
		Confidence:          proposal.ConfidenceMedium,
		Rationale:           "source fix rationale attempt 2",
		ProposedSQLURI:      "s3://bucket/sql/sf2",
		DiffURI:             "s3://bucket/diff/sf2",
		CandidateFixSQLURI:  "s3://bucket/candidate/sql/sf2",
		CandidateFixDiffURI: "",
		SourceResolved:      true,
		Model:               "claude-3-5-sonnet",
		CreatedAt:           now,
	}
	require.NoError(t, repo.Insert(ctx, p2), "insert second proposal with SourceResolved=true")

	var got2 row
	require.NoError(t, tx.GetContext(ctx, &got2,
		`SELECT candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved
		 FROM proposal WHERE release_id=$1 AND node_id=$2 AND attempt=$3`,
		p2.ReleaseID, p2.NodeID, p2.Attempt,
	), "read back second source-fix row")
	require.Equal(t, "s3://bucket/candidate/sql/sf2", got2.CandidateFixSQLURI, "candidate_fix_sql_uri second row")
	require.True(t, got2.SourceResolved, "source_resolved must be true on second row")

	require.NoError(t, tx.Commit())
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

// TestInsert_PersistsSourceLocation inserts a proposal with repo, commit_sha,
// and file_path set and verifies they are written to and read back from the DB.
func TestInsert_PersistsSourceLocation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-1",
		NodeID:         "model.p.orders_d",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		CreatedAt:      time.Now(),
	}
	require.NoError(t, repo.Insert(ctx, p))

	var got struct {
		Repo      string `db:"repo"`
		CommitSha string `db:"commit_sha"`
		FilePath  string `db:"file_path"`
	}
	require.NoError(t, db.GetContext(ctx, &got,
		`SELECT repo, commit_sha, file_path FROM proposal WHERE release_id=$1 AND node_id=$2 AND attempt=$3`,
		"r-1", "model.p.orders_d", 1))
	require.Equal(t, "owner/continuo-dbt-demo", got.Repo)
	require.Equal(t, "abc123", got.CommitSha)
	require.Equal(t, "services/service-3/models/orders_d.sql", got.FilePath)
}
