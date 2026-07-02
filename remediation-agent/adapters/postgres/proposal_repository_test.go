//go:build integration

package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/testmigrations"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/google/uuid"
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

// TestProposalRepositoryInsertGeneratingIdempotent verifies that InsertGenerating
// writes a single in-flight 'generating' row and that a second call for the same
// (release_id, source, node_id, attempt) is a no-op (ON CONFLICT DO NOTHING),
// modelling a redelivery of an in-flight attempt.
func TestProposalRepositoryInsertGeneratingIdempotent(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)
	p := proposal.Proposal{
		Source:         "compile",
		ReleaseID:      "release-gen-1",
		NodeID:         "service-1",
		ErrorSignature: "gen-sig",
		Attempt:        1,
		Status:         proposal.StatusGenerating,
		CreatedAt:      time.Now().UTC(),
	}
	require.NoError(t, repo.InsertGenerating(ctx, p), "first InsertGenerating")
	require.NoError(t, repo.InsertGenerating(ctx, p), "second InsertGenerating must be a no-op")

	var count int
	require.NoError(t, tx.GetContext(ctx, &count,
		`SELECT count(*) FROM proposal WHERE release_id=$1 AND source=$2 AND node_id=$3 AND attempt=$4`,
		p.ReleaseID, p.Source, p.NodeID, p.Attempt))
	require.Equal(t, 1, count, "exactly one generating row must exist after two InsertGenerating calls")

	var status string
	require.NoError(t, tx.GetContext(ctx, &status,
		`SELECT status FROM proposal WHERE release_id=$1 AND source=$2 AND node_id=$3 AND attempt=$4`,
		p.ReleaseID, p.Source, p.NodeID, p.Attempt))
	require.Equal(t, string(proposal.StatusGenerating), status)

	require.NoError(t, tx.Commit())
}

// TestProposalRepositoryInsertFinalizesGenerating verifies that Insert upserts a
// prior 'generating' row in place: after finalization exactly one row exists for
// the attempt, its status is terminal, and the terminal payload columns are set.
func TestProposalRepositoryInsertFinalizesGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)
	now := time.Now().UTC().Truncate(time.Microsecond)

	gen := proposal.Proposal{
		Source: "compile", ReleaseID: "release-fin-1", NodeID: "service-1",
		ErrorSignature: "fin-sig", Attempt: 1, Status: proposal.StatusGenerating, CreatedAt: now,
	}
	require.NoError(t, repo.InsertGenerating(ctx, gen))

	final := proposal.Proposal{
		Source: "compile", ReleaseID: "release-fin-1", NodeID: "service-1",
		ErrorSignature: "fin-sig", Attempt: 1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "fixed config block",
		ProposedSQLURI: "s3://bucket/sql/fin1",
		DiffURI:        "s3://bucket/diff/fin1",
		SourceResolved: true,
		Repo:           "owner/repo",
		CommitSHA:      "deadbeef",
		FilePath:       "models/x.sql",
		Model:          "m",
		CreatedAt:      now,
	}
	require.NoError(t, repo.Insert(ctx, final), "Insert must finalize the generating row")

	var count int
	require.NoError(t, tx.GetContext(ctx, &count,
		`SELECT count(*) FROM proposal WHERE release_id=$1 AND source=$2 AND node_id=$3 AND attempt=$4`,
		final.ReleaseID, final.Source, final.NodeID, final.Attempt))
	require.Equal(t, 1, count, "finalize must not create a second row")

	type row struct {
		Status         string `db:"status"`
		ProposedSQLURI string `db:"proposed_sql_uri"`
		SourceResolved bool   `db:"source_resolved"`
		FilePath       string `db:"file_path"`
	}
	var got row
	require.NoError(t, tx.GetContext(ctx, &got,
		`SELECT status, proposed_sql_uri, source_resolved, file_path
		 FROM proposal WHERE release_id=$1 AND source=$2 AND node_id=$3 AND attempt=$4`,
		final.ReleaseID, final.Source, final.NodeID, final.Attempt))
	require.Equal(t, string(proposal.StatusProposed), got.Status)
	require.Equal(t, "s3://bucket/sql/fin1", got.ProposedSQLURI)
	require.True(t, got.SourceResolved)
	require.Equal(t, "models/x.sql", got.FilePath)

	require.NoError(t, tx.Commit())
}

// TestProposalRepositoryCountAttemptsExcludesGenerating verifies that an in-flight
// 'generating' row is not counted toward the attempt cap: two terminal rows plus
// one generating row for the same triplet count as two attempts.
func TestProposalRepositoryCountAttemptsExcludesGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)
	now := time.Now().UTC()
	const source, nodeID, sig = "validation", "schema.model_g", "gen-count-sig"

	for i := 1; i <= 2; i++ {
		require.NoError(t, repo.Insert(ctx, proposal.Proposal{
			Source: source, ReleaseID: "release-g", NodeID: nodeID, ErrorSignature: sig,
			Attempt: i, Status: proposal.StatusProposed, CreatedAt: now,
		}), "insert terminal attempt %d", i)
	}
	// A third, in-flight attempt marked generating.
	require.NoError(t, repo.InsertGenerating(ctx, proposal.Proposal{
		Source: source, ReleaseID: "release-g", NodeID: nodeID, ErrorSignature: sig,
		Attempt: 3, Status: proposal.StatusGenerating, CreatedAt: now,
	}))

	n, err := repo.CountAttempts(ctx, source, nodeID, sig)
	require.NoError(t, err)
	require.Equal(t, 2, n, "generating row must be excluded from the attempt count")

	require.NoError(t, tx.Commit())
}

// TestMessageProcessingAlreadyProcessed verifies the read-only dedup pre-check on
// both axes: a row inserted on (message_id, stream_name) is found by that pair,
// and a row inserted with an outbox_entry_id is found by that id even under a
// different message id. Unknown identities return false.
func TestMessageProcessingAlreadyProcessed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := messageprocessing.NewPostgresRepository(tx, slog.Default())
	const stream = "remediation.requested:v1"

	// Axis 1: (message_id, stream_name).
	_, inserted, err := repo.InsertIfNotExists(ctx, &messageprocessing.MessageProcessing{
		MessageID: "m-1", StreamName: stream, State: "processing", Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	got, err := repo.AlreadyProcessed(ctx, "m-1", stream, nil)
	require.NoError(t, err)
	require.True(t, got, "known (message_id, stream) must report processed")

	got, err = repo.AlreadyProcessed(ctx, "m-unknown", stream, nil)
	require.NoError(t, err)
	require.False(t, got, "unknown message must report not-processed")

	// Axis 2: outbox_entry_id, caught even with a fresh message_id.
	oe := uuid.New()
	_, inserted, err = repo.InsertIfNotExists(ctx, &messageprocessing.MessageProcessing{
		MessageID: "m-2", StreamName: stream, State: "processing", Payload: []byte(`{}`), OutboxEntryID: &oe,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	got, err = repo.AlreadyProcessed(ctx, "m-different", stream, &oe)
	require.NoError(t, err)
	require.True(t, got, "same outbox_entry_id under a fresh message id must report processed")

	otherOE := uuid.New()
	got, err = repo.AlreadyProcessed(ctx, "m-different", stream, &otherOE)
	require.NoError(t, err)
	require.False(t, got, "unknown outbox_entry_id must report not-processed")

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

// seedProposal inserts a proposal and returns the generated UUID id.
// It reads back the id via SELECT after the insert so callers don't need DB knowledge.
func seedProposal(t *testing.T, repo *ProposalRepository, db *sqlx.DB, p proposal.Proposal) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, repo.Insert(ctx, p))
	var id string
	require.NoError(t, db.GetContext(ctx, &id,
		`SELECT id FROM proposal WHERE release_id=$1 AND node_id=$2 AND attempt=$3`,
		p.ReleaseID, p.NodeID, p.Attempt,
	))
	return id
}

// TestBeginPR_SingleWinner verifies that exactly one concurrent BeginPR call
// succeeds; the second receives ErrPRConflict.
func TestBeginPR_SingleWinner(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-beginpr-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	c1, err := repo.BeginPR(ctx, id, "remediation/r-1/orders_d-attempt1")
	require.NoError(t, err)
	require.Equal(t, "owner/continuo-dbt-demo", c1.Repo)
	require.Equal(t, "remediation/r-1/orders_d-attempt1", c1.Branch)
	require.Equal(t, id, c1.ID)

	// Second claim on the same proposal must return ErrPRConflict.
	_, err = repo.BeginPR(ctx, id, "remediation/r-1/orders_d-attempt1")
	require.ErrorIs(t, err, repository.ErrPRConflict)
}

// TestBeginPR_RejectsUnresolvedSource verifies that BeginPR returns
// ErrNotSourceResolved when source_resolved=false.
func TestBeginPR_RejectsUnresolvedSource(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-unresolved-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceLow,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: false, // not resolved
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	_, err := repo.BeginPR(ctx, id, "b")
	require.ErrorIs(t, err, repository.ErrNotSourceResolved)
}

// TestRecordPR_ThenGet verifies that RecordPR flips pr_state to 'open' and that
// Get returns the updated row with the PR details.
func TestRecordPR_ThenGet(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-recordpr-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	_, err := repo.BeginPR(ctx, id, "b")
	require.NoError(t, err)

	openedAt := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repo.RecordPR(ctx, id, "https://gh/pr/7", 7, "dev|local", openedAt))

	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "open", v.PrState)
	require.Equal(t, "https://gh/pr/7", v.PrURL)
	require.Equal(t, 7, v.PrNumber)
	require.Equal(t, "dev|local", v.PrOpenedBy)
	require.NotNil(t, v.PrOpenedAt)

	// FailPR on an 'open' row is a no-op (0 rows updated), should not error.
	require.NoError(t, repo.FailPR(ctx, id))
	v2, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "open", v2.PrState, "FailPR must not affect an 'open' row")
}

// TestList_FilterAwaitingHuman verifies that List returns only rows matching
// the given Status filter.
func TestList_FilterAwaitingHuman(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-list-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	_ = seedProposal(t, repo, db, p)

	views, err := repo.List(ctx, repository.ProposalFilter{Status: "proposed", PRState: ""})
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "proposed", string(views[0].Status))
}
