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
	"github.com/carolsimone/continuo/pkg/streams"
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
	require.NoError(t, repo.Upsert(ctx, p), "insert proposal with source-fix fields")

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
	require.NoError(t, repo.Upsert(ctx, p2), "insert second proposal with SourceResolved=true")

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
	require.NoError(t, repo.Upsert(ctx, final), "Upsert must finalize the generating row")

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
		require.NoError(t, repo.Upsert(ctx, proposal.Proposal{
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
	const stream = streams.RemediationRequestedV1

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

// TestMessageProcessingAlreadyProcessedScopedByStream is the regression guard for
// the cross-stream dedup collision: AlreadyProcessed must match the table's scoped
// (outbox_entry_id, stream_name) identity, so one consumer group having processed
// an upstream outbox entry on its stream must not suppress a DIFFERENT group's
// message that carries the same outbox_entry_id on another stream.
func TestMessageProcessingAlreadyProcessedScopedByStream(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := messageprocessing.NewPostgresRepository(tx, slog.Default())
	const streamA = streams.RemediationRequestedV1
	const streamB = streams.RemediationProposedV1

	// Group A processed outbox entry `oe` on streamA (message m-A).
	oe := uuid.New()
	_, inserted, err := repo.InsertIfNotExists(ctx, &messageprocessing.MessageProcessing{
		MessageID: "m-A", StreamName: streamA, State: "processing", Payload: []byte(`{}`), OutboxEntryID: &oe,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	// Different stream, same outbox entry → must NOT be suppressed.
	got, err := repo.AlreadyProcessed(ctx, "m-B", streamB, &oe)
	require.NoError(t, err)
	require.False(t, got, "same outbox_entry_id on a different stream must NOT report processed")

	// Same stream, same outbox entry (fresh message id) → the outbox redelivery
	// axis still catches it.
	got, err = repo.AlreadyProcessed(ctx, "m-B", streamA, &oe)
	require.NoError(t, err)
	require.True(t, got, "same outbox_entry_id on the same stream must report processed")

	// Primary (message_id, stream_name) axis still works with a nil outbox entry.
	got, err = repo.AlreadyProcessed(ctx, "m-A", streamA, nil)
	require.NoError(t, err)
	require.True(t, got, "known (message_id, stream) must report processed")

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
		require.NoError(t, repo.Upsert(ctx, p), "insert attempt %d", i)
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
	require.NoError(t, repo.Upsert(ctx, other), "insert other node attempt")

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
	require.NoError(t, repo.Upsert(ctx, p))

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
	require.NoError(t, repo.Upsert(ctx, p))
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

	c1, err := repo.BeginPR(ctx, id, "remediation/r-1/orders_d-attempt1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "owner/continuo-dbt-demo", c1.Repo)
	require.Equal(t, "remediation/r-1/orders_d-attempt1", c1.Branch)
	require.Equal(t, id, c1.ID)

	// Second claim on the same proposal must return ErrPRConflict.
	_, err = repo.BeginPR(ctx, id, "remediation/r-1/orders_d-attempt1", time.Now().UTC())
	require.ErrorIs(t, err, repository.ErrPRConflict)
}

// TestBeginPR_ListsAsStuckOpening verifies that a claimed proposal
// (pr_state='opening') is surfaced by ListStuckOpening with the fields the
// reconciler's opening sweep needs — including the pr_claimed_at BeginPR
// stamped — to recompute the deterministic branch and age the claim, and
// that it drops out of the listing (with pr_claimed_at cleared) once recorded.
func TestBeginPR_ListsAsStuckOpening(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-opening-1",
		NodeID:         "model.p.orders",
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

	claimedAt := time.Now().UTC().Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "remediation/r-opening-1/model-p-orders-attempt1", claimedAt)
	require.NoError(t, err)

	stuck, next, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Nil(t, next, "a page under the limit must report no next cursor")
	require.Len(t, stuck, 1)
	require.Equal(t, id, stuck[0].ID)
	require.Equal(t, "owner/continuo-dbt-demo", stuck[0].Repo)
	require.Equal(t, "r-opening-1", stuck[0].ReleaseID)
	require.Equal(t, "model.p.orders", stuck[0].NodeID)
	require.Equal(t, 1, stuck[0].Attempt)
	require.NotNil(t, stuck[0].ClaimedAt, "BeginPR must stamp pr_claimed_at")
	require.WithinDuration(t, claimedAt, *stuck[0].ClaimedAt, time.Second)

	// Once recorded, the row is no longer a stuck-opening claim, and its
	// pr_claimed_at is cleared so a future re-claim never inherits this one.
	hit, err := repo.RecordPR(ctx, id, "https://gh/pr/1", 1, "dev", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, hit)
	stuck, _, err = repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Empty(t, stuck, "a recorded PR must no longer be listed as stuck opening")

	var stillClaimed *time.Time
	require.NoError(t, db.GetContext(ctx, &stillClaimed, `SELECT pr_claimed_at FROM proposal WHERE id=$1`, id))
	require.Nil(t, stillClaimed, "RecordPR must clear pr_claimed_at back to NULL")
}

// TestOpeningTransition_TriggerStampsClaimedAtWhenWriterOmitsIt verifies the
// trigger's fill-when-NULL guard: a bare UPDATE that moves pr_state to
// 'opening' without setting pr_claimed_at — modelling a proposal-service
// binary that predates the column and cannot be taught about it — is stamped
// with the actual wall-clock moment of the transition by the database
// itself, not left NULL.
// This closes the gap the opening sweep cannot close on its own: an
// unmeasurable claim can never be judged stale, so a claim that stayed NULL
// forever would never be recoverable.
func TestOpeningTransition_TriggerStampsClaimedAtWhenWriterOmitsIt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-opening-null",
		NodeID:         "model.p.legacy",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	// Bypass BeginPR to simulate a pre-upgrade binary's write: it moves
	// pr_state to 'opening' without knowing pr_claimed_at exists.
	before := time.Now().UTC()
	_, err := db.ExecContext(ctx, `UPDATE proposal SET pr_state='opening' WHERE id=$1`, id)
	require.NoError(t, err)

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NotNil(t, stuck[0].ClaimedAt, "the trigger must stamp pr_claimed_at even when the writer never sets it")
	require.WithinDuration(t, before, *stuck[0].ClaimedAt, 5*time.Second,
		"the stamped value must be the transition's own moment, not an earlier fabricated time")
}

// TestOpeningTransition_TriggerDoesNotOverrideExplicitClaimedAt verifies the
// trigger only fills in a value a writer's UPDATE left NULL: BeginPR's own
// explicit pr_claimed_at (from the service's Clock port) passes through
// unchanged.
func TestOpeningTransition_TriggerDoesNotOverrideExplicitClaimedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-opening-explicit",
		NodeID:         "model.p.explicit",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	claimedAt := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "remediation/r-opening-explicit/model-p-explicit-attempt1", claimedAt)
	require.NoError(t, err)

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NotNil(t, stuck[0].ClaimedAt)
	require.Equal(t, claimedAt, stuck[0].ClaimedAt.UTC(),
		"the trigger must not override a pr_claimed_at the writer set explicitly, even a backdated one")
}

// TestOpeningTransition_ExitClearsStaleTimestampFromWriterThatOmitsColumn
// verifies the invariant the trigger's exit branch guarantees: an old binary
// that predates pr_claimed_at can leave 'opening' (its FailPR-equivalent
// UPDATE never mentions the column) and later re-enter 'opening' on the same
// row (its BeginPR-equivalent UPDATE does not mention it either), all without
// ever writing to pr_claimed_at itself. Both UPDATEs are simulated here
// exactly as such a binary would issue them — bare SET pr_state, no
// pr_claimed_at in sight. The trigger clears the column on the exit UPDATE
// and re-stamps it fresh on the re-entry UPDATE, so the second claim's age is
// always computed from its own claim time, never inherited from a claim that
// already ended.
func TestOpeningTransition_ExitClearsStaleTimestampFromWriterThatOmitsColumn(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-opening-legacy",
		NodeID:         "model.p.legacy_reclaim",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	// First claim, taken by a current binary an hour ago (BeginPR sets
	// pr_claimed_at explicitly) — old enough that, if it leaked into a second
	// claim's age, the sweep's grace period would judge it stale immediately.
	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "remediation/r-opening-legacy/model-p-legacy_reclaim-attempt1", firstClaim)
	require.NoError(t, err)

	// An old binary's FailPR-equivalent: it predates pr_claimed_at, so its
	// UPDATE never references the column.
	_, err = db.ExecContext(ctx, `UPDATE proposal SET pr_state='failed' WHERE id=$1`, id)
	require.NoError(t, err)

	var afterExit *time.Time
	require.NoError(t, db.GetContext(ctx, &afterExit, `SELECT pr_claimed_at FROM proposal WHERE id=$1`, id))
	require.Nil(t, afterExit, "the exit transition must clear pr_claimed_at even though the writer's UPDATE never mentioned the column")

	// The same old binary re-claims the row — again without mentioning
	// pr_claimed_at.
	before := time.Now().UTC()
	_, err = db.ExecContext(ctx, `UPDATE proposal SET pr_state='opening' WHERE id=$1`, id)
	require.NoError(t, err)

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NotNil(t, stuck[0].ClaimedAt)
	require.WithinDuration(t, before, *stuck[0].ClaimedAt, 5*time.Second,
		"the second claim must age from its own (fresh) transition moment")
	require.True(t, stuck[0].ClaimedAt.After(firstClaim),
		"the second claim's age must never be computed from the first claim's timestamp")
}

// TestFailStuckOpeningPR_RemovesFromStuckOpeningAndAllowsReclaim verifies
// that FailStuckOpeningPR drops a claim out of ListStuckOpening, clears
// pr_claimed_at, and returns pr_state to 'failed', which BeginPR's CAS
// accepts for a retry — and that the re-claim ages from its own (second)
// pr_claimed_at, never the first.
func TestFailStuckOpeningPR_RemovesFromStuckOpeningAndAllowsReclaim(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-opening-2",
		NodeID:         "model.p.customers",
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
		FilePath:       "services/service-3/models/customers_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "remediation/r-opening-2/model-p-customers-attempt1", firstClaim)
	require.NoError(t, err)

	hit, err := repo.FailStuckOpeningPR(ctx, id, firstClaim)
	require.NoError(t, err)
	require.True(t, hit)

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Empty(t, stuck, "a failed claim must no longer be listed as stuck opening")

	var clearedClaim *time.Time
	require.NoError(t, db.GetContext(ctx, &clearedClaim, `SELECT pr_claimed_at FROM proposal WHERE id=$1`, id))
	require.Nil(t, clearedClaim, "FailStuckOpeningPR must clear pr_claimed_at back to NULL")

	// The proposal is re-claimable after FailPR, and ages from the SECOND
	// claim time, not the first (which was an hour older).
	secondClaim := time.Now().UTC().Truncate(time.Microsecond)
	claim, err := repo.BeginPR(ctx, id, "remediation/r-opening-2/model-p-customers-attempt1", secondClaim)
	require.NoError(t, err, "a 'failed' proposal must be re-claimable")
	require.Equal(t, id, claim.ID)

	stuck, _, err = repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NotNil(t, stuck[0].ClaimedAt)
	require.WithinDuration(t, secondClaim, *stuck[0].ClaimedAt, time.Second,
		"a re-claim must age from its own claim time, not the earlier failed one")
}

// TestFailStuckOpeningPR_CASGuardsAgainstReClaim is the regression test for
// the lost-update every caller of FailStuckOpeningPR must avoid — the
// reconciler's opening sweep releasing a stale claim it listed earlier in a
// pass, or the ui-service PR-creation route releasing the claim its own
// BeginPullRequest took after a downstream failure: the call must only fail
// the exact claim identified by (id, observedClaimedAt). If the row was
// released and re-claimed in between — the row now carries a different
// pr_claimed_at — the call must be a no-op that leaves the fresh claim
// completely untouched, never a blind unconditional fail.
func TestFailStuckOpeningPR_CASGuardsAgainstReClaim(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-cas-1",
		NodeID:         "model.p.cas",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-dbt-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "remediation/r-cas-1/model-p-cas-attempt1", firstClaim)
	require.NoError(t, err)

	// Simulate a second reconciler racing in: the row is released (an
	// operator retries) and re-claimed with a fresh pr_claimed_at, between
	// the first reconciler's list and its fail call below.
	releaseHit, err := repo.FailStuckOpeningPR(ctx, id, firstClaim)
	require.NoError(t, err)
	require.True(t, releaseHit)
	secondClaim := time.Now().UTC().Truncate(time.Microsecond)
	_, err = repo.BeginPR(ctx, id, "remediation/r-cas-1/model-p-cas-attempt1", secondClaim)
	require.NoError(t, err)

	// The first reconciler now acts on the STALE observed claim time
	// (firstClaim). The CAS must miss: the row's pr_claimed_at is secondClaim.
	hit, err := repo.FailStuckOpeningPR(ctx, id, firstClaim)
	require.NoError(t, err)
	require.False(t, hit, "the CAS must miss when pr_claimed_at no longer matches the observed value")

	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "opening", v.PrState, "the fresh claim must be untouched by the stale fail")

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NotNil(t, stuck[0].ClaimedAt)
	require.Equal(t, secondClaim, stuck[0].ClaimedAt.UTC(),
		"the row must still carry the second (fresh) claim time, undisturbed")

	// Now fail with the CORRECT (current) observed claim time — the CAS hits.
	hit, err = repo.FailStuckOpeningPR(ctx, id, secondClaim)
	require.NoError(t, err)
	require.True(t, hit, "the CAS must hit when the observed claim time matches the row's current pr_claimed_at")

	v, err = repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "failed", v.PrState)
	var clearedClaim *time.Time
	require.NoError(t, db.GetContext(ctx, &clearedClaim, `SELECT pr_claimed_at FROM proposal WHERE id=$1`, id))
	require.Nil(t, clearedClaim, "a successful CAS must clear pr_claimed_at back to NULL")
}

// TestListStuckOpening_CursorRotatesAcrossPages verifies the keyset
// pagination that lets the reconciler's opening sweep rotate past a stuck
// prefix instead of re-reading the same oldest rows every pass: with three
// 'opening' claims and a page size of 2, the first page returns the two
// oldest with a non-nil next cursor, and resuming from that cursor returns
// exactly the third (oldest-created third) row with a nil next cursor.
func TestListStuckOpening_CursorRotatesAcrossPages(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	var ids []string
	for i, node := range []string{"model.p.c1", "model.p.c2", "model.p.c3"} {
		p := proposal.Proposal{
			Source:         "validation",
			ReleaseID:      "r-cursor-1",
			NodeID:         node,
			ErrorSignature: "sig",
			Attempt:        1,
			Status:         proposal.StatusProposed,
			SourceResolved: true,
			Repo:           "owner/continuo-dbt-demo",
			// Strictly increasing created_at so the (created_at, id) order is
			// deterministic and matches insertion order.
			CreatedAt: time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		id := seedProposal(t, repo, db, p)
		branch := fmt.Sprintf("remediation/r-cursor-1/%s-attempt1", node)
		_, err := repo.BeginPR(ctx, id, branch, time.Now().UTC())
		require.NoError(t, err)
		ids = append(ids, id)
	}

	page1, next1, err := repo.ListStuckOpening(ctx, 2, nil)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.Equal(t, ids[0], page1[0].ID)
	require.Equal(t, ids[1], page1[1].ID)
	require.NotNil(t, next1, "a full page must report a next cursor when more rows exist")

	page2, next2, err := repo.ListStuckOpening(ctx, 2, next1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, ids[2], page2[0].ID)
	require.Nil(t, next2, "the last page must report a nil next cursor")

	// Resuming from the end wraps to nothing further; a caller that then
	// passes a nil cursor (as the reconciler does after a nil next) sees the
	// full set again from the start, completing the rotation.
	page3, next3, err := repo.ListStuckOpening(ctx, 2, nil)
	require.NoError(t, err)
	require.Len(t, page3, 2)
	require.Equal(t, ids[0], page3[0].ID, "a nil cursor always restarts the rotation from the oldest row")
	require.NotNil(t, next3)
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

	_, err := repo.BeginPR(ctx, id, "b", time.Now().UTC())
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

	_, err := repo.BeginPR(ctx, id, "b", time.Now().UTC())
	require.NoError(t, err)

	openedAt := time.Now().UTC().Truncate(time.Microsecond)
	hit, err := repo.RecordPR(ctx, id, "https://gh/pr/7", 7, "dev|local", openedAt)
	require.NoError(t, err)
	require.True(t, hit, "RecordPR must fire the CAS while the row is still 'opening'")

	// A second RecordPR call against the same now-'open' row is a no-op: the
	// CAS misses because pr_state is no longer 'opening', so nothing is
	// overwritten by a caller racing to record the same claim a second time.
	hit, err = repo.RecordPR(ctx, id, "https://gh/pr/should-not-apply", 99, "someone-else", time.Now().UTC())
	require.NoError(t, err)
	require.False(t, hit, "RecordPR must not fire once the row has left 'opening'")

	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "open", v.PrState)
	require.Equal(t, "https://gh/pr/7", v.PrURL)
	require.Equal(t, 7, v.PrNumber)
	require.Equal(t, "dev|local", v.PrOpenedBy)
	require.NotNil(t, v.PrOpenedAt)
	require.Equal(t, "https://gh/pr/7", v.PrURL, "the CAS-missed second RecordPR call must not have overwritten pr_url")
	require.Equal(t, 7, v.PrNumber, "the CAS-missed second RecordPR call must not have overwritten pr_number")

	// FailStuckOpeningPR on an 'open' row is a no-op (0 rows updated, hit=false),
	// should not error, regardless of the observed timestamp passed in.
	hit, err = repo.FailStuckOpeningPR(ctx, id, time.Now().UTC())
	require.NoError(t, err)
	require.False(t, hit, "FailStuckOpeningPR must not affect an 'open' row")
	v2, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "open", v2.PrState, "FailStuckOpeningPR must not affect an 'open' row")
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

// TestRecordPROutcome_CASAndListOpen drives a proposal through
// open -> merged and verifies: ListOpenPullRequests only surfaces 'open' rows,
// the CAS fires exactly once, pr_closed_at is persisted, and rows in
// non-'open' states are never transitioned.
func TestRecordPROutcome_CASAndListOpen(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	// Seed one proposal and walk it to pr_state='open'.
	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "rel-1",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig-1",
		Attempt:        1,
		Status:         "proposed",
		SourceResolved: true,
		Repo:           "acme/dbt-repo",
		CommitSHA:      "abc123",
		FilePath:       "models/orders.sql",
		CreatedAt:      time.Now().UTC(),
	}
	require.NoError(t, repo.Upsert(ctx, p))
	var id string
	require.NoError(t, db.GetContext(ctx, &id,
		`SELECT id FROM proposal WHERE release_id='rel-1' AND node_id='model.p.orders'`))
	_, err := repo.BeginPR(ctx, id, "remediation/rel-1/model-p-orders-attempt1", time.Now().UTC())
	require.NoError(t, err)
	openedAt := time.Now().UTC()
	_, err = repo.RecordPR(ctx, id, "http://gh/pull/1", 1, "dev", openedAt)
	require.NoError(t, err)

	// The open PR is listed with the fields the reconciler needs.
	open, err := repo.ListOpenPullRequests(ctx, 10)
	require.NoError(t, err)
	require.Len(t, open, 1)
	require.Equal(t, id, open[0].ID)
	require.Equal(t, "acme/dbt-repo", open[0].Repo)
	require.Equal(t, 1, open[0].PRNumber)
	require.Equal(t, "rel-1", open[0].ReleaseID)
	require.Equal(t, "model.p.orders", open[0].NodeID)
	require.Equal(t, 1, open[0].Attempt)

	// First CAS fires; second is an idempotent no-op.
	closedAt := time.Now().UTC().Truncate(time.Microsecond)
	hit, err := repo.RecordPROutcome(ctx, id, proposal.PROutcomeMerged, closedAt)
	require.NoError(t, err)
	require.True(t, hit, "first RecordPROutcome must fire the CAS")
	hit, err = repo.RecordPROutcome(ctx, id, proposal.PROutcomeRejected, closedAt)
	require.NoError(t, err)
	require.False(t, hit, "second RecordPROutcome must be a no-op")

	// The row is terminal 'merged' with pr_closed_at set, and no longer listed.
	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "merged", v.PrState)
	require.NotNil(t, v.PrClosedAt)
	require.WithinDuration(t, closedAt, *v.PrClosedAt, time.Second)
	open, err = repo.ListOpenPullRequests(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, open)
}

// TestProposalRepository_FileEditsRoundTripAndLegacySynthesis verifies that a
// non-empty Edits slice round-trips through Get unchanged, and that a row
// written with Edits=nil but non-empty legacy scalar columns (file_path,
// proposed_sql_uri, diff_uri) reads back as a single synthesized FileEdit.
func TestProposalRepository_FileEditsRoundTripAndLegacySynthesis(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	edits := []proposal.FileEdit{
		{Path: "contracts/a.yml", ContentURI: "s3://b/1.content", DiffURI: "s3://b/1.diff"},
		{Path: "scripts/a.py", ContentURI: "s3://b/2.content", DiffURI: "s3://b/2.diff"},
	}
	p := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-file-edits-1",
		NodeID:         "model.p.multi_edit",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "rationale",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
		Edits:          edits,
	}
	id := seedProposal(t, repo, db, p)

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, edits, got.Edits, "a non-empty Edits slice must round-trip unchanged")

	// The stored JSONB must use the documented snake_case wire keys
	// ("path", "content_uri", "diff_uri"), not the Go field names. A Go-level
	// round-trip through the same struct on both write and read cannot catch
	// a PascalCase regression, so this asserts the raw column contents
	// directly: a wrong key name here means ->>'path' etc. return NULL.
	var path, contentURI, diffURI string
	require.NoError(t, db.GetContext(ctx, &path,
		`SELECT file_edits->0->>'path' FROM proposal WHERE id=$1`, id))
	require.NoError(t, db.GetContext(ctx, &contentURI,
		`SELECT file_edits->0->>'content_uri' FROM proposal WHERE id=$1`, id))
	require.NoError(t, db.GetContext(ctx, &diffURI,
		`SELECT file_edits->0->>'diff_uri' FROM proposal WHERE id=$1`, id))
	require.Equal(t, edits[0].Path, path, "stored JSON must use the snake_case key \"path\"")
	require.Equal(t, edits[0].ContentURI, contentURI, "stored JSON must use the snake_case key \"content_uri\"")
	require.Equal(t, edits[0].DiffURI, diffURI, "stored JSON must use the snake_case key \"diff_uri\"")

	legacy := proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-file-edits-1",
		NodeID:         "analytics.legacy_node",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		Confidence:     proposal.ConfidenceHigh,
		Rationale:      "rationale",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
		Edits:          nil, // simulates a pre-V12 writer
		FilePath:       "models/x.sql",
		ProposedSQLURI: "s3://b/x.sql",
		DiffURI:        "s3://b/x.diff",
	}
	legacyID := seedProposal(t, repo, db, legacy)

	got2, err := repo.Get(ctx, legacyID)
	require.NoError(t, err)
	require.Equal(t,
		[]proposal.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		got2.Edits,
		"a row with an empty file_edits array must synthesize one edit from the legacy scalar columns",
	)
}

// TestRecordPROutcome_RejectedAndNonOpenRows verifies the rejected transition
// and that rows with an empty pr_state, or 'opening', or 'failed', are never
// transitioned nor listed.
func TestRecordPROutcome_RejectedAndNonOpenRows(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	seed := func(node string) string {
		p := proposal.Proposal{
			Source: "validation", ReleaseID: "rel-2", NodeID: node,
			ErrorSignature: "sig", Attempt: 1, Status: "proposed",
			SourceResolved: true, Repo: "acme/dbt-repo", CreatedAt: time.Now().UTC(),
		}
		require.NoError(t, repo.Upsert(ctx, p))
		var id string
		require.NoError(t, db.GetContext(ctx, &id,
			`SELECT id FROM proposal WHERE release_id='rel-2' AND node_id=$1`, node))
		return id
	}

	// Row A reaches 'open' then is rejected.
	a := seed("model.p.a")
	_, err := repo.BeginPR(ctx, a, "remediation/rel-2/model-p-a-attempt1", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, a, "http://gh/pull/2", 2, "dev", time.Now().UTC())
	require.NoError(t, err)
	hit, err := repo.RecordPROutcome(ctx, a, proposal.PROutcomeRejected, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, hit)
	v, err := repo.Get(ctx, a)
	require.NoError(t, err)
	require.Equal(t, "rejected", v.PrState)

	// Row B stays in 'opening' (claimed, never recorded): not listed, CAS misses.
	b := seed("model.p.b")
	_, err = repo.BeginPR(ctx, b, "remediation/rel-2/model-p-b-attempt1", time.Now().UTC())
	require.NoError(t, err)
	open, err := repo.ListOpenPullRequests(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, open, "'opening' and 'rejected' rows must not be listed")
	hit, err = repo.RecordPROutcome(ctx, b, proposal.PROutcomeMerged, time.Now().UTC())
	require.NoError(t, err)
	require.False(t, hit, "a non-'open' row must never be transitioned")
	v, err = repo.Get(ctx, b)
	require.NoError(t, err)
	require.Equal(t, "opening", v.PrState)
}

// TestBeginPR_ClaimCarriesFileEdits verifies the projection the pull-request
// writer actually consumes: BeginPR's RETURNING clause must surface the
// file_edits column on the claim, with every edit in the order it was
// written. It also covers a row whose file_edits is empty but whose scalar
// columns describe a single file — the shape a row written before the column
// existed has — which must claim as one synthesized edit rather than none.
func TestBeginPR_ClaimCarriesFileEdits(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	edits := []proposal.FileEdit{
		{Path: "contracts/a.yml", ContentURI: "s3://b/1.content", DiffURI: "s3://b/1.diff"},
		{Path: "scripts/a.py", ContentURI: "s3://b/2.content", DiffURI: "s3://b/2.diff"},
	}
	multiID := seedProposal(t, repo, db, proposal.Proposal{
		Source: "validation", ReleaseID: "rel-claim-edits", NodeID: "model.p.multi",
		ErrorSignature: "sig", Attempt: 1, Status: proposal.StatusProposed,
		SourceResolved: true, Repo: "acme/dbt-repo", CommitSHA: "sha1",
		CreatedAt: time.Now().UTC(), Edits: edits,
	})

	claim, err := repo.BeginPR(ctx, multiID, "remediation/rel-claim-edits/multi-attempt1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, edits, claim.Edits, "every edit must reach the claim, in order")

	legacyID := seedProposal(t, repo, db, proposal.Proposal{
		Source: "validation", ReleaseID: "rel-claim-edits", NodeID: "model.p.single",
		ErrorSignature: "sig", Attempt: 1, Status: proposal.StatusProposed,
		SourceResolved: true, Repo: "acme/dbt-repo", CommitSHA: "sha1",
		CreatedAt: time.Now().UTC(), Edits: nil,
		FilePath: "models/x.sql", ProposedSQLURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff",
	})

	legacyClaim, err := repo.BeginPR(ctx, legacyID, "remediation/rel-claim-edits/single-attempt1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t,
		[]proposal.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		legacyClaim.Edits,
		"a row with no file_edits must claim as one edit synthesized from the single-file columns",
	)
}

// TestProposalRepositoryVerifyingLifecycle verifies the round trip a shadow
// release drives: a proposal upserted with status='verifying' carries its
// shadow_release_id and trigger_payload, is surfaced by ListVerifying, and
// MarkVerified finalizes it to 'proposed' and removes it from the verifying
// list. A second MarkVerified call is an idempotent no-op (hit=false) since
// the row is no longer 'verifying'.
func TestProposalRepositoryVerifyingLifecycle(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	trigger := []byte(`{"source":"validation","node_id":"model.p.orders","message_id":"123-0"}`)
	p := proposal.Proposal{
		Source:          "validation",
		ReleaseID:       "rel-verify-1",
		NodeID:          "model.p.orders",
		ErrorSignature:  "sig-verify-1",
		Attempt:         1,
		Status:          proposal.StatusVerifying,
		ShadowReleaseID: "shadow-rel-abc",
		TriggerPayload:  trigger,
		Confidence:      proposal.ConfidenceHigh,
		Rationale:       "proposed fix awaiting shadow verification",
		Model:           "claude-3-5-sonnet",
		CreatedAt:       time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	verifying, err := repo.ListVerifying(ctx)
	require.NoError(t, err)
	require.Len(t, verifying, 1, "the verifying proposal must be listed")
	require.Equal(t, id, verifying[0].ID)
	require.Equal(t, string(proposal.StatusVerifying), string(verifying[0].Status))
	require.Equal(t, "shadow-rel-abc", verifying[0].ShadowReleaseID, "shadow_release_id must round-trip")
	// JSONB is a binary format: Postgres re-serializes it on read (e.g. adding
	// a space after ':' and ','), so the column never preserves the writer's
	// exact bytes. JSONEq compares the two payloads for JSON-semantic
	// equality instead of a literal byte match.
	require.JSONEq(t, string(trigger), string(verifying[0].TriggerPayload), "trigger_payload must round-trip semantically")

	hit, err := repo.MarkVerified(ctx, id)
	require.NoError(t, err)
	require.True(t, hit, "first MarkVerified must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(proposal.StatusProposed), string(got.Status), "MarkVerified must flip status to 'proposed'")

	verifying, err = repo.ListVerifying(ctx)
	require.NoError(t, err)
	require.Empty(t, verifying, "a finalized proposal must no longer be listed as verifying")

	hit, err = repo.MarkVerified(ctx, id)
	require.NoError(t, err)
	require.False(t, hit, "second MarkVerified must be a no-op: the row is no longer 'verifying'")
}

// TestProposalRepositoryMarkVerifyFailed verifies that MarkVerifyFailed
// finalizes a verifying proposal to 'failed' and records verify_error, and
// that a second call is an idempotent no-op once the row has moved on.
func TestProposalRepositoryMarkVerifyFailed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	p := proposal.Proposal{
		Source:          "validation",
		ReleaseID:       "rel-verify-2",
		NodeID:          "model.p.customers",
		ErrorSignature:  "sig-verify-2",
		Attempt:         1,
		Status:          proposal.StatusVerifying,
		ShadowReleaseID: "shadow-rel-def",
		TriggerPayload:  []byte(`{"source":"validation","node_id":"model.p.customers"}`),
		Confidence:      proposal.ConfidenceMedium,
		Rationale:       "proposed fix awaiting shadow verification",
		Model:           "claude-3-5-sonnet",
		CreatedAt:       time.Now().UTC(),
	}
	id := seedProposal(t, repo, db, p)

	hit, err := repo.MarkVerifyFailed(ctx, id, "shadow release rel-shadow-def failed: validation rejected 3 rows")
	require.NoError(t, err)
	require.True(t, hit, "first MarkVerifyFailed must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(proposal.StatusFailed), string(got.Status), "MarkVerifyFailed must flip status to 'failed'")
	require.Equal(t, "shadow release rel-shadow-def failed: validation rejected 3 rows", got.VerifyError)

	verifying, err := repo.ListVerifying(ctx)
	require.NoError(t, err)
	require.Empty(t, verifying, "a proposal failed out of verification must no longer be listed as verifying")

	hit, err = repo.MarkVerifyFailed(ctx, id, "a different error")
	require.NoError(t, err)
	require.False(t, hit, "second MarkVerifyFailed must be a no-op: the row is no longer 'verifying'")
	got2, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "shadow release rel-shadow-def failed: validation rejected 3 rows", got2.VerifyError,
		"the no-op call must not overwrite the already-recorded verify_error")
}

// TestListVerifying_OrderedOldestFirstAndLimited seeds 25 verifying proposals
// with distinct, staggered created_at values and verifies that ListVerifying
// returns exactly 20 of them (its documented cap), ordered oldest first — the
// two properties a polling reconciler depends on so one stuck row can never
// starve every other in-flight verification.
func TestListVerifying_OrderedOldestFirstAndLimited(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	const total = 25
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < total; i++ {
		seedProposal(t, repo, db, proposal.Proposal{
			Source:          "validation",
			ReleaseID:       "rel-verify-order",
			NodeID:          fmt.Sprintf("model.p.node_%02d", i),
			ErrorSignature:  "sig-verify-order",
			Attempt:         1,
			Status:          proposal.StatusVerifying,
			ShadowReleaseID: fmt.Sprintf("shadow-rel-%02d", i),
			Confidence:      proposal.ConfidenceLow,
			Model:           "claude-3-5-sonnet",
			CreatedAt:       base.Add(time.Duration(i) * time.Minute),
		})
	}

	verifying, err := repo.ListVerifying(ctx)
	require.NoError(t, err)
	require.Len(t, verifying, 20, "ListVerifying must cap at 20 rows even though 25 are in flight")

	for i, v := range verifying {
		require.Equal(t, fmt.Sprintf("shadow-rel-%02d", i), v.ShadowReleaseID,
			"row %d must be the %d-th oldest proposal (oldest first)", i, i)
	}
	for i := 1; i < len(verifying); i++ {
		require.False(t, verifying[i].CreatedAt.Before(verifying[i-1].CreatedAt),
			"ListVerifying must be ordered oldest-created_at first")
	}
}

// TestProposalRepositoryCountAttemptsExcludesVerifying verifies that an
// in-flight 'verifying' row is not counted toward the attempt cap, for the
// same reason an in-flight 'generating' row is not: the shadow release has
// not concluded, so counting it would inflate the attempt cap and shift the
// attempt number on a redelivery of the trigger that started it.
func TestProposalRepositoryCountAttemptsExcludesVerifying(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewProposalRepository(tx)
	now := time.Now().UTC()
	const source, nodeID, sig = "validation", "schema.model_v", "verify-count-sig"

	for i := 1; i <= 2; i++ {
		require.NoError(t, repo.Upsert(ctx, proposal.Proposal{
			Source: source, ReleaseID: "release-v", NodeID: nodeID, ErrorSignature: sig,
			Attempt: i, Status: proposal.StatusProposed, CreatedAt: now,
		}), "insert terminal attempt %d", i)
	}
	// A third, in-flight attempt marked verifying: a shadow release is still
	// running for it.
	require.NoError(t, repo.Upsert(ctx, proposal.Proposal{
		Source: source, ReleaseID: "release-v", NodeID: nodeID, ErrorSignature: sig,
		Attempt: 3, Status: proposal.StatusVerifying, ShadowReleaseID: "shadow-count-1", CreatedAt: now,
	}))

	n, err := repo.CountAttempts(ctx, source, nodeID, sig)
	require.NoError(t, err)
	require.Equal(t, 2, n, "a verifying row must be excluded from the attempt count")

	require.NoError(t, tx.Commit())
}

// TestList_FilterByFailingNode verifies that List can address the attempts of
// one failing node in one release. A fixer assembling evidence for attempt N+1
// reads exactly that slice; without the filter it would read the whole table
// and show the model attempts from unrelated nodes and releases as if they were
// its own history.
func TestList_FilterByFailingNode(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	base := func(release, source, node string, attempt int) proposal.Proposal {
		return proposal.Proposal{
			Source: source, ReleaseID: release, NodeID: node,
			ErrorSignature: "sig", Attempt: attempt,
			Status: proposal.StatusFailed, CreatedAt: time.Now().UTC(),
		}
	}
	_ = seedProposal(t, repo, db, base("rel-node-a", "validation", "analytics.py_kpis", 1))
	_ = seedProposal(t, repo, db, base("rel-node-a", "validation", "analytics.py_kpis", 2))
	_ = seedProposal(t, repo, db, base("rel-node-a", "validation", "analytics.other", 1))
	_ = seedProposal(t, repo, db, base("rel-node-a", "compile", "analytics.py_kpis", 1))
	_ = seedProposal(t, repo, db, base("rel-node-b", "validation", "analytics.py_kpis", 1))

	views, err := repo.List(ctx, repository.ProposalFilter{
		ReleaseID: "rel-node-a", Source: "validation", NodeID: "analytics.py_kpis",
	})
	require.NoError(t, err)
	require.Len(t, views, 2)
	for _, v := range views {
		require.Equal(t, "rel-node-a", v.ReleaseID)
		require.Equal(t, "validation", v.Source)
		require.Equal(t, "analytics.py_kpis", v.NodeID)
	}
}

// TestProposalRepositoryFailGenerating verifies that FailGenerating closes out
// only the in-flight rows of the failure it names: a 'generating' row for that
// (source, node_id, error_signature) triple becomes 'failed' with the reason as
// its rationale, while a row already terminal and a row belonging to a
// different failure are both left exactly as they were. Running it again moves
// nothing, since no row is generating any more.
func TestProposalRepositoryFailGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db)

	inFlight := proposal.Proposal{
		Source: "validation", ReleaseID: "rel-fg-1", NodeID: "analytics.py_kpis",
		ErrorSignature: "sig-fg-1", Attempt: 2, Status: proposal.StatusGenerating,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, repo.InsertGenerating(ctx, inFlight))

	// Same failure, already terminal — the attempt whose rejection started the
	// one above. It must not be rewritten.
	settled := inFlight
	settled.Attempt = 1
	settled.Status = proposal.StatusFailed
	settled.Rationale = "the shadow release rejected this fix"
	settledID := seedProposal(t, repo, db, settled)

	// A different node's in-flight attempt must be untouched.
	other := inFlight
	other.NodeID = "analytics.py_other"
	other.ErrorSignature = "sig-fg-2"
	require.NoError(t, repo.InsertGenerating(ctx, other))

	n, err := repo.FailGenerating(ctx, "validation", "analytics.py_kpis", "sig-fg-1",
		"the next fix attempt could not be started: github unreachable")
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the named failure's in-flight row may move")

	var got struct {
		Status    string `db:"status"`
		Rationale string `db:"rationale"`
	}
	require.NoError(t, db.GetContext(ctx, &got,
		`SELECT status, rationale FROM proposal WHERE release_id=$1 AND node_id=$2 AND attempt=$3`,
		"rel-fg-1", "analytics.py_kpis", 2))
	require.Equal(t, string(proposal.StatusFailed), got.Status)
	require.Equal(t, "the next fix attempt could not be started: github unreachable", got.Rationale)

	stillSettled, err := repo.Get(ctx, settledID)
	require.NoError(t, err)
	require.Equal(t, "the shadow release rejected this fix", stillSettled.Rationale,
		"a row that already reached a terminal state must not be rewritten")

	var otherStatus string
	require.NoError(t, db.GetContext(ctx, &otherStatus,
		`SELECT status FROM proposal WHERE node_id=$1 AND attempt=$2`, "analytics.py_other", 2))
	require.Equal(t, string(proposal.StatusGenerating), otherStatus,
		"another failure's in-flight attempt must be left alone")

	n, err = repo.FailGenerating(ctx, "validation", "analytics.py_kpis", "sig-fg-1", "again")
	require.NoError(t, err)
	require.Zero(t, n, "a second call moves nothing: no row is generating any more")
}
