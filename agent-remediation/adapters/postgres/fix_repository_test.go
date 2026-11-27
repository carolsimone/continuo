//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB connects to an externally migrated, dedicated integration database.
// Every test starts with empty application tables and deletes only the rows it
// creates. The package runs serially; the database must have no other writers.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadPostgres(v)
	require.Empty(t, v.Missing(), "set POSTGRES_* to a dedicated, externally migrated test database")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dsn, err := cfg.DSN()
	require.NoError(t, err)
	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	require.NoError(t, err, "connect to the externally migrated test database")
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Probe the current schema before any writes; missing tables or columns
	// require external Flyway migration, never DDL from the test.
	_, err = db.ExecContext(ctx, `SELECT release_check_id, projects FROM fix LIMIT 0`)
	require.NoError(t, err, "run the agent_remediation Flyway migrations before integration tests")
	for _, table := range []string{"fix", "fix_pull_request", "message_processing", OutboxTable} {
		var count int
		require.NoError(t, db.GetContext(ctx, &count, "SELECT count(*) FROM "+table),
			"run the agent_remediation Flyway migrations before integration tests")
		require.Zero(t, count, "refusing non-empty test database: %s contains pre-existing rows", table)
	}

	// The empty-table guard establishes that the serial test owns every row
	// inserted during its lifetime. Cleanup deletes those concrete UUIDs;
	// child pull-request rows are removed by the fix foreign-key cascade.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, table := range []string{OutboxTable, "message_processing", "fix"} {
			var ids []string
			if !assert.NoError(t, db.SelectContext(cleanupCtx, &ids, "SELECT id FROM "+table)) {
				continue
			}
			if len(ids) > 0 {
				_, err := db.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE id = ANY($1::uuid[])", pq.Array(ids))
				assert.NoError(t, err, "delete test-owned rows from %s", table)
			}
		}
	})
	return db
}

// TestFixRepositorySourceFixFields inserts a fix with all three
// source-fix columns populated and verifies they round-trip through the DB.
// A second row with SourceResolved=true verifies the boolean is persisted
// independently of the URI fields.
func TestFixRepositorySourceFixFields(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Insert a fix with all three source-fix fields set.
	p := fix.Fix{
		Source:              "validation",
		ReleaseID:           "release-sf-1",
		NodeID:              "schema.model_sf",
		ErrorSignature:      "err-sig-sf",
		Try:                 1,
		Status:              fix.StatusProposed,
		Confidence:          fix.ConfidenceHigh,
		Rationale:           "source fix rationale",
		ProposedSQLURI:      "s3://bucket/sql/sf1",
		DiffURI:             "s3://bucket/diff/sf1",
		CandidateFixSQLURI:  "s3://bucket/candidate/sql/sf1",
		CandidateFixDiffURI: "s3://bucket/candidate/diff/sf1",
		SourceResolved:      true,
		Model:               "claude-3-5-sonnet",
		CreatedAt:           now,
	}
	require.NoError(t, repo.Upsert(ctx, p), "insert fix with source-fix fields")

	// Read back and verify round-trip.
	type row struct {
		CandidateFixSQLURI  string `db:"candidate_fix_sql_uri"`
		CandidateFixDiffURI string `db:"candidate_fix_diff_uri"`
		SourceResolved      bool   `db:"source_resolved"`
	}
	var got row
	require.NoError(t, tx.GetContext(ctx, &got,
		`SELECT candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved
		 FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		p.ReleaseID, p.NodeID, p.Try,
	), "read back source-fix fields")
	require.Equal(t, p.CandidateFixSQLURI, got.CandidateFixSQLURI, "candidate_fix_sql_uri round-trip")
	require.Equal(t, p.CandidateFixDiffURI, got.CandidateFixDiffURI, "candidate_fix_diff_uri round-trip")
	require.True(t, got.SourceResolved, "source_resolved must be true")

	// Insert a second row with SourceResolved=true and non-empty CandidateFixSQLURI
	// to verify the column stores distinct true values across rows.
	p2 := fix.Fix{
		Source:              "validation",
		ReleaseID:           "release-sf-1",
		NodeID:              "schema.model_sf",
		ErrorSignature:      "err-sig-sf",
		Try:                 2,
		Status:              fix.StatusProposed,
		Confidence:          fix.ConfidenceMedium,
		Rationale:           "source fix rationale try 2",
		ProposedSQLURI:      "s3://bucket/sql/sf2",
		DiffURI:             "s3://bucket/diff/sf2",
		CandidateFixSQLURI:  "s3://bucket/candidate/sql/sf2",
		CandidateFixDiffURI: "",
		SourceResolved:      true,
		Model:               "claude-3-5-sonnet",
		CreatedAt:           now,
	}
	require.NoError(t, repo.Upsert(ctx, p2), "insert second fix with SourceResolved=true")

	var got2 row
	require.NoError(t, tx.GetContext(ctx, &got2,
		`SELECT candidate_fix_sql_uri, candidate_fix_diff_uri, source_resolved
		 FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		p2.ReleaseID, p2.NodeID, p2.Try,
	), "read back second source-fix row")
	require.Equal(t, "s3://bucket/candidate/sql/sf2", got2.CandidateFixSQLURI, "candidate_fix_sql_uri second row")
	require.True(t, got2.SourceResolved, "source_resolved must be true on second row")

	require.NoError(t, tx.Commit())
}

// TestFixRepositoryInsertGeneratingIdempotent verifies that InsertGenerating
// writes a single in-flight 'generating' row and that a second call for the same
// (release_id, source, node_id, try) is a no-op (ON CONFLICT DO NOTHING),
// modelling a redelivery of an in-flight try.
func TestFixRepositoryInsertGeneratingIdempotent(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)
	p := fix.Fix{
		Source:         "compile",
		ReleaseID:      "release-gen-1",
		NodeID:         "service-1",
		ErrorSignature: "gen-sig",
		Try:            1,
		Status:         fix.StatusGenerating,
		CreatedAt:      time.Now().UTC(),
	}
	require.NoError(t, repo.InsertGenerating(ctx, p), "first InsertGenerating")
	require.NoError(t, repo.InsertGenerating(ctx, p), "second InsertGenerating must be a no-op")

	var count int
	require.NoError(t, tx.GetContext(ctx, &count,
		`SELECT count(*) FROM fix WHERE release_id=$1 AND source=$2 AND node_id=$3 AND try=$4`,
		p.ReleaseID, p.Source, p.NodeID, p.Try))
	require.Equal(t, 1, count, "exactly one generating row must exist after two InsertGenerating calls")

	var status string
	require.NoError(t, tx.GetContext(ctx, &status,
		`SELECT status FROM fix WHERE release_id=$1 AND source=$2 AND node_id=$3 AND try=$4`,
		p.ReleaseID, p.Source, p.NodeID, p.Try))
	require.Equal(t, string(fix.StatusGenerating), status)

	require.NoError(t, tx.Commit())
}

// TestFixRepositoryInsertFinalizesGenerating verifies that Insert upserts a
// prior 'generating' row in place: after finalization exactly one row exists for
// the try, its status is terminal, and the terminal payload columns are set.
func TestFixRepositoryInsertFinalizesGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)
	now := time.Now().UTC().Truncate(time.Microsecond)

	gen := fix.Fix{
		Source: "compile", ReleaseID: "release-fin-1", NodeID: "service-1",
		ErrorSignature: "fin-sig", Try: 1, Status: fix.StatusGenerating, CreatedAt: now,
	}
	require.NoError(t, repo.InsertGenerating(ctx, gen))

	final := fix.Fix{
		Source: "compile", ReleaseID: "release-fin-1", NodeID: "service-1",
		ErrorSignature: "fin-sig", Try: 1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
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
		`SELECT count(*) FROM fix WHERE release_id=$1 AND source=$2 AND node_id=$3 AND try=$4`,
		final.ReleaseID, final.Source, final.NodeID, final.Try))
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
		 FROM fix WHERE release_id=$1 AND source=$2 AND node_id=$3 AND try=$4`,
		final.ReleaseID, final.Source, final.NodeID, final.Try))
	require.Equal(t, string(fix.StatusProposed), got.Status)
	require.Equal(t, "s3://bucket/sql/fin1", got.ProposedSQLURI)
	require.True(t, got.SourceResolved)
	require.Equal(t, "models/x.sql", got.FilePath)

	require.NoError(t, tx.Commit())
}

// TestFixRepositoryCountTriesExcludesGenerating verifies that an in-flight
// 'generating' row is not counted toward the try cap: two terminal rows plus
// one generating row for the same triplet count as two tries.
func TestFixRepositoryCountTriesExcludesGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)
	now := time.Now().UTC()
	const source, nodeID, sig = "validation", "schema.model_g", "gen-count-sig"

	for i := 1; i <= 2; i++ {
		require.NoError(t, repo.Upsert(ctx, fix.Fix{
			Source: source, ReleaseID: "release-g", NodeID: nodeID, ErrorSignature: sig,
			Try: i, Status: fix.StatusProposed, CreatedAt: now,
		}), "insert terminal try %d", i)
	}
	// A third, in-flight try marked generating.
	require.NoError(t, repo.InsertGenerating(ctx, fix.Fix{
		Source: source, ReleaseID: "release-g", NodeID: nodeID, ErrorSignature: sig,
		Try: 3, Status: fix.StatusGenerating, CreatedAt: now,
	}))

	n, err := repo.CountTries(ctx, "release-g", 1)
	require.NoError(t, err)
	require.Equal(t, 2, n, "generating row must be excluded from the try count")

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
	const stream = streams.HealTriggeredV1

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
	const streamA = streams.HealTriggeredV1
	const streamB = streams.FixProposedV1

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

// TestFixRepositoryCountTries inserts 3 tries for release-1 round
// 1 (across different nodes, since one try now addresses the release's
// whole failing set rather than a single node), 1 for the same failure
// signature under a later release, and 1 for a second round of release-1,
// verifying that CountTries counts per (release_id, round): a
// later release's tries are its own, never charged to an earlier
// release's budget, and a fresh round never inherits an earlier round's
// exhausted count.
func TestFixRepositoryCountTries(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)

	const source = "validation"
	const sig = "err-sig-abc"
	now := time.Now().UTC()

	// Insert 3 tries for release-1 round 1, each addressing a different node.
	for i := 1; i <= 3; i++ {
		p := fix.Fix{
			Source:         source,
			ReleaseID:      "release-1",
			NodeID:         fmt.Sprintf("schema.model_%d", i),
			ErrorSignature: sig,
			Try:            i,
			Status:         fix.StatusProposed,
			Confidence:     fix.ConfidenceHigh,
			Rationale:      fmt.Sprintf("try %d rationale", i),
			ProposedSQLURI: fmt.Sprintf("s3://bucket/sql/%d", i),
			DiffURI:        fmt.Sprintf("s3://bucket/diff/%d", i),
			Model:          "claude-3-5-sonnet",
			CreatedAt:      now,
		}
		require.NoError(t, repo.Upsert(ctx, p), "insert try %d", i)
	}

	count, err := repo.CountTries(ctx, "release-1", 1)
	require.NoError(t, err)
	require.Equal(t, 3, count, "expected 3 tries for release-1 round 1")

	// The same failure signature under a LATER release starts its own budget.
	require.NoError(t, repo.Upsert(ctx, fix.Fix{
		Source: source, ReleaseID: "release-2", NodeID: "schema.model_a", ErrorSignature: sig,
		Try: 1, Status: fix.StatusProposed, CreatedAt: now,
	}), "insert later-release try")

	count, err = repo.CountTries(ctx, "release-1", 1)
	require.NoError(t, err)
	require.Equal(t, 3, count, "a later release's try must not be charged to release-1")
	count, err = repo.CountTries(ctx, "release-2", 1)
	require.NoError(t, err)
	require.Equal(t, 1, count, "release-2 has exactly its own try")

	// Round 2 of release-1: a "try again" starts a fresh budget.
	require.NoError(t, repo.Upsert(ctx, fix.Fix{
		Source: source, ReleaseID: "release-1", Round: 2, NodeID: "schema.model_a", ErrorSignature: sig,
		Try: 4, Status: fix.StatusProposed, CreatedAt: now,
	}))
	count, err = repo.CountTries(ctx, "release-1", 1)
	require.NoError(t, err)
	require.Equal(t, 3, count, "round 1 keeps its own count")
	count, err = repo.CountTries(ctx, "release-1", 2)
	require.NoError(t, err)
	require.Equal(t, 1, count, "round 2 counts only its own tries")

	require.NoError(t, tx.Commit())
}

// TestInsert_PersistsSourceLocation inserts a fix with repo, commit_sha,
// and file_path set and verifies they are written to and read back from the DB.
func TestInsert_PersistsSourceLocation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-1",
		NodeID:         "model.p.orders_d",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
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
		`SELECT repo, commit_sha, file_path FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		"r-1", "model.p.orders_d", 1))
	require.Equal(t, "owner/continuo-demo", got.Repo)
	require.Equal(t, "abc123", got.CommitSha)
	require.Equal(t, "services/service-3/models/orders_d.sql", got.FilePath)
}

// seedFix inserts a fix and returns the generated UUID id.
// It reads back the id via SELECT after the insert so callers don't need DB knowledge.
func seedFix(t *testing.T, repo *FixRepository, db *sqlx.DB, p fix.Fix) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, repo.Upsert(ctx, p))
	var id string
	require.NoError(t, db.GetContext(ctx, &id,
		`SELECT id FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		p.ReleaseID, p.NodeID, p.Try,
	))
	return id
}

// TestBeginPR_SingleWinner verifies that exactly one concurrent BeginPR call
// succeeds; the second receives ErrPRConflict.
func TestBeginPR_SingleWinner(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-beginpr-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	c1, err := repo.BeginPR(ctx, id, "", "remediation/r-1/orders_d-try1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "owner/continuo-demo", c1.Repo)
	require.Equal(t, "remediation/r-1/orders_d-try1", c1.Branch)
	require.Equal(t, id, c1.ID)

	// Second claim on the same fix must return ErrPRConflict.
	_, err = repo.BeginPR(ctx, id, "", "remediation/r-1/orders_d-try1", time.Now().UTC())
	require.ErrorIs(t, err, repository.ErrPRConflict)
}

// TestBeginPR_ListsAsStuckOpening verifies that a claimed fix
// (pr_state='opening') is surfaced by ListStuckOpening with the fields the
// reconciler's opening sweep needs — including the pr_claimed_at BeginPR
// stamped — to recompute the deterministic branch and age the claim, and
// that it drops out of the listing (with pr_claimed_at cleared) once recorded.
func TestBeginPR_ListsAsStuckOpening(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-opening-1",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	claimedAt := time.Now().UTC().Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "", "remediation/r-opening-1/model-p-orders-try1", claimedAt)
	require.NoError(t, err)

	stuck, next, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Nil(t, next, "a page under the limit must report no next cursor")
	require.Len(t, stuck, 1)
	require.Equal(t, id, stuck[0].ID)
	require.Equal(t, "owner/continuo-demo", stuck[0].Repo)
	require.Equal(t, "r-opening-1", stuck[0].ReleaseID)
	require.Equal(t, "model.p.orders", stuck[0].NodeID)
	require.Equal(t, 1, stuck[0].Try)
	require.NotNil(t, stuck[0].ClaimedAt, "BeginPR must stamp pr_claimed_at")
	require.WithinDuration(t, claimedAt, *stuck[0].ClaimedAt, time.Second)

	// Once recorded, the row is no longer a stuck-opening claim, and its
	// pr_claimed_at is cleared so a future re-claim never inherits this one.
	hit, err := repo.RecordPR(ctx, id, "", "https://gh/pr/1", 1, "dev", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, hit)
	stuck, _, err = repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Empty(t, stuck, "a recorded PR must no longer be listed as stuck opening")

	var stillClaimed *time.Time
	require.NoError(t, db.GetContext(ctx, &stillClaimed,
		`SELECT pr_claimed_at FROM fix_pull_request WHERE fix_id=$1 AND project=''`, id))
	require.Nil(t, stillClaimed, "RecordPR must clear pr_claimed_at back to NULL")
}

// TestOpeningTransition_TriggerStampsClaimedAtWhenWriterOmitsIt verifies the
// trigger's fill-when-NULL guard: a bare UPDATE that moves pr_state to
// 'opening' without setting pr_claimed_at — modelling a fix-service
// binary that predates the column and cannot be taught about it — is stamped
// with the actual wall-clock moment of the transition by the database
// itself, not left NULL.
// This closes the gap the opening sweep cannot close on its own: an
// unmeasurable claim can never be judged stale, so a claim that stayed NULL
// forever would never be recoverable.
func TestOpeningTransition_TriggerStampsClaimedAtWhenWriterOmitsIt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-opening-null",
		NodeID:         "model.p.legacy",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	// Bypass BeginPR to simulate a pre-upgrade binary's write: it moves
	// pr_state to 'opening' without knowing pr_claimed_at exists.
	before := time.Now().UTC()
	_, err := db.ExecContext(ctx, `UPDATE fix SET pr_state='opening' WHERE id=$1`, id)
	require.NoError(t, err)

	// The opening sweep now reads the child fix_pull_request table, so a
	// bare parent UPDATE is no longer surfaced by ListStuckOpening; assert the
	// still-present parent trigger directly on its (legacy) column instead.
	var claimedAt *time.Time
	require.NoError(t, db.GetContext(ctx, &claimedAt, `SELECT pr_claimed_at FROM fix WHERE id=$1`, id))
	require.NotNil(t, claimedAt, "the trigger must stamp pr_claimed_at even when the writer never sets it")
	require.WithinDuration(t, before, *claimedAt, 5*time.Second,
		"the stamped value must be the transition's own moment, not an earlier fabricated time")
}

// TestOpeningTransition_TriggerDoesNotOverrideExplicitClaimedAt verifies the
// trigger only fills in a value a writer's UPDATE left NULL: BeginPR's own
// explicit pr_claimed_at (from the service's Clock port) passes through
// unchanged.
func TestOpeningTransition_TriggerDoesNotOverrideExplicitClaimedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-opening-explicit",
		NodeID:         "model.p.explicit",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	claimedAt := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "", "remediation/r-opening-explicit/model-p-explicit-try1", claimedAt)
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
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-opening-legacy",
		NodeID:         "model.p.legacy_reclaim",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	// First claim, taken by a current binary an hour ago with an explicit
	// pr_claimed_at — old enough that, if it leaked into a second claim's age,
	// the sweep's grace period would judge it stale immediately. The child
	// fix_pull_request table now owns the opening lifecycle, so this test
	// exercises the still-present parent trigger directly on the parent's
	// (legacy) pr_* columns.
	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := db.ExecContext(ctx, `UPDATE fix SET pr_state='opening', pr_claimed_at=$2 WHERE id=$1`, id, firstClaim)
	require.NoError(t, err)

	// An old binary's FailPR-equivalent: it predates pr_claimed_at, so its
	// UPDATE never references the column. The exit trigger must still clear it.
	_, err = db.ExecContext(ctx, `UPDATE fix SET pr_state='failed' WHERE id=$1`, id)
	require.NoError(t, err)

	var afterExit *time.Time
	require.NoError(t, db.GetContext(ctx, &afterExit, `SELECT pr_claimed_at FROM fix WHERE id=$1`, id))
	require.Nil(t, afterExit, "the exit transition must clear pr_claimed_at even though the writer's UPDATE never mentioned the column")

	// The same old binary re-claims the row — again without mentioning
	// pr_claimed_at — and the fill-when-NULL trigger stamps a fresh time.
	before := time.Now().UTC()
	_, err = db.ExecContext(ctx, `UPDATE fix SET pr_state='opening' WHERE id=$1`, id)
	require.NoError(t, err)

	var reclaimed *time.Time
	require.NoError(t, db.GetContext(ctx, &reclaimed, `SELECT pr_claimed_at FROM fix WHERE id=$1`, id))
	require.NotNil(t, reclaimed)
	require.WithinDuration(t, before, *reclaimed, 5*time.Second,
		"the second claim must age from its own (fresh) transition moment")
	require.True(t, reclaimed.After(firstClaim),
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
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-opening-2",
		NodeID:         "model.p.customers",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/customers_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "", "remediation/r-opening-2/model-p-customers-try1", firstClaim)
	require.NoError(t, err)

	hit, err := repo.FailStuckOpeningPR(ctx, id, "", firstClaim)
	require.NoError(t, err)
	require.True(t, hit)

	stuck, _, err := repo.ListStuckOpening(ctx, 10, nil)
	require.NoError(t, err)
	require.Empty(t, stuck, "a failed claim must no longer be listed as stuck opening")

	var clearedClaim *time.Time
	require.NoError(t, db.GetContext(ctx, &clearedClaim,
		`SELECT pr_claimed_at FROM fix_pull_request WHERE fix_id=$1 AND project=''`, id))
	require.Nil(t, clearedClaim, "FailStuckOpeningPR must clear pr_claimed_at back to NULL")

	// The fix is re-claimable after FailPR, and ages from the SECOND
	// claim time, not the first (which was an hour older).
	secondClaim := time.Now().UTC().Truncate(time.Microsecond)
	claim, err := repo.BeginPR(ctx, id, "", "remediation/r-opening-2/model-p-customers-try1", secondClaim)
	require.NoError(t, err, "a 'failed' fix must be re-claimable")
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
// pass, or the ui PR-creation route releasing the claim its own
// BeginPullRequest took after a downstream failure: the call must only fail
// the exact claim identified by (id, observedClaimedAt). If the row was
// released and re-claimed in between — the row now carries a different
// pr_claimed_at — the call must be a no-op that leaves the fresh claim
// completely untouched, never a blind unconditional fail.
func TestFailStuckOpeningPR_CASGuardsAgainstReClaim(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-cas-1",
		NodeID:         "model.p.cas",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	firstClaim := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	_, err := repo.BeginPR(ctx, id, "", "remediation/r-cas-1/model-p-cas-try1", firstClaim)
	require.NoError(t, err)

	// Simulate a second reconciler racing in: the row is released (an
	// operator retries) and re-claimed with a fresh pr_claimed_at, between
	// the first reconciler's list and its fail call below.
	releaseHit, err := repo.FailStuckOpeningPR(ctx, id, "", firstClaim)
	require.NoError(t, err)
	require.True(t, releaseHit)
	secondClaim := time.Now().UTC().Truncate(time.Microsecond)
	_, err = repo.BeginPR(ctx, id, "", "remediation/r-cas-1/model-p-cas-try1", secondClaim)
	require.NoError(t, err)

	// The first reconciler now acts on the STALE observed claim time
	// (firstClaim). The CAS must miss: the row's pr_claimed_at is secondClaim.
	hit, err := repo.FailStuckOpeningPR(ctx, id, "", firstClaim)
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
	hit, err = repo.FailStuckOpeningPR(ctx, id, "", secondClaim)
	require.NoError(t, err)
	require.True(t, hit, "the CAS must hit when the observed claim time matches the row's current pr_claimed_at")

	v, err = repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "failed", v.PrState)
	var clearedClaim *time.Time
	require.NoError(t, db.GetContext(ctx, &clearedClaim,
		`SELECT pr_claimed_at FROM fix_pull_request WHERE fix_id=$1 AND project=''`, id))
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
	repo := NewFixRepository(db, nil)

	var ids []string
	for i, node := range []string{"model.p.c1", "model.p.c2", "model.p.c3"} {
		p := fix.Fix{
			Source:         "validation",
			ReleaseID:      "r-cursor-1",
			NodeID:         node,
			ErrorSignature: "sig",
			// Distinct try per row: (release_id, try) is the unique key.
			Try:            i + 1,
			Status:         fix.StatusProposed,
			SourceResolved: true,
			Repo:           "owner/continuo-demo",
			// Strictly increasing created_at so the (created_at, id) order is
			// deterministic and matches insertion order.
			CreatedAt: time.Now().UTC().Add(time.Duration(i) * time.Second),
		}
		id := seedFix(t, repo, db, p)
		branch := fmt.Sprintf("remediation/r-cursor-1/%s-try1", node)
		_, err := repo.BeginPR(ctx, id, "", branch, time.Now().UTC())
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

// TestListStuckOpening_MultipleProjectsSameFixPaginateDistinctly is the
// regression test for the F5 cursor fix: one fix split across two projects
// has two 'opening' children that share the parent's (created_at, id). With a
// page size of 1 the two siblings land on different pages, so resuming from the
// first page's cursor must reach the second child, not skip past it. A
// (created_at, id)-only cursor stranded the second sibling forever, because its
// strict `>` excluded a row sharing that exact key; the project tiebreak fixes it.
func TestListStuckOpening_MultipleProjectsSameFixPaginateDistinctly(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-cursor")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-cursor/try1/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-cursor/try1/finance", time.Now().UTC())
	require.NoError(t, err)

	page1, next1, err := repo.ListStuckOpening(ctx, 1, nil)
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.Equal(t, id, page1[0].ID)
	require.NotNil(t, next1, "a sibling child remains, so a next cursor must be reported")

	page2, next2, err := repo.ListStuckOpening(ctx, 1, next1)
	require.NoError(t, err)
	require.Len(t, page2, 1, "resuming after the first sibling must reach the second, not skip it")
	require.Equal(t, id, page2[0].ID)
	require.Nil(t, next2, "the second sibling is the last row, so no further cursor")

	require.ElementsMatch(t, []string{"core", "finance"},
		[]string{page1[0].Project, page2[0].Project},
		"both per-project children of one fix must be visited across the pages")
}

// TestBeginPR_RejectsUnresolvedSource verifies that BeginPR returns
// ErrNotSourceResolved when source_resolved=false.
func TestBeginPR_RejectsUnresolvedSource(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-unresolved-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceLow,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: false, // not resolved
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	_, err := repo.BeginPR(ctx, id, "", "b", time.Now().UTC())
	require.ErrorIs(t, err, repository.ErrNotSourceResolved)
}

// TestBeginPR_RejectsATryThatIsNotProposed pins the invariant the PR
// claim itself has to carry: only a try that reached 'proposed' may be
// turned into a pull request.
//
// A python contract fix is persisted as 'verifying' with source_resolved=true
// while a release check is still judging it, and a failed one becomes
// 'failed' with only its status changed. Claiming on source_resolved and
// pr_state alone therefore let a caller open a PR for a fix still being
// verified, or for one a release check had already failed. The UI's own
// status==='proposed' predicate is not that invariant: it does not run for a
// direct or stale gRPC client.
func TestBeginPR_RejectsATryThatIsNotProposed(t *testing.T) {
	for name, status := range map[string]fix.Status{
		"still being verified by a release check": fix.StatusVerifying,
		"failed by its release check":             fix.StatusFailed,
		"still being generated":                   fix.StatusGenerating,
	} {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			ctx := context.Background()
			repo := NewFixRepository(db, nil)

			id := seedFix(t, repo, db, fix.Fix{
				Source:         "validation",
				ReleaseID:      "r-notproposed-" + string(status),
				NodeID:         "analytics.py_daily_kpis",
				ErrorSignature: "sig",
				Try:            1,
				Status:         status,
				ReleaseCheckID: "verify-r-1-analytics.py_daily_kpis-a1",
				Confidence:     fix.ConfidenceHigh,
				Rationale:      "rationale",
				SourceResolved: true,
				Repo:           "owner/demo",
				CommitSHA:      "deadbeef",
				FilePath:       "services/svc-py/contracts/py_daily_kpis.yml",
				Model:          "test-model",
				CreatedAt:      time.Now().UTC(),
			})

			_, err := repo.BeginPR(ctx, id, "", "b", time.Now().UTC())
			require.ErrorIs(t, err, repository.ErrNotProposed,
				"a fix that is not proposed must not be claimable for a pull request")

			var prState string
			require.NoError(t, db.GetContext(ctx, &prState, `SELECT pr_state FROM fix WHERE id=$1`, id))
			require.Equal(t, "", prState, "a refused claim must leave the row unclaimed")
		})
	}
}

// TestRecordPR_ThenGet verifies that RecordPR flips pr_state to 'open' and that
// Get returns the updated row with the PR details.
func TestRecordPR_ThenGet(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-recordpr-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	_, err := repo.BeginPR(ctx, id, "", "b", time.Now().UTC())
	require.NoError(t, err)

	openedAt := time.Now().UTC().Truncate(time.Microsecond)
	hit, err := repo.RecordPR(ctx, id, "", "https://gh/pr/7", 7, "dev|local", openedAt)
	require.NoError(t, err)
	require.True(t, hit, "RecordPR must fire the CAS while the row is still 'opening'")

	// A second RecordPR call against the same now-'open' row is a no-op: the
	// CAS misses because pr_state is no longer 'opening', so nothing is
	// overwritten by a caller racing to record the same claim a second time.
	hit, err = repo.RecordPR(ctx, id, "", "https://gh/pr/should-not-apply", 99, "someone-else", time.Now().UTC())
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
	hit, err = repo.FailStuckOpeningPR(ctx, id, "", time.Now().UTC())
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
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-list-1",
		NodeID:         "model.orders_d",
		ErrorSignature: "sig",
		Try:            1,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		ProposedSQLURI: "s3://bucket/sql/1",
		DiffURI:        "s3://bucket/diff/1",
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "abc123",
		FilePath:       "services/service-3/models/orders_d.sql",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	_ = seedFix(t, repo, db, p)

	views, err := repo.List(ctx, repository.FixFilter{Status: "proposed", PRState: ""})
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, "proposed", string(views[0].Status))
}

// TestRecordPROutcome_CASAndListOpen drives a fix through
// open -> merged and verifies: ListOpenPullRequests only surfaces 'open' rows,
// the CAS fires exactly once, pr_closed_at is persisted, and rows in
// non-'open' states are never transitioned.
func TestRecordPROutcome_CASAndListOpen(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	// Seed one fix and walk it to pr_state='open'.
	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "rel-1",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig-1",
		Try:            1,
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
		`SELECT id FROM fix WHERE release_id='rel-1' AND node_id='model.p.orders'`))
	_, err := repo.BeginPR(ctx, id, "", "remediation/rel-1/model-p-orders-try1", time.Now().UTC())
	require.NoError(t, err)
	openedAt := time.Now().UTC()
	_, err = repo.RecordPR(ctx, id, "", "http://gh/pull/1", 1, "dev", openedAt)
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
	require.Equal(t, 1, open[0].Try)

	// First CAS fires; second is an idempotent no-op.
	closedAt := time.Now().UTC().Truncate(time.Microsecond)
	hit, err := repo.RecordPROutcome(ctx, id, "", fix.PROutcomeMerged, closedAt)
	require.NoError(t, err)
	require.True(t, hit, "first RecordPROutcome must fire the CAS")
	hit, err = repo.RecordPROutcome(ctx, id, "", fix.PROutcomeRejected, closedAt)
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

// TestFixRepository_FileEditsRoundTripAndLegacySynthesis verifies that a
// non-empty Edits slice — including TargetNodeID, the node whose source each
// edit changes — round-trips through Get unchanged, along with the batched
// ResolvedNodeIDs, NodeOutcomes, and ReleaseChecks fields, and that a row
// written with Edits=nil but non-empty legacy scalar columns (file_path,
// proposed_sql_uri, diff_uri) reads back as a single synthesized FileEdit.
func TestFixRepository_FileEditsRoundTripAndLegacySynthesis(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	edits := []fix.FileEdit{
		{Path: "contracts/a.yml", ContentURI: "s3://b/1.content", DiffURI: "s3://b/1.diff", TargetNodeID: "model.p.multi_edit"},
		{Path: "scripts/a.py", ContentURI: "s3://b/2.content", DiffURI: "s3://b/2.diff", TargetNodeID: "model.p.ancestor"},
	}
	resolvedNodeIDs := []string{"model.p.ancestor", "model.p.multi_edit"}
	nodeOutcomes := map[string]fix.NodeOutcome{
		"model.p.multi_edit": {Status: fix.StatusProposed},
		"model.p.ancestor":   {Status: fix.StatusSkipped, Reason: "no source"},
	}
	releaseChecks := []fix.ReleaseCheck{
		{Project: "service-1", Kind: "dbt", CheckID: "verify-r-service-1-a1"},
	}
	p := fix.Fix{
		Source:          "validation",
		ReleaseID:       "r-file-edits-1",
		NodeID:          "model.p.multi_edit",
		ResolvedNodeIDs: resolvedNodeIDs,
		ErrorSignature:  "sig",
		Try:             1,
		Status:          fix.StatusProposed,
		NodeOutcomes:    nodeOutcomes,
		ReleaseChecks:   releaseChecks,
		Confidence:      fix.ConfidenceHigh,
		Rationale:       "rationale",
		Model:           "claude-3-5-sonnet",
		CreatedAt:       time.Now().UTC(),
		Edits:           edits,
	}
	id := seedFix(t, repo, db, p)

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, edits, got.Edits, "a non-empty Edits slice, including TargetNodeID, must round-trip unchanged")
	require.Equal(t, "model.p.multi_edit", got.Edits[0].TargetNodeID, "Edits[0].TargetNodeID must round-trip")
	require.Equal(t, resolvedNodeIDs, got.ResolvedNodeIDs, "ResolvedNodeIDs must round-trip")
	require.Equal(t, nodeOutcomes, got.NodeOutcomes, "NodeOutcomes must round-trip")
	require.Equal(t, releaseChecks, got.ReleaseChecks, "ReleaseChecks must round-trip")

	// The stored JSONB must use the documented snake_case wire keys
	// ("path", "content_uri", "diff_uri"), not the Go field names. A Go-level
	// round-trip through the same struct on both write and read cannot catch
	// a PascalCase regression, so this asserts the raw column contents
	// directly: a wrong key name here means ->>'path' etc. return NULL.
	var path, contentURI, diffURI string
	require.NoError(t, db.GetContext(ctx, &path,
		`SELECT file_edits->0->>'path' FROM fix WHERE id=$1`, id))
	require.NoError(t, db.GetContext(ctx, &contentURI,
		`SELECT file_edits->0->>'content_uri' FROM fix WHERE id=$1`, id))
	require.NoError(t, db.GetContext(ctx, &diffURI,
		`SELECT file_edits->0->>'diff_uri' FROM fix WHERE id=$1`, id))
	require.Equal(t, edits[0].Path, path, "stored JSON must use the snake_case key \"path\"")
	require.Equal(t, edits[0].ContentURI, contentURI, "stored JSON must use the snake_case key \"content_uri\"")
	require.Equal(t, edits[0].DiffURI, diffURI, "stored JSON must use the snake_case key \"diff_uri\"")

	legacy := fix.Fix{
		Source:         "validation",
		ReleaseID:      "r-file-edits-1",
		NodeID:         "analytics.legacy_node",
		ErrorSignature: "sig",
		// Distinct try: (release_id, try) is the unique key.
		Try:            2,
		Status:         fix.StatusProposed,
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "rationale",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
		Edits:          nil, // simulates a pre-V12 writer
		FilePath:       "models/x.sql",
		ProposedSQLURI: "s3://b/x.sql",
		DiffURI:        "s3://b/x.diff",
	}
	legacyID := seedFix(t, repo, db, legacy)

	got2, err := repo.Get(ctx, legacyID)
	require.NoError(t, err)
	require.Equal(t,
		[]fix.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		got2.Edits,
		"a row with an empty file_edits array must synthesize one edit from the legacy scalar columns",
	)
	require.Equal(t, []string{"analytics.legacy_node"}, got2.ResolvedNodeIDs,
		"a row with an empty resolved_node_ids array must synthesize one entry from the legacy node_id column")
}

// TestRecordPROutcome_RejectedAndNonOpenRows verifies the rejected transition
// and that rows with an empty pr_state, or 'opening', or 'failed', are never
// transitioned nor listed.
func TestRecordPROutcome_RejectedAndNonOpenRows(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	seed := func(node string, try int) string {
		p := fix.Fix{
			Source: "validation", ReleaseID: "rel-2", NodeID: node,
			ErrorSignature: "sig", Try: try, Status: "proposed",
			SourceResolved: true, Repo: "acme/dbt-repo", CreatedAt: time.Now().UTC(),
		}
		require.NoError(t, repo.Upsert(ctx, p))
		var id string
		require.NoError(t, db.GetContext(ctx, &id,
			`SELECT id FROM fix WHERE release_id='rel-2' AND node_id=$1`, node))
		return id
	}

	// Row A reaches 'open' then is rejected.
	a := seed("model.p.a", 1)
	_, err := repo.BeginPR(ctx, a, "", "remediation/rel-2/model-p-a-try1", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, a, "", "http://gh/pull/2", 2, "dev", time.Now().UTC())
	require.NoError(t, err)
	hit, err := repo.RecordPROutcome(ctx, a, "", fix.PROutcomeRejected, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, hit)
	v, err := repo.Get(ctx, a)
	require.NoError(t, err)
	require.Equal(t, "rejected", v.PrState)

	// Row B stays in 'opening' (claimed, never recorded): not listed, CAS misses.
	// Distinct try: (release_id, try) is the unique key.
	b := seed("model.p.b", 2)
	_, err = repo.BeginPR(ctx, b, "", "remediation/rel-2/model-p-b-try1", time.Now().UTC())
	require.NoError(t, err)
	open, err := repo.ListOpenPullRequests(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, open, "'opening' and 'rejected' rows must not be listed")
	hit, err = repo.RecordPROutcome(ctx, b, "", fix.PROutcomeMerged, time.Now().UTC())
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
	repo := NewFixRepository(db, nil)

	edits := []fix.FileEdit{
		{Path: "contracts/a.yml", ContentURI: "s3://b/1.content", DiffURI: "s3://b/1.diff"},
		{Path: "scripts/a.py", ContentURI: "s3://b/2.content", DiffURI: "s3://b/2.diff"},
		{Path: "models/table_b_v2.sql", RenamedFrom: "models/table_b.sql", ContentURI: "s3://b/3.content", DiffURI: "s3://b/3.diff"},
	}
	multiID := seedFix(t, repo, db, fix.Fix{
		Source: "validation", ReleaseID: "rel-claim-edits", NodeID: "model.p.multi",
		ErrorSignature: "sig", Try: 1, Status: fix.StatusProposed,
		SourceResolved: true, Repo: "acme/dbt-repo", CommitSHA: "sha1",
		CreatedAt: time.Now().UTC(), Edits: edits,
	})

	claim, err := repo.BeginPR(ctx, multiID, "", "remediation/rel-claim-edits/multi-try1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, edits, claim.Edits, "every edit must reach the claim, in order, a moved file with the path it moves from")

	legacyID := seedFix(t, repo, db, fix.Fix{
		Source: "validation", ReleaseID: "rel-claim-edits", NodeID: "model.p.single",
		// Distinct try: (release_id, try) is the unique key.
		ErrorSignature: "sig", Try: 2, Status: fix.StatusProposed,
		SourceResolved: true, Repo: "acme/dbt-repo", CommitSHA: "sha1",
		CreatedAt: time.Now().UTC(), Edits: nil,
		FilePath: "models/x.sql", ProposedSQLURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff",
	})

	legacyClaim, err := repo.BeginPR(ctx, legacyID, "", "remediation/rel-claim-edits/single-try1", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t,
		[]fix.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		legacyClaim.Edits,
		"a row with no file_edits must claim as one edit synthesized from the single-file columns",
	)
	require.Equal(t, []string{"model.p.single"}, legacyClaim.ResolvedNodeIDs,
		"a row with no resolved_node_ids must claim with one entry synthesized from node_id")
}

// verifiedFix is the Verified the runner hands MarkFixVerified once every
// release check of fixID passed, with release checks as the final
// summary of each release check.
func verifiedFix(t *testing.T, fixID string, releaseChecks []fix.ReleaseCheck) remediation.Verified {
	t.Helper()
	checked, err := remediation.NewChecked(remediation.Draft{}, nil)
	require.NoError(t, err)
	verified, err := remediation.NewVerified(fixID, []remediation.Checked{checked}, nil, releaseChecks)
	require.NoError(t, err)
	return verified
}

// TestFixRepositoryVerifyingLifecycle verifies the round trip a
// release check drives: a fix upserted with status='verifying'
// carries its release_check_id, and MarkFixVerified
// finalizes it to 'proposed' with the final summary of each release check. A second
// MarkFixVerified call is an idempotent no-op (hit=false) since the row is no
// longer 'verifying'.
func TestFixRepositoryVerifyingLifecycle(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "rel-verify-1",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig-verify-1",
		Try:            1,
		Status:         fix.StatusVerifying,
		ReleaseCheckID: "verify-rel-abc",
		Confidence:     fix.ConfidenceHigh,
		Rationale:      "proposed fix awaiting its release check",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	stored, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(fix.StatusVerifying), string(stored.Status))
	require.Equal(t, "verify-rel-abc", stored.ReleaseCheckID, "release_check_id must round-trip")

	final := []fix.ReleaseCheck{{Project: "svc", Kind: "dbt", CheckID: "verify-rel-abc", Phase: fix.PhasePassed}}
	verified := verifiedFix(t, id, final)
	hit, err := repo.MarkFixVerified(ctx, verified)
	require.NoError(t, err)
	require.True(t, hit, "first MarkFixVerified must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(fix.StatusProposed), string(got.Status), "MarkFixVerified must flip status to 'proposed'")
	require.Equal(t, final, got.ReleaseChecks, "the final summary of each release check is written with the status")

	hit, err = repo.MarkFixVerified(ctx, verified)
	require.NoError(t, err)
	require.False(t, hit, "second MarkFixVerified must be a no-op: the row is no longer 'verifying'")
}

// TestFixRepositoryUpsertLeavesTriggerPayloadAlone pins that Upsert does
// not write the fix.trigger_payload column: a row it inserts keeps the
// column's default '{}', and a row it finalizes keeps whatever the column
// already held.
func TestFixRepositoryUpsertLeavesTriggerPayloadAlone(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "rel-trigger-payload",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig-trigger-payload",
		Try:            1,
		Status:         fix.StatusVerifying,
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	var payload string
	require.NoError(t, db.GetContext(ctx, &payload, `SELECT trigger_payload::text FROM fix WHERE id = $1`, id))
	require.JSONEq(t, `{}`, payload, "an inserted row keeps the column's default")

	_, err := db.ExecContext(ctx, `UPDATE fix SET trigger_payload = '{"kept":true}'::jsonb WHERE id = $1`, id)
	require.NoError(t, err)
	p.Status = fix.StatusProposed
	require.NoError(t, repo.Upsert(ctx, p))

	require.NoError(t, db.GetContext(ctx, &payload, `SELECT trigger_payload::text FROM fix WHERE id = $1`, id))
	require.JSONEq(t, `{"kept":true}`, payload, "finalizing a row leaves the column as it was")
}

// TestFixRepositoryMarkVerifyFailed verifies that MarkVerifyFailed
// finalizes a verifying fix to 'failed' and records verify_error, and
// that a second call is an idempotent no-op once the row has moved on.
func TestFixRepositoryMarkVerifyFailed(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	p := fix.Fix{
		Source:         "validation",
		ReleaseID:      "rel-verify-2",
		NodeID:         "model.p.customers",
		ErrorSignature: "sig-verify-2",
		Try:            1,
		Status:         fix.StatusVerifying,
		ReleaseCheckID: "verify-rel-def",
		Confidence:     fix.ConfidenceMedium,
		Rationale:      "proposed fix awaiting its release check",
		Model:          "claude-3-5-sonnet",
		CreatedAt:      time.Now().UTC(),
	}
	id := seedFix(t, repo, db, p)

	hit, err := repo.MarkVerifyFailed(ctx, id, "release check verify-rel-def failed: validation rejected 3 rows", nil)
	require.NoError(t, err)
	require.True(t, hit, "first MarkVerifyFailed must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(fix.StatusFailed), string(got.Status), "MarkVerifyFailed must flip status to 'failed'")
	require.Equal(t, "release check verify-rel-def failed: validation rejected 3 rows", got.VerifyError)

	hit, err = repo.MarkVerifyFailed(ctx, id, "a different error", nil)
	require.NoError(t, err)
	require.False(t, hit, "second MarkVerifyFailed must be a no-op: the row is no longer 'verifying'")
	got2, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "release check verify-rel-def failed: validation rejected 3 rows", got2.VerifyError,
		"the no-op call must not overwrite the already-recorded verify_error")
}

// TestFixRepository_KeepsTheNodeOutcomeCode verifies that the reason code
// of a node outcome survives the node_outcomes column through Get and List, that
// the stored JSON names it "code", and that an outcome with no code stores no
// "code" key at all.
func TestFixRepository_KeepsTheNodeOutcomeCode(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	outcomes := map[string]fix.NodeOutcome{
		"model.p.handed": {Status: fix.StatusEscalated, Reason: "the tries are spent", Code: model.HandoverReasonTriesSpent},
		"model.p.fixed":  {Status: fix.StatusProposed},
	}
	id := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "r-outcome-code-1",
		NodeID:          "model.p.handed",
		ResolvedNodeIDs: []string{"model.p.fixed", "model.p.handed"},
		ErrorSignature:  "sig",
		Try:             1,
		Status:          fix.StatusProposed,
		NodeOutcomes:    outcomes,
		CreatedAt:       time.Now().UTC(),
	})

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, outcomes, got.NodeOutcomes, "Get must return the code of the outcome that carries one")

	listed, err := repo.List(ctx, repository.FixFilter{ReleaseID: "r-outcome-code-1"})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, outcomes, listed[0].NodeOutcomes, "List must return the code of the outcome that carries one")

	var code string
	require.NoError(t, db.GetContext(ctx, &code,
		`SELECT node_outcomes->'model.p.handed'->>'code' FROM fix WHERE id=$1`, id))
	require.Equal(t, string(model.HandoverReasonTriesSpent), code, "stored JSON must use the key \"code\"")

	var hasCode bool
	require.NoError(t, db.GetContext(ctx, &hasCode,
		`SELECT node_outcomes->'model.p.fixed' ? 'code' FROM fix WHERE id=$1`, id))
	require.False(t, hasCode, "an outcome with no code stores no \"code\" key")
}

// TestFixRepositoryMarkFixVerifiedRewritesNodeOutcomes verifies that the
// UPDATE finalizing a try also finalizes its per-node outcomes: every node
// that was waiting on its release check becomes 'proposed', while a node the
// try had already settled keeps the status, reason and code it was
// recorded with.
// Without the rewrite the row would report itself proposed while its own nodes
// still read as verifying, and every reader of node_outcomes would keep showing
// them as in flight.
func TestFixRepositoryMarkFixVerifiedRewritesNodeOutcomes(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	id := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "rel-verify-outcomes-1",
		NodeID:          "model.p.a",
		ResolvedNodeIDs: []string{"model.p.a", "model.p.b", "model.p.c"},
		ErrorSignature:  "sig-verify-outcomes-1",
		Try:             1,
		Status:          fix.StatusVerifying,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"model.p.a": {Status: fix.StatusVerifying},
			"model.p.b": {Status: fix.StatusSkipped, Reason: "no source to fix"},
			"model.p.c": {Status: fix.StatusSkipped, Reason: "the warehouse timed out", Code: model.HandoverReasonClassifierInfra},
		},
		ReleaseCheckID: "verify-rel-verify-outcomes-1",
		CreatedAt:      time.Now().UTC(),
	})

	hit, err := repo.MarkFixVerified(ctx, verifiedFix(t, id, nil))
	require.NoError(t, err)
	require.True(t, hit, "MarkFixVerified must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(fix.StatusProposed), string(got.Status))
	require.Equal(t, map[string]fix.NodeOutcome{
		"model.p.a": {Status: fix.StatusProposed},
		"model.p.b": {Status: fix.StatusSkipped, Reason: "no source to fix"},
		"model.p.c": {Status: fix.StatusSkipped, Reason: "the warehouse timed out", Code: model.HandoverReasonClassifierInfra},
	}, got.NodeOutcomes, "every verifying node becomes proposed; a settled node is untouched, its code included")

	// A row carrying no per-node outcomes at all: the rewrite aggregates over
	// zero entries, which must still write an empty object rather than the SQL
	// NULL the column forbids.
	emptyID := seedFix(t, repo, db, fix.Fix{
		Source:         "validation",
		ReleaseID:      "rel-verify-outcomes-1",
		NodeID:         "model.p.no_outcomes",
		ErrorSignature: "sig-verify-outcomes-1",
		Try:            2,
		Status:         fix.StatusVerifying,
		ReleaseCheckID: "verify-rel-verify-outcomes-2",
		CreatedAt:      time.Now().UTC(),
	})

	hit, err = repo.MarkFixVerified(ctx, verifiedFix(t, emptyID, nil))
	require.NoError(t, err)
	require.True(t, hit, "a row with no per-node outcomes must still finalize")

	var stored string
	require.NoError(t, db.GetContext(ctx, &stored,
		`SELECT node_outcomes::text FROM fix WHERE id=$1`, emptyID))
	require.JSONEq(t, `{}`, stored, "an outcome-less row must keep an empty object, never NULL")
}

// TestFixRepositoryMarkVerifyFailedRewritesNodeOutcomes is the mirror of
// the rewrite above for a failed try: every node that was waiting on
// its release check becomes 'failed', and a node the try had already settled
// keeps its own outcome, its code included.
func TestFixRepositoryMarkVerifyFailedRewritesNodeOutcomes(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	id := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "rel-verify-outcomes-2",
		NodeID:          "model.p.a",
		ResolvedNodeIDs: []string{"model.p.a", "model.p.b", "model.p.c"},
		ErrorSignature:  "sig-verify-outcomes-2",
		Try:             1,
		Status:          fix.StatusVerifying,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"model.p.a": {Status: fix.StatusVerifying},
			"model.p.b": {Status: fix.StatusSkipped, Reason: "no source to fix"},
			"model.p.c": {Status: fix.StatusFailed, Reason: "the model could not determine a safe fix", Code: model.HandoverReasonDraftsSpent},
		},
		ReleaseCheckID: "verify-rel-verify-outcomes-3",
		CreatedAt:      time.Now().UTC(),
	})

	hit, err := repo.MarkVerifyFailed(ctx, id, "model.p.a: still broken", nil)
	require.NoError(t, err)
	require.True(t, hit, "MarkVerifyFailed must fire the CAS")

	got, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, string(fix.StatusFailed), string(got.Status))
	require.Equal(t, "model.p.a: still broken", got.VerifyError)
	require.Equal(t, map[string]fix.NodeOutcome{
		"model.p.a": {Status: fix.StatusFailed},
		"model.p.b": {Status: fix.StatusSkipped, Reason: "no source to fix"},
		"model.p.c": {Status: fix.StatusFailed, Reason: "the model could not determine a safe fix", Code: model.HandoverReasonDraftsSpent},
	}, got.NodeOutcomes, "every verifying node becomes failed; a settled node is untouched, its code included")
}

// TestFixRepositoryCountTriesExcludesVerifying verifies that an
// in-flight 'verifying' row is not counted toward the try cap, for the
// same reason an in-flight 'generating' row is not: the release check has
// not concluded, so counting it would inflate the try cap and shift the
// try number on a redelivery of the trigger that started it.
func TestFixRepositoryCountTriesExcludesVerifying(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	repo := NewFixRepository(tx, nil)
	now := time.Now().UTC()
	const source, nodeID, sig = "validation", "schema.model_v", "verify-count-sig"

	for i := 1; i <= 2; i++ {
		require.NoError(t, repo.Upsert(ctx, fix.Fix{
			Source: source, ReleaseID: "release-v", NodeID: nodeID, ErrorSignature: sig,
			Try: i, Status: fix.StatusProposed, CreatedAt: now,
		}), "insert terminal try %d", i)
	}
	// A third, in-flight try marked verifying: a release check is still
	// running for it.
	require.NoError(t, repo.Upsert(ctx, fix.Fix{
		Source: source, ReleaseID: "release-v", NodeID: nodeID, ErrorSignature: sig,
		Try: 3, Status: fix.StatusVerifying, ReleaseCheckID: "verify-count-1", CreatedAt: now,
	}))

	n, err := repo.CountTries(ctx, "release-v", 1)
	require.NoError(t, err)
	require.Equal(t, 2, n, "a verifying row must be excluded from the try count")

	require.NoError(t, tx.Commit())
}

// TestList_FilterByReleaseAndSource verifies that List can address the tries
// made for one source of failure in one release. The previous-tries step reads
// exactly that slice; without the filter it would read the whole table and show
// the model tries from unrelated releases as if they were its own history.
func TestList_FilterByReleaseAndSource(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	base := func(release, source, node string, try int) fix.Fix {
		return fix.Fix{
			Source: source, ReleaseID: release, NodeID: node,
			ResolvedNodeIDs: []string{node},
			ErrorSignature:  "sig", Try: try,
			Status: fix.StatusFailed, CreatedAt: time.Now().UTC(),
		}
	}
	// Distinct try per row within rel-node-a: (release_id, try) is
	// the unique key.
	_ = seedFix(t, repo, db, base("rel-node-a", "validation", "analytics.py_kpis", 1))
	_ = seedFix(t, repo, db, base("rel-node-a", "validation", "analytics.py_kpis", 2))
	_ = seedFix(t, repo, db, base("rel-node-a", "validation", "analytics.other", 3))
	_ = seedFix(t, repo, db, base("rel-node-a", "compile", "analytics.py_kpis", 4))
	_ = seedFix(t, repo, db, base("rel-node-b", "validation", "analytics.py_kpis", 1))

	views, err := repo.List(ctx, repository.FixFilter{ReleaseID: "rel-node-a", Source: "validation"})
	require.NoError(t, err)
	require.Len(t, views, 3)
	for _, v := range views {
		require.Equal(t, "rel-node-a", v.ReleaseID)
		require.Equal(t, "validation", v.Source)
	}
}

// TestList_FilterByReleaseID verifies that a ReleaseID-only filter returns
// every fix of that release regardless of source or node — the filter a
// release page reads to show the whole release's remediation history.
func TestList_FilterByReleaseID(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	base := func(release, source, node string, try int) fix.Fix {
		return fix.Fix{
			Source: source, ReleaseID: release, NodeID: node,
			ErrorSignature: "sig", Try: try,
			Status: fix.StatusFailed, CreatedAt: time.Now().UTC(),
		}
	}
	// Distinct try per row within release-1: (release_id, try) is
	// the unique key.
	_ = seedFix(t, repo, db, base("release-1", "validation", "analytics.py_kpis", 1))
	_ = seedFix(t, repo, db, base("release-1", "compile", "analytics.other", 2))
	_ = seedFix(t, repo, db, base("release-2", "validation", "analytics.py_kpis", 1))

	views, err := repo.List(ctx, repository.FixFilter{ReleaseID: "release-1"})
	require.NoError(t, err)
	require.Len(t, views, 2, "only release-1's rows must be returned")
	for _, v := range views {
		require.Equal(t, "release-1", v.ReleaseID)
	}
}

// TestFixRepositoryFailGenerating verifies that FailGenerating closes out
// only releaseID's in-flight row: that row becomes 'failed' with the reason
// as its rationale, while a row of the same release already terminal is left
// exactly as it was. Running it again moves nothing, since no row is
// generating any more.
func TestFixRepositoryFailGenerating(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	inFlight := fix.Fix{
		Source: "validation", ReleaseID: "rel-fg-1", NodeID: "analytics.py_kpis",
		ErrorSignature: "sig-fg-1", Try: 2, Status: fix.StatusGenerating,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, repo.InsertGenerating(ctx, inFlight))

	// Same release, already terminal — the try whose rejection started the
	// one above. It must not be rewritten.
	settled := inFlight
	settled.Try = 1
	settled.Status = fix.StatusFailed
	settled.Rationale = "the release check failed this fix"
	settledID := seedFix(t, repo, db, settled)

	n, err := repo.FailGenerating(ctx, "rel-fg-1",
		"the next fix try could not be started: github unreachable")
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the release's own in-flight row may move")

	var got struct {
		Status    string `db:"status"`
		Rationale string `db:"rationale"`
	}
	require.NoError(t, db.GetContext(ctx, &got,
		`SELECT status, rationale FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		"rel-fg-1", "analytics.py_kpis", 2))
	require.Equal(t, string(fix.StatusFailed), got.Status)
	require.Equal(t, "the next fix try could not be started: github unreachable", got.Rationale)

	stillSettled, err := repo.Get(ctx, settledID)
	require.NoError(t, err)
	require.Equal(t, "the release check failed this fix", stillSettled.Rationale,
		"a row that already reached a terminal state must not be rewritten")

	n, err = repo.FailGenerating(ctx, "rel-fg-1", "again")
	require.NoError(t, err)
	require.Zero(t, n, "a second call moves nothing: no row is generating any more")
}

// TestFixRepositoryFailGenerating_LeavesAnotherReleasesTryAlone is
// the case a bare status='generating' match cannot distinguish, and the one
// that makes this write dangerous.
//
// The same node failing the same way in two releases at once is ordinary — a
// re-release of the same broken commit, or two candidates in flight — and
// both releases can have their own try generating at the same moment. A
// write matching on status alone, without the release id, reaches into the
// other release's in-flight try and marks it failed, which is both a
// wrong record and a spent try: CountTries counts terminal rows, so a
// release loses one of the tries it is allowed to a failure that happened
// somewhere else entirely.
func TestFixRepositoryFailGenerating_LeavesAnotherReleasesTryAlone(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	mine := fix.Fix{
		Source: "validation", ReleaseID: "rel-mine", NodeID: "analytics.py_kpis",
		ErrorSignature: "sig-shared", Try: 2, Status: fix.StatusGenerating,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, repo.InsertGenerating(ctx, mine))

	// The same node, failing the same way, being fixed under a different
	// release — in flight at the same moment.
	theirs := mine
	theirs.ReleaseID = "rel-theirs"
	require.NoError(t, repo.InsertGenerating(ctx, theirs))

	n, err := repo.FailGenerating(ctx, "rel-mine",
		"the next fix try could not be started: github unreachable")
	require.NoError(t, err)
	require.Equal(t, 1, n, "exactly one row — this release's own in-flight try — may move")

	var theirStatus string
	require.NoError(t, db.GetContext(ctx, &theirStatus,
		`SELECT status FROM fix WHERE release_id=$1 AND node_id=$2 AND try=$3`,
		"rel-theirs", "analytics.py_kpis", 2))
	require.Equal(t, string(fix.StatusGenerating), theirStatus,
		"another release's in-flight try must not be failed, nor its try budget spent")

	count, err := repo.CountTries(ctx, "rel-mine", 1)
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"only the try that actually failed may count against this release's budget")
	count, err = repo.CountTries(ctx, "rel-theirs", 1)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"the other release's try is still in flight and its budget untouched")
}

// perProjectRepoPaths maps the two projects the per-(fix, project) PR
// tests split a fix across to their project roots within the repo.
var perProjectRepoPaths = map[string]string{"core": "services/core", "finance": "services/finance"}

// seedTwoProjectFix inserts the fixture those tests share: a proposed,
// source-resolved fix whose file_edits span two projects — services/core
// (member "na") and services/finance (member "nb") — with both nodes fixed, so
// BeginPR can claim each project independently and each claim narrows to its
// own edit and member.
func seedTwoProjectFix(t *testing.T, repo *FixRepository, db *sqlx.DB, releaseID string) string {
	t.Helper()
	return seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       releaseID,
		NodeID:          "na",
		ResolvedNodeIDs: []string{"na", "nb"},
		ErrorSignature:  "sig",
		Try:             1,
		Status:          fix.StatusProposed,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"na": {Status: fix.StatusProposed},
			"nb": {Status: fix.StatusProposed},
		},
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "sha",
		Model:          "m",
		CreatedAt:      time.Now().UTC(),
		Edits: []fix.FileEdit{
			{Path: "services/core/models/a.sql", ContentURI: "s3://b/core.content", DiffURI: "s3://b/core.diff", TargetNodeID: "na", MemberNodeIDs: []string{"na"}},
			{Path: "services/finance/models/b.sql", ContentURI: "s3://b/fin.content", DiffURI: "s3://b/fin.diff", TargetNodeID: "nb", MemberNodeIDs: []string{"nb"}},
		},
	})
}

// prStatesByProject returns each child pull request's pr_state keyed by project,
// so a test can assert one project's CAS moved without disturbing another's.
func prStatesByProject(t *testing.T, db *sqlx.DB, id string) map[string]string {
	t.Helper()
	var rows []struct {
		Project string `db:"project"`
		PrState string `db:"pr_state"`
	}
	require.NoError(t, db.SelectContext(context.Background(), &rows,
		`SELECT project, pr_state FROM fix_pull_request WHERE fix_id=$1`, id))
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Project] = r.PrState
	}
	return out
}

// TestBeginPR_ClaimsPerProject verifies that BeginPR claims one (fix,
// project) pull request at a time: the child row is created on first claim, a
// second claim of the same project conflicts, and a different project on the
// same fix is an independent claim that still succeeds. Each claim is
// narrowed to exactly its project's edit and the member that edit resolves.
func TestBeginPR_ClaimsPerProject(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-1")

	coreClaim, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-1/core", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "core", coreClaim.Project)
	require.Equal(t, []string{"na"}, coreClaim.ResolvedNodeIDs, "a core claim resolves only core's member")
	require.Len(t, coreClaim.Edits, 1, "a core claim carries only core's edit")
	require.Equal(t, "services/core/models/a.sql", coreClaim.Edits[0].Path)

	_, err = repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-1/core", time.Now().UTC())
	require.ErrorIs(t, err, repository.ErrPRConflict, "a second claim of the same project must conflict")

	finClaim, err := repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-1/finance", time.Now().UTC())
	require.NoError(t, err, "a different project on the same fix is an independent claim")
	require.Equal(t, "finance", finClaim.Project)
	require.Equal(t, []string{"nb"}, finClaim.ResolvedNodeIDs)
	require.Len(t, finClaim.Edits, 1)
	require.Equal(t, "services/finance/models/b.sql", finClaim.Edits[0].Path)

	states := prStatesByProject(t, db, id)
	require.Equal(t, "opening", states["core"])
	require.Equal(t, "opening", states["finance"])
}

// TestBeginPR_ReturnsTheClaimNodesApprovers verifies that a claim carries the
// approvers of the nodes it fixes and no others: the whole-fix claim names
// the fixed nodes' approvers (a handed-over node's approver stays on the row), a
// per-project claim only its own members', and a node the row records no
// approver for has no entry.
func TestBeginPR_ReturnsTheClaimNodesApprovers(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	approvers := []fix.Approver{
		{NodeID: "na", Owner: "core-team", Source: model.ApproverSourceBundle},
		{NodeID: "nb", Owner: "finance-team", Source: model.ApproverSourceContractFile},
		{NodeID: "nc", Owner: "growth-team", Source: model.ApproverSourceBundle},
		{NodeID: "nd", Source: model.ApproverSourceNone},
	}
	id := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "r-claim-approvers",
		NodeID:          "na",
		ResolvedNodeIDs: []string{"na", "nb", "nc", "nd", "ne"},
		ErrorSignature:  "sig",
		Try:             1,
		Status:          fix.StatusProposed,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"na": {Status: fix.StatusProposed},
			"nb": {Status: fix.StatusProposed},
			"nc": {Status: fix.StatusSkipped, Reason: "no remediation type matches", Code: model.HandoverReasonNoType},
			"nd": {Status: fix.StatusProposed},
			"ne": {Status: fix.StatusProposed},
		},
		Approvers:      approvers,
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "sha",
		Model:          "m",
		CreatedAt:      time.Now().UTC(),
		Edits: []fix.FileEdit{
			{Path: "services/core/models/a.sql", ContentURI: "s3://b/core.content", DiffURI: "s3://b/core.diff", TargetNodeID: "na", MemberNodeIDs: []string{"na", "nd", "ne"}},
			{Path: "services/finance/models/b.sql", ContentURI: "s3://b/fin.content", DiffURI: "s3://b/fin.diff", TargetNodeID: "nb", MemberNodeIDs: []string{"nb"}},
		},
	})

	coreClaim, err := repo.BeginPR(ctx, id, "core", "remediation/r-claim-approvers/core", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, []string{"na", "nd", "ne"}, coreClaim.ResolvedNodeIDs)
	require.Equal(t, []fix.Approver{approvers[0], approvers[3]}, coreClaim.Approvers,
		"the core claim names its own members' approvers; ne has none recorded")

	finClaim, err := repo.BeginPR(ctx, id, "finance", "remediation/r-claim-approvers/finance", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, []fix.Approver{approvers[1]}, finClaim.Approvers)

	wholeID := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "r-claim-approvers",
		NodeID:          "na",
		ResolvedNodeIDs: []string{"na", "nc"},
		ErrorSignature:  "sig",
		Try:             2,
		Status:          fix.StatusProposed,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"na": {Status: fix.StatusProposed},
			"nc": {Status: fix.StatusSkipped, Reason: "no remediation type matches", Code: model.HandoverReasonNoType},
		},
		Approvers:      approvers[:3],
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "sha",
		CreatedAt:      time.Now().UTC(),
		Edits:          []fix.FileEdit{{Path: "services/core/models/a.sql", ContentURI: "s3://b/c", DiffURI: "s3://b/d", TargetNodeID: "na", MemberNodeIDs: []string{"na"}}},
	})
	wholeClaim, err := repo.BeginPR(ctx, wholeID, "", "remediation/r-claim-approvers/try2", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, []string{"na"}, wholeClaim.ResolvedNodeIDs)
	require.Equal(t, []fix.Approver{approvers[0]}, wholeClaim.Approvers,
		"the whole-fix claim names the fixed node's approver, not the handed-over node's")

	legacyID := seedFix(t, repo, db, fix.Fix{
		Source: "validation", ReleaseID: "r-claim-approvers", NodeID: "nl", ErrorSignature: "sig", Try: 3,
		Status: fix.StatusProposed, SourceResolved: true, Repo: "owner/continuo-demo", CommitSHA: "sha", CreatedAt: time.Now().UTC(),
	})
	legacyClaim, err := repo.BeginPR(ctx, legacyID, "", "remediation/r-claim-approvers/try3", time.Now().UTC())
	require.NoError(t, err)
	require.Empty(t, legacyClaim.Approvers, "a row recorded without approvers claims with none")
}

// TestBeginPR_LegacyProjectReclaimsWholeFix verifies that the "" project
// re-claims a legacy child row (the shape the V17 backfill writes) left in
// 'failed' by an earlier try to open the whole-fix PR, and that its
// claim carries every edit and the full fixed set rather than any per-project narrowing.
func TestBeginPR_LegacyProjectReclaimsWholeFix(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-legacy")

	// A legacy child row as the V17 backfill writes it: project='' left in
	// 'failed' by an earlier try to open the whole-fix PR.
	_, err := db.ExecContext(ctx,
		`INSERT INTO fix_pull_request (fix_id, project, repo, branch, pr_state)
		 VALUES ($1, '', 'owner/continuo-demo', 'remediation/r-per-svc-legacy/try1', 'failed')`, id)
	require.NoError(t, err)

	claim, err := repo.BeginPR(ctx, id, "", "remediation/r-per-svc-legacy/try1", time.Now().UTC())
	require.NoError(t, err, "a 'failed' legacy child row must be re-claimable")
	require.Equal(t, "", claim.Project)
	require.Equal(t, []string{"na", "nb"}, claim.ResolvedNodeIDs, "the legacy project claims the whole fixed set")
	require.Len(t, claim.Edits, 2, "the legacy project claims every edit")

	var state string
	require.NoError(t, db.GetContext(ctx, &state,
		`SELECT pr_state FROM fix_pull_request WHERE fix_id=$1 AND project=''`, id))
	require.Equal(t, "opening", state, "the re-claim moves the legacy row 'failed' -> 'opening'")
}

// TestRecordPR_TargetsOneProject verifies that RecordPR's CAS moves only the
// named project's child row 'opening' -> 'open', leaving another project's
// concurrent 'opening' claim on the same fix untouched.
func TestRecordPR_TargetsOneProject(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-record")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-record/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-record/finance", time.Now().UTC())
	require.NoError(t, err)

	hit, err := repo.RecordPR(ctx, id, "core", "https://gh/pr/10", 10, "dev", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, hit)

	states := prStatesByProject(t, db, id)
	require.Equal(t, "open", states["core"])
	require.Equal(t, "opening", states["finance"], "recording core must not touch finance")
}

// TestRecordPROutcome_TargetsOneProject verifies that RecordPROutcome's CAS
// moves only the named project's child row 'open' -> outcome, fires exactly
// once, and leaves another open project on the same fix untouched.
func TestRecordPROutcome_TargetsOneProject(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-outcome")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-outcome/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-outcome/finance", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "core", "https://gh/pr/10", 10, "dev", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "finance", "https://gh/pr/11", 11, "dev", time.Now().UTC())
	require.NoError(t, err)

	closedAt := time.Now().UTC().Truncate(time.Microsecond)
	hit, err := repo.RecordPROutcome(ctx, id, "core", fix.PROutcomeMerged, closedAt)
	require.NoError(t, err)
	require.True(t, hit, "first RecordPROutcome for core must fire the CAS")
	hit, err = repo.RecordPROutcome(ctx, id, "core", fix.PROutcomeMerged, closedAt)
	require.NoError(t, err)
	require.False(t, hit, "a second RecordPROutcome for core is a no-op")

	states := prStatesByProject(t, db, id)
	require.Equal(t, "merged", states["core"])
	require.Equal(t, "open", states["finance"], "merging core must not touch finance")
}

// TestListOpenPullRequests_OneEntryPerOpenProject verifies that a fix
// split into two open per-project PRs yields one ListOpenPullRequests entry per
// open child row, each carrying its Project and that project's pr_number.
func TestListOpenPullRequests_OneEntryPerOpenProject(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-listopen")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-listopen/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-listopen/finance", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "core", "https://gh/pr/10", 10, "dev", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "finance", "https://gh/pr/11", 11, "dev", time.Now().UTC())
	require.NoError(t, err)

	open, err := repo.ListOpenPullRequests(ctx, 10)
	require.NoError(t, err)
	require.Len(t, open, 2, "two open child rows on one fix must yield two entries")

	byProject := make(map[string]fix.OpenPR, len(open))
	for _, o := range open {
		byProject[o.Project] = o
	}
	require.Contains(t, byProject, "core")
	require.Contains(t, byProject, "finance")
	require.Equal(t, id, byProject["core"].ID)
	require.Equal(t, 10, byProject["core"].PRNumber)
	require.Equal(t, id, byProject["finance"].ID)
	require.Equal(t, 11, byProject["finance"].PRNumber)
}

// TestGet_CarriesPerProjectPullRequests verifies that Get returns every child
// pull request on the fix in View.PullRequests with its own state, and
// derives the singular Pr* view fields from the first child (ordered by
// project) so existing readers still render a live PR.
func TestGet_CarriesPerProjectPullRequests(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-per-svc-get")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-per-svc-get/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.BeginPR(ctx, id, "finance", "remediation/r-per-svc-get/finance", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "core", "https://gh/pr/20", 20, "dev", time.Now().UTC())
	require.NoError(t, err)

	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Len(t, v.PullRequests, 2, "the view must carry both child pull requests")

	byProject := make(map[string]fix.PullRequest, len(v.PullRequests))
	for _, pr := range v.PullRequests {
		byProject[pr.Project] = pr
	}
	require.Equal(t, "open", byProject["core"].PrState)
	require.Equal(t, "https://gh/pr/20", byProject["core"].PrURL)
	require.Equal(t, 20, byProject["core"].PrNumber)
	require.Equal(t, "opening", byProject["finance"].PrState)

	// Singular fields derive from the first child ordered by project ("core").
	require.Equal(t, "open", v.PrState)
	require.Equal(t, "https://gh/pr/20", v.PrURL)
	require.Equal(t, 20, v.PrNumber)
}

// TestBeginPR_ClaimCASReChecksParentPreconditionsAtomically pins F1: the claim
// CAS re-checks source_resolved and status='proposed' on the parent inside the
// same UPDATE that moves the child row to 'opening'. BeginPR's pre-read gives
// the ErrNotProposed/ErrNotSourceResolved ladder for a bad state at call time,
// but the correlated EXISTS is what closes the pre-read→CAS race: a status or
// source_resolved change committed in that window must yield 0 rows, never a
// claim. The test drives the exact CAS statement the repository issues
// (beginPRClaimCAS) against a claimable child row while the parent is first
// non-proposed and then not source-resolved — removing the EXISTS guard makes
// the UPDATE match the row and this test fail.
func TestBeginPR_ClaimCASReChecksParentPreconditionsAtomically(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedTwoProjectFix(t, repo, db, "r-cas-guard")

	// A claimable child row (pr_state='' default) for the project.
	_, err := db.ExecContext(ctx,
		`INSERT INTO fix_pull_request (fix_id, project, repo) VALUES ($1, 'core', 'owner/continuo-demo')`, id)
	require.NoError(t, err)

	// While the parent is proposed + source_resolved, the CAS claims the row.
	var claimedAt time.Time
	require.NoError(t, db.GetContext(ctx, &claimedAt, beginPRClaimCAS, id, "core", time.Now().UTC()),
		"the CAS must claim a claimable child row while the parent is proposed and source-resolved")

	// Reset the child row to claimable and flip the parent to a non-proposed
	// status. The correlated EXISTS must now refuse the claim (0 rows), even
	// though the child row itself is claimable.
	resetChild := func() {
		t.Helper()
		_, e := db.ExecContext(ctx,
			`UPDATE fix_pull_request SET pr_state='', pr_claimed_at=NULL WHERE fix_id=$1 AND project='core'`, id)
		require.NoError(t, e)
	}
	resetChild()
	_, err = db.ExecContext(ctx, `UPDATE fix SET status='failed' WHERE id=$1`, id)
	require.NoError(t, err)
	var out time.Time
	err = db.GetContext(ctx, &out, beginPRClaimCAS, id, "core", time.Now().UTC())
	require.ErrorIs(t, err, sql.ErrNoRows, "the CAS must refuse a claim while the parent is not 'proposed'")

	// Same guard for source_resolved=false.
	_, err = db.ExecContext(ctx, `UPDATE fix SET status='proposed', source_resolved=false WHERE id=$1`, id)
	require.NoError(t, err)
	resetChild()
	err = db.GetContext(ctx, &out, beginPRClaimCAS, id, "core", time.Now().UTC())
	require.ErrorIs(t, err, sql.ErrNoRows, "the CAS must refuse a claim while the parent is not source-resolved")

	// The child row was never moved out of the claimable state by the refused CASes.
	var state string
	require.NoError(t, db.GetContext(ctx, &state,
		`SELECT pr_state FROM fix_pull_request WHERE fix_id=$1 AND project='core'`, id))
	require.Equal(t, "", state, "a refused CAS must leave the child row claimable")
}

// TestList_FilterPRStateMatchesChildRows pins F2: List's pr_state filter matches
// the fix_pull_request child rows (the parent's own pr_state column is a
// legacy read-mirror nothing writes), with a fallback to the parent column for a
// legacy fix that never split into child rows.
func TestList_FilterPRStateMatchesChildRows(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	// A fix with a merged child PR.
	id := seedTwoProjectFix(t, repo, db, "r-list-prstate")
	_, err := repo.BeginPR(ctx, id, "core", "remediation/r-list-prstate/core", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPR(ctx, id, "core", "https://gh/pr/1", 1, "dev", time.Now().UTC())
	require.NoError(t, err)
	_, err = repo.RecordPROutcome(ctx, id, "core", fix.PROutcomeMerged, time.Now().UTC())
	require.NoError(t, err)

	merged, err := repo.List(ctx, repository.FixFilter{PRState: "merged"})
	require.NoError(t, err)
	require.Len(t, merged, 1, "a fix with a merged child PR must match pr_state=merged")
	require.Equal(t, id, merged[0].ID)

	open, err := repo.List(ctx, repository.FixFilter{PRState: "open"})
	require.NoError(t, err)
	require.Empty(t, open, "the same fix must not match pr_state=open")

	// Legacy fallback: a fix with no child rows matches on its own (legacy)
	// parent pr_state column.
	legacyID := seedFix(t, repo, db, fix.Fix{
		Source: "validation", ReleaseID: "r-list-prstate", NodeID: "legacy.n",
		ErrorSignature: "sig", Try: 2, Status: fix.StatusProposed,
		SourceResolved: true, Repo: "owner/continuo-demo", CreatedAt: time.Now().UTC(),
	})
	_, err = db.ExecContext(ctx, `UPDATE fix SET pr_state='open' WHERE id=$1`, legacyID)
	require.NoError(t, err)

	openLegacy, err := repo.List(ctx, repository.FixFilter{PRState: "open"})
	require.NoError(t, err)
	require.Len(t, openLegacy, 1, "a legacy fix with no child rows matches on its parent pr_state")
	require.Equal(t, legacyID, openLegacy[0].ID)
}

// TestBeginPR_PerProjectClaimOverMemberlessEditsResolvesNoMembers pins F3: a
// per-project claim whose filtered edits carry no member_node_ids (a fix
// generated before the codec fix) resolves NO members — never the whole fixed
// set — so a project's PR can only claim nodes its edits explicitly attribute.
// The legacy "" whole-fix claim over the same row still resolves the full
// fixed set.
func TestBeginPR_PerProjectClaimOverMemberlessEditsResolvesNoMembers(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, perProjectRepoPaths)

	id := seedFix(t, repo, db, fix.Fix{
		Source:          "validation",
		ReleaseID:       "r-memberless-edits",
		NodeID:          "na",
		ResolvedNodeIDs: []string{"na", "nb"},
		ErrorSignature:  "sig",
		Try:             1,
		Status:          fix.StatusProposed,
		NodeOutcomes: map[string]fix.NodeOutcome{
			"na": {Status: fix.StatusProposed},
			"nb": {Status: fix.StatusProposed},
		},
		SourceResolved: true,
		Repo:           "owner/continuo-demo",
		CommitSHA:      "sha",
		Model:          "m",
		CreatedAt:      time.Now().UTC(),
		// Edits carry NO MemberNodeIDs (pre-codec-fix shape) but map to projects.
		Edits: []fix.FileEdit{
			{Path: "services/core/models/a.sql", ContentURI: "s3://b/core.content", DiffURI: "s3://b/core.diff"},
			{Path: "services/finance/models/b.sql", ContentURI: "s3://b/fin.content", DiffURI: "s3://b/fin.diff"},
		},
	})

	coreClaim, err := repo.BeginPR(ctx, id, "core", "remediation/r-memberless-edits/core", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "core", coreClaim.Project)
	require.Empty(t, coreClaim.ResolvedNodeIDs, "a per-project claim over memberless edits resolves no members")
	require.Len(t, coreClaim.Edits, 1, "the claim still carries only its project's edit")
	require.Equal(t, "services/core/models/a.sql", coreClaim.Edits[0].Path)

	legacyClaim, err := repo.BeginPR(ctx, id, "", "remediation/r-memberless-edits/all", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, "", legacyClaim.Project)
	require.Equal(t, []string{"na", "nb"}, legacyClaim.ResolvedNodeIDs,
		"the whole-fix claim still resolves the full fixed set")
	require.Len(t, legacyClaim.Edits, 2)
}

// TestFixRepositoryFailGenerating_FailsNodeOutcomes verifies FailGenerating
// carries the terminal status into every per-node outcome, not just the row.
// The UI reads node_outcomes[node].status in preference to the row status
// (fixStatusForNode: `node_outcomes?.[node]?.status ?? status`), so a
// per-node entry left at 'generating' keeps the FIX cell spinning and hides
// "Try again" even though the row is failed — the exact orphan symptom this
// recovery must clear. Any node already terminal is left as it was, its code
// included.
func TestFixRepositoryFailGenerating_FailsNodeOutcomes(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewFixRepository(db, nil)

	inFlight := fix.Fix{
		Source: "compile", ReleaseID: "rel-no-1", Try: 1, Status: fix.StatusGenerating,
		CreatedAt:       time.Now().UTC(),
		ResolvedNodeIDs: []string{"core", "analytics.b", "analytics.c"},
		NodeOutcomes: map[string]fix.NodeOutcome{
			"core":        {Status: fix.StatusGenerating},
			"analytics.b": {Status: fix.StatusGenerating},
			"analytics.c": {Status: fix.StatusSkipped, Reason: "the parser refuses this construct", Code: model.HandoverReasonClassifierUnhealableParse},
		},
	}
	inFlight.NormalizeRepresentativeViews()
	require.NoError(t, repo.InsertGenerating(ctx, inFlight))

	n, err := repo.FailGenerating(ctx, "rel-no-1", "poison-dropped after exhausting redelivery")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	for _, node := range []string{"core", "analytics.b"} {
		var got struct {
			Status string `db:"status"`
			Reason string `db:"reason"`
		}
		require.NoError(t, db.GetContext(ctx, &got,
			`SELECT node_outcomes->$2->>'status' AS status,
			        COALESCE(node_outcomes->$2->>'reason','') AS reason
			   FROM fix WHERE release_id=$1 AND try=$3`,
			"rel-no-1", node, 1))
		require.Equal(t, string(fix.StatusFailed), got.Status, "node %s status must be failed", node)
		require.Equal(t, "poison-dropped after exhausting redelivery", got.Reason, "node %s reason", node)
	}

	got, err := repo.List(ctx, repository.FixFilter{ReleaseID: "rel-no-1"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, fix.NodeOutcome{
		Status: fix.StatusSkipped, Reason: "the parser refuses this construct", Code: model.HandoverReasonClassifierUnhealableParse,
	}, got[0].NodeOutcomes["analytics.c"], "a node already settled keeps its outcome and its code")
}

// TestUpdateReleaseCheckPhase_TouchesOnlyThatRun verifies that
// UpdateReleaseCheckPhase rewrites only the element matching the given
// check_id, leaving the row's other release checks — and their phases —
// untouched.
func TestUpdateReleaseCheckPhase_TouchesOnlyThatRun(t *testing.T) {
	db := newTestDB(t)
	repo := NewFixRepository(db, nil)
	ctx := context.Background()
	id := seedVerifying(t, db, "rel-1", []fix.ReleaseCheck{
		{Project: "core", Kind: "dbt", CheckID: "verify-rel-1-core-a1", Phase: fix.PhaseQueued},
		{Project: "ops", Kind: "dbt", CheckID: "verify-rel-1-ops-a1", Phase: fix.PhaseQueued},
	})
	at := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	require.NoError(t, repo.UpdateReleaseCheckPhase(ctx, id, "verify-rel-1-ops-a1", fix.PhaseRunning, &at))

	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Len(t, v.ReleaseChecks, 2)
	assert.Equal(t, fix.PhaseQueued, v.ReleaseChecks[0].Phase)
	assert.Equal(t, fix.PhaseRunning, v.ReleaseChecks[1].Phase)
	require.NotNil(t, v.ReleaseChecks[1].ActivatedAt)
	assert.True(t, v.ReleaseChecks[1].ActivatedAt.Equal(at))
}

// TestMarkVerifyFailed_StoresTheFinalReleaseChecks verifies that
// MarkVerifyFailed writes the caller's final summary of each release check — including the
// failed run's phase and error — in the same statement that finalizes the
// row's status.
func TestMarkVerifyFailed_StoresTheFinalReleaseChecks(t *testing.T) {
	db := newTestDB(t)
	repo := NewFixRepository(db, nil)
	ctx := context.Background()
	id := seedVerifying(t, db, "rel-1", []fix.ReleaseCheck{{Project: "core", Kind: "dbt", CheckID: "verify-rel-1-core-a1", Phase: fix.PhaseRunning}})
	final := []fix.ReleaseCheck{{Project: "core", Kind: "dbt", CheckID: "verify-rel-1-core-a1", Phase: fix.PhaseFailed, Error: "model.core.orders: column x does not exist"}}
	hit, err := repo.MarkVerifyFailed(ctx, id, "model.core.orders: column x does not exist", final)
	require.NoError(t, err)
	require.True(t, hit)
	v, err := repo.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, fix.StatusFailed, v.Status)
	assert.Equal(t, fix.PhaseFailed, v.ReleaseChecks[0].Phase)
	assert.Equal(t, "model.core.orders: column x does not exist", v.ReleaseChecks[0].Error)
}

// TestList_FiltersByProject verifies that FixFilter.Project matches
// fixes whose projects array contains it, narrowing a listing to one
// team's work.
func TestList_FiltersByProject(t *testing.T) {
	db := newTestDB(t)
	repo := NewFixRepository(db, nil)
	ctx := context.Background()
	seedTerminal(t, db, "rel-1", []string{"core"})
	seedTerminal(t, db, "rel-2", []string{"ops", "core"})
	seedTerminal(t, db, "rel-3", []string{"ops"})
	rows, err := repo.List(ctx, repository.FixFilter{Project: "core"})
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, r := range rows {
		ids[r.ReleaseID] = true
	}
	assert.Equal(t, map[string]bool{"rel-1": true, "rel-2": true}, ids)
}

// TestFixSchemaReleaseCheckContract checks the externally migrated schema
// exposes the current release check fields and the projects GIN index.
func TestFixSchemaReleaseCheckContract(t *testing.T) {
	db := newTestDB(t)
	var columns []string
	require.NoError(t, db.Select(&columns, `SELECT column_name FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name=$1
		AND column_name IN ($2,$3,$4) ORDER BY column_name`,
		"fix", "release_check_id", "shadow_release_id", "projects"))
	require.Equal(t, []string{"projects", "release_check_id"}, columns)
	var indexDefinition string
	require.NoError(t, db.Get(&indexDefinition, `SELECT indexdef FROM pg_indexes
		WHERE schemaname=current_schema() AND tablename=$1 AND indexname=$2`,
		"fix", "idx_fix_projects"))
	require.Contains(t, indexDefinition, "USING gin (projects)")
}

// seedVerifying records one try for releaseID parked in 'verifying' with
// the given release checks, and returns the row id.
func seedVerifying(t *testing.T, db *sqlx.DB, releaseID string, vs []fix.ReleaseCheck) string {
	t.Helper()
	repo := NewFixRepository(db, nil)
	ctx := context.Background()
	p := fix.Fix{
		Source: "validation", ReleaseID: releaseID, Round: 1, NodeID: "model.core.orders",
		ResolvedNodeIDs: []string{"model.core.orders"}, ErrorSignature: "sig", Try: 1,
		Status: fix.StatusVerifying, ReleaseChecks: vs, Projects: fix.UnionProjects(releaseCheckProjectNames(vs)),
		CreatedAt: time.Now().UTC(),
	}
	p.NormalizeRepresentativeViews()
	require.NoError(t, repo.InsertGenerating(ctx, p))
	require.NoError(t, repo.Upsert(ctx, p))
	var id string
	require.NoError(t, db.Get(&id, `SELECT id FROM fix WHERE release_id = $1 AND try = 1`, releaseID))
	return id
}

// seedTerminal records one 'proposed' try for releaseID touching the
// given projects.
func seedTerminal(t *testing.T, db *sqlx.DB, releaseID string, projects []string) {
	t.Helper()
	repo := NewFixRepository(db, nil)
	p := fix.Fix{
		Source: "validation", ReleaseID: releaseID, Round: 1, NodeID: "model.core.orders",
		ResolvedNodeIDs: []string{"model.core.orders"}, ErrorSignature: "sig", Try: 1,
		Status: fix.StatusProposed, Projects: projects, CreatedAt: time.Now().UTC(),
	}
	p.NormalizeRepresentativeViews()
	require.NoError(t, repo.Upsert(context.Background(), p))
}

func releaseCheckProjectNames(vs []fix.ReleaseCheck) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Project)
	}
	return out
}
