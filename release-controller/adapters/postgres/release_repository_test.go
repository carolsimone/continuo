//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDeleter is a test double for ports.CandidateSQLDeleter that records
// which prefixes it was asked to delete and can be configured to return an
// error on every call.
type fakeDeleter struct {
	mu      sync.Mutex
	called  []string
	failErr error
}

func (f *fakeDeleter) DeletePrefix(_ context.Context, prefix string) error {
	f.mu.Lock()
	f.called = append(f.called, prefix)
	f.mu.Unlock()
	return f.failErr
}

func (f *fakeDeleter) prefixes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.called))
	copy(out, f.called)
	sort.Strings(out)
	return out
}

func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("RELEASE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	_, _ = db.Exec("TRUNCATE releases, current_prod, release_controller_outbox, message_processing, service_prod RESTART IDENTITY CASCADE")
	return db
}

func TestReleaseRepository_SaveAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	r := release.New("rA", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), r))

	got, err := repo.Get(context.Background(), "rA")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rA", got.ID())
	assert.Equal(t, release.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedService())
}

func TestReleaseRepository_BootstrapRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	boot := release.New("r-boot", "svc-a", "sha-a", true, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, boot))
	plain := release.New("r-plain", "svc-a", "sha-a", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, plain))

	gotBoot, err := repo.Get(ctx, "r-boot")
	require.NoError(t, err)
	require.NotNil(t, gotBoot)
	assert.True(t, gotBoot.IsBootstrap())

	gotPlain, err := repo.Get(ctx, "r-plain")
	require.NoError(t, err)
	require.NotNil(t, gotPlain)
	assert.False(t, gotPlain.IsBootstrap())
}

func TestReleaseRepository_NextQueuedAndActive(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	older := release.New("rOLD", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	newer := release.New("rNEW", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(200, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), older))
	require.NoError(t, repo.Save(context.Background(), newer))

	q, err := repo.NextQueuedRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, q)
	assert.Equal(t, "rOLD", q.ID())

	require.NoError(t, q.TransitionToParsing(time.Unix(300, 0).UTC()))
	require.NoError(t, repo.Save(context.Background(), q))

	a, err := repo.ActiveRelease(context.Background())
	require.NoError(t, err)
	require.NotNil(t, a)
	assert.Equal(t, "rOLD", a.ID())
}

func TestReleaseRepository_ChangedServiceRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	r := release.New("rCS", "service-x", "img-1", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCS")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "service-x", got.ChangedService())
	assert.Equal(t, map[string]string{"service-x": "img-1"}, got.ImageTags())
}

func TestReleaseRepository_SetAssembledImageTagsRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	r := release.New("rIT", "svc-a", "tag-a", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	// Simulate AdvanceQueue overwriting image_tags with the assembled set.
	r.SetAssembledImageTags(map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"})
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rIT")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}, got.ImageTags())
}

func TestReleaseRepository_ListPaginatesNewestFirst(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	for i, id := range []string{"r1", "r2", "r3"} {
		r := release.New(id, "svc", "t", false, "acme/demo", "deadbeef", time.Unix(int64(100+i), 0).UTC())
		require.NoError(t, repo.Save(ctx, r))
	}
	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "r3", page1[0].ID())
	assert.Equal(t, "r2", page1[1].ID())
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 2, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "r1", page2[0].ID())
	assert.Nil(t, next2)
}

func TestReleaseRepository_ListTiebreaksByReleaseIDOnEqualTimestamp(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	ts := time.Unix(200, 0).UTC()
	ra := release.New("ra", "svc", "t", false, "acme/demo", "deadbeef", ts)
	rb := release.New("rb", "svc", "t", false, "acme/demo", "deadbeef", ts)
	require.NoError(t, repo.Save(ctx, ra))
	require.NoError(t, repo.Save(ctx, rb))

	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "rb", page1[0].ID(), "higher release_id sorts first under DESC when timestamps are equal")
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 1, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "ra", page2[0].ID())
	assert.Nil(t, next2)
}

func TestReleaseRepository_ListFiltersByStatus(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	a := release.New("ra", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, a))
	b := release.New("rb", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(101, 0).UTC())
	require.NoError(t, b.TransitionToParsing(time.Unix(102, 0).UTC()))
	require.NoError(t, b.TransitionToValidating(nil, nil, time.Unix(103, 0).UTC()))
	require.NoError(t, b.TransitionToRejected("validation_failed", []string{"x"}, time.Unix(104, 0).UTC()))
	require.NoError(t, repo.Save(ctx, b))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "rb", items[0].ID())
}

func TestReleaseRepository_PerNodeResultsRoundTrip(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	r := release.New("rp", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	r.RecordValidationResults([]release.NodeValidationResult{{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 9}})
	require.NoError(t, r.TransitionToRejected("validation_failed", []string{"a"}, time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rp")
	require.NoError(t, err)
	require.Len(t, got.PerNodeResults(), 1)
	assert.Equal(t, "k/a.log", got.PerNodeResults()[0].DBTLogURI)
}

func TestReleaseRepository_DeleteResolvedBeforeKeepsCurrentProd(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	mkRejected := func(id string, ts int64) {
		r := release.New(id, "svc", "t", false, "acme/demo", "deadbeef", time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToRejected("validation_failed", nil, time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkPromoted := func(id string, ts int64) {
		r := release.New(id, "svc", "t", false, "acme/demo", "deadbeef", time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToPromoted(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkRejected("old-rejected", 100)
	mkPromoted("old-promoted", 100)
	mkRejected("old-keep", 100)
	r := release.New("received-young", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteResolvedBefore(ctx, cutoff, []string{"old-keep"})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	gone, _ := repo.Get(ctx, "old-rejected")
	assert.Nil(t, gone)
	promotedGone, _ := repo.Get(ctx, "old-promoted")
	assert.Nil(t, promotedGone, "promoted releases older than cutoff should be deleted")
	kept, _ := repo.Get(ctx, "old-keep")
	assert.NotNil(t, kept)
	young, _ := repo.Get(ctx, "received-young")
	assert.NotNil(t, young)
}

func TestReleaseRepository_DeleteResolvedBeforeKeepsServiceProdRefs(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	spRepo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()

	mkPromoted := func(id string, ts int64) {
		r := release.New(id, "svc", "t", false, "acme/demo", "deadbeef", time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToPromoted(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}

	// Two old promoted releases; svc-a's pointer references sp-ref, the other has no pointer.
	mkPromoted("sp-ref", 100)
	mkPromoted("sp-unref", 100)
	// svc-a still points at sp-ref (its last promoted release).
	require.NoError(t, spRepo.Upsert(ctx, release.NewServiceProd("svc-a", "sp-ref", "s3://a", "t1", time.Unix(100, 0).UTC())))

	// Collect keep IDs by reading service_prod (mirroring what PruneResolvedReleases does).
	sps, err := spRepo.List(ctx)
	require.NoError(t, err)
	keepIDs := make([]string, 0, len(sps))
	for _, sp := range sps {
		keepIDs = append(keepIDs, sp.ReleaseID())
	}

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteResolvedBefore(ctx, cutoff, keepIDs)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the unreferenced old release should be deleted")

	// sp-ref is still reachable (service_prod points at it).
	kept, _ := repo.Get(ctx, "sp-ref")
	assert.NotNil(t, kept, "release referenced by service_prod must survive retention")

	// sp-unref has no pointer and is older than cutoff — it should be gone.
	gone, _ := repo.Get(ctx, "sp-unref")
	assert.Nil(t, gone, "unreferenced old terminal release must be pruned")
}

func TestReleaseRepository_DeleteResolvedBeforeEmptyKeepSlice(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	r := release.New("old-prom", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, r.TransitionToPromoted(time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteResolvedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "an empty keep set should delete all matching old terminal releases")

	gone, _ := repo.Get(ctx, "old-prom")
	assert.Nil(t, gone)
}

// TestReleaseRepository_Load_ReturnsRow verifies that Load returns the same
// row content as Get for an existing release.
func TestReleaseRepository_Load_ReturnsRow(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	r := release.New("rLoad", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewReleaseRepository(tx, nil)

	got, err := txRepo.Load(ctx, "rLoad")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rLoad", got.ID())
	assert.Equal(t, release.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedService())
}

// TestReleaseRepository_Load_ReturnsNilForMissingRow verifies Load matches
// Get's absent-row behaviour (nil, nil error) rather than surfacing
// sql.ErrNoRows.
func TestReleaseRepository_Load_ReturnsNilForMissingRow(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewReleaseRepository(tx, nil)

	got, err := txRepo.Load(ctx, "does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestReleaseRepository_Load_BlocksConcurrentLoad verifies that Load's
// FOR UPDATE lock serializes a second transaction's Load on the same row:
// it must not observe the row until the first transaction commits.
func TestReleaseRepository_Load_BlocksConcurrentLoad(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()
	r := release.New("rLock", "svc", "t", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	repo1 := postgres.NewReleaseRepository(tx1, nil)
	got1, err := repo1.Load(ctx, "rLock")
	require.NoError(t, err)
	require.NotNil(t, got1)

	unblocked := make(chan struct{})
	go func() {
		tx2, err := db.BeginTxx(ctx, nil)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = tx2.Rollback() }()
		repo2 := postgres.NewReleaseRepository(tx2, nil)
		_, err = repo2.Load(ctx, "rLock")
		if !assert.NoError(t, err) {
			return
		}
		close(unblocked)
	}()

	select {
	case <-unblocked:
		t.Fatal("second Load must block while the first transaction holds the row lock")
	case <-time.After(300 * time.Millisecond):
		// expected: still blocked
	}

	require.NoError(t, tx1.Commit())

	select {
	case <-unblocked:
		// expected: unblocks once tx1 commits
	case <-time.After(5 * time.Second):
		t.Fatal("second Load did not unblock after the first transaction committed")
	}
}

func TestReleaseRepository_RoundTripsProvenance(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	r := release.New("rPROV", "svc-a", "img-1", false, "acme/demo", "deadbeefcafe1234", time.Unix(100, 0).UTC())
	r.SetCodeBundleURI("s3://b/code-bundles/rPROV/bundle.json")
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rPROV")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "acme/demo", got.Repo())
	assert.Equal(t, "deadbeefcafe1234", got.CommitSHA())
	assert.Equal(t, "s3://b/code-bundles/rPROV/bundle.json", got.CodeBundleURI())
}

// TestReleaseRepository_CodeBundleURIUpdatesAfterCreation verifies that
// code_bundle_uri set via SetCodeBundleURI after the initial Save (the
// SetAssembledImageTags-style mutable path — the URI is unknown at receive
// time and only known once the parse result arrives) is persisted on a
// second Save, unlike the truly immutable repo/commit_sha provenance fields.
func TestReleaseRepository_CodeBundleURIUpdatesAfterCreation(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	r := release.New("rCBU", "svc-a", "img-1", false, "acme/demo", "deadbeef", time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCBU")
	require.NoError(t, err)
	assert.Equal(t, "", got.CodeBundleURI(), "code_bundle_uri defaults to empty until the parse result arrives")

	got.SetCodeBundleURI("s3://b/code-bundles/rCBU/bundle.json")
	require.NoError(t, repo.Save(ctx, got))

	reloaded, err := repo.Get(ctx, "rCBU")
	require.NoError(t, err)
	assert.Equal(t, "s3://b/code-bundles/rCBU/bundle.json", reloaded.CodeBundleURI())
}

func TestReleaseRepository_RoundTripsCandidateSQLURI(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseRepository(db, nil)
	ctx := context.Background()

	// Build a release that already has a candidate topology containing a node
	// with a CandidateSQLURI. Rehydrate bypasses the state machine so we can
	// inject the topology directly, mirroring how the repository reconstructs
	// releases from Postgres.
	topo := release.Topology{
		{UniqueID: "n", CandidateSQLURI: "s3://b/candidate-sql/r/n.sql"},
	}
	r := release.Rehydrate(release.RehydrateInput{
		ID:                "rCSURI",
		Status:            release.StatusValidating,
		ChangedService:    "svc-a",
		ImageTags:         map[string]string{"svc-a": "img-1"},
		CandidateTopology: topo,
		ValidationNodeIDs: []string{"n"},
		Repo:              "acme/demo",
		CommitSHA:         "deadbeef",
		CreatedAt:         time.Unix(100, 0).UTC(),
	})
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCSURI")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.CandidateTopology(), 1)
	assert.Equal(t, "s3://b/candidate-sql/r/n.sql", got.CandidateTopology()[0].CandidateSQLURI,
		"candidate_sql_uri must survive a JSONB round-trip through Postgres")
}

// TestReleaseRepository_DeleteResolvedBefore_DeletesCandidateSQLPrefixes
// verifies that DeleteResolvedBefore calls the CandidateSQLDeleter with the
// correct prefix for each pruned release, and that a deleter error does not
// abort the prune (soft-fail).
func TestReleaseRepository_DeleteResolvedBefore_DeletesCandidateSQLPrefixes(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	mkTerminal := func(id string, ts int64) {
		r := release.New(id, "svc", "t", false, "acme/demo", "deadbeef", time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.TransitionToRejected("validation_failed", nil, time.Unix(ts+3, 0).UTC()))
		repo := postgres.NewReleaseRepository(db, nil)
		require.NoError(t, repo.Save(ctx, r))
	}

	mkTerminal("prune-a", 100)
	mkTerminal("prune-b", 101)
	mkTerminal("keep-c", 102)

	cutoff := time.Unix(1000, 0).UTC()

	t.Run("deleter is called with correct prefixes", func(t *testing.T) {
		fd := &fakeDeleter{}
		repo := postgres.NewReleaseRepository(db, fd)
		n, err := repo.DeleteResolvedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		got := fd.prefixes()
		assert.Equal(t, []string{
			"candidate-sql/prune-a/",
			"candidate-sql/prune-b/",
		}, got, "deleter must be called once per pruned release with the correct prefix")
	})

	// Reseed because the previous sub-test deleted prune-a and prune-b.
	mkTerminal("prune-d", 103)
	mkTerminal("prune-e", 104)

	t.Run("deleter error does not fail the prune", func(t *testing.T) {
		fd := &fakeDeleter{failErr: errors.New("s3 unavailable")}
		repo := postgres.NewReleaseRepository(db, fd)
		n, err := repo.DeleteResolvedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err, "prune must succeed even when S3 deletion fails")
		assert.Equal(t, 2, n, "both releases must be counted as pruned despite S3 error")

		// Both prefixes were attempted despite the first failing.
		got := fd.prefixes()
		assert.Len(t, got, 2, "deleter must be attempted for every pruned release")
	})
}
