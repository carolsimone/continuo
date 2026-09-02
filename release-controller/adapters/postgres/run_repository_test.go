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
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
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
	_, _ = db.Exec("TRUNCATE release_pipeline_runs, current_prod, release_controller_outbox, message_processing, service_prod RESTART IDENTITY CASCADE")
	return db
}

func TestRunRepository_SaveAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	r := pipeline.NewCandidate("rA", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), r))

	got, err := repo.Get(context.Background(), "rA")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rA", got.ID())
	assert.Equal(t, pipeline.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedService())
	assert.Equal(t, release.ManifestKindDbt, got.ManifestKind())
}

// TestRunRepository_ManifestKindRoundTrips verifies that the manifest kind a
// run was constructed with (dbt or python) survives a Save/Get round trip
// through the manifest_kind column, which is immutable after insert.
func TestRunRepository_ManifestKindRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rel-kind", "svc-py", "img:1", false, "acme/py", "cafebabe",
		release.ManifestKindPython, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rel-kind")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, release.ManifestKindPython, got.ManifestKind())
}

func TestRunRepository_BootstrapRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	boot := pipeline.NewCandidate("r-boot", "svc-a", "sha-a", true, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, boot))
	plain := pipeline.NewCandidate("r-plain", "svc-a", "sha-a", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
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

func TestRunRepository_NextQueuedAndActive(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	older := pipeline.NewCandidate("rOLD", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	newer := pipeline.NewCandidate("rNEW", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(200, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), older))
	require.NoError(t, repo.Save(context.Background(), newer))

	q, err := repo.NextQueued(context.Background())
	require.NoError(t, err)
	require.NotNil(t, q)
	assert.Equal(t, "rOLD", q.ID())

	require.NoError(t, q.TransitionToParsing(time.Unix(300, 0).UTC()))
	require.NoError(t, repo.Save(context.Background(), q))

	a, err := repo.Active(context.Background())
	require.NoError(t, err)
	require.NotNil(t, a)
	assert.Equal(t, "rOLD", a.ID())
}

func TestRunRepository_ChangedServiceRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rCS", "service-x", "img-1", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCS")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "service-x", got.ChangedService())
	assert.Equal(t, map[string]string{"service-x": "img-1"}, got.ImageTags())
}

func TestRunRepository_SetAssembledImageTagsRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rIT", "svc-a", "tag-a", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	// Simulate AdvanceQueue overwriting image_tags with the assembled set.
	r.SetAssembledImageTags(map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"})
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rIT")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}, got.ImageTags())
}

func TestRunRepository_ListPaginatesNewestFirst(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	for i, id := range []string{"r1", "r2", "r3"} {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(int64(100+i), 0).UTC())
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

func TestRunRepository_ListTiebreaksByRunIDOnEqualTimestamp(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	ts := time.Unix(200, 0).UTC()
	ra := pipeline.NewCandidate("ra", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, ts)
	rb := pipeline.NewCandidate("rb", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, ts)
	require.NoError(t, repo.Save(ctx, ra))
	require.NoError(t, repo.Save(ctx, rb))

	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "rb", page1[0].ID(), "higher run_id sorts first under DESC when timestamps are equal")
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 1, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "ra", page2[0].ID())
	assert.Nil(t, next2)
}

func TestRunRepository_ListFiltersByStatus(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	a := pipeline.NewCandidate("ra", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, a))
	b := pipeline.NewCandidate("rb", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(101, 0).UTC())
	require.NoError(t, b.TransitionToParsing(time.Unix(102, 0).UTC()))
	require.NoError(t, b.TransitionToValidating(nil, nil, time.Unix(103, 0).UTC()))
	require.NoError(t, b.Fail("validation_failed", "", []string{"x"}, time.Unix(104, 0).UTC()))
	require.NoError(t, repo.Save(ctx, b))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "rb", items[0].ID())
}

func TestRunRepository_PerNodeResultsRoundTrip(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	r := pipeline.NewCandidate("rp", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	r.RecordValidationResults([]pipeline.NodeValidationResult{{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 9}})
	require.NoError(t, r.Fail("validation_failed", "", []string{"a"}, time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rp")
	require.NoError(t, err)
	require.Len(t, got.PerNodeResults(), 1)
	assert.Equal(t, "k/a.log", got.PerNodeResults()[0].DBTLogURI)
}

func TestRunRepository_RoundTripsFailDetail(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rel-detail", "finance", "tag", false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(102, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rel-detail")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "duplicate_table", got.FailReason())
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		got.FailDetail())
}

// TestRunRepository_RoundTripsRemediationRoundAndPayload verifies that a
// rejected candidate's remediation round and rejection payload persist across
// a StartRemediationRound + Save + Get round trip, and that a candidate
// rejected without ever setting a payload reads back nil.
func TestRunRepository_RoundTripsRemediationRoundAndPayload(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rel-remediation", "finance", "tag", false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(300, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(301, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table", "",
		[]string{"analytics.orders"}, time.Unix(302, 0).UTC()))
	r.SetRejectionPayload([]byte(`{"release_id":"rel-x"}`))
	require.NoError(t, repo.Save(ctx, r))

	n, err := r.StartRemediationRound(time.Unix(303, 0).UTC())
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rel-remediation")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 2, got.RemediationRound())
	assert.JSONEq(t, `{"release_id":"rel-x"}`, string(got.RejectionPayload()))

	noPayload := pipeline.NewCandidate("rel-no-payload", "finance", "tag", false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(310, 0).UTC())
	require.NoError(t, noPayload.TransitionToParsing(time.Unix(311, 0).UTC()))
	require.NoError(t, noPayload.Fail("parse_failed", "",
		nil, time.Unix(312, 0).UTC()))
	require.NoError(t, repo.Save(ctx, noPayload))

	gotNoPayload, err := repo.Get(ctx, "rel-no-payload")
	require.NoError(t, err)
	require.NotNil(t, gotNoPayload)
	assert.Equal(t, 1, gotNoPayload.RemediationRound())
	assert.Nil(t, gotNoPayload.RejectionPayload())
}

// TestRunRepository_List_IncludesFailDetail guards against List's SELECT
// silently dropping fail_detail. List returns the same fully-hydrated
// pipeline.Run aggregate as Get/Load/NextQueued/Active, so a caller reading
// FailDetail() off a List result must see the same value Get would return
// for that row — not a permanently empty string because List's column list
// forgot one column the other four queries carry.
func TestRunRepository_List_IncludesFailDetail(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rel-list-detail", "finance", "tag", false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(200, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(201, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(202, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)

	var got *pipeline.Run
	for _, item := range items {
		if item.ID() == "rel-list-detail" {
			got = item
		}
	}
	require.NotNil(t, got, "rel-list-detail must be present in the List result")
	assert.Equal(t,
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		got.FailDetail())
}

func TestRunRepository_DeleteFinishedBeforeKeepsCurrentProd(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	mkRejected := func(id string, ts int64) {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Fail("validation_failed", "", nil, time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkPromoted := func(id string, ts int64) {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Promote(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkRejected("old-rejected", 100)
	mkPromoted("old-promoted", 100)
	mkRejected("old-keep", 100)
	r := pipeline.NewCandidate("received-young", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"old-keep"})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	gone, _ := repo.Get(ctx, "old-rejected")
	assert.Nil(t, gone)
	promotedGone, _ := repo.Get(ctx, "old-promoted")
	assert.Nil(t, promotedGone, "promoted runs older than cutoff should be deleted")
	kept, _ := repo.Get(ctx, "old-keep")
	assert.NotNil(t, kept)
	young, _ := repo.Get(ctx, "received-young")
	assert.NotNil(t, young)
}

func TestRunRepository_DeleteFinishedBeforeKeepsServiceProdRefs(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	spRepo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()

	mkPromoted := func(id string, ts int64) {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Promote(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}

	// Two old promoted runs; svc-a's pointer references sp-ref, the other has no pointer.
	mkPromoted("sp-ref", 100)
	mkPromoted("sp-unref", 100)
	// svc-a still points at sp-ref (its last promoted run).
	require.NoError(t, spRepo.Upsert(ctx, release.NewServiceProd("svc-a", "sp-ref", "s3://a", "t1", release.ManifestKindDbt, time.Unix(100, 0).UTC())))

	// Collect keep IDs by reading service_prod (mirroring what the prune handler does).
	sps, err := spRepo.List(ctx)
	require.NoError(t, err)
	keepIDs := make([]string, 0, len(sps))
	for _, sp := range sps {
		keepIDs = append(keepIDs, sp.ReleaseID())
	}

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, keepIDs)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the unreferenced old run should be deleted")

	// sp-ref is still reachable (service_prod points at it).
	kept, _ := repo.Get(ctx, "sp-ref")
	assert.NotNil(t, kept, "run referenced by service_prod must survive retention")

	// sp-unref has no pointer and is older than cutoff — it should be gone.
	gone, _ := repo.Get(ctx, "sp-unref")
	assert.Nil(t, gone, "unreferenced old terminal run must be pruned")
}

func TestRunRepository_DeleteFinishedBeforeEmptyKeepSlice(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("old-prom", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, r.Promote(time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "an empty keep set should delete all matching old terminal runs")

	gone, _ := repo.Get(ctx, "old-prom")
	assert.Nil(t, gone)
}

// TestRunRepository_Load_ReturnsRow verifies that Load returns the same row
// content as Get for an existing run.
func TestRunRepository_Load_ReturnsRow(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	r := pipeline.NewCandidate("rLoad", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewRunRepository(tx, nil)

	got, err := txRepo.Load(ctx, "rLoad")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rLoad", got.ID())
	assert.Equal(t, pipeline.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedService())
}

// TestRunRepository_Load_ReturnsNilForMissingRow verifies Load matches Get's
// absent-row behaviour (nil, nil error) rather than surfacing sql.ErrNoRows.
func TestRunRepository_Load_ReturnsNilForMissingRow(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewRunRepository(tx, nil)

	got, err := txRepo.Load(ctx, "does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestRunRepository_Load_BlocksConcurrentLoad verifies that Load's FOR UPDATE
// lock serializes a second transaction's Load on the same row: it must not
// observe the row until the first transaction commits.
func TestRunRepository_Load_BlocksConcurrentLoad(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	r := pipeline.NewCandidate("rLock", "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	repo1 := postgres.NewRunRepository(tx1, nil)
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
		repo2 := postgres.NewRunRepository(tx2, nil)
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

func TestRunRepository_RoundTripsProvenance(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rPROV", "svc-a", "img-1", false, "acme/demo", "deadbeefcafe1234", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	r.SetCodeBundleURI("s3://b/code-bundles/rPROV/bundle.json")
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rPROV")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "acme/demo", got.Repo())
	assert.Equal(t, "deadbeefcafe1234", got.CommitSHA())
	assert.Equal(t, "s3://b/code-bundles/rPROV/bundle.json", got.CodeBundleURI())
}

// TestRunRepository_CodeBundleURIUpdatesAfterCreation verifies that
// code_bundle_uri set via SetCodeBundleURI after the initial Save (the
// SetAssembledImageTags-style mutable path — the URI is unknown at receive
// time and only known once the parse result arrives) is persisted on a
// second Save, unlike the truly immutable repo/commit_sha provenance fields.
func TestRunRepository_CodeBundleURIUpdatesAfterCreation(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewCandidate("rCBU", "svc-a", "img-1", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
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

func TestRunRepository_RoundTripsCandidateArtifactURI(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	// Build a run that already has a candidate topology containing a node
	// with a CandidateArtifactURI. Rehydrate bypasses the state machine so we can
	// inject the topology directly, mirroring how the repository reconstructs
	// runs from Postgres.
	topo := release.Topology{
		{UniqueID: "n", CandidateArtifactURI: "s3://b/candidate-sql/r/n.sql"},
	}
	r := pipeline.Rehydrate(pipeline.RehydrateInput{
		ID:                "rCSURI",
		Kind:              pipeline.KindCandidate,
		Status:            pipeline.StatusValidating,
		ChangedService:    "svc-a",
		ImageTags:         map[string]string{"svc-a": "img-1"},
		CandidateTopology: topo,
		ValidationNodeIDs: []string{"n"},
		Repo:              "acme/demo",
		CommitSHA:         "deadbeef",
		ManifestKind:      release.ManifestKindDbt,
		CreatedAt:         time.Unix(100, 0).UTC(),
	})
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCSURI")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.CandidateTopology(), 1)
	assert.Equal(t, "s3://b/candidate-sql/r/n.sql", got.CandidateTopology()[0].CandidateArtifactURI,
		"candidate_artifact_uri must survive a JSONB round-trip through Postgres")
}

// TestRunRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes verifies
// that DeleteFinishedBefore calls the CandidateSQLDeleter with the correct
// candidate-sql/<id>/ AND code-bundles/<id>/ prefixes for each pruned run,
// and that a deleter error does not abort the prune (soft-fail).
func TestRunRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	mkTerminal := func(id string, ts int64) {
		r := pipeline.NewCandidate(id, "svc", "t", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Fail("validation_failed", "", nil, time.Unix(ts+3, 0).UTC()))
		repo := postgres.NewRunRepository(db, nil)
		require.NoError(t, repo.Save(ctx, r))
	}

	mkTerminal("prune-a", 100)
	mkTerminal("prune-b", 101)
	mkTerminal("keep-c", 102)

	cutoff := time.Unix(1000, 0).UTC()

	t.Run("deleter is called with correct prefixes", func(t *testing.T) {
		fd := &fakeDeleter{}
		repo := postgres.NewRunRepository(db, fd)
		n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		got := fd.prefixes()
		assert.Equal(t, []string{
			"candidate-sql/prune-a/",
			"candidate-sql/prune-b/",
			"code-bundles/prune-a/",
			"code-bundles/prune-b/",
		}, got, "deleter must be called with both the candidate-sql and code-bundles prefix for each pruned run")
	})

	// Reseed because the previous sub-test deleted prune-a and prune-b.
	mkTerminal("prune-d", 103)
	mkTerminal("prune-e", 104)

	t.Run("deleter error does not fail the prune", func(t *testing.T) {
		fd := &fakeDeleter{failErr: errors.New("s3 unavailable")}
		repo := postgres.NewRunRepository(db, fd)
		n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err, "prune must succeed even when S3 deletion fails")
		assert.Equal(t, 2, n, "both runs must be counted as pruned despite S3 error")

		// Both the candidate-sql and code-bundles prefix were attempted for each
		// pruned run despite every call failing.
		got := fd.prefixes()
		assert.Len(t, got, 4, "deleter must be attempted for both prefixes of every pruned run")
	})
}

// TestRunRepository_VerificationKindRoundTrips verifies that a run's kind
// (candidate or verification) and a verification's source-overlay URI
// survive a Save/Get round trip through run_kind and source_overlay_uri,
// both immutable after insert.
func TestRunRepository_VerificationKindRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	verification := pipeline.NewVerification("r-verify", "svc-a", "img-1", "rel-orig", 1,
		"s3://bucket/svc-a/r-verify/source-overlay.tar.gz", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, verification))
	candidate := pipeline.NewCandidate("r-plain-candidate", "svc-a", "img-1", false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, candidate))

	gotVerification, err := repo.Get(ctx, "r-verify")
	require.NoError(t, err)
	require.NotNil(t, gotVerification)
	assert.Equal(t, pipeline.KindVerification, gotVerification.Kind())
	assert.Equal(t, "s3://bucket/svc-a/r-verify/source-overlay.tar.gz", gotVerification.SourceOverlayURI())
	assert.Equal(t, "rel-orig", gotVerification.VerifiesReleaseID())
	assert.Equal(t, 1, gotVerification.Attempt())

	gotCandidate, err := repo.Get(ctx, "r-plain-candidate")
	require.NoError(t, err)
	require.NotNil(t, gotCandidate)
	assert.Equal(t, pipeline.KindCandidate, gotCandidate.Kind())
	assert.Equal(t, "", gotCandidate.SourceOverlayURI())
}

// TestRunRepository_DeleteFinishedBeforePrunesPassedVerification verifies
// that a verification run resolved into the terminal 'passed' status is
// pruned by DeleteFinishedBefore just like any other terminal run — its rows
// and S3 prefixes must not accumulate forever.
func TestRunRepository_DeleteFinishedBeforePrunesPassedVerification(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()

	r := pipeline.NewVerification("r-verify-passed", "svc", "t", "rel-orig", 1, "", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(nil, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, r.Pass(time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a passed verification run older than cutoff must be pruned")

	gone, _ := repo.Get(ctx, "r-verify-passed")
	assert.Nil(t, gone, "passed verification run must be deleted by DeleteFinishedBefore")
}

func TestRunRepository_QueueInterleavesBothKindsByCreatedAt(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Save(ctx, pipeline.NewCandidate("rel-a", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, t0)))
	require.NoError(t, repo.Save(ctx, pipeline.NewVerification("verify-rel-a-core-a1", "core", "img", "rel-a", 1, "", release.ManifestKindDbt, t0.Add(time.Second))))
	require.NoError(t, repo.Save(ctx, pipeline.NewCandidate("rel-b", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, t0.Add(2*time.Second))))

	next, err := repo.NextQueued(ctx)
	require.NoError(t, err)
	require.Equal(t, "rel-a", next.ID())
	require.NoError(t, next.TransitionToCompiling(t0.Add(3*time.Second)))
	require.NoError(t, repo.Save(ctx, next))

	active, err := repo.Active(ctx)
	require.NoError(t, err)
	require.Equal(t, "rel-a", active.ID(), "the active slice is kind-blind")
	next, err = repo.NextQueued(ctx)
	require.NoError(t, err)
	require.Equal(t, "verify-rel-a-core-a1", next.ID(), "a verification takes its turn in created_at order, ahead of a later candidate")
}

func TestRunRepository_ListFiltersByKindAndVerifiedRelease(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewRunRepository(db, nil)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Save(ctx, pipeline.NewCandidate("rel-a", "core", "img", false, "org/r", "sha", release.ManifestKindDbt, t0)))
	require.NoError(t, repo.Save(ctx, pipeline.NewVerification("verify-rel-a-core-a1", "core", "img", "rel-a", 1, "", release.ManifestKindDbt, t0.Add(time.Second))))
	require.NoError(t, repo.Save(ctx, pipeline.NewVerification("verify-rel-z-core-a1", "core", "img", "rel-z", 1, "", release.ManifestKindDbt, t0.Add(2*time.Second))))

	cand := pipeline.KindCandidate
	items, _, err := repo.List(ctx, repository.ListFilter{Kind: &cand})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "rel-a", items[0].ID())

	verifies := "rel-a"
	items, _, err = repo.List(ctx, repository.ListFilter{VerifiesReleaseID: &verifies})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "verify-rel-a-core-a1", items[0].ID())
}
