//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
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

// connectTestDB opens the shared release database and leaves its rows alone.
func connectTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("RELEASE_TEST_PG_DSN")
	if dsn == "" {
		testdeps.Unavailable(t, "RELEASE_TEST_PG_DSN not set")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	return db
}

// openTestDB opens the shared release database with the tables the tests write
// emptied.
func openTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db := connectTestDB(t)
	_, _ = db.Exec("TRUNCATE release_checks, live_release, release_controller_outbox, message_processing, production_pointers RESTART IDENTITY CASCADE")
	_, err := db.Exec("UPDATE promotion_sequence SET last_seq = 0 WHERE id = 1")
	require.NoError(t, err, "promotion_sequence is missing: run make test-deps-up to apply the release migrations")
	return db
}

func TestReleaseCheckRepository_SaveAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	r := releasecheck.NewCandidate("rA", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(context.Background(), r))

	got, err := repo.Get(context.Background(), "rA")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rA", got.ID())
	assert.Equal(t, releasecheck.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedProject())
	assert.Equal(t, release.ManifestKindDbt, got.ManifestKind())
}

// TestReleaseCheckRepository_SaveRejectsCrossKindIDCollision proves the upsert refuses a
// cross-kind id collision atomically: once a candidate owns an id, a
// fix's release check racing on the same id (both having read no row first) does not
// silently take it over — Save reports ErrCheckKindConflict and the candidate row
// is left intact, so the losing caller answers 409 instead of stranding a check
// its kind-specific endpoint would 404.
func TestReleaseCheckRepository_SaveRejectsCrossKindIDCollision(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	cand := releasecheck.NewCandidate("rClash", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, cand))

	verify := releasecheck.NewFix("rClash", "svc", "t", "rOrig", 1, 1, "", release.ManifestKindDbt, time.Unix(200, 0).UTC())
	require.ErrorIs(t, repo.Save(ctx, verify), repository.ErrCheckKindConflict)

	got, err := repo.Get(ctx, "rClash")
	require.NoError(t, err)
	require.Equal(t, releasecheck.KindCandidate, got.Kind())
}

// TestReleaseCheckRepository_CreateInsertsOnlyAFreeID proves Create is insert-only: it
// writes a check whose id is free, and for a taken id of either kind it reports
// false and leaves the stored row exactly as it was.
func TestReleaseCheckRepository_CreateInsertsOnlyAFreeID(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	first := releasecheck.NewCandidate("rCreate", "svc", "img:1", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	inserted, err := repo.Create(ctx, first)
	require.NoError(t, err)
	require.True(t, inserted)

	second := releasecheck.NewCandidate("rCreate", "svc", "img:2", true, false, "acme/fork", "cafebabe", release.ManifestKindPython, time.Unix(200, 0).UTC())
	inserted, err = repo.Create(ctx, second)
	require.NoError(t, err)
	require.False(t, inserted)

	verify := releasecheck.NewFix("rCreate", "svc", "img:3", "rOrig", 1, 1, "", release.ManifestKindDbt, time.Unix(300, 0).UTC())
	inserted, err = repo.Create(ctx, verify)
	require.NoError(t, err)
	require.False(t, inserted)

	got, err := repo.Get(ctx, "rCreate")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, releasecheck.KindCandidate, got.Kind())
	assert.Equal(t, "img:1", got.ImageTags()["svc"])
	assert.False(t, got.IsBootstrap())
	assert.Equal(t, "acme/demo", got.Repo())
	assert.Equal(t, release.ManifestKindDbt, got.ManifestKind())
	assert.Equal(t, time.Unix(100, 0).UTC(), got.CreatedAt().UTC())
}

// TestReleaseCheckRepository_ManifestKindRoundTrips verifies that the manifest kind a
// check was constructed with (dbt or python) survives a Save/Get round trip
// through the manifest_kind column, which is immutable after insert.
func TestReleaseCheckRepository_ManifestKindRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rel-kind", "svc-py", "img:1", false, false, "acme/py", "cafebabe",
		release.ManifestKindPython, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rel-kind")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, release.ManifestKindPython, got.ManifestKind())
}

// TestReleaseCheckRepository_SavesAnOntologyCheck proves the manifest_kind column accepts
// the kind ontology and returns it unchanged, and still refuses a kind no
// release can carry.
func TestReleaseCheckRepository_SavesAnOntologyCheck(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rel-ontology", "company-ontology", "img:1", false, false, "acme/ontology", "cafebabe",
		release.ManifestKindOntology, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rel-ontology")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, release.ManifestKindOntology, got.ManifestKind())

	_, err = db.ExecContext(ctx, `
		INSERT INTO release_checks
		  (check_id, status, image_tags, created_at, repo, commit_sha, changed_project, manifest_kind, check_kind)
		VALUES ('rel-bogus', 'received', '{}', $1, 'acme/x', 'cafebabe', 'svc', 'bogus', 'candidate')`,
		time.Unix(100, 0).UTC())
	require.Error(t, err, "the constraint accepts only the kinds a release can carry")
	assert.Contains(t, err.Error(), "release_checks_manifest_kind_check")
}

// TestReleaseCheckRepository_ActiveCandidateProjectsWithKind proves the list holds the
// projects of the candidate checks of one manifest kind that have not ended, each
// once and sorted, and leaves out the checks that ended, the checks of other kinds
// and the release checks of fixes.
func TestReleaseCheckRepository_ActiveCandidateProjectsWithKind(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	now := time.Unix(100, 0).UTC()

	save := func(r *releasecheck.Check) {
		t.Helper()
		require.NoError(t, repo.Save(ctx, r))
	}
	candidate := func(id, project string, kind release.ManifestKind) *releasecheck.Check {
		return releasecheck.NewCandidate(id, project, "img:1", false, false, "acme/x", "cafebabe", kind, now)
	}
	queued := func(id, project string, kind release.ManifestKind) { save(candidate(id, project, kind)) }
	ended := func(id, project string, kind release.ManifestKind) {
		r := candidate(id, project, kind)
		require.NoError(t, r.Fail("invalid_contract", "", nil, now))
		save(r)
	}
	parsing := func(id, project string, kind release.ManifestKind) {
		r := candidate(id, project, kind)
		require.NoError(t, r.TransitionToParsing(now))
		save(r)
	}

	queued("r1", "ontology-b", release.ManifestKindOntology)
	parsing("r2", "ontology-a", release.ManifestKindOntology)
	queued("r3", "ontology-a", release.ManifestKindOntology)
	ended("r4", "ontology-ended", release.ManifestKindOntology)
	queued("r5", "svc-dbt", release.ManifestKindDbt)
	queued("r6", "svc-py", release.ManifestKindPython)
	save(releasecheck.NewFix("v1", "ontology-verified", "img:1", "r0", 1, 1, "", release.ManifestKindOntology, now))

	got, err := repo.ActiveCandidateProjectsWithKind(ctx, release.ManifestKindOntology)
	require.NoError(t, err)
	assert.Equal(t, []string{"ontology-a", "ontology-b"}, got)

	got, err = repo.ActiveCandidateProjectsWithKind(ctx, release.ManifestKindDbt)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc-dbt"}, got)

	got, err = repo.ActiveCandidateProjectsWithKind(ctx, release.ManifestKindPython)
	require.NoError(t, err)
	assert.Equal(t, []string{"svc-py"}, got)

	got, err = repo.ActiveCandidateProjectsWithKind(ctx, release.ManifestKind("none"))
	require.NoError(t, err)
	assert.Empty(t, got, "a kind no check holds yields an empty list, not an error")
}

func TestReleaseCheckRepository_BootstrapRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	boot := releasecheck.NewCandidate("r-boot", "svc-a", "sha-a", true, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, boot))
	plain := releasecheck.NewCandidate("r-plain", "svc-a", "sha-a", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
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

func TestReleaseCheckRepository_NextQueuedAndActive(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	older := releasecheck.NewCandidate("rOLD", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	newer := releasecheck.NewCandidate("rNEW", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(200, 0).UTC())
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

func TestReleaseCheckRepository_ChangedProjectRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rCS", "service-x", "img-1", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCS")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "service-x", got.ChangedProject())
	assert.Equal(t, map[string]string{"service-x": "img-1"}, got.ImageTags())
}

func TestReleaseCheckRepository_SetAssembledImageTagsRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rIT", "svc-a", "tag-a", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	// Simulate AdvanceQueue overwriting image_tags with the assembled set.
	r.SetAssembledImageTags(map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"})
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rIT")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc-a": "tag-a", "svc-b": "tag-b"}, got.ImageTags())
}

func TestReleaseCheckRepository_ListPaginatesNewestFirst(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	for i, id := range []string{"r1", "r2", "r3"} {
		r := releasecheck.NewCandidate(id, "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(int64(100+i), 0).UTC())
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

func TestReleaseCheckRepository_ListTiebreaksByCheckIDOnEqualTimestamp(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	ts := time.Unix(200, 0).UTC()
	ra := releasecheck.NewCandidate("ra", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, ts)
	rb := releasecheck.NewCandidate("rb", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, ts)
	require.NoError(t, repo.Save(ctx, ra))
	require.NoError(t, repo.Save(ctx, rb))

	page1, next, err := repo.List(ctx, repository.ListFilter{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "rb", page1[0].ID(), "higher check_id sorts first under DESC when timestamps are equal")
	require.NotNil(t, next)

	page2, next2, err := repo.List(ctx, repository.ListFilter{Limit: 1, Cursor: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "ra", page2[0].ID())
	assert.Nil(t, next2)
}

func TestReleaseCheckRepository_ListFiltersByStatus(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	a := releasecheck.NewCandidate("ra", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, a))
	b := releasecheck.NewCandidate("rb", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(101, 0).UTC())
	require.NoError(t, b.TransitionToParsing(time.Unix(102, 0).UTC()))
	require.NoError(t, b.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(103, 0).UTC()))
	require.NoError(t, b.Fail("validation_failed", "", []string{"x"}, time.Unix(104, 0).UTC()))
	require.NoError(t, repo.Save(ctx, b))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "rb", items[0].ID())
}

func TestReleaseCheckRepository_PerNodeResultsRoundTrip(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	r := releasecheck.NewCandidate("rp", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(102, 0).UTC()))
	r.RecordValidationResults([]releasecheck.NodeValidationResult{{NodeID: "a", Status: "failed", DBTLogURI: "k/a.log", DurationMS: 9}})
	// A parse-leg result reports through its own Detail rather than a log, so
	// the column has to carry that text back out of JSONB unchanged.
	r.RecordStageResults("parse", []releasecheck.NodeValidationResult{{
		NodeID: "b", Status: "failed", FilePath: "models/b.sql", NodeType: "dbt-model",
		Detail: "Expecting ). Line 3, Col: 12.",
	}})
	require.NoError(t, r.Fail("validation_failed", "", []string{"a"}, time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rp")
	require.NoError(t, err)
	require.Len(t, got.PerNodeResults(), 2)
	assert.Equal(t, "k/a.log", got.PerNodeResults()[0].DBTLogURI)
	assert.Empty(t, got.PerNodeResults()[0].Detail)
	assert.Equal(t, "Expecting ). Line 3, Col: 12.", got.PerNodeResults()[1].Detail)
}

func TestReleaseCheckRepository_RoundTripsFailDetail(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rel-detail", "finance", "tag", false, false, "owner/repo", "abc123",
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

// TestReleaseCheckRepository_RoundTripsRemediationRoundAndPayload verifies that a
// rejected candidate's remediation round and rejection payload persist across
// a StartRemediationRound + Save + Get round trip, and that a candidate
// rejected without ever setting a payload reads back nil.
func TestReleaseCheckRepository_RoundTripsRemediationRoundAndPayload(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rel-remediation", "finance", "tag", false, false, "owner/repo", "abc123",
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

	noPayload := releasecheck.NewCandidate("rel-no-payload", "finance", "tag", false, false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(310, 0).UTC())
	require.NoError(t, noPayload.TransitionToParsing(time.Unix(311, 0).UTC()))
	require.NoError(t, noPayload.Fail("invalid_artifact", "",
		nil, time.Unix(312, 0).UTC()))
	require.NoError(t, repo.Save(ctx, noPayload))

	gotNoPayload, err := repo.Get(ctx, "rel-no-payload")
	require.NoError(t, err)
	require.NotNil(t, gotNoPayload)
	assert.Equal(t, 1, gotNoPayload.RemediationRound())
	assert.Nil(t, gotNoPayload.RejectionPayload())
}

// TestReleaseCheckRepository_List_IncludesFailDetail guards against List's SELECT
// silently dropping fail_detail. List returns the same fully-hydrated
// releasecheck.Check aggregate as Get/Load/NextQueued/Active, so a caller reading
// FailDetail() off a List result must see the same value Get would return
// for that row — not a permanently empty string because List's column list
// forgot one column the other four queries carry.
func TestReleaseCheckRepository_List_IncludesFailDetail(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rel-list-detail", "finance", "tag", false, false, "owner/repo", "abc123",
		release.ManifestKindDbt, time.Unix(200, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(201, 0).UTC()))
	require.NoError(t, r.Fail("duplicate_table",
		"analytics.orders is produced by finance (models/orders.sql) and marketing (models/orders.sql)",
		[]string{"analytics.orders"}, time.Unix(202, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	rejected := "rejected"
	items, _, err := repo.List(ctx, repository.ListFilter{Status: &rejected, Limit: 10})
	require.NoError(t, err)

	var got *releasecheck.Check
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

func TestReleaseCheckRepository_DeleteFinishedBeforeKeepsLiveRelease(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	mkRejected := func(id string, ts int64) {
		r := releasecheck.NewCandidate(id, "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Fail("validation_failed", "", nil, time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkPromoted := func(id string, ts int64) {
		r := releasecheck.NewCandidate(id, "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Promote(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}
	mkRejected("old-rejected", 100)
	mkPromoted("old-promoted", 100)
	mkRejected("old-keep", 100)
	r := releasecheck.NewCandidate("received-young", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"old-keep"})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	gone, _ := repo.Get(ctx, "old-rejected")
	assert.Nil(t, gone)
	promotedGone, _ := repo.Get(ctx, "old-promoted")
	assert.Nil(t, promotedGone, "promoted checks older than cutoff should be deleted")
	kept, _ := repo.Get(ctx, "old-keep")
	assert.NotNil(t, kept)
	young, _ := repo.Get(ctx, "received-young")
	assert.NotNil(t, young)
}

func TestReleaseCheckRepository_DeleteFinishedBeforeKeepsProductionPointerRefs(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	pointerRepo := postgres.NewProductionPointerRepository(db)
	ctx := context.Background()

	mkPromoted := func(id string, ts int64) {
		r := releasecheck.NewCandidate(id, "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Promote(time.Unix(ts+3, 0).UTC()))
		require.NoError(t, repo.Save(ctx, r))
	}

	// Two old promoted checks; svc-a's pointer references rel-pointed, the other has no pointer.
	mkPromoted("rel-pointed", 100)
	mkPromoted("rel-unpointed", 100)
	// svc-a still points at rel-pointed (its last promoted check).
	require.NoError(t, pointerRepo.Upsert(ctx, release.NewProductionPointer("svc-a", "rel-pointed", "s3://a", "t1", release.ManifestKindDbt, time.Unix(100, 0).UTC())))

	// Collect keep IDs from the production pointers (mirroring what the prune handler does).
	pointers, err := pointerRepo.List(ctx)
	require.NoError(t, err)
	keepIDs := make([]string, 0, len(pointers))
	for _, pointer := range pointers {
		keepIDs = append(keepIDs, pointer.ReleaseID())
	}

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, keepIDs)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the unreferenced old check should be deleted")

	// rel-pointed is still reachable: svc-a's production pointer names it.
	kept, _ := repo.Get(ctx, "rel-pointed")
	assert.NotNil(t, kept, "a check a production pointer names must survive retention")

	// rel-unpointed has no pointer and is older than cutoff — it should be gone.
	gone, _ := repo.Get(ctx, "rel-unpointed")
	assert.Nil(t, gone, "unreferenced old terminal check must be pruned")
}

func TestReleaseCheckRepository_DeleteFinishedBeforeEmptyKeepSlice(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("old-prom", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, r.Promote(time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "an empty keep set should delete all matching old terminal checks")

	gone, _ := repo.Get(ctx, "old-prom")
	assert.Nil(t, gone)
}

// TestReleaseCheckRepository_Load_ReturnsRow verifies that Load returns the same row
// content as Get for an existing check.
func TestReleaseCheckRepository_Load_ReturnsRow(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	r := releasecheck.NewCandidate("rLoad", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewReleaseCheckRepository(tx, nil)

	got, err := txRepo.Load(ctx, "rLoad")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "rLoad", got.ID())
	assert.Equal(t, releasecheck.StatusReceived, got.Status())
	assert.Equal(t, "svc", got.ChangedProject())
}

// TestReleaseCheckRepository_Load_ReturnsNilForMissingRow verifies Load matches Get's
// absent-row behaviour (nil, nil error) rather than surfacing sql.ErrNoRows.
func TestReleaseCheckRepository_Load_ReturnsNilForMissingRow(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	txRepo := postgres.NewReleaseCheckRepository(tx, nil)

	got, err := txRepo.Load(ctx, "does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestReleaseCheckRepository_Load_BlocksConcurrentLoad verifies that Load's FOR UPDATE
// lock serializes a second transaction's Load on the same row: it must not
// observe the row until the first transaction commits.
func TestReleaseCheckRepository_Load_BlocksConcurrentLoad(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	r := releasecheck.NewCandidate("rLock", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	tx1, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	repo1 := postgres.NewReleaseCheckRepository(tx1, nil)
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
		repo2 := postgres.NewReleaseCheckRepository(tx2, nil)
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

func TestReleaseCheckRepository_RoundTripsProvenance(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rPROV", "svc-a", "img-1", false, false, "acme/demo", "deadbeefcafe1234", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	r.SetCodeBundleURI("s3://b/code-bundles/rPROV/bundle.json")
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rPROV")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "acme/demo", got.Repo())
	assert.Equal(t, "deadbeefcafe1234", got.CommitSHA())
	assert.Equal(t, "s3://b/code-bundles/rPROV/bundle.json", got.CodeBundleURI())
}

// TestReleaseCheckRepository_CodeBundleURIUpdatesAfterCreation verifies that
// code_bundle_uri set via SetCodeBundleURI after the initial Save (the
// SetAssembledImageTags-style mutable path — the URI is unknown at receive
// time and only known once the parse result arrives) is persisted on a
// second Save, unlike the truly immutable repo/commit_sha provenance fields.
func TestReleaseCheckRepository_CodeBundleURIUpdatesAfterCreation(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rCBU", "svc-a", "img-1", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
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

// TestReleaseCheckRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes verifies
// that DeleteFinishedBefore calls the CandidateSQLDeleter with the correct
// candidate-sql/<id>/ AND code-bundles/<id>/ prefixes for each pruned check,
// and that a deleter error does not abort the prune (soft-fail).
func TestReleaseCheckRepository_DeleteFinishedBefore_DeletesCandidateSQLPrefixes(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	mkTerminal := func(id string, ts int64) {
		r := releasecheck.NewCandidate(id, "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(ts, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(ts+1, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(ts+2, 0).UTC()))
		require.NoError(t, r.Fail("validation_failed", "", nil, time.Unix(ts+3, 0).UTC()))
		repo := postgres.NewReleaseCheckRepository(db, nil)
		require.NoError(t, repo.Save(ctx, r))
	}

	mkTerminal("prune-a", 100)
	mkTerminal("prune-b", 101)
	mkTerminal("keep-c", 102)

	cutoff := time.Unix(1000, 0).UTC()

	t.Run("deleter is called with correct prefixes", func(t *testing.T) {
		fd := &fakeDeleter{}
		repo := postgres.NewReleaseCheckRepository(db, fd)
		n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		got := fd.prefixes()
		assert.Equal(t, []string{
			"candidate-sql/prune-a/",
			"candidate-sql/prune-b/",
			"code-bundles/prune-a/",
			"code-bundles/prune-b/",
			"tenants/default/topologies/prune-a/contracts.json.gz",
			"tenants/default/topologies/prune-a/topology.json.gz",
			"tenants/default/topologies/prune-b/contracts.json.gz",
			"tenants/default/topologies/prune-b/topology.json.gz",
		}, got, "a rejected check loses its candidate SQL, its code bundle, its contract bundle and its topology artifact")
	})

	// Reseed because the previous sub-test deleted prune-a and prune-b.
	mkTerminal("prune-d", 103)
	mkTerminal("prune-e", 104)

	t.Run("deleter error does not fail the prune", func(t *testing.T) {
		fd := &fakeDeleter{failErr: errors.New("s3 unavailable")}
		repo := postgres.NewReleaseCheckRepository(db, fd)
		n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err, "prune must succeed even when S3 deletion fails")
		assert.Equal(t, 2, n, "both checks must be counted as pruned despite S3 error")

		// Every object of every pruned check was attempted despite every call failing.
		got := fd.prefixes()
		assert.Len(t, got, 8, "deleter must be attempted for every object of every pruned check")
	})

	t.Run("a promoted check keeps its topology artifact", func(t *testing.T) {
		r := releasecheck.NewCandidate("prune-promoted", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(105, 0).UTC())
		require.NoError(t, r.TransitionToParsing(time.Unix(106, 0).UTC()))
		require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(107, 0).UTC()))
		require.NoError(t, r.Promote(time.Unix(108, 0).UTC()))
		require.NoError(t, postgres.NewReleaseCheckRepository(db, nil).Save(ctx, r))

		fd := &fakeDeleter{}
		n, err := postgres.NewReleaseCheckRepository(db, fd).DeleteFinishedBefore(ctx, cutoff, []string{"keep-c"})
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.Equal(t, []string{"candidate-sql/prune-promoted/", "code-bundles/prune-promoted/"}, fd.prefixes(),
			"a promoted check's artifact records a topology that ran in production and is kept")
	})
}

func TestReleaseCheckRepository_StoresTheContractBundleAndTheLibraryVersion(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rCB", "svc-py", "img:1", false, false, "acme/py", "cafebabe",
		release.ManifestKindPython, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rCB")
	require.NoError(t, err)
	assert.Equal(t, release.ContractBundleRef{}, got.CandidateContractBundle(), "no bundle until the parse result arrives")
	assert.Empty(t, got.ContinuoContracts())

	bundle := release.ContractBundleRef{
		URI:    "s3://b/tenants/default/topologies/rCB/contracts.json.gz",
		SHA256: "9b0c4d1e7a52f3860be1d9c4a7f08e35b216d0a94c7e53f1a8b20d6e49c71f3a",
	}
	got.SetCandidateContractBundle(bundle)
	got.SetContinuoContracts("0.2.0")
	require.NoError(t, repo.Save(ctx, got))

	reloaded, err := repo.Get(ctx, "rCB")
	require.NoError(t, err)
	assert.Equal(t, bundle, reloaded.CandidateContractBundle())
	assert.Equal(t, "0.2.0", reloaded.ContinuoContracts())
	assert.Equal(t, release.ManifestKindPython, reloaded.ManifestKind())

	listed, _, err := repo.List(ctx, repository.ListFilter{})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, bundle, listed[0].CandidateContractBundle(), "List reads the same columns as Get")
}

// The parse timings a check records survive a save and a load, through the upsert
// that updates a stored check (the parse result arrives after the check is created)
// and through List, which reads the same columns as Get.
func TestReleaseCheckRepository_SavesAndLoadsTheParseTimings(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rPT", "svc", "img:1", false, false, "acme/demo", "cafebabe",
		release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))

	got, err := repo.Get(ctx, "rPT")
	require.NoError(t, err)
	assert.Equal(t, release.ParseTimings{}, got.ParseTimings(), "no timings until the parse result arrives")

	want := release.NewParseTimings(4180, 1260, map[model.ContractFamily]int{"dbt": 3, "python": 2})
	got.SetParseTimings(want)
	require.NoError(t, repo.Save(ctx, got))

	reloaded, err := repo.Get(ctx, "rPT")
	require.NoError(t, err)
	assert.Equal(t, want, reloaded.ParseTimings())

	listed, _, err := repo.List(ctx, repository.ListFilter{})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, want, listed[0].ParseTimings(), "List reads the same columns as Get")

	var stored struct {
		ParseMs           int64  `db:"parse_ms"`
		ContractsMs       int64  `db:"contracts_ms"`
		ContractsByFamily string `db:"contracts_by_family"`
	}
	require.NoError(t, db.Get(&stored, `SELECT parse_ms, contracts_ms, contracts_by_family::text AS contracts_by_family
		FROM release_checks WHERE check_id = 'rPT'`))
	assert.EqualValues(t, 4180, stored.ParseMs)
	assert.EqualValues(t, 1260, stored.ContractsMs)
	assert.JSONEq(t, `{"dbt": 3, "python": 2}`, stored.ContractsByFamily)
}

// An absent timing is NULL in its column and absent again after the load, and an
// empty count (a release with no contracts) is stored as an empty object, not
// NULL: the presence of each field survives the round trip.
func TestReleaseCheckRepository_TheParseTimingsKeepTheirPresenceThroughStorage(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	cases := map[string]struct {
		timings                              release.ParseTimings
		wantParseMsNull, wantContractsMsNull bool
		wantByFamily                         sql.NullString
	}{
		"none recorded": {
			timings:             release.ParseTimings{},
			wantParseMsNull:     true,
			wantContractsMsNull: true,
		},
		"two zero durations": {
			timings:             release.NewParseTimings(0, 0, nil),
			wantParseMsNull:     true,
			wantContractsMsNull: true,
		},
		"durations without a count": {
			timings: release.NewParseTimings(310, 45, nil),
		},
		"an empty count": {
			timings:      release.NewParseTimings(310, 45, map[model.ContractFamily]int{}),
			wantByFamily: sql.NullString{String: `{}`, Valid: true},
		},
		"one zero duration": {
			timings:      release.NewParseTimings(0, 45, map[model.ContractFamily]int{"python": 1}),
			wantByFamily: sql.NullString{String: `{"python": 1}`, Valid: true},
		},
	}
	i := 0
	for name, tc := range cases {
		i++
		id := fmt.Sprintf("rPR%d", i)
		t.Run(name, func(t *testing.T) {
			r := releasecheck.NewCandidate(id, "svc", "img:1", false, false, "acme/demo", "cafebabe",
				release.ManifestKindDbt, time.Unix(100, 0).UTC())
			r.SetParseTimings(tc.timings)
			created, err := repo.Create(ctx, r)
			require.NoError(t, err)
			require.True(t, created)

			var stored struct {
				ParseMs           sql.NullInt64  `db:"parse_ms"`
				ContractsMs       sql.NullInt64  `db:"contracts_ms"`
				ContractsByFamily sql.NullString `db:"contracts_by_family"`
			}
			require.NoError(t, db.Get(&stored, `SELECT parse_ms, contracts_ms, contracts_by_family::text AS contracts_by_family
				FROM release_checks WHERE check_id = $1`, id))
			assert.Equal(t, tc.wantParseMsNull, !stored.ParseMs.Valid, "parse_ms is NULL")
			assert.Equal(t, tc.wantContractsMsNull, !stored.ContractsMs.Valid, "contracts_ms is NULL")
			assert.Equal(t, tc.wantByFamily.Valid, stored.ContractsByFamily.Valid, "contracts_by_family is NULL")
			if tc.wantByFamily.Valid {
				assert.JSONEq(t, tc.wantByFamily.String, stored.ContractsByFamily.String)
			}

			got, err := repo.Get(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, tc.timings, got.ParseTimings())
		})
	}
}

// A stored row that holds one duration without the other is read back with
// neither, so a loaded check keeps the rule that the two are set together; the
// count beside them is kept.
func TestReleaseCheckRepository_ARowWithOnlyOneDurationLoadsWithNeither(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	for i, column := range []string{"parse_ms", "contracts_ms"} {
		id := fmt.Sprintf("rPO%d", i)
		t.Run(column+" NULL", func(t *testing.T) {
			r := releasecheck.NewCandidate(id, "svc", "img:1", false, false, "acme/demo", "cafebabe",
				release.ManifestKindDbt, time.Unix(100, 0).UTC())
			r.SetParseTimings(release.NewParseTimings(310, 45, map[model.ContractFamily]int{"dbt": 2}))
			created, err := repo.Create(ctx, r)
			require.NoError(t, err)
			require.True(t, created)
			_, err = db.Exec(`UPDATE release_checks SET `+column+` = NULL WHERE check_id = $1`, id)
			require.NoError(t, err)

			got, err := repo.Get(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, release.ParseTimings{ContractsByFamily: map[model.ContractFamily]int{"dbt": 2}}, got.ParseTimings())
		})
	}
}

// The manifest_kind CHECK of both tables that hold a kind admits python
// and refuses an unknown value, and each table keeps exactly one CHECK on
// the column.
func TestReleaseCheckRepository_TheMigrationAcceptsPython(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	pointerRepo := postgres.NewProductionPointerRepository(db)
	ctx := context.Background()
	now := time.Unix(100, 0).UTC()

	require.NoError(t, repo.Save(ctx, releasecheck.NewCandidate("rPy", "svc-py", "img:1", false, false, "acme/py", "cafebabe",
		release.ManifestKindPython, now)))
	require.NoError(t, pointerRepo.Upsert(ctx, release.NewProductionPointer("svc-py", "rPy", "s3://b/svc-py/rPy/odcs-bundle.json", "img:1",
		release.ManifestKindPython, now)))
	pointer, err := pointerRepo.Get(ctx, "svc-py")
	require.NoError(t, err)
	require.NotNil(t, pointer)
	assert.Equal(t, release.ManifestKindPython, pointer.ManifestKind())

	err = repo.Save(ctx, releasecheck.NewCandidate("rOther", "svc-x", "img:1", false, false, "acme/x", "cafebabe",
		release.ManifestKind("other"), now))
	require.Error(t, err, "release_checks refuses a kind outside dbt, python and ontology")
	err = pointerRepo.Upsert(ctx, release.NewProductionPointer("svc-x", "rOther", "s3://b/svc-x/x", "img:1", release.ManifestKind("other"), now))
	require.Error(t, err, "production_pointers refuses a kind outside dbt, python and ontology")

	for _, table := range []string{"release_checks", "production_pointers"} {
		var n int
		require.NoError(t, db.GetContext(ctx, &n, `
			SELECT count(*) FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid
			WHERE con.contype = 'c' AND rel.relname = $1 AND pg_get_constraintdef(con.oid) LIKE '%manifest_kind%'`, table))
		assert.Equal(t, 1, n, "%s keeps exactly one CHECK on manifest_kind", table)
	}
}

// A check that was never promoted loses its contract bundle with its other
// artifacts; a promoted check keeps it, since its contracts are the ones that ran
// in production.
func TestDeleteFinishedBefore_DeletesTheContractBundleOfANeverPromotedCheck(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	cutoff := time.Unix(1000, 0).UTC()

	rejected := releasecheck.NewCandidate("cb-rejected", "svc-py", "t", false, false, "acme/py", "deadbeef", release.ManifestKindPython, time.Unix(100, 0).UTC())
	require.NoError(t, rejected.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, rejected.Fail("invalid_contract", "", nil, time.Unix(102, 0).UTC()))
	promoted := releasecheck.NewCandidate("cb-promoted", "svc-py", "t", false, false, "acme/py", "deadbeef", release.ManifestKindPython, time.Unix(100, 0).UTC())
	require.NoError(t, promoted.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, promoted.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, promoted.Promote(time.Unix(103, 0).UTC()))
	for _, r := range []*releasecheck.Check{rejected, promoted} {
		require.NoError(t, postgres.NewReleaseCheckRepository(db, nil).Save(ctx, r))
	}

	fd := &fakeDeleter{}
	n, err := postgres.NewReleaseCheckRepository(db, fd).DeleteFinishedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	got := fd.prefixes()
	assert.Contains(t, got, "tenants/default/topologies/cb-rejected/contracts.json.gz")
	assert.NotContains(t, got, "tenants/default/topologies/cb-promoted/contracts.json.gz")
}

// TestReleaseCheckRepository_FixKindRoundTrips verifies that a check's kind
// (candidate or fix), a fix's source-overlay URI and the round and number of
// the try it proves survive a Save/Get round trip through check_kind,
// source_overlay_uri, remediation_round and try.
func TestReleaseCheckRepository_FixKindRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	fix := releasecheck.NewFix("r-verify", "svc-a", "img-1", "rel-orig", 2, 3,
		"s3://bucket/svc-a/r-verify/source-overlay.tar.gz", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, fix))
	candidate := releasecheck.NewCandidate("r-plain-candidate", "svc-a", "img-1", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, candidate))

	gotFix, err := repo.Get(ctx, "r-verify")
	require.NoError(t, err)
	require.NotNil(t, gotFix)
	assert.Equal(t, releasecheck.KindFix, gotFix.Kind())
	assert.Equal(t, "s3://bucket/svc-a/r-verify/source-overlay.tar.gz", gotFix.SourceOverlayURI())
	assert.Equal(t, "rel-orig", gotFix.VerifiesReleaseID())
	assert.Equal(t, 2, gotFix.RemediationRound(), "a fix's round round-trips through remediation_round")
	assert.Equal(t, 3, gotFix.Try())

	gotCandidate, err := repo.Get(ctx, "r-plain-candidate")
	require.NoError(t, err)
	require.NotNil(t, gotCandidate)
	assert.Equal(t, releasecheck.KindCandidate, gotCandidate.Kind())
	assert.Equal(t, "", gotCandidate.SourceOverlayURI())
}

// TestReleaseCheckRepository_DeleteFinishedBeforePrunesPassedFix verifies
// that a fix's release check resolved into the terminal 'passed' status is
// pruned by DeleteFinishedBefore just like any other terminal check — its rows
// and S3 prefixes must not accumulate forever.
func TestReleaseCheckRepository_DeleteFinishedBeforePrunesPassedFix(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewFix("r-verify-passed", "svc", "t", "rel-orig", 1, 1, "", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, r.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, r.TransitionToValidating(release.TopologyRef{}, nil, time.Unix(102, 0).UTC()))
	require.NoError(t, r.Pass(time.Unix(103, 0).UTC()))
	require.NoError(t, repo.Save(ctx, r))

	cutoff := time.Unix(1000, 0).UTC()
	n, err := repo.DeleteFinishedBefore(ctx, cutoff, []string{})
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a fix's passed release check older than cutoff must be pruned")

	gone, _ := repo.Get(ctx, "r-verify-passed")
	assert.Nil(t, gone, "a fix's passed release check must be deleted by DeleteFinishedBefore")
}

func TestReleaseCheckRepository_QueueInterleavesBothKindsByCreatedAt(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Save(ctx, releasecheck.NewCandidate("rel-a", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, t0)))
	require.NoError(t, repo.Save(ctx, releasecheck.NewFix("verify-rel-a-core-r1-t1", "core", "img", "rel-a", 1, 1, "", release.ManifestKindDbt, t0.Add(time.Second))))
	require.NoError(t, repo.Save(ctx, releasecheck.NewCandidate("rel-b", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, t0.Add(2*time.Second))))

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
	require.Equal(t, "verify-rel-a-core-r1-t1", next.ID(), "a fix's release check takes its turn in created_at order, ahead of a later candidate")
}

func TestReleaseCheckRepository_ListFiltersByKindAndVerifiedRelease(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Save(ctx, releasecheck.NewCandidate("rel-a", "core", "img", false, false, "org/r", "sha", release.ManifestKindDbt, t0)))
	require.NoError(t, repo.Save(ctx, releasecheck.NewFix("verify-rel-a-core-r1-t1", "core", "img", "rel-a", 1, 1, "", release.ManifestKindDbt, t0.Add(time.Second))))
	require.NoError(t, repo.Save(ctx, releasecheck.NewFix("verify-rel-z-core-r1-t1", "core", "img", "rel-z", 1, 1, "", release.ManifestKindDbt, t0.Add(2*time.Second))))

	cand := releasecheck.KindCandidate
	items, _, err := repo.List(ctx, repository.ListFilter{Kind: &cand})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "rel-a", items[0].ID())

	verifies := "rel-a"
	items, _, err = repo.List(ctx, repository.ListFilter{VerifiesReleaseID: &verifies})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "verify-rel-a-core-r1-t1", items[0].ID())
}

// While maintenance holds candidate releases, the queue activates only
// the release checks of fixes: NextQueuedOfKind finds the oldest received check of one
// kind, past any older check of the other kind.
func TestReleaseCheckRepository_NextQueuedOfKind(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	cand := releasecheck.NewCandidate("rCAND", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	verify := releasecheck.NewFix("rVERIFY", "svc", "t", "rCAND", 1, 1, "", release.ManifestKindPython, time.Unix(200, 0).UTC())
	require.NoError(t, repo.Save(ctx, cand))
	require.NoError(t, repo.Save(ctx, verify))

	v, err := repo.NextQueuedOfKind(ctx, releasecheck.KindFix)
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.Equal(t, "rVERIFY", v.ID())

	c, err := repo.NextQueuedOfKind(ctx, releasecheck.KindCandidate)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "rCAND", c.ID())

	require.NoError(t, v.TransitionToParsing(time.Unix(300, 0).UTC()))
	require.NoError(t, repo.Save(ctx, v))
	none, err := repo.NextQueuedOfKind(ctx, releasecheck.KindFix)
	require.NoError(t, err)
	assert.Nil(t, none, "an activated fix's release check is no longer queued")
}

// TestReleaseCheckRepository_RoundTripsCandidateTopologyRef pins the three columns that
// locate a check's topology artifact: a reference set after the check was first
// inserted is written by the upsert, and a check without one keeps NULL columns,
// so "has no artifact" is the single predicate candidate_topology_uri IS NULL.
func TestReleaseCheckRepository_RoundTripsCandidateTopologyRef(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rRef", "svc", "t", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))
	var uriIsNull bool
	require.NoError(t, db.Get(&uriIsNull, `SELECT candidate_topology_uri IS NULL FROM release_checks WHERE check_id = 'rRef'`))
	assert.True(t, uriIsNull, "a check without an artifact stores NULL, not an empty string")
	got, err := repo.Get(ctx, "rRef")
	require.NoError(t, err)
	assert.True(t, got.CandidateTopologyRef().IsZero())

	ref := release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/rRef/topology.json.gz", SHA256: "9f86d081884c7d65", NodeCount: 42}
	require.NoError(t, got.TransitionToParsing(time.Unix(101, 0).UTC()))
	require.NoError(t, got.TransitionToValidating(ref, []string{"n"}, time.Unix(102, 0).UTC()))
	require.NoError(t, repo.Save(ctx, got))

	reread, err := repo.Get(ctx, "rRef")
	require.NoError(t, err)
	require.NotNil(t, reread)
	assert.Equal(t, ref, reread.CandidateTopologyRef(), "the upsert must write the reference recorded after the first insert")
}

func TestReleaseCheckRepository_RebaselineRoundTrips(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()
	inserted, err := repo.Create(ctx, releasecheck.NewCandidate("r-rebase", "svc-a", "sha-a", true, true, "acme/demo", "deadbeef",
		release.ManifestKindDbt, time.Unix(100, 0).UTC()))
	require.NoError(t, err)
	require.True(t, inserted)
	got, err := repo.Get(ctx, "r-rebase")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.IsBootstrap())
	assert.True(t, got.IsRebaseline())
}

func TestReleaseCheckRepository_RefusesRebaselineWithoutBootstrap(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	r := releasecheck.Rehydrate(releasecheck.RehydrateInput{ID: "r-bad", Kind: releasecheck.KindCandidate, Status: releasecheck.StatusReceived,
		ChangedProject: "svc-a", ImageTags: map[string]string{"svc-a": "sha-a"}, ManifestKind: release.ManifestKindDbt,
		Repo: "acme/demo", CommitSHA: "deadbeef", Rebaseline: true, CreatedAt: time.Unix(100, 0).UTC()})
	_, err := repo.Create(context.Background(), r)
	require.ErrorContains(t, err, "release_checks_rebaseline_needs_bootstrap")
}

// TestReleaseCheckRepository_StoresAndReadsTheNotices verifies a check's seed reload
// notices round-trip through release_checks.notices: a new check reads
// back none, the notices set at parse time are written by the upsert, and
// warehouse findings applied later replace them on the next save.
func TestReleaseCheckRepository_StoresAndReadsTheNotices(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewReleaseCheckRepository(db, nil)
	ctx := context.Background()

	r := releasecheck.NewCandidate("rNOTICES", "finance", "img-1", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, time.Unix(100, 0).UTC())
	require.NoError(t, repo.Save(ctx, r))
	got, err := repo.Get(ctx, "rNOTICES")
	require.NoError(t, err)
	assert.Empty(t, got.Notices())

	notice := release.Notice{
		Kind: "seed_reload", NodeID: "analytics.seed_fx_rates_eur", Project: "finance",
		EngineFamily: warehouse.SchemaBound, Change: seedreload.ChangeColumns,
		Columns:           release.ColumnChanges{Added: []string{"source"}, Removed: []string{}, Retyped: []string{}},
		Load:              seedreload.Load{Operation: model.OperationRun, DropsTable: true, Reason: "columns changed; +full_refresh: true re-creates the table on every load"},
		Rebuilds:          []seedreload.Rebuild{{Key: "analytics.fx_rates_current", Materialized: "view", Schedule: "hourly"}},
		RefreshOnSchedule: []seedreload.Boundary{{Key: "analytics.fx_transactions_eur", Materialized: "table", Schedule: "daily"}},
		Warehouse:         release.WarehouseFindings{OutsideContinuo: []release.OutsideObject{}},
		Prediction:        "reloads",
	}
	got.SetNotices([]release.Notice{notice})
	require.NoError(t, repo.Save(ctx, got))
	got, err = repo.Get(ctx, "rNOTICES")
	require.NoError(t, err)
	assert.Equal(t, []release.Notice{notice}, got.Notices())

	require.True(t, got.ApplyWarehouseFindings("analytics.seed_fx_rates_eur", seedreload.Findings{Checked: true, Completeness: "exact",
		Dependents: []seedreload.Dependent{{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1}}}))
	require.NoError(t, repo.Save(ctx, got))
	got, err = repo.Get(ctx, "rNOTICES")
	require.NoError(t, err)
	require.Len(t, got.Notices(), 1)
	assert.Equal(t, "stops", got.Notices()[0].Prediction)
	assert.Equal(t, []release.OutsideObject{{Object: "bi.fx_rates_report", Kind: "view", Owner: "bi"}}, got.Notices()[0].Warehouse.OutsideContinuo)
}
