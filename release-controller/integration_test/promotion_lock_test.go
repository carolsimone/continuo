//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lastSeq reads the install's promotion sequence counter.
func lastSeq(t *testing.T, db interface {
	Get(dest interface{}, query string, args ...interface{}) error
}) int64 {
	t.Helper()
	var seq int64
	require.NoError(t, db.Get(&seq, `SELECT last_seq FROM promotion_sequence WHERE id = 1`))
	return seq
}

// A real promotion reads current_prod, takes the next promotion seq and upserts
// current_prod. It must do all three under the release-queue lock so a
// concurrent announce cannot read current_prod, take a higher seq and clobber
// the promotion. AnnounceTopology therefore blocks while another caller holds
// the lock, and takes its seq only once the lock is free.
func TestIntegration_AnnounceTopology_SerializesUnderTheReleaseQueueLock(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	topo := release.Topology{{UniqueID: "a", ServiceName: "svc", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}}}

	holder := deps.NewUoW()
	require.NoError(t, holder.Begin(ctx))
	defer holder.Rollback() //nolint:errcheck
	require.NoError(t, holder.LockReleaseQueue(ctx))

	done := make(chan error, 1)
	go func() {
		_, err := handlers.AnnounceTopology(ctx, deps, "bench-1", topo)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("AnnounceTopology returned (err=%v) while another caller held the release-queue lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	assert.Equal(t, int64(0), lastSeq(t, db), "no seq is taken while the lock is held elsewhere")

	require.NoError(t, holder.Rollback())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("AnnounceTopology did not finish after the release-queue lock was released")
	}
	assert.Equal(t, int64(1), lastSeq(t, db), "the announcement took exactly one seq once the lock was free")
}

// ReannounceCurrentProd reads current_prod, takes the next seq and upserts
// current_prod, and must serialise with a concurrent promotion the same way.
func TestIntegration_ReannounceCurrentProd_SerializesUnderTheReleaseQueueLock(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_uri, topology_sha256, node_count, promotion_seq, updated_at)
		VALUES (1, 'rLive', 's3://test-bucket/tenants/default/topologies/rLive/topology.json.gz', 'deadbeef', 1, 0, now())`)
	require.NoError(t, err)

	holder := deps.NewUoW()
	require.NoError(t, holder.Begin(ctx))
	defer holder.Rollback() //nolint:errcheck
	require.NoError(t, holder.LockReleaseQueue(ctx))

	done := make(chan error, 1)
	go func() {
		_, err := handlers.ReannounceCurrentProd(ctx, deps)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("ReannounceCurrentProd returned (err=%v) while another caller held the release-queue lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	assert.Equal(t, int64(0), lastSeq(t, db), "no seq is taken while the lock is held elsewhere")

	require.NoError(t, holder.Rollback())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("ReannounceCurrentProd did not finish after the release-queue lock was released")
	}
	assert.Equal(t, int64(1), lastSeq(t, db), "the re-announcement took exactly one seq once the lock was free")
}

// The validation-passed promotion (promoteToProduction) reads current_prod,
// takes the next seq and moves current_prod. Held behind the release-queue
// lock, HandleValidationResult must not promote: no seq is taken and the run
// stays Validating until the lock is free, after which it promotes exactly once.
func TestIntegration_PromoteToProduction_SerializesUnderTheReleaseQueueLock(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()

	// Drive a candidate to Validating on a fresh install.
	require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
		Service: "service-1", ReleaseID: "rA", ImageTag: "sha-rA", Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx, deps))
	require.NoError(t, handlers.HandleCompileResult(ctx, deps, handlers.HandleCompileResultInput{ReleaseID: "rA", Status: "ok"}))
	require.NoError(t, handlers.HandleParsedManifest(ctx, deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA", Status: "ok",
		TopologyRef: putTopology(t, deps, "rA", release.Topology{
			{UniqueID: "a", ServiceName: "service-1", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}},
			{UniqueID: "b", ServiceName: "service-1", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"a"}},
		}),
	}))
	for _, n := range []handlers.NodeValidationResultInput{
		{ReleaseID: "rA", Stage: "validation", NodeID: "a", Status: "ok"},
		{ReleaseID: "rA", Stage: "validation", NodeID: "b", Status: "ok"},
	} {
		require.NoError(t, handlers.HandleNodeValidationResult(ctx, deps, n))
	}
	r, err := deps.NewUoW().RunRepo().Get(ctx, "rA")
	require.NoError(t, err)
	require.Equal(t, pipeline.StatusValidating, r.Status())
	seqBefore := lastSeq(t, db)

	holder := deps.NewUoW()
	require.NoError(t, holder.Begin(ctx))
	defer holder.Rollback() //nolint:errcheck
	require.NoError(t, holder.LockReleaseQueue(ctx))

	done := make(chan error, 1)
	go func() {
		done <- handlers.HandleValidationResult(ctx, deps, handlers.HandleValidationResultInput{ReleaseID: "rA", AggregateStatus: "ok"})
	}()

	select {
	case err := <-done:
		t.Fatalf("HandleValidationResult promoted (err=%v) while another caller held the release-queue lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	assert.Equal(t, seqBefore, lastSeq(t, db), "no promotion seq is taken while the lock is held elsewhere")
	r, err = deps.NewUoW().RunRepo().Get(ctx, "rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusValidating, r.Status(), "the run does not promote while the lock is held elsewhere")

	require.NoError(t, holder.Rollback())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("HandleValidationResult did not finish after the release-queue lock was released")
	}

	r, err = deps.NewUoW().RunRepo().Get(ctx, "rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
	assert.Equal(t, seqBefore+1, lastSeq(t, db), "the promotion took exactly one seq once the lock was free")
	var promoted int
	require.NoError(t, db.Get(&promoted,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleasePromotedV2))
	assert.Equal(t, 1, promoted)
}
