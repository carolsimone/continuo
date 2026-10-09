//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Against the Flyway-built schema: a legacy current_prod row gets its artifact
// reference, its snapshot cleared, seq 1 and one release.promoted:v2 row; a
// second start changes nothing.
func TestIntegration_BackfillCurrentProdArtifact(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLive', '[{"unique_id":"a","service_name":"svc","node_type":"dbt-model","upstream_unique_ids":[]}]'::jsonb, now())`)
	require.NoError(t, err)

	require.NoError(t, handlers.BackfillCurrentProdArtifact(ctx, deps))
	require.NoError(t, handlers.BackfillCurrentProdArtifact(ctx, deps))

	var row struct {
		URI          *string `db:"topology_uri"`
		SnapshotNull bool    `db:"snapshot_null"`
		Seq          int64   `db:"promotion_seq"`
		NodeCount    *int    `db:"node_count"`
	}
	require.NoError(t, db.Get(&row,
		`SELECT topology_uri, topology_snapshot IS NULL AS snapshot_null, promotion_seq, node_count FROM current_prod WHERE id = 1`))
	require.NotNil(t, row.URI)
	assert.True(t, row.SnapshotNull)
	assert.Equal(t, int64(1), row.Seq)
	require.NotNil(t, row.NodeCount)
	assert.Equal(t, 1, *row.NodeCount)

	var announced int
	require.NoError(t, db.Get(&announced,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleasePromotedV2))
	assert.Equal(t, 1, announced)
}

// Two replicas starting together (a rolling update runs the old and the new
// pod at once) both run the step. The release-queue lock is what lets one
// write and re-announce while the other waits and then finds nothing left to
// do. A holder transaction keeps the lock taken, so the test does not depend
// on goroutine timing: the step must block behind the holder, and once the
// holder lets go it backfills and re-announces exactly once.
func TestIntegration_BackfillCurrentProdArtifact_TwoStartsAtOnce(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLive', '[{"unique_id":"a","service_name":"svc","node_type":"dbt-model","upstream_unique_ids":[]}]'::jsonb, now())`)
	require.NoError(t, err)

	holder := deps.NewUoW()
	require.NoError(t, holder.Begin(ctx))
	defer holder.Rollback() //nolint:errcheck
	require.NoError(t, holder.LockReleaseQueue(ctx))

	done := make(chan error, 1)
	go func() { done <- handlers.BackfillCurrentProdArtifact(ctx, deps) }()

	select {
	case err := <-done:
		t.Fatalf("the backfill returned (err=%v) while another start held the release-queue lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	var blockedSeq int64
	require.NoError(t, db.Get(&blockedSeq, `SELECT promotion_seq FROM current_prod WHERE id = 1`))
	assert.Equal(t, int64(0), blockedSeq, "no seq is taken while the lock is held elsewhere")
	var blockedAnnounced int
	require.NoError(t, db.Get(&blockedAnnounced,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleasePromotedV2))
	assert.Equal(t, 0, blockedAnnounced)

	require.NoError(t, holder.Rollback())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the backfill did not finish after the release-queue lock was released")
	}

	// The second start finds the artifact already recorded and does nothing.
	require.NoError(t, handlers.BackfillCurrentProdArtifact(ctx, deps))

	var row struct {
		URI *string `db:"topology_uri"`
		Seq int64   `db:"promotion_seq"`
	}
	require.NoError(t, db.Get(&row, `SELECT topology_uri, promotion_seq FROM current_prod WHERE id = 1`))
	require.NotNil(t, row.URI)
	assert.Equal(t, int64(1), row.Seq, "one re-announcement took one seq")
	var announced int
	require.NoError(t, db.Get(&announced,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleasePromotedV2))
	assert.Equal(t, 1, announced)
}
