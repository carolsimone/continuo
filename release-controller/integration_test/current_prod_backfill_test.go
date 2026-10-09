//go:build integration

package integration_test

import (
	"context"
	"testing"

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
// pod at once) both run the step: the release-queue lock lets one write and
// re-announce, and the other then finds nothing left to do.
func TestIntegration_BackfillCurrentProdArtifact_TwoStartsAtOnce(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLive', '[{"unique_id":"a","service_name":"svc","node_type":"dbt-model","upstream_unique_ids":[]}]'::jsonb, now())`)
	require.NoError(t, err)

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- handlers.BackfillCurrentProdArtifact(ctx, deps) }()
	}
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	var seq int64
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM current_prod WHERE id = 1`))
	assert.Equal(t, int64(1), seq, "one re-announcement took one seq")
	var announced int
	require.NoError(t, db.Get(&announced,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1`, streams.ReleasePromotedV2))
	assert.Equal(t, 1, announced)
}
