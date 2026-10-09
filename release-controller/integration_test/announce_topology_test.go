//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// On a cold install — no current_prod row — announcements still work, take
// increasing seqs, and never create current_prod.
func TestIntegration_AnnounceTopology_WithoutCurrentProd(t *testing.T) {
	_, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	topo := release.Topology{{UniqueID: "e2e.a", ServiceName: "e2e", NodeType: "dbt-model", UpstreamUniqueIDs: []string{}}}

	first, err := handlers.AnnounceTopology(ctx, deps, "e2e-seed-1", topo)
	require.NoError(t, err)
	second, err := handlers.AnnounceTopology(ctx, deps, "e2e-seed-2", topo)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.PromotionSeq)
	assert.Equal(t, int64(2), second.PromotionSeq)

	var rows int
	require.NoError(t, db.Get(&rows, `SELECT count(*) FROM current_prod`))
	assert.Equal(t, 0, rows)
	require.NoError(t, db.Get(&rows,
		`SELECT count(*) FROM release_controller_outbox WHERE stream_name = $1 AND event_type = 'release_promoted_v2'`,
		streams.ReleasePromotedV2))
	assert.Equal(t, 2, rows)

	_, err = handlers.ReannounceCurrentProd(ctx, deps)
	require.ErrorIs(t, err, handlers.ErrNoCurrentProd)
}
