package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/topology"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/topologyretention"
	"github.com/carolsimone/continuo/state/service/uow"
)

// TestTopologyRetention_PrunesOnlyOldUnreferencedReleases runs the pruner the
// retention sweeper calls: a release loaded before the cutoff is deleted with
// its nodes, while the live release, a release a running run names and a
// recent release are kept.
func TestTopologyRetention_PrunesOnlyOldUnreferencedReleases(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, execRepo, catalogRepo, svcports.SystemClock{}, logger)
	}
	const tenant = "t-retention"

	for _, releaseID := range []string{"rel-old", "rel-live", "rel-referenced", "rel-recent"} {
		rel, err := topology.NewRelease(tenant, releaseID, 1, "s3://continuo/k/"+releaseID, "sha-"+releaseID,
			[]topology.NodeSpec{promotedNode("raw", "orders", "dbt-seed")})
		require.NoError(t, err)
		u := factory()
		require.NoError(t, u.Begin(ctx))
		require.NoError(t, u.Topology().LockTopology(ctx))
		_, err = u.Topology().SaveRelease(ctx, rel)
		require.NoError(t, err)
		require.NoError(t, u.Commit())
	}
	_, err := db.Exec(`UPDATE topology_releases SET loaded_at = now() - interval '30 days'
		WHERE tenant_id = $1 AND release_id IN ('rel-old', 'rel-live', 'rel-referenced')`, tenant)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, 'rel-live', 1, now())`, tenant)
	require.NoError(t, err)
	_, err = db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status, kind, release_id)
		VALUES ($1, 'promote-seed-retained', 'running', now(), 'completed', 'promote_seed', 'rel-referenced')`, uuid.New())
	require.NoError(t, err)

	pruned, err := topologyretention.NewPruner(factory, logger).Prune(ctx, 7*24*time.Hour, 1000)
	require.NoError(t, err)

	assert.Equal(t, int64(1), pruned)
	var kept []string
	require.NoError(t, db.Select(&kept,
		`SELECT release_id FROM topology_releases WHERE tenant_id = $1 ORDER BY release_id COLLATE "C"`, tenant))
	assert.Equal(t, []string{"rel-live", "rel-recent", "rel-referenced"}, kept)
	assert.Zero(t, countOf(t, db, `SELECT count(*) FROM topology_nodes WHERE tenant_id = $1 AND release_id = 'rel-old'`, tenant),
		"the pruned release's nodes go with it")
}
