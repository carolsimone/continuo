//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/ports"
)

var _ repository.LiveScheduleModels = (*postgres.LiveScheduleModelRepository)(nil)

// The live release of liveScheduleRelease holds four models of "daily", two of
// "seeds", one of "other" and one of no schedule; the release that is not live
// holds a fifth "daily" model.
func TestLiveScheduleModels_CountTheLiveReleasesModelsPerScheduleName(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)
	repo := postgres.NewLiveScheduleModelRepository(db)

	counts, err := repo.CountByScheduleName(ctx, tenant)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"daily": 4, "seeds": 2, "other": 1}, counts,
		"the model of no schedule and the release that is not live are not counted")

	for name, want := range map[string]int{"daily": 4, "seeds": 2, "other": 1, "weekly": 0, "": 0} {
		got, err := repo.CountOf(ctx, tenant, name)
		require.NoError(t, err, name)
		assert.Equal(t, want, got, "models of %q", name)
	}
}

// Each tenant's counts come from its own live release, and a tenant with no
// live release has no models.
func TestLiveScheduleModels_AreOfTheTenantsLiveRelease(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant, _ := liveScheduleRelease(t, db)
	other := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, other, "rel-2", 9, "sha-x",
		graphSpec("theirs", "dbt-model", "daily"), graphSpec("theirs_2", "dbt-model", "weekly")))
	require.NoError(t, err)
	makeTenantLive(t, db, other, "rel-2", 9)
	stored := newTopoTenant(t, db)
	_, err = saveTopoRelease(t, db, topoRelease(t, stored, "rel-1", 1, "sha-y", graphSpec("a", "dbt-model", "daily")))
	require.NoError(t, err)
	repo := postgres.NewLiveScheduleModelRepository(db)

	theirs, err := repo.CountByScheduleName(ctx, other)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"daily": 1, "weekly": 1}, theirs)
	weekly, err := repo.CountOf(ctx, tenant, "weekly")
	require.NoError(t, err)
	assert.Zero(t, weekly, "another tenant's weekly is not this tenant's")
	daily, err := repo.CountOf(ctx, other, "daily")
	require.NoError(t, err)
	assert.Equal(t, 1, daily)

	none, err := repo.CountByScheduleName(ctx, stored)
	require.NoError(t, err)
	assert.Empty(t, none, "a release that was stored but never made live has no models")
	zero, err := repo.CountOf(ctx, stored, "daily")
	require.NoError(t, err)
	assert.Zero(t, zero)
}

// The unit of work counts the models in its transaction: a release the
// transaction made live, not yet committed, is the one it counts, while the
// pool still sees none.
func TestLiveScheduleModels_TheUnitOfWorkCountsInItsTransaction(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := newTopoTenant(t, db)
	u := postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger(), nil)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	_, err := u.Topology().SaveRelease(ctx, topoRelease(t, tenant, "rel-1", 1, "sha-1", graphSpec("orders", "dbt-model", "daily")))
	require.NoError(t, err)
	require.NoError(t, u.Topology().SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: "rel-1", PromotionSeq: 1}, time.Now()))

	inTx, err := u.LiveScheduleModels().CountOf(ctx, tenant, "daily")
	require.NoError(t, err)
	assert.Equal(t, 1, inTx, "the transaction counts the release it made live")
	onPool, err := postgres.NewLiveScheduleModelRepository(db).CountOf(ctx, tenant, "daily")
	require.NoError(t, err)
	assert.Zero(t, onPool, "the pool does not see an uncommitted release")
}

// ListSchedules counts a schedule's models and the topology tiles count its
// nodes, both of the live release: the two counts are one, for every schedule
// name, and neither counts the model of no schedule nor a release that is not
// live. A tenant with no live release has neither.
func TestLiveScheduleModels_AgreeWithTheScheduleGraphSummaries(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	models := postgres.NewLiveScheduleModelRepository(db)
	graphs := postgres.NewScheduleGraphRepository(db)

	agree := func(tenant string) map[string]int {
		counts, err := models.CountByScheduleName(ctx, tenant)
		require.NoError(t, err)
		summaries, err := graphs.Summaries(ctx, tenant)
		require.NoError(t, err)
		nodes := make(map[string]int, len(summaries))
		for _, s := range summaries {
			nodes[s.ScheduleName] = s.NodeCount
		}
		assert.Equal(t, counts, nodes, "the models counted for a schedule are its nodes counted for its tile")
		return counts
	}

	tenant, _ := liveScheduleRelease(t, db)
	assert.Equal(t, map[string]int{"daily": 4, "seeds": 2, "other": 1}, agree(tenant))

	stored := newTopoTenant(t, db)
	_, err := saveTopoRelease(t, db, topoRelease(t, stored, "rel-1", 1, "sha-y", graphSpec("a", "dbt-model", "daily")))
	require.NoError(t, err)
	assert.Empty(t, agree(stored), "a release that was stored but never made live has no models")
}
