//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/repository"
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
