package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With no tenant there is no export to be late: the freshness is unknown.
func TestFreshness_NoTenantIsUnknown(t *testing.T) {
	f := NewFreshness(&fakeTenants{}, newFakeStore())

	at, known, err := f.OldestNewestExport(context.Background())

	require.NoError(t, err)
	assert.False(t, known)
	assert.True(t, at.IsZero())
}

// A tenant with no export holds the freshness back to the zero time, whatever
// the other tenants stored.
func TestFreshness_ATenantWithNoExportIsTheZeroTime(t *testing.T) {
	store := newFakeStore()
	store.put("a", time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC), 1)
	f := NewFreshness(&fakeTenants{tenants: []string{"a", "b"}}, store)

	at, known, err := f.OldestNewestExport(context.Background())

	require.NoError(t, err)
	assert.True(t, known)
	assert.True(t, at.IsZero(), "b has no export")
}

// The freshness is the older of the tenants' newest exports, not the newest
// export of any of them.
func TestFreshness_IsTheOlderOfTheTenantsNewestExports(t *testing.T) {
	store := newFakeStore()
	day := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)
	store.put("a", day.Add(-24*time.Hour), 1)
	aNewest := day
	store.put("a", aNewest, 1)
	store.put("b", day.Add(time.Hour), 1)
	f := NewFreshness(&fakeTenants{tenants: []string{"a", "b"}}, store)

	at, known, err := f.OldestNewestExport(context.Background())

	require.NoError(t, err)
	assert.True(t, known)
	assert.Equal(t, aNewest, at.UTC())
}

// A tenant list or a store listing that fails is the caller's error: no
// freshness is reported.
func TestFreshness_ReturnsTheReadErrors(t *testing.T) {
	tenantsDown := errors.New("neo4j: connection refused")
	_, _, err := NewFreshness(&fakeTenants{err: tenantsDown}, newFakeStore()).OldestNewestExport(context.Background())
	require.ErrorIs(t, err, tenantsDown)

	store := newFakeStore()
	store.listErr = errors.New("SlowDown")
	_, _, err = NewFreshness(&fakeTenants{tenants: []string{"a"}}, store).OldestNewestExport(context.Background())
	require.ErrorIs(t, err, store.listErr)
}
