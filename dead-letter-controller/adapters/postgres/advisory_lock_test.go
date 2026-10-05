//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdvisoryLock_SecondHolderIsRefusedUntilRelease(t *testing.T) {
	db := testDB(t)
	a, b := NewAdvisoryLock(db), NewAdvisoryLock(db)
	release, ok, err := a.TryAcquire(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = b.TryAcquire(context.Background())
	require.NoError(t, err)
	assert.False(t, ok, "a second holder must be refused")
	release()
	release2, ok, err := b.TryAcquire(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	release2()
}
