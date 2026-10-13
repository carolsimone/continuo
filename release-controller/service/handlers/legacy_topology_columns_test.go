package handlers_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeLegacyColumns reports the columns it was given, or fails with err.
type fakeLegacyColumns struct {
	present []string
	err     error
}

func (f fakeLegacyColumns) Present(context.Context) ([]string, error) { return f.present, f.err }

var _ ports.LegacyTopologyColumns = fakeLegacyColumns{}

func TestCheckLegacyTopologyColumnsDropped_PassesOnceTheColumnsAreGone(t *testing.T) {
	require.NoError(t, handlers.CheckLegacyTopologyColumnsDropped(context.Background(), fakeLegacyColumns{present: []string{}}))
}

// A database that still has a column is refused: the error names the columns
// that remain, the migrate Job that drops them, and the release to upgrade
// through first.
func TestCheckLegacyTopologyColumnsDropped_RefusesWhileAColumnRemains(t *testing.T) {
	err := handlers.CheckLegacyTopologyColumnsDropped(context.Background(), fakeLegacyColumns{
		present: []string{"current_prod.topology_snapshot", "release_pipeline_runs.candidate_topology"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, handlers.ErrLegacyTopologyColumnsPresent)
	assert.Contains(t, err.Error(), "current_prod.topology_snapshot, release_pipeline_runs.candidate_topology")
	assert.Contains(t, err.Error(), "db-init-migrate")
	assert.Regexp(t, `upgrade to continuo [0-9]+\.[0-9]+\.[0-9]+ and let release-controller start once before upgrading to this release`, err.Error())
}

// A schema read that fails is not the same refusal: the caller learns the
// schema could not be read, not that the columns are present.
func TestCheckLegacyTopologyColumnsDropped_ReportsAFailedRead(t *testing.T) {
	boom := errors.New("connection refused")
	err := handlers.CheckLegacyTopologyColumnsDropped(context.Background(), fakeLegacyColumns{err: boom})
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, handlers.ErrLegacyTopologyColumnsPresent)
}
