package neo4jinfra

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sweep calls Prune until a call returns fewer than limit, then AfterSweep, as
// pkg/retention does.
func sweep(t *testing.T, s *MarkerSweeper, limit int) {
	t.Helper()
	defer s.AfterSweep()
	for {
		n, err := s.Prune(context.Background(), 24*time.Hour, limit)
		require.NoError(t, err)
		if n < int64(limit) {
			return
		}
	}
}

// A tenant whose prune always fails is logged once per sweep, however many
// passes the sweep takes, and is tried again in the next sweep.
func TestMarkerSweeper_LogsAFailingTenantOncePerSweep(t *testing.T) {
	const limit = 10
	var logs bytes.Buffer
	s := NewMarkerSweeper(nil, slog.New(slog.NewTextHandler(&logs, nil)))
	s.listTenants = func(context.Context, int64) ([]string, error) { return []string{"bad", "good"}, nil }
	badCalls, goodCalls := 0, 0
	goodPassesInSweep := 0
	s.pruneOne = func(_ context.Context, tenantID string, _, limit int64) (int64, error) {
		if tenantID == "bad" {
			badCalls++
			return 0, errors.New("neo4j refused")
		}
		goodCalls++
		goodPassesInSweep++
		if goodPassesInSweep <= 3 {
			return limit, nil
		}
		goodPassesInSweep = 0
		return limit - 1, nil
	}
	failures := func() int {
		return strings.Count(logs.String(), "processed-message marker sweep of a tenant failed")
	}

	sweep(t, s, limit)
	assert.Equal(t, 1, failures(), "one log line for the sweep's four passes")
	assert.Equal(t, 1, badCalls)
	assert.Equal(t, 4, goodCalls)

	sweep(t, s, limit)
	assert.Equal(t, 2, failures(), "the next sweep tries the tenant again")
	assert.Equal(t, 2, badCalls)
}

// A failed tenant is skipped until the sweeper says its sweep ended, which a
// sweep cut short by the batch cap does without a short pass.
func TestMarkerSweeper_AfterSweepForgetsTheFailedTenants(t *testing.T) {
	const limit = 10
	s := NewMarkerSweeper(nil, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	s.listTenants = func(context.Context, int64) ([]string, error) { return []string{"bad", "good"}, nil }
	badCalls := 0
	s.pruneOne = func(_ context.Context, tenantID string, _, limit int64) (int64, error) {
		if tenantID == "bad" {
			badCalls++
			return 0, errors.New("neo4j refused")
		}
		return limit, nil
	}

	for range 3 {
		n, err := s.Prune(context.Background(), 24*time.Hour, limit)
		require.NoError(t, err)
		require.Equal(t, int64(limit), n, "healthy tenants keep every pass full, as under a batch cap")
	}
	assert.Equal(t, 1, badCalls, "the failed tenant is skipped for the rest of the sweep")

	s.AfterSweep()
	_, err := s.Prune(context.Background(), 24*time.Hour, limit)
	require.NoError(t, err)
	assert.Equal(t, 2, badCalls, "the next sweep tries the tenant again")
}
