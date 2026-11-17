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

func TestContractHistorySweeper_RefusesAnInvalidSweep(t *testing.T) {
	sweeper := NewContractHistorySweeper(nil, quietLogger())
	for _, tc := range []struct {
		name      string
		retention time.Duration
		limit     int
	}{
		{"zero retention", 0, 10},
		{"negative retention", -time.Hour, 10},
		{"sub-millisecond retention", time.Microsecond, 10},
		{"zero limit", time.Hour, 0},
		{"negative limit", time.Hour, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := sweeper.Prune(context.Background(), tc.retention, tc.limit)
			assert.Error(t, err)
			assert.Zero(t, n)
		})
	}
}

// The limit counts every tenant's deletions together: a pass visits the
// tenants in turn until it has deleted limit, and hands each only what is left.
func TestContractHistorySweeper_LimitBoundsTheWholePass(t *testing.T) {
	s := NewContractHistorySweeper(nil, quietLogger())
	s.listTenants = func(context.Context, int64) ([]string, error) { return []string{"a", "b", "c"}, nil }
	var asked []string
	s.pruneOne = func(_ context.Context, tenantID string, _, limit int64) (int64, error) {
		asked = append(asked, tenantID)
		return min(limit, 6), nil
	}

	got, err := s.Prune(context.Background(), time.Hour, 8)
	require.NoError(t, err)
	assert.Equal(t, int64(8), got)
	assert.Equal(t, []string{"a", "b"}, asked, "the pass ends once it has deleted its limit")
}

// A tenant whose prune always fails is logged once per sweep, however many
// passes the sweep takes, and is tried again in the next sweep.
func TestContractHistorySweeper_LogsAFailingTenantOncePerSweep(t *testing.T) {
	const limit = 10
	var logs bytes.Buffer
	s := NewContractHistorySweeper(nil, slog.New(slog.NewTextHandler(&logs, nil)))
	s.listTenants = func(context.Context, int64) ([]string, error) { return []string{"bad", "good"}, nil }
	badCalls, goodCalls := 0, 0
	s.pruneOne = func(_ context.Context, tenantID string, _, limit int64) (int64, error) {
		if tenantID == "bad" {
			badCalls++
			return 0, errors.New("neo4j refused")
		}
		goodCalls++
		if goodCalls%4 == 0 {
			return limit - 1, nil
		}
		return limit, nil
	}
	failures := func() int {
		return strings.Count(logs.String(), "contract history sweep of a tenant failed")
	}
	sweep := func() {
		defer s.AfterSweep()
		for {
			n, err := s.Prune(context.Background(), 24*time.Hour, limit)
			require.NoError(t, err)
			if n < limit {
				return
			}
		}
	}

	sweep()
	assert.Equal(t, 1, failures(), "one log line for the sweep's four passes")
	assert.Equal(t, 1, badCalls)
	assert.Equal(t, 4, goodCalls)

	sweep()
	assert.Equal(t, 2, failures(), "the next sweep tries the tenant again")
	assert.Equal(t, 2, badCalls)
}

// A cancelled context ends the pass with its error and logs nothing, since
// only a shutdown cancels it.
func TestContractHistorySweeper_ACancelledPassLogsNoError(t *testing.T) {
	var logs bytes.Buffer
	s := NewContractHistorySweeper(nil, slog.New(slog.NewTextHandler(&logs, nil)))
	s.listTenants = func(context.Context, int64) ([]string, error) { return []string{"a"}, nil }
	ctx, cancel := context.WithCancel(context.Background())
	s.pruneOne = func(context.Context, string, int64, int64) (int64, error) {
		cancel()
		return 0, context.Canceled
	}

	_, err := s.Prune(ctx, time.Hour, 10)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, logs.String(), "level=ERROR")
}
