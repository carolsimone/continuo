package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_DAGLatestSnapshot verifies the ui exposes per-schedule topology
// aggregates and the schedule graph carries the live promotion seq (its
// promotion_seq field). Both surfaces back the homepage "DAG Latest Snapshot"
// section.
func TestE2E_DAGLatestSnapshot(t *testing.T) {
	base := os.Getenv("UI_HTTP_BASE")
	if base == "" {
		t.Skip("UI_HTTP_BASE not set — skipping ui HTTP assertions")
	}

	t.Logf("Verifying topology endpoints at %s", base)

	// Topology may take a moment to load after services start; poll for up to 60s.
	deadline := time.Now().Add(60 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		body := mustGetJSON(t, fmt.Sprintf("%s/api/topology/schedules", base))
		raw, ok := body["schedules"].([]interface{})
		if ok && len(raw) > 0 {
			for _, item := range raw {
				s, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				if name, _ := s["schedule_name"].(string); name != "" {
					if count, _ := s["node_count"].(float64); count > 0 {
						found = true
						break
					}
				}
			}
			if found {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	require.True(t, found, "expected at least one schedule with node_count > 0 in /api/topology/schedules")

	t.Run("schedule_graph_exposes_promotion_seq", func(t *testing.T) {
		// Pick the first available schedule name from the topology listing.
		body := mustGetJSON(t, fmt.Sprintf("%s/api/topology/schedules", base))
		raw, _ := body["schedules"].([]interface{})
		require.NotEmpty(t, raw, "topology listing must be non-empty")

		first, ok := raw[0].(map[string]interface{})
		require.True(t, ok, "first schedule entry must be a JSON object")
		scheduleName, _ := first["schedule_name"].(string)
		require.NotEmpty(t, scheduleName, "first schedule must have a name")

		graph := mustGetJSON(t, fmt.Sprintf("%s/api/schedules/%s/graph", base, scheduleName))
		seq, ok := graph["promotion_seq"].(float64)
		require.True(t, ok, "promotion_seq must be present on /graph response (got %v)", graph["promotion_seq"])
		assert.Greater(t, seq, float64(0), "promotion_seq must be > 0 once a promotion has been applied")
		assert.NotContains(t, graph, "topology_generation", "the schedule graph carries the seq only as promotion_seq")

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		clients := setupClients(t, ctx)
		assert.Equal(t, float64(readLivePointer(ctx, clients).promotionSeq), seq,
			"the schedule graph's promotion_seq is the live promotion seq")
	})

	t.Run("topology_summary_fields_are_well_shaped", func(t *testing.T) {
		body := mustGetJSON(t, fmt.Sprintf("%s/api/topology/schedules", base))
		raw, _ := body["schedules"].([]interface{})
		require.NotEmpty(t, raw)

		for _, item := range raw {
			s, ok := item.(map[string]interface{})
			require.True(t, ok)
			scheduleName, hasName := s["schedule_name"].(string)
			require.True(t, hasName)
			require.NotEmpty(t, scheduleName)

			_, hasCount := s["node_count"].(float64)
			require.True(t, hasCount, "node_count must be present on schedule %q", scheduleName)

			// last_updated_at may be null (string-or-null) — only assert type when present
			if updated, ok := s["last_updated_at"]; ok && updated != nil {
				_, isString := updated.(string)
				assert.True(t, isString, "last_updated_at must be string or null on schedule %q (got %T)", scheduleName, updated)
			}
		}
	})
}
