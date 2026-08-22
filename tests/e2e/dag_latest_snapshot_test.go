package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_DAGLatestSnapshot verifies the ui exposes per-schedule
// topology aggregates and the schedule graph carries topology_generation.
// Both surfaces back the homepage "DAG Latest Snapshot" section.
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

	t.Run("schedule_graph_exposes_topology_generation", func(t *testing.T) {
		// Pick the first available schedule name from the topology listing.
		body := mustGetJSON(t, fmt.Sprintf("%s/api/topology/schedules", base))
		raw, _ := body["schedules"].([]interface{})
		require.NotEmpty(t, raw, "topology listing must be non-empty")

		first, ok := raw[0].(map[string]interface{})
		require.True(t, ok, "first schedule entry must be a JSON object")
		scheduleName, _ := first["schedule_name"].(string)
		require.NotEmpty(t, scheduleName, "first schedule must have a name")

		graph := mustGetJSON(t, fmt.Sprintf("%s/api/schedules/%s/graph", base, scheduleName))
		gen, ok := graph["topology_generation"].(float64)
		require.True(t, ok, "topology_generation must be present on /graph response (got %v)", graph["topology_generation"])
		assert.Greater(t, gen, float64(0),
			"topology_generation must be > 0 once the orchestrator has ingested a manifest")
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
