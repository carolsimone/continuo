package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testScheduleName        = "e2e-schedule"
	testSchemaName          = "e2e_schema"
	testOwner               = "test"
	failureTestScheduleName = "e2e-schedule-failure"
)

// tableServiceMap maps each happy-path table to its owning service
var tableServiceMap = map[string]string{
	"seed_table_1": "service-1",
	"seed_table_2": "service-1",
	"seed_table_3": "service-1",
	"table_a":      "service-1",
	"table_b":      "service-1",
	"table_c":      "service-1",
	"table_d":      "service-3",
	"table_e":      "service-3",
	"table_f":      "service-3",
	"table_g":      "service-2",
	"table_h":      "service-2",
	"table_i":      "service-3",
	"table_j":      "service-3",
}

// getServiceNameForTable returns the service name for a happy-path table
func getServiceNameForTable(tableName string) string {
	svc, ok := tableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}

// triggerGraphLoad triggers a graph update via the ui-service HTTP endpoint and
// waits until the manifest-controller confirms the trigger was processed (via a
// new manifest.loaded:v1 message) and both test schedules appear in schedule_catalog.
// No topology state is mutated before triggering — the system is expected to handle
// pre-existing Table nodes correctly, matching production behaviour.
func triggerGraphLoad(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	// Record the last message ID on manifest.loaded:v1 before triggering so the
	// poll below can prove this specific trigger was processed, not a prior one.
	entries, err := clients.redisClient.XRevRangeN(ctx, "manifest.loaded:v1", "+", "-", 1).Result()
	lastManifestID := "0"
	if err == nil && len(entries) > 0 {
		lastManifestID = entries[0].ID
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"local"}`),
	)
	require.NoError(t, err, "POST /api/graph/update: request failed")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))

	t.Log("Published update.graph:v1 via ui-service — waiting for manifest-controller to publish manifest.loaded:v1...")

	// Wait for the manifest-controller to respond to this specific trigger.
	// XRange with "("+lastManifestID as start returns only messages with ID
	// strictly greater than lastManifestID, so pre-existing messages are ignored.
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, "manifest.loaded:v1", "("+lastManifestID, "+").Result()
		if err != nil {
			return false, nil
		}
		return len(msgs) > 0, nil
	}, "Timeout waiting for manifest-controller to publish manifest.loaded:v1")

	t.Log("manifest-controller published manifest.loaded:v1 — waiting for schedule_catalog to be populated...")

	// Wait for the full async chain: orchestrator consumed manifest.loaded:v1,
	// updated Neo4j, published schedules.loaded:v1, state populated schedule_catalog.
	// Both schedules are always loaded together from a single manifest trigger.
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var count int
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog
			 WHERE schedule_name IN ('e2e-schedule', 'e2e-schedule-failure')
			   AND removed_at IS NULL`,
		).Scan(&count); err != nil {
			return false, err
		}
		return count >= 2, nil
	}, "Timeout waiting for schedule_catalog to be populated with both schedules")

	t.Log("schedule_catalog populated for both schedules — catalog and graph are in sync")
}
